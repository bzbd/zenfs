// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <ctime>
#include <iostream>
#include <map>
#include <tuple>

#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "snapshot.h"

#define KB (1024)
#define MB (1024 * KB)

#define ZENFS_DEBUG
#include "utils.h"

/* Number of reserved zones for op log
 * Two non-offline op log zones are needed to be able
 * to roll the log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_OP_LOG_ZONES (2)

/* Number of reserved zones for metadata snapshot
 */
#define ZENFS_SNAPSHOT_ZONES (2)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      open_for_write_(false) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));

  memset(&wr_ctx.io_ctx, 0, sizeof(wr_ctx.io_ctx));
  wr_ctx.fd = zbd_->GetWriteFD();
  wr_ctx.iocbs[0] = &wr_ctx.iocb;
  wr_ctx.inflight = 0;

  if (io_setup(1, &wr_ctx.io_ctx) < 0) {
    fprintf(stderr, "Failed to allocate io context\n");
  }
}

bool Zone::IsUsed() { return (used_capacity_ > 0) || open_for_write_; }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::CloseWR() {
  assert(open_for_write_);
  Sync();

  std::lock_guard<std::mutex> lock(zbd_->zone_resources_mtx_);
  if (Close().ok()) {
    assert(!open_for_write_);
    zbd_->NotifyIOZoneClosed();
  }

  if (capacity_ == 0) zbd_->NotifyIOZoneFull();
}

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());

  ret = zbd_reset_zones(zbd_->GetWriteFD(), start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(zbd_->GetReadFD(), start_, zone_sz, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) {
	return IOStatus::IOError("Zone report failed\n");
  }

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  // assert(open_for_write_);

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  open_for_write_ = false;

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int step = 128 << 10; // 128KB per write
  int fd = zbd_->GetWriteFD();
  int ret;
  IOStatus s;

  if (capacity_ < size) return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  /* Make sure we don't have any outstanding writes */
  s = Sync();
  if (!s.ok()) return s;

  while (left) {
	ret = pwrite(fd, ptr, left, wp_);
	/*
	if(left > step) {
	  ret = pwrite(fd, ptr, step, wp_);
	} else {
	  ret = pwrite(fd, ptr, left, wp_);
	}
	*/
	if (ret < 0) {
	  fprintf(stderr, "Failed to complete io - pwrite failed!\n");
	  return IOStatus::IOError("Write failed");
	}

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
  }

  return IOStatus::OK();
}

IOStatus Zone::Sync() {
  struct io_event events[1];
  struct timespec timeout;
  int ret;
  timeout.tv_sec = 1;
  timeout.tv_nsec = 0;

  if (wr_ctx.inflight == 0) return IOStatus::OK();

  ret = io_getevents(wr_ctx.io_ctx, 1, 1, events, &timeout);
  if (ret != 1) {
    fprintf(stderr, "Failed to complete io - timeout ret: %d\n", ret);
    return IOStatus::IOError("Failed to complete io - timeout?");
  }

  ret = events[0].res;
  if (ret != (int)(wr_ctx.iocb.u.c.nbytes)) {
    if (ret >= 0) {
      /* TODO: we need to handle this case and keep on submittin' until we're done*/
      fprintf(stderr, "failed to complete io - short write\n");
      return IOStatus::IOError("Failed to complete io - short write");
    } else {
      return IOStatus::IOError("Failed to complete io - io error");
    }
  }

  wr_ctx.inflight = 0;

  return IOStatus::OK();
}

IOStatus Zone::Append_async(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int ret;
  IOStatus s;

  assert((size % zbd_->GetBlockSize()) == 0);

  /* Make sure we don't have any outstanding writes */
  s = Sync();
  if (!s.ok()) return s;

  if (capacity_ < size) return IOStatus::NoSpace("Not enough capacity for append");

  io_prep_pwrite(&wr_ctx.iocb, wr_ctx.fd, data, size, wp_);

  ret = io_submit(wr_ctx.io_ctx, 1, wr_ctx.iocbs);
  if (ret < 0) {
    fprintf(stderr, "Failed to submit io\n");
    return IOStatus::IOError("Failed to submit io");
  }

  wr_ctx.inflight = size;
  ptr += size;
  wp_ += size;
  capacity_ -= size;
  left -= size;

  return IOStatus::OK();
}

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone *zone) : start_(start), length_(length), zone_(zone) {}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones_)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

std::vector<ZoneStat> ZonedBlockDevice::GetStat() {
  std::vector<ZoneStat> stat;
  for (const auto z : io_zones_) {
    ZoneStat zone_stat;
    zone_stat.start_position = z->start_;
    zone_stat.free_capacity = z->capacity_;
    zone_stat.used_capacity = z->used_capacity_;
    zone_stat.reclaim_capacity = z->max_capacity_ - z->used_capacity_;
    stat.emplace_back(std::move(zone_stat));
  }
  // sort by trash descending order
  std::sort(stat.begin(), stat.end(), [](const ZoneStat &l, const ZoneStat &r) {
    return l.free_capacity < r.free_capacity ||
           (l.free_capacity == r.free_capacity && l.used_capacity < r.used_capacity);
  });
  return stat;
}

BackgroundWorker::BackgroundWorker(bool run_at_beginning) {
  {
    std::unique_lock<std::mutex> lk(job_mtx_);
    if (run_at_beginning) {
      Run();
    }
  }

  worker_ = std::thread(&BackgroundWorker::ProcessJobs, this);
}

BackgroundWorker::~BackgroundWorker() {
  {
    std::unique_lock<std::mutex> lk(job_mtx_);
    Terminate();
    job_cv_.notify_all();
  }

  worker_.join();
  for (auto &job : jobs_) {
    (*job)();
  }
}

void BackgroundWorker::Wait() { state_ = kWaiting; }

void BackgroundWorker::Run() { state_ = kRunning; }

void BackgroundWorker::Terminate() { state_ = kTerminated; }

void BackgroundWorker::ProcessJobs() {
  while (true) {
    {
      std::unique_lock<std::mutex> lk(job_mtx_);
      job_cv_.wait(lk, [this]() { return !jobs_.empty() || state_ == kTerminated; });
      if (state_ == kTerminated) {
        return;
      }
      job_now_.swap(jobs_.front());
      jobs_.pop_front();
    }
    (*job_now_)();
  }
}

void BackgroundWorker::SubmitJob(std::function<void()> fn) {
  std::unique_lock<std::mutex> lk(job_mtx_);
  jobs_.push_back(std::make_unique<SimpleJob>(fn));
  job_cv_.notify_one();
}

void BackgroundWorker::SubmitJob(std::unique_ptr<BackgroundJob> &&job) {
  std::unique_lock<std::mutex> lk(job_mtx_);
  jobs_.push_back(std::move(job));
  job_cv_.notify_one();
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname, std::shared_ptr<Logger> logger, std::shared_ptr<ZenFSMetrics> metrics)
    : filename_("/dev/" + bdevname), logger_(logger), metrics_(metrics) {
  Info(logger_, "New Zoned Block Device: %s (with metrics enabled)", filename_.c_str());
}

std::string ZonedBlockDevice::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus ZonedBlockDevice::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0, 5);  // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument("Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::Open(bool readonly) {
  struct zbd_zone *zone_rep;
  unsigned int reported_zones;
  uint64_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  int ret;

  read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT | O_EXCL, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open zoned block device: " + ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned block device (32 required)");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need 3 open zones for meta data writes , the rest can be used for files
   */
  max_nr_active_io_zones_ = info.max_nr_active_zones - 3;
  max_nr_open_io_zones_ = info.max_nr_active_zones - 3;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n", info.nr_zones,
       info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep, &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_OP_LOG_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        op_zones_.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  m = 0;
  // initialize metadata snapshop zones
  while (m < ZENFS_SNAPSHOT_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        snapshot_zones_.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        io_zones_.push_back(newZone);
        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) || zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
      }
    }
  }

  free(zone_rep);
  start_time_ = time(NULL);

  meta_worker_.reset(new BackgroundWorker());
  data_worker_.reset(new BackgroundWorker());

  return IOStatus::OK();
}

void ZonedBlockDevice::NotifyIOZoneFull() {
  active_io_zones_--;
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyIOZoneClosed() {
  open_io_zones_--;
  zone_resources_.notify_one();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones_) {
    free += z->capacity_;
  }
  return free;
}

int ZonedBlockDevice::GetResetableZones() {
  int zones = 0;
  for (const auto z : io_zones_) {
    if (z->IsFull() && z->used_capacity_ == 0) {
      zones++;
    }
  }
  return zones;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones_) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones_) {
    // if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
    // Not only count full zones, but also open zones.
    uint64_t zone_written_size = (z->wp_ - z->start_);
    reclaimable += (zone_written_size - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::ReportSpaceUtilization() {
  auto free_space = GetFreeSpace() >> 30;
  Info(logger_, "zbd free space %lu GB \n", free_space);
  metrics_->ReportGeneral(ZENFS_LABEL(FREE_SPACE, SIZE), free_space);

  auto used_space = GetUsedSpace() >> 30;
  Info(logger_, "zbd used(valid) space %lu GB\n", used_space);
  metrics_->ReportGeneral(ZENFS_LABEL(USED_SPACE, SIZE), used_space);

  auto reclaimable_space = GetReclaimableSpace() >> 30;
  Info(logger_, "zbd reclaimable space %lu GB\n", reclaimable_space);
  metrics_->ReportGeneral(ZENFS_LABEL(RECLAIMABLE_SPACE, SIZE), reclaimable_space);

  auto resetable_zones = GetResetableZones();
  Info(logger_, "zbd resetable zones %d\n", resetable_zones);
  metrics_->ReportGeneral(ZENFS_LABEL(RESETABLE_ZONES, COUNT), resetable_zones);

  // log garbage distribution
  // garbage percent: [0%, <10%, <20% ... <100%, 100%]
  int zone_gc_stat[12] = {0};
  for (const auto z : io_zones_) {
    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      continue;
    }
    double garbage_rate = double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;
  }

  Info(logger_, "Zone Garbage Stats: [%d %d %d %d %d %d %d %d %d %d %d %d]\n", zone_gc_stat[0], zone_gc_stat[1],
       zone_gc_stat[2], zone_gc_stat[3], zone_gc_stat[4], zone_gc_stat[5], zone_gc_stat[6], zone_gc_stat[7],
       zone_gc_stat[8], zone_gc_stat[9], zone_gc_stat[10], zone_gc_stat[11]);
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  // io_zones_mtx_.lock();

  for (const auto z : io_zones_) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active, active_io_zones_.load(), open_io_zones_.load());

  // io_zones_mtx_.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones_) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n", z->start_, used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  meta_worker_.reset(nullptr);
  data_worker_.reset(nullptr);

  for (const auto z : op_zones_) {
    delete z;
  }

  for (const auto z : snapshot_zones_) {
    delete z;
  }

  for (const auto z : io_zones_) {
    delete z;
  }

  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_MEH (2)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime, Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime >= 0 && file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) || (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_MEH;

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  return LIFETIME_DIFF_NOT_GOOD;
}

Zone *ZonedBlockDevice::AllocateMetaZone() {
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_LABEL(META_ALLOC, LATENCY),
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_LABEL(META_ALLOC, QPS), 1);

  for (const auto z : op_zones_) {
    if (z->IsEmpty()) {
      return z;
    }
  }

  return nullptr;
}

Zone *ZonedBlockDevice::AllocateSnapshotZone() {
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_LABEL(META_ALLOC, LATENCY),
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_LABEL(META_ALLOC, QPS), 1);

  for (const auto z : snapshot_zones_) {
    if (z->IsEmpty()) {
      return z;
    }
  }

  return nullptr;
}

void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones_) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) active_io_zones_--;
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}

Zone *ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint file_lifetime, bool is_wal) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  Status s;

  // We reserve one more free zone for WAL files in case RocksDB delay close WAL files.
  int reserved_zones = 1;

  ZenFSMetricsLatencyGuard guard(metrics_, is_wal
                               ? ZENFS_LABEL_DETAILED(IO_ALLOC, WAL, LATENCY)
                               : ZENFS_LABEL_DETAILED(IO_ALLOC, NON_WAL, LATENCY),
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_LABEL(IO_ALLOC, QPS), 1);

  auto t0 = std::chrono::system_clock::now();

  // For general data, we need both two locks. 
  // The general data thread can give up lock to WAL thread during iteration.
  if (!is_wal) {
	io_zones_mtx_.lock();
  }

  // Make sure WAL allocation has better priority
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this, is_wal, reserved_zones, file_lifetime] {
      if (is_wal) {
        return open_io_zones_.load() < max_nr_open_io_zones_;
      } else {
        return open_io_zones_.load() < max_nr_open_io_zones_ - reserved_zones;
      }
      return false;
    });
  }

  wal_zones_mtx_.lock();

  auto t1 = std::chrono::system_clock::now();

  // Reset any unused zones and finish used zones under capacity threshold
  for (int i = 0; !is_wal && i < io_zones_.size(); i++) {
    const auto z = io_zones_[i];

    if (z->open_for_write_ || z->IsEmpty() || (z->IsFull() && z->IsUsed())) {
      continue;
    }

	// Note that current function cannot be accessed concurrently, so this is safe.
	if (z->processing_) continue;

	// Reset an unused zone (valud = 0)
    if (!z->IsUsed()) {
      FinishOrReset(z, true);
      continue;
    }

    // Finish an almost FULL zone
    if (!is_wal && (z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
      FinishOrReset(z, false);
    }
  }

  /* Try to fill an already open zone(with the best life time diff) */
  for (const auto z : io_zones_) {
	if(z->processing_) continue;

    if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
      unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);

      if (diff <= best_diff || allocated_zone == nullptr) {
        allocated_zone = z;
        best_diff = diff;
	  }

    }
  }

  auto t2 = std::chrono::system_clock::now();

  // If we did not find a good match, allocate an empty one
  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    if (active_io_zones_.load() < max_nr_active_io_zones_) {
      for (const auto z : io_zones_) {
		if(z->processing_) continue;

        if ((!z->open_for_write_) && z->IsEmpty()) {
          z->lifetime_ = file_lifetime;
          allocated_zone = z;
          active_io_zones_++;
          new_zone = 1;
          break;
        }
      }
    }
  }

  if (allocated_zone) {
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    Debug(logger_, "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n", new_zone,
          allocated_zone->start_, allocated_zone->wp_, allocated_zone->lifetime_, file_lifetime);
  }

  LogZoneStats();
  wal_zones_mtx_.unlock();
  if (!is_wal) {
    io_zones_mtx_.unlock();
  }

  auto t5 = std::chrono::system_clock::now();

  metrics_->ReportGeneral(ZENFS_LABEL(OPEN_ZONES, COUNT), open_io_zones_);
  metrics_->ReportGeneral(ZENFS_LABEL(ACTIVE_ZONES, COUNT), active_io_zones_);

  std::stringstream ss;
  ss << " is_wal = " << is_wal << " a/o zones " << active_io_zones_.load() << "," << open_io_zones_.load()
     << " lock wait: " << TimeDiff(t0, t1) << ", reset: " << TimeDiff(t1, t2) << ", other: " << TimeDiff(t2, t5)
     << ", wlfh: " << file_lifetime << ", pending_bg_work: " << pending_bg_work_ << "\n";
  Info(logger_, "%s", ss.str().c_str());

  return allocated_zone;
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }

uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream, const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, op_zones_);
  json_stream << ",\"meta_snapshot\":";
  EncodeJsonZone(json_stream, snapshot_zones_);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones_);
  json_stream << "}";
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot,
                                       const ZenFSSnapshotOptions &options) {
  for (auto &zone : io_zones_) snapshot.emplace_back(*zone, options);
}

// Callers should make sure the target zone is not been used before submitting this job.
// Before entering this function. target zone's mutex was already taken.
void ZonedBlockDevice::FinishOrReset(Zone *z, bool reset) {
  z->processing_ = true;
  data_worker_->SubmitJob([&, z]() {
    // double check
    if (z->open_for_write_ || z->IsEmpty()) {
	  std::cout << "shouldn't happend!" << std::endl;
	  z->processing_ = false;
      return;
    }

    pending_bg_work_++;
    bool full = z->IsFull();

	// Finish only (don't reset)
    if (!reset && !full && !z->Finish().ok()) {
      Warn(logger_, "Failed finishing zone");
    }
    if (!full) {
      active_io_zones_--;
    }

	// Reset only (no need to finish first)
    if (reset) {
      z->Reset();
    }
    pending_bg_work_--;
	z->processing_ = false;
  });
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
