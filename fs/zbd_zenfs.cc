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

#include <fstream>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "utilities/trace/bytedance_metrics_reporter.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      open_for_write_(false),
      state_(ZONE_UNKNOWN) {
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
  open_for_write_ = false;

  const std::lock_guard<std::mutex> lk(zbd_->zone_resources_mtx_);

  if (Close().ok()) {
    state_ = ZONE_IDLE;
    zbd_->NotifyIOZoneClosed();
  }

  if (capacity_ == 0) {
    state_ = ZONE_FULL;
    zbd_->NotifyIOZoneFull();
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds> (std::chrono::system_clock::now() - zbd_->opened_at[start_ >> 30]).count();
  Info(zbd_->logger_, "%s opened for %ld ns", zbd_->opened_files[start_ >> 30].c_str(), elapsed);
  zbd_->opened_files.erase(start_ >> 30);

  zbd_->LogZoneStatsInternal();
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());

  ret = zbd_reset_zones(zbd_->GetWriteFD(), start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(zbd_->GetReadFD(), start_, zone_sz, ZBD_RO_ALL, &z,
                         &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

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

  assert(!open_for_write_);

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;
  IOStatus s;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  /* Make sure we don't have any outstanding writes */
  s = Sync();
  if (!s.ok())
    return s;

  while (left) {
    ret = pwrite(fd, ptr, size, wp_);
    if (ret < 0) return IOStatus::IOError("Write failed");

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
  
  if (wr_ctx.inflight == 0)
    return IOStatus::OK();

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
  if (!s.ok())
    return s;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

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

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone *zone)
    : start_(start), length_(length), zone_(zone) {}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

std::vector<ZoneStat> ZonedBlockDevice::GetStat() {
  std::vector<ZoneStat> stat;
  for (const auto z : io_zones) {
    ZoneStat zone_stat;
    zone_stat.total_capacity = z->max_capacity_;
    zone_stat.write_position = z->wp_;
    zone_stat.start_position = z->start_;
    stat.emplace_back(std::move(zone_stat));
  }
  return stat;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger)
    : ZonedBlockDevice(bdevname, logger, "",
                       std::make_shared<ByteDanceMetricsReporterFactory>()) {}

static std::string write_latency_metric_name = "zenfs_write_latency";
static std::string read_latency_metric_name = "zenfs_read_latency";
static std::string sync_latency_metric_name = "zenfs_sync_latency";
static std::string io_alloc_latency_metric_name = "zenfs_io_alloc_latency";
static std::string io_alloc_actual_latency_metric_name = "zenfs_io_actual_alloc_latency";
static std::string meta_alloc_latency_metric_name = "zenfs_meta_alloc_latency";
static std::string roll_latency_metric_name = "zenfs_roll_latency";

static std::string write_qps_metric_name = "zenfs_write_qps";
static std::string read_qps_metric_name = "zenfs_read_qps";
static std::string sync_qps_metric_name = "zenfs_sync_qps";
static std::string io_alloc_qps_metric_name = "zenfs_io_alloc_qps";
static std::string meta_alloc_qps_metric_name = "zenfs_meta_alloc_qps";
static std::string roll_qps_metric_name = "zenfs_roll_qps";

static std::string write_throughput_metric_name = "zenfs_write_throughput";
static std::string roll_throughput_metric_name = "zenfs_roll_throughput";

static std::string active_zones_metric_name = "zenfs_active_zones";
static std::string open_zones_metric_name = "zenfs_open_zones";

ZonedBlockDevice::ZonedBlockDevice(
    std::string bdevname, std::shared_ptr<Logger> logger,
    std::string bytedance_tags,
    std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory)
    : filename_("/dev/" + bdevname),
      logger_(logger),
      metrics_reporter_factory_(metrics_reporter_factory),
      // A short advice for new developers: BE SURE TO STORE `bytedance_tags_` somewhere,
      // and pass the stored `bytedance_tags_` to the reporters. Otherwise the metrics
      // library will panic with `std::logic_error`.
      bytedance_tags_(bytedance_tags),
      write_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          write_latency_metric_name, bytedance_tags_, logger.get())),
      read_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          read_latency_metric_name, bytedance_tags_, logger.get())),
      sync_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          sync_latency_metric_name, bytedance_tags_, logger.get())),
      meta_alloc_latency_reporter_(
          *metrics_reporter_factory_->BuildHistReporter(
              meta_alloc_latency_metric_name, bytedance_tags_, logger.get())),
      io_alloc_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          io_alloc_latency_metric_name, bytedance_tags_, logger.get())),
      io_alloc_actual_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          io_alloc_actual_latency_metric_name, bytedance_tags_, logger.get())),
      roll_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          roll_latency_metric_name, bytedance_tags_, logger.get())),
      write_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          write_qps_metric_name, bytedance_tags_, logger.get())),
      read_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          read_qps_metric_name, bytedance_tags_, logger.get())),
      sync_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          sync_qps_metric_name, bytedance_tags_, logger.get())),
      meta_alloc_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          meta_alloc_qps_metric_name, bytedance_tags_, logger.get())),
      io_alloc_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          io_alloc_qps_metric_name, bytedance_tags_, logger.get())),
      roll_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          roll_qps_metric_name, bytedance_tags_, logger.get())),
      write_throughput_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          write_throughput_metric_name, bytedance_tags_, logger.get())),
      roll_throughput_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          roll_throughput_metric_name, bytedance_tags_, logger.get())),
      active_zones_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          active_zones_metric_name, bytedance_tags_, logger.get())),
      open_zones_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          open_zones_metric_name, bytedance_tags_, logger.get())) {
  Info(logger_, "New Zoned Block Device: %s (with metrics enabled)",
       filename_.c_str());
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
    return IOStatus::InvalidArgument(
        "Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
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
    return IOStatus::InvalidArgument("Failed to open zoned block device: " +
                                     ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device: " +
                                     ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT | O_EXCL, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open zoned block device: " +
                                       ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need one open zone for meta data writes, the rest can be used for files
   */
  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - 1;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - 1;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
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
        io_zones.push_back(newZone);
        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
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

  return IOStatus::OK();
}

void ZonedBlockDevice::NotifyIOZoneFull() {
  active_io_zones_--;
  zone_resources_.notify_all();
}

void ZonedBlockDevice::NotifyIOZoneClosed() {
  open_io_zones_--;
  zone_resources_.notify_all();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  const std::lock_guard<std::mutex> lock(io_zones_mtx);
  LogZoneStatsInternal();  
}

void ZonedBlockDevice::LogZoneStatsInternal() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
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
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
  for (const auto& x : opened_files) {
    auto opened_time = opened_at[x.first];
    time_t tt = std::chrono::system_clock::to_time_t(opened_time);
    tm local_tm = *localtime(&tt);
    Info(logger_, "Zone %d: %s opened at %02d:%02d:%02d\n", x.first, x.second.c_str(), local_tm.tm_hour, local_tm.tm_min, local_tm.tm_sec);
  }
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_MEH (2)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime >= 0 && file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime == file_lifetime)
    return LIFETIME_DIFF_MEH;

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  return LIFETIME_DIFF_NOT_GOOD;
}

Zone *ZonedBlockDevice::AllocateMetaZone() {
  LatencyHistGuard guard(&meta_alloc_latency_reporter_);
  meta_alloc_qps_reporter_.AddCount(1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (!z->IsUsed()) {
      if (!z->IsEmpty()) {
        if (!z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          continue;
        }
      }
      return z;
    }
  }
  return nullptr;
}

void ZonedBlockDevice::InitializeZoneState() {
  for (auto zone : io_zones) {
    if (zone->IsEmpty()) {
      zone->state_ = ZONE_EMPTY;
    } else if (zone->IsFull()) {
      zone->state_ = ZONE_FULL;
    } else {
      zone->state_ = ZONE_IDLE;
    }
  }
}

void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) active_io_zones_--;
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}

Zone *ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint file_lifetime, const std::string &filename, bool wal_fast_path) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  Status s;

  LatencyHistGuard guard(&io_alloc_latency_reporter_);
  io_alloc_qps_reporter_.AddCount(1);

  /* Make sure we are below the zone open limit */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, wal_fast_path, &filename] {
    if (!wal_fast_path) {
      if (open_io_zones_.load() < max_nr_open_io_zones_ - 3) return true;
    } else {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
    }

    Info(logger_, "Waiting for lock: %s", filename.c_str());
    return false;
  });

  io_zones_mtx.lock();

  LatencyHistGuard guard_actual(&io_alloc_actual_latency_reporter_);

  int reset_cnt = wal_fast_path ? 0 : 3;

  /* Reset any unused zones and finish used zones under capacity treshold*/
  for (const auto z : io_zones) {
    if (reset_cnt <= 0) {
      break;
    }
    if (z->state_ == ZONE_FULL && z->used_capacity_ == 0) {
      z->state_ = ZONE_EMPTY;
      s = z->Reset();
      if (!s.ok()) {
        Warn(logger_, "Failed resetting zone !");
      }
      reset_cnt -= 1;
      continue;
    }
  }

  /* Try to fill an already open zone(with the best life time diff) */
  int real_open_io_zones = 0;
  int real_idle_zones = 0;
  for (const auto z : io_zones) {
    if (z->state_ == ZONE_IDLE) {
      unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
      if (diff <= best_diff) {
        allocated_zone = z;
        best_diff = diff;
      }
      real_idle_zones += 1;
    }

    if (z->state_ == ZONE_IN_USE) {
      real_open_io_zones++;
    }
  }

  if (real_open_io_zones != open_io_zones_ || real_idle_zones + real_open_io_zones != active_io_zones_) {
    std::cerr << std::dec << "Open Zone: " << real_open_io_zones << "!=" << open_io_zones_ << "(stat)" << std::endl;
    std::cerr << "Active Zone: " << real_idle_zones + real_open_io_zones << "!=" << active_io_zones_ << "(stat)" << std::endl;
  }

  /* If we did not find a good match, allocate an empty one */
  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    if (active_io_zones_.load() < max_nr_active_io_zones_) {
      for (const auto z : io_zones) {
        if (z->state_ == ZONE_EMPTY) {
          z->lifetime_ = file_lifetime;
          allocated_zone = z;
          active_io_zones_++;
          new_zone = 1;
          break;
        }
      }
    } else {
      Warn(logger_, "could not find a good match, but no zone could be finished\n");
    }
  }

  if (allocated_zone) {
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    allocated_zone->state_ = ZONE_IN_USE;
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);

    opened_files[allocated_zone->start_ >> 30] = filename;
    opened_at[allocated_zone->start_ >> 30] = std::chrono::system_clock::now();
  }

  LogZoneStatsInternal();

  io_zones_mtx.unlock();

  // In case there are more open zones
  zone_resources_.notify_all();
  lk.unlock();

  open_zones_reporter_.AddRecord(open_io_zones_);
  active_zones_reporter_.AddRecord(active_io_zones_);
  if (active_io_zones_ < 0) {
    std::cerr << "active zone -> negative!" << std::endl;
  }

  return allocated_zone;
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }
uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
