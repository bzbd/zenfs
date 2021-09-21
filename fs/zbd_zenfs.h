// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libaio.h>
#include <libaio.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/metrics_reporter.h"
#include "zbd_stat.h"

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;

struct zenfs_aio_ctx {
  struct iocb iocb;
  struct iocb *iocbs[1];
  io_context_t io_ctx;
  int inflight;
  int fd;
};

/* From prespective of foreground thread, a single zone could be one of these
 * status.
 * kEmpty        | Zone is empty. Could be meta zone or data zone.
 * kActive       | This data zone is open for write.
 * kReadOnly     | This data zone is read only.
 * kMetaLog      | This zone is for meta logging.
 * kMetaSnapshot | This zone is used to store meta snapshots.
 */
enum ZoneState { kEmpty = 0, kActive, kReadOnly, kMetaLog, kMetaSnapshot };

class Zone {
  ZonedBlockDevice *zbd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;
  struct zenfs_aio_ctx wr_ctx;
  ZoneState state_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  IOStatus Append_async(char *data, uint32_t size);
  IOStatus Sync();
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();

  void EncodeJson(std::ostream &json_stream);

  void CloseWR(); /* Done writing */
};

class BackgroundJob {
 public:
  BackgroundJob(std::function<int(void *)> fn, void *arg)
       : fn_(fn), arg_(arg) {}
   std::function<int(void *)> fn_;
  void *arg_;
  virtual void operator()() { fn_(arg_); }
  virtual ~BackgroundJob() {}
};

class ErrorHandlingBGJob : public BackgroundJob {
 public:
  ErrorHandlingBGJob(std::function<int(void *)> fn, void *arg,
                      std::function<void(int)> handler)
       : BackgroundJob(fn, arg), handler_(handler) {}
   ErrorHandlingBGJob(std::function<int(void *)> fn, void *arg,
                      std::function<void(int)> &&handler)
       : BackgroundJob(fn, arg), handler_(handler) {}
   std::function<void(int)> handler_;
   virtual void operator()() override { handler_(fn_(arg_)); }
};

class BackgroundWorker {
  enum WorkingState { kWaiting = 0, kRunning, kTerminated } state_;
  std::thread worker_;
  std::list<BackgroundJob> jobs_;
  std::unique_ptr<BackgroundJob> job_now_;
  std::mutex job_mtx_;
  std::condition_variable job_cv_;

 public:
  BackgroundWorker(bool run_at_beginning = true);
  ~BackgroundWorker();
  void Wait();
  void Run();
  void Terminate();
  void ProcessJobs();
  void SubmitJob(std::function<int(void *)> fn, void *arg);
  void SubmitJob(BackgroundJob &&job);
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;
  std::mutex wal_zones_mtx;
  std::vector<Zone *> meta_zones;
  std::vector<Zone *> meta_snapshot_zones;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::shared_ptr<BackgroundWorker> meta_worker_;
  std::shared_ptr<BackgroundWorker> data_worker_;
  std::list<Zone *> active_zones_list_;
  std::mutex active_zone_list_mtx_;

  std::atomic<int> fg_request_;

  // If a thread is allocating a zone fro WAL files, other
  // thread shouldn't take `io_zones_mtx` (see AllocateZone())
  std::atomic<uint32_t> wal_zone_allocating_{0};

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;

  uint32_t max_nr_active_io_zones_;
  uint32_t max_nr_open_io_zones_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  std::mutex metazone_reset_mtx_;
  std::condition_variable metazone_reset_cv_;

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  explicit ZonedBlockDevice(
      std::string bdevname, std::shared_ptr<Logger> logger,
      std::string bytedance_tags,
      std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory);

  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly = false);
  IOStatus CheckScheduler();

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, bool is_wal);
  Zone *AllocateMetaZone();

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  void ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint64_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  uint32_t GetMaxActiveZones() { return max_nr_active_io_zones_ + 1; };
  uint32_t GetMaxOpenZones() { return max_nr_open_io_zones_ + 1; };

  std::vector<Zone *> GetMetaZones() { return meta_zones; }
  std::vector<Zone *> GetMetaSnapshotZones() { return meta_snapshot_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  bool SetMaxActiveZones(uint32_t max_active) {
    if (max_active == 0) /* No limit */
      return true;
    if (max_active <= GetMaxActiveZones()) {
      max_nr_active_io_zones_ = max_active - 1;
      return true;
    } else {
      return false;
    }
  }

  bool SetMaxOpenZones(uint32_t max_open) {
    if (max_open == 0) /* No limit */
      return true;
    if (max_open <= GetMaxOpenZones()) {
      max_nr_open_io_zones_ = max_open - 1;
      return true;
    } else {
      return false;
    }
  }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();

  void EncodeJson(std::ostream &json_stream);

  std::vector<ZoneStat> GetStat();

  std::shared_ptr<CurriedMetricsReporterFactory> metrics_reporter_factory_;
  std::string bytedance_tags_;

  using LatencyReporter = HistReporterHandle &;
  LatencyReporter write_latency_reporter_;
  LatencyReporter read_latency_reporter_;
  LatencyReporter sync_latency_reporter_;
  LatencyReporter meta_alloc_latency_reporter_;
  LatencyReporter io_alloc_wal_latency_reporter_;
  LatencyReporter io_alloc_wal_actual_latency_reporter_;
  LatencyReporter io_alloc_non_wal_latency_reporter_;
  LatencyReporter io_alloc_non_wal_actual_latency_reporter_;
  LatencyReporter roll_latency_reporter_;

  using QPSReporter = CountReporterHandle &;
  QPSReporter write_qps_reporter_;
  QPSReporter read_qps_reporter_;
  QPSReporter sync_qps_reporter_;
  QPSReporter meta_alloc_qps_reporter_;
  QPSReporter io_alloc_qps_reporter_;
  QPSReporter roll_qps_reporter_;

  using ThroughputReporter = CountReporterHandle &;
  ThroughputReporter write_throughput_reporter_;
  ThroughputReporter roll_throughput_reporter_;

  using DataReporter = HistReporterHandle &;
  DataReporter active_zones_reporter_;
  DataReporter open_zones_reporter_;

 private:
  std::string ErrorToString(int err);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
