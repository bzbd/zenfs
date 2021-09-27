// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

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
#include <memory>
#include <mutex>
#include <queue>
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
 * kActive       | This data zone is ready for write.
 * kOccupied     | This data zone is occupied by other write thread.
 * kReadOnly     | This data zone is read only.
 * kMetaLog      | This zone is for meta logging.
 * kMetaSnapshot | This zone is used to store meta snapshots.
 */
enum ZoneState { kEmpty = 0, kActive, kOccupied, kReadOnly, kMetaLog, kMetaSnapshot };

class Zone {
  public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);
  
  ZonedBlockDevice *zbd_;
  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;
  struct zenfs_aio_ctx wr_ctx;
  std::atomic<ZoneState> state_;

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

// Abstract class as interface.
// operator() must be overrided in order to execute the function.
class BackgroundJob {
 public:
  virtual void operator()() = 0;
  virtual ~BackgroundJob() {}
};

// For simple jobs that needs no return value nor argument.
class SimpleJob : public BackgroundJob {
 public:
  std::function<void()> fn_;
  // No default allowed.
  SimpleJob() = delete;
  SimpleJob(std::function<void()> fn) : fn_(fn) {}
  virtual void operator()() override { fn_(); }
  virtual ~SimpleJob() {}
};

template<typename arg_t, typename ret_t>
class GeneralJob : public BackgroundJob {
 public:
  // Job requires argument and return value for error handling
  std::function<ret_t(arg_t)> fn_;
  // Argument for execution
  arg_t arg_;
  // Error handler
  std::function<void(ret_t)> hdl_;
  // No default allowed
  GeneralJob() = delete;
  GeneralJob(std::function<ret_t(arg_t)> fn, arg_t arg,
             std::function<void(ret_t)> hdl) : fn_(fn), arg_(arg), hdl_(hdl) {}
  virtual void operator()() override { hdl_(fn_(arg_)); }
  virtual ~GeneralJob() {}
};

class BackgroundWorker {
  enum WorkingState { kWaiting = 0, kRunning, kTerminated } state_;
  std::thread worker_;
  std::list<std::unique_ptr<BackgroundJob>> jobs_;
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
  // For simple jobs that could be handled in a lambda function.
  void SubmitJob(std::function<void()> fn);
  // For derived jobs which needs arguments
  void SubmitJob(std::unique_ptr<BackgroundJob>&& job);
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  std::list<Zone *> io_zones;
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
  std::vector<Zone *> active_zones_vec_;
  static const int reserved_active_zones = 2;
  std::mutex active_zone_vec_mtx_;

  std::atomic<int> high_pri_requset_;

  // If a thread is allocating a zone fro WAL files, other
  // thread shouldn't take `io_zones_mtx` (see AllocateZone())
  std::atomic<uint32_t> wal_zone_allocating_{0};

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;

  uint32_t max_nr_active_io_zones_;
  uint32_t max_nr_open_io_zones_;

  template <class T>
  void EncodeJsonZone(std::ostream &json_stream,
                      const T zones);

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

  // Jump to AllocateDataZone in this test branch.
  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, bool high_pri);

  /* Allocate a data zone from active zone vector.
   * High priority requests are able to utilize extra reservation zones for
   * lower latency.
   * Low priority requests shall wait until there is no high priority requests,
   * then get a data zone aside reservation zones.
   * When no available zones to allocate, nullptr will returned. 
   */
  Zone *AllocateDataZone(Env::WriteLifeTimeHint lifetime, bool high_pri);
  Zone *AllocateMetaZone();

  /* Submit a background job to replace read only zones in active zone vector.
   * with resources.
   * Function called when one active zone is turned into read only state.
   */
  void BgReplaceReadOnlyZones();

  /* Tool function of BgReplaceReadOnlyZones().
   * Get a suitable zone from io_zones to replace read only zone in active zone vector.
   * Nullptr is returned when no resource available.
   */
  Zone* BgGetIOZoneToWrite();
  void BgResetDataZone(Zone* target);
  
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
