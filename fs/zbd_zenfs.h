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

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/metrics_reporter.h"
#include "zbd_stat.h"

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;
class ZoneSnapshot;
class ZenFSSnapshotOptions;

struct zenfs_aio_ctx {
  struct iocb iocb;
  struct iocb *iocbs[1];
  io_context_t io_ctx;
  int inflight;
  int fd;
};

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

  // If current zone is been resetting or finishing.
  std::atomic<bool> processing_{false};

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

template <typename ARG_T, typename RET_T>
class GeneralJob : public BackgroundJob {
 public:
  // Job requires argument and return value for error handling
  std::function<RET_T(ARG_T)> fn_;
  // Argument for execution
  ARG_T arg_;
  // Error handler
  std::function<void(RET_T)> hdl_;
  // No default allowed
  GeneralJob() = delete;
  GeneralJob(std::function<RET_T(ARG_T)> fn, ARG_T arg, std::function<void(RET_T)> hdl)
      : fn_(fn), arg_(arg), hdl_(hdl) {}
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
  void SubmitJob(std::unique_ptr<BackgroundJob> &&job);
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones_;
  std::mutex io_zones_mtx_;
  std::mutex wal_zones_mtx_;
  // meta log zones used to keep track of running record of metadata
  std::vector<Zone *> op_zones_;
  // snapshot zones used to recover entire file system
  std::vector<Zone *> snapshot_zones_;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  // If a thread is allocating a zone fro WAL files, other
  // thread shouldn't take `io_zones_mtx_` (see AllocateZone())
  std::atomic<uint32_t> wal_zone_allocating_{0};

  std::atomic<int> pending_bg_work_{0};

  std::atomic<long> active_io_zones_{0};
  std::atomic<long> open_io_zones_{0};
  std::condition_variable zone_resources_;

  uint32_t max_nr_active_io_zones_;
  uint32_t max_nr_open_io_zones_;

  void EncodeJsonZone(std::ostream &json_stream, const std::vector<Zone *> zones);

 public:
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  std::mutex metazone_reset_mtx_;
  std::condition_variable metazone_reset_cv_;

  std::shared_ptr<ZenFSMetrics> metrics_;

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics = std::make_shared<NoZenFSMetrics>());

  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly = false);
  IOStatus CheckScheduler();

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, bool is_wal);
  Zone *AllocateMetaZone();
  Zone *AllocateSnapshotZone();

  void FinishOrReset(Zone *z, bool reset = false);

  int GetResetableZones();
  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();
  void ReportSpaceUtilization();

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

  std::vector<Zone *> GetOpZones() { return op_zones_; }
  std::vector<Zone *> GetSnapshotZones() { return snapshot_zones_; }

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
  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }
  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot,
                       const ZenFSSnapshotOptions &options);

  std::unique_ptr<BackgroundWorker> meta_worker_;
  std::unique_ptr<BackgroundWorker> data_worker_;

 private:
  std::string ErrorToString(int err);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
