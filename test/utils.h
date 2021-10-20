#include <sys/stat.h>
#include <sys/types.h>

#include <dirent.h>
#include <fcntl.h>

#include <gflags/gflags.h>

#include <rocksdb/file_system.h>
#include <fs/fs_zenfs.h>

#include <atomic>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <streambuf>
#include <thread>

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(zbd, "", "Path to a zoned block device.");
DEFINE_string(aux_path, "",
"Path for auxiliary file storage (log and lock files).");
DEFINE_bool(force, false, "Force file system creation.");
DEFINE_string(path, "", "File path");
DEFINE_int32(finish_threshold, 0, "Finish used zones if less than x% left");
DEFINE_string(restore_path, "", "Path to restore files");
DEFINE_string(backup_path, "", "Path to backup files");
DEFINE_int32(max_active_zones, 0, "Max active zone limit");
DEFINE_int32(max_open_zones, 0, "Max active zone limit");

namespace ROCKSDB_NAMESPACE {

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

ZonedBlockDevice *zbd_open(bool readonly, std::shared_ptr<Logger> logger) {
  ZonedBlockDevice *zbd = new ZonedBlockDevice(FLAGS_zbd, logger);
  IOStatus open_status = zbd->Open(readonly);

  if (!open_status.ok()) {
    fprintf(stderr, "Failed to open zoned block device: %s, error: %s\n",
            FLAGS_zbd.c_str(), open_status.ToString().c_str());
    delete zbd;
    return nullptr;
  }

  return zbd;
}

Status zenfs_mount(ZonedBlockDevice *zbd, ZenFS **zenFS, bool readonly, std::shared_ptr<Logger> logger) {
  Status s;

  *zenFS = new ZenFS(zbd, FileSystem::Default(), logger);
  s = (*zenFS)->Mount(readonly);
  if (!s.ok()) {
    delete *zenFS;
    *zenFS = nullptr;
  }

  return s;
}

static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm *log_start = std::localtime(&t);
  char buf[40];

  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_test";

  return ss.str();
}
}
