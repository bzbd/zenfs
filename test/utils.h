#include <dirent.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <rocksdb/file_system.h>
#include <third-party/zenfs/fs/fs_zenfs.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <atomic>
#include <boost/fiber/buffered_channel.hpp>
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

bool find_sub_string(std::string const& inputString, std::string const& subString) {
  std::size_t found = inputString.find(subString);
  if (found!=std::string::npos) {
    return false;
  }

  return true;
}

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
