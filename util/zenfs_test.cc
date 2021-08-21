#include <dirent.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <rocksdb/file_system.h>
#include <rocksdb/plugin/zenfs/fs/fs_zenfs.h>
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

namespace ROCKSDB_NAMESPACE {

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

Status zenfs_mount(ZonedBlockDevice *zbd, ZenFS **zenFS, bool readonly,
                   std::shared_ptr<Logger> logger) {
  Status s;

  *zenFS = new ZenFS(zbd, FileSystem::Default(), logger);
  s = (*zenFS)->Mount(readonly);
  if (!s.ok()) {
    delete *zenFS;
    *zenFS = nullptr;
  }

  return s;
}

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm *log_start = std::localtime(&t);
  char buf[40];

  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_test";

  return ss.str();
}

int test() {
  std::shared_ptr<Logger> logger;
  Status s;

  s = Env::Default()->NewLogger(GetLogFilename(FLAGS_zbd), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }

  ZonedBlockDevice *zbd = zbd_open(false, logger);
  if (zbd == nullptr) return 1;

  ZenFS *zenFS;
  s = zenfs_mount(zbd, &zenFS, false, logger);
  if (!s.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            s.ToString().c_str());
    return 1;
  }

  std::vector<std::thread> thread_handlers;
  std::atomic<int> counter(0);
  std::random_device random_device;

  struct TaskInfo {
    FSWritableFile *file;
    int file_id;
  };

  typedef boost::fibers::buffered_channel<TaskInfo> channel_t;

  auto task = [zenFS, &counter, &random_device](int i) {
    std::default_random_engine e1(random_device());
    std::uniform_int_distribution<int> uniform_dist(3000, 5000);

    for (int g = 0; g < 10000; g++) {
      auto file_id = counter.fetch_add(1, std::memory_order_relaxed);
      char f[100] = {0};
      IOStatus s;
      IOOptions iopts;
      IODebugContext dbg;

      sprintf(f, "zenfs_test/test_file_%d.sst", file_id);

      {
        FileOptions fopts;
        std::unique_ptr<FSWritableFile> f_file;
        s = zenFS->NewWritableFile(f, fopts, &f_file, &dbg);
        if (!s.ok()) {
          std::cerr << "[#" << file_id
                    << "] failed to create new file: " << s.ToString()
                    << std::endl;
        }
        std::cerr << "+" << f << std::endl;
        char test_data[4096] = {0};
        Slice data(test_data, 4096);
        auto file_size = uniform_dist(e1);

        std::vector<std::thread> writer_threads;
        channel_t chan(128);

        for (int i = 0; i < 3; i++) {
          writer_threads.emplace_back(std::thread([&chan]() {
            IOStatus s;
            IOOptions iopts;
            IODebugContext dbg;
            TaskInfo info;
            char test_data[4096] = {0};
            while (boost::fibers::channel_op_status::success ==
                   chan.pop(info)) {
              s = info.file->Append(test_data, iopts, &dbg);
              if (!s.ok()) {
                std::cerr << "[#" << info.file_id
                          << "] failed to append: " << s.ToString()
                          << std::endl;
              }
              s = info.file->Sync(iopts, &dbg);
              if (!s.ok()) {
                std::cerr << "[#" << info.file_id
                          << "] failed to sync: " << s.ToString() << std::endl;
              }
            }
          }));
        }

        for (int t = 0; t < file_size; t++) {
          TaskInfo info;
          info.file = f_file.get();
          info.file_id = file_id;
          chan.push(info);
        }
        chan.close();
        for (auto &&thread : writer_threads) {
          thread.join();
        }
      }

      s = zenFS->DeleteFile(f, iopts, &dbg);
      if (!s.ok()) {
        std::cerr << "[#" << file_id << "] failed to delete: " << s.ToString()
                  << std::endl;
      }

      std::cerr << "-" << f << std::endl;
    }
  };

  auto stat_thread = std::thread([&zenFS]() {
    std::vector<ZoneStat> stat;
    while (true) {
      stat = zenFS->GetStat();
      std::cerr << "GetStat" << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  });

  for (int i = 0; i < 40; i++) {
    thread_handlers.emplace_back(std::thread(task, i));
  }

  for (auto &&thread : thread_handlers) {
    thread.join();
  }

  stat_thread.join();

  delete zenFS;
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  +" <command> [OPTIONS]...\nCommands: mkfs, list, ls-uuid, df, backup, restore");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  return ROCKSDB_NAMESPACE::test();
}
