#include "utils.h"

namespace ROCKSDB_NAMESPACE {

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
    fprintf(stderr, "Failed to mount filesystem, error: %s\n", s.ToString().c_str());
    return 1;
  }

  std::vector<std::thread> thread_handlers;
  std::atomic<int> counter(0);
  std::random_device random_device;

  struct TaskInfo {
    FSWritableFile *file;
    int file_id;
  };

  char buffer[1048576] = {0};
  Slice slice(buffer, 1048576);

  auto task = [zenFS, &counter, &random_device, slice](int i) {
    std::default_random_engine e1(random_device());
    std::uniform_int_distribution<int> uniform_dist(300, 500);

    for (int g = 0; g < 10000; g++) {
      auto file_id = counter.fetch_add(1, std::memory_order_relaxed);
      char f[100] = {0};
      IOStatus s;
      IOOptions iopts;
      IODebugContext dbg;

      sprintf(f, "zenfs_test/test_file_%d.log", file_id);

      {
        FileOptions fopts;
        std::unique_ptr<FSWritableFile> f_file;
        s = zenFS->NewWritableFile(f, fopts, &f_file, &dbg);
        if (!s.ok()) {
          std::cerr << "[#" << file_id << "] failed to create new file: " << s.ToString() << std::endl;
        }
        std::cerr << "+" << f << std::endl;
        auto file_size = uniform_dist(e1);

        std::vector<std::thread> writer_threads;

        for (int t = 0; t < file_size; t++) {
          IOStatus s;
          IOOptions iopts;
          IODebugContext dbg;
          s = f_file->Append(slice, iopts, &dbg);
          if (!s.ok()) {
            std::cerr << "[#" << file_id << "] failed to append: " << s.ToString() << std::endl;
          }
          s = f_file->Sync(iopts, &dbg);
          if (!s.ok()) {
            std::cerr << "[#" << file_id << "] failed to sync: " << s.ToString() << std::endl;
          }
        }
      }

      // s = zenFS->DeleteFile(f, iopts, &dbg);
      // if (!s.ok()) {
      //   std::cerr << "[#" << file_id << "] failed to delete: " <<
      //   s.ToString()
      //             << std::endl;
      // }

      // std::cerr << "-" << f << std::endl;
    }
  };

  for (int i = 0; i < 40; i++) {
    thread_handlers.emplace_back(std::thread(task, i));
  }

  for (auto &&thread : thread_handlers) {
    thread.join();
  }

  delete zenFS;
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          +" <command> [OPTIONS]...\nCommands: mkfs, list, "
                           "ls-uuid, df, backup, restore");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  return ROCKSDB_NAMESPACE::test();
}
