#include <dirent.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <rocksdb/file_system.h>
#include <rocksdb/plugin/zenfs/fs/fs_zenfs.h>
#include <rocksdb/plugin/zenfs/fs/zbd_zenfs.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <thread>

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

namespace ROCKSDB_NAMESPACE {

std::string question = "Question of Life, the Universe, and Everything";

int deep_thought(void* arg) {
  int elapsed_time = rand() % 100000;
  std::this_thread::sleep_for(std::chrono::microseconds(elapsed_time));
  return 42;
}

int test() {
  BackgroundWorker bg_worker;
  int num_jobs = 1000000;
  for (int i = 0; i < num_jobs; i++) {
    bg_worker.SubmitJob(&deep_thought, &question);
  }
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
