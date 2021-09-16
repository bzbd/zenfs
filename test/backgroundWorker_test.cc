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
  int elapsed_time = rand() % 1000;
  std::this_thread::sleep_for(std::chrono::microseconds(elapsed_time));
  return 42;
}

// Assume this arg contains zone number that you wanna operate with, which is a int
int arg_deep_thought(void* arg) {
  int elapsed_time = rand() % 1000;
  std::this_thread::sleep_for(std::chrono::microseconds(elapsed_time));
  // Get the correct type manually
  int zone_number = *(int*) arg;
  // Do the job with the arg brings in.
  std::cout << "operating zone:" << zone_number << std::endl;
  // return value shows is this operation successfully done or not,
  // most commonly, 0 stands for success, other stands for failed,
  // For jobs that could failed, using the errorhandlingbgjob which 
  // will get this return value and process error handling.
  return 0;
}
  
std:vector<int> zone_numbers;
  
int test() {
  BackgroundWorker bg_worker;
  int num_jobs = 1000000;
  for (int i = 0; i < num_jobs; i++) {
    // Assume job for each zone.
    zone_numbers.emplace_back(i);
    // Submit the job with zone_number[i] where hold the zone number.
    bg_worker.SubmitJob(&ROCKSDB_NAMESPACE::arg_deep_thought, &zone_numbers[i]);
  }
  
//   for (int i = 0; i < num_jobs; i++) {
//     bg_worker.SubmitJob(&ROCKSDB_NAMESPACE::deep_thought, &question);
//   }
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
