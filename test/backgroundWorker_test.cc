#include <dirent.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <rocksdb/file_system.h>
#include <rocksdb/plugin/zenfs/fs/fs_zenfs.h>
#include <rocksdb/plugin/zenfs/fs/zbd_zenfs.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <numeric>
#include <stdlib.h>
#include <string>
#include <thread>

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

namespace ROCKSDB_NAMESPACE {

const int num_jobs = 100000;
std::vector<int> zone_numbers(num_jobs);
std::atomic<int> actual_zone_number_sum(0);

void reset_zone_numbers_vec_and_sum() {
  actual_zone_number_sum = 0;

  for (int i = 0; i < zone_numbers.size(); i++) {
    zone_numbers[i] = i;
  }
}

// Assume this arg contains zone number that you wanna operate with, which is a int
int sum_zone_number(void* arg) {
  int elapsed_time = rand() % 1000;
  std::this_thread::sleep_for(std::chrono::microseconds(elapsed_time));
  // Get the correct type manually
  int zone_number = *(int*) arg;
  // Do the job with the arg brings in.
  actual_zone_number_sum += zone_number;

  // return value shows is this operation successfully done or not,
  // most commonly, 0 stands for success, other stands for failed,
  // For jobs that could failed, using the errorhandlingbgjob which 
  // will get this return value and process error handling.
  return 0;
}

int test_sum_in_background_worker() {
  reset_zone_numbers_vec_and_sum();

  {
    BackgroundWorker bg_worker;
    for (int i = 0; i < zone_numbers.size(); i++) {
      // Submit the job with zone_number[i] where hold the zone number.
      bg_worker.SubmitJob(&ROCKSDB_NAMESPACE::sum_zone_number, &zone_numbers[i]);
    }
  }

  const int expected_zone_number_sum =
      std::accumulate(zone_numbers.cbegin(), zone_numbers.cend(), 0);

  assert(actual_zone_number_sum == expected_zone_number_sum);

  return 0;
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                            +" <command> [OPTIONS]...\nCommands: mkfs, list, "
                             "ls-uuid, df, backup, restore");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  return ROCKSDB_NAMESPACE::test_sum_in_background_worker();
}
