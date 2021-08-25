// This header file is used for debugging, do not use the functions here unless
// you understand what you are doing.
//
// guokuankuan@bytedance.com
//
#pragma once
#include <chrono>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <sstream>

#ifdef ZENFS_DEBUG

// zone_id: filename, create_time
static std::map<uint64_t, std::vector<std::pair<std::string, std::string>>>
    opened_files;
// filenames
static std::set<std::string> opening_files;

// Avoid std::cerr print interleave cross multiple concurrent threads.
class LineWriter {
 private:
  std::ostringstream st;

 public:
  template <typename T>
  LineWriter& operator<<(T const& t) {
    st << t;
    return *this;
  }
  ~LineWriter() {
    std::string s = st.str();
    std::cerr << s;
  }
};

// Print all opened/opening files, opening files means these files are waiting
// for zone allocation mutex.
inline void PrintZoneFiles() {
  LineWriter() << std::this_thread::get_id() << " Opening Files:\n";
  {
    LineWriter w;
    w << "\t";
    for (const auto& fname : opening_files) {
      w << fname << ", ";
    }
    w << "\n";
  }

  LineWriter() << std::this_thread::get_id() << " Opened Files:\n";
  {
    LineWriter w;
    for (const auto& mp : opened_files) {
      w << "\t" << mp.first << " : ";
      for (const auto& pair : mp.second) {
        w << " {" << pair.first << ", " << pair.second << "}, ";
      }
      w << "\n";
    }
    w << "\n";
  }
}

inline uint64_t GetCurrentTimeNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t TimeDiff(
    const std::chrono::time_point<std::chrono::system_clock> before,
    const std::chrono::time_point<std::chrono::system_clock> after) {
  return std::chrono::duration_cast<std::chrono::microseconds>(after - before)
      .count();
}

// Get string represent of current time, e.g.: 22:30:11
inline const std::string CurrentTime() {
  time_t now = time(0);
  struct tm tstruct;
  char buf[80];
  tstruct = *localtime(&now);
  strftime(buf, sizeof(buf), "%H:%M:%S", &tstruct);
  return buf;
}

inline std::string GetDateString() {
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  return std::ctime(&now_time);
}

// For quick time trace debugging
thread_local std::map<std::string, uint64_t> TimeTrace;

#define TIME_TRACE_START(tag) TimeTrace[tag] = GetCurrentTimeNanos();

#define TIME_TRACE_END(tag) \
  TimeTrace[tag] = GetCurrentTimeNanos() - TimeTrace[tag];

#define PRINT_TIME_TRACE()                                     \
  for (const auto& item : TimeTrace) {                         \
    printf("\t%s : %ld ns\n", item.first.data(), item.second); \
  }                                                            \
  printf("\n");

// Limit for capture long latency (millisecond level)
#define LONG_LATENCY_THRESHOLD 50 * 1000 * 1000
#else
#define TIME_TRACE_START(tag)
#define TIME_TRACE_END(tag)
#define PRINT_TIME_TRACE()
#endif
