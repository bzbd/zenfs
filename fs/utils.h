#pragma once
#include <chrono>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <sstream>

#ifdef ZENFS_DEBUG
thread_local std::map<std::string, uint64_t> TimeTrace;

inline uint64_t GetCurrentTimeNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}


class AtomicWriter {
private:
    std::ostringstream st;

 public:
    template <typename T> 
    AtomicWriter& operator<<(T const& t) {
       st << t;
       return *this;
    }
    ~AtomicWriter() {
       std::string s = st.str();
       std::cerr << s;
       //fprintf(stderr,"%s", s.c_str());
       // write(2,s.c_str(),s.size());
    }
 };

inline uint64_t TimeDiff(const std::chrono::time_point<std::chrono::system_clock> before,
		         const std::chrono::time_point<std::chrono::system_clock> after) {
	  return std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();
}

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

#define TIME_TRACE_START(tag) \
  TimeTrace[tag] = GetCurrentTimeNanos();

#define TIME_TRACE_END(tag) \
  TimeTrace[tag] = GetCurrentTimeNanos() - TimeTrace[tag];

#define PRINT_TIME_TRACE()                               \
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

