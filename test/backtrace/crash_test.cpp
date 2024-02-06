#include "common/exception.h"
#include "common/macros.h"
#include "gtest/gtest.h"

// NOLINTBEGIN

namespace bustub {

TEST(CrashTest, PtrAccess) {
  // ASAN will show the full backtrace
  int *p = nullptr;
  *p = 2;
}

TEST(CrashTest, GtestAssert) {
  // Gtest will show the line that failed
  ASSERT_TRUE(false);
}

TEST(CrashTest, Assert) {
  // Default assertion implementation, no backtrace, only lineno
  BUSTUB_ASSERT(false, "assert failure");
}

TEST(CrashTest, Ensure) {
  // Full stacktrace provided by backward-cpp
  BUSTUB_ENSURE(false, "assert failure");
}

TEST(CrashTest, Throw) {}

}  // namespace bustub

// NOLINTEND
