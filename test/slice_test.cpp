//
// Created by caipengbo on 2021/1/13.
//
#include <cassert>
#include <iostream>
#include "gtest/gtest.h"

using namespace std;
using namespace testing;

class Slice {

 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) {
    cout << "Call Slice(const char* d, size_t n)" << endl;
  }

  // Create a slice that refers to the contents of "s"
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {
    cout << "Slice(const std::string& s)" << endl;
  }

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) {
    cout << "Slice(const char* s)" << endl;
  }

  // Intentionally copyable.
  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

 private:
  // 仅保存了 指针 和 size
  const char* data_;
  size_t size_;
};

TEST(SliceTest, TEST1) {
  string str = "1234";
  Slice s = str;
  cout << s.data() << endl;
//  ASSERT_EQ(5, 5);
}

void EncodeFixed64(char* dst, uint64_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
  buffer[4] = static_cast<uint8_t>(value >> 32);
  buffer[5] = static_cast<uint8_t>(value >> 40);
  buffer[6] = static_cast<uint8_t>(value >> 48);
  buffer[7] = static_cast<uint8_t>(value >> 56);
}

TEST(SliceTest, TEST2) {
  string s;
  s.clear();
  s.resize(12);
  EncodeFixed64(&s[0], 98);
  cout << s << endl;
//  ASSERT_EQ(5, 5);
}






int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}