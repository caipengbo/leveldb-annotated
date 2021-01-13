//
// Created by Myth on 11/16/2020.
//
#include <cassert>
#include <iostream>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "gtest/gtest.h"

using namespace std;
using namespace leveldb;
using namespace testing;

TEST(MainTest, HelloTest) {
  cout << "hello" << endl;
  ASSERT_EQ(5, 5);
}
TEST(MainTest, TestLevelDB) {
  leveldb::DB *db;
  // DB的（设置）选项，还有 ReadOptions 和 WriteOptions
  leveldb::Options options;
  options.create_if_missing = true;  // 设置选项的内容，如果数据库不存在，就创建一个

  // 打开数据库，name是一个目录，数据库产生的文件都会在此目录中
  leveldb::Status status = leveldb::DB::Open(options, "leveldb_run_dir", &db);
  ASSERT_TRUE(status.ok());
  // Status代表操作数据库的结果
  // 数据库有Put、Get、Delete和Write等操作数据的接口
  status = db->Put(WriteOptions(), "Key1", "Value1");
  ASSERT_TRUE(status.ok());
  string res;
  status = db->Get(ReadOptions(), "Key1", &res);
  ASSERT_TRUE(status.ok());
  cout << res << endl;

  std::string value, key1 = "key1", key2 = "key2";
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
  if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

  s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.ok()) {
    // 批量插入
    leveldb::WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, value);
    s = db->Write(leveldb::WriteOptions(), &batch);
  }
  // 使用迭代器进行遍历
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
  delete it;

  delete db;
}
TEST(MainTest, TestPut) {
  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = DB::Open(options, "test_put_db", &db);
  ASSERT_TRUE(status.ok());
  status = db->Put(leveldb::WriteOptions(), "key1", "value1");
  ASSERT_TRUE(status.ok());
}
TEST(MainTest, TestGet) {
  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = DB::Open(options, "test_put_db", &db);
  ASSERT_TRUE(status.ok());
  std::string value;
  status = db->Get(leveldb::ReadOptions(), "key1", &value);
  ASSERT_TRUE(status.ok());
  std::cout << value << std::endl;
}
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}