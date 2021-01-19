// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// TableCache 缓存的 value
struct TableAndFile {
  // 文件句柄
  RandomAccessFile* file;
  // 是指向内存中这个SSTable文件对应的Table结构指针，table结构在内存中，
  // 保存了SSTable的index内容以及用来指示block cache用的cache_id
  Table* table;
};
// 缓存的 key 是sstable文件名

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

// 在缓存中查找内容（file_name）, 若缓存中没找到则从文件中查找（并放入Cache）
// TableCache::FindTable()->Table::Open()
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  // 组装 Key
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  // 在缓存中没找到，则去
  if (*handle == nullptr) {
    // 以 .ldb 后缀读取文件
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 以.ldb打开失败，再以 .sst 打开
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // 读取sstable file，创建 Table 实体 table
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 将打开的文件放入 cache
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

// 此方法就是查找ldb文件中是否存在key，若存在则执行handle_result函数
// TableCache::Get()->TableCache::FindTable()->Table::Open()
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  // 去缓存中查找
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    // 找到之后，取出数据(TableAndFile)
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);  // 取出该Key的数据
    cache_->Release(handle);  // 释放该缓存的引用
  }
  return s;
}

// 删除ldb文件在tableCache中缓存(当文件被删除的时候)
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
