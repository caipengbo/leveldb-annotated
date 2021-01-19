// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;  // 每个Slot上链表的指针
  LRUHandle* next;
  LRUHandle* prev;
  // 每个cache节点的开销（一般是内存使用量）
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;     // Whether entry is in the cache.(在 lru_ 和 in_use_链表中都可称为in the Cache)
  uint32_t refs;     // References, including cache reference, if present.
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks(实现了简单的HashTable, 去除了大量的适配、移植逻辑)
// and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested（比许多内置的hash表快）.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  // 注意初始化的时候，调用了一次 Resize()
  HandleTable() : length_(0), elems_(0), list_(nullptr) {
    Resize();
  }
  ~HandleTable() { delete[] list_; }

  // 查
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  // 插入元素（返回旧的元素，用于回收）
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;  // 当前Hash桶的数目
  uint32_t elems_;  // 当前hash表共存了多少元素
  LRUHandle** list_;  // 节点指针 数组，代表桶数组，每个桶是一个Node（LRUHandle），是链表的表头（使用链地址法解决冲突）

  // Return a pointer to slot that points to a cache entry that matches key/hash.
  // If there is no such cache entry, return a pointer to the trailing slot in the corresponding linked list.
  // 找到该 key/hash 对应的节点，返回该节点
  // 未找到， 返回当前 slot 对应的链表的尾部
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    // 找到 Slot
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    // 遍历 Slot 链表
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // 扩容
  void Resize() {
    // 初始有4个槽位
    uint32_t new_length = 4;
    // 4 8 16 32 64  128  256  寻找大于 elem_ 的第一个 2的N次方的数
    // 为什么使用2的N次方呢？（Java HashMap 的实现一样）
    // 有两个原因：
    //    1. 可以使用&计算hash位置 (n - 1) & hash
    //    2. 可以快速迁移（快速计算新的位置）hash & (new_length - 1)
    while (new_length < elems_) {
      new_length *= 2;
    }
    // 分配 新的Hash数组
    LRUHandle** new_list = new LRUHandle*[new_length];
    // new_list == nullptr
    // memset这个函数功能强大，但是要初始化要赋值0，不能赋其他值，造成结果不可预知，
    // 这样指针也会被赋值为NULL，不会成为野指针，其它类型的对象都会赋成 0.
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    // 旧 table 的 将旧 Table 中的数据迁移到新 Table
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];  // 此处可以查询：HashMap为什么容量总使用2的N次幂
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
// 以 LRUCache 的粒度进行加锁（分段锁）, SharedLRUCache 包含多个（默认16个）LRUCache
// 维护了 两个链表（in_use_, lru_） 和 一个Hash表(table_)
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  // 注意：这个容量是内存占用量，不是Table的大小
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  // LRUHandle的charge和
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // LRU list, 会被淘汰
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // 正在被客户端使用的LRUHandle
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

// 缓存被使用
// 该数据将从 链表上（in_use_或者lru_）被删除，然后插入到 in_use_链表
void LRUCache::Ref(LRUHandle* e) {
  // e->refs == 1 说明该Cache节点仅在链表中（没有被客户端使用）
  // e->in_cache 说明 在 lru_ 或者在 in_use 链表中
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

// 缓存不被使用
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    // 调用已经注册好的deleter函数
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // 数据在 Cache 中，没人使用
    // No longer in use; move to lru_ list.
    // 将数据加到到 lru 链表中，因为是头插法，所以符合 LRU 规则
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// 从双向链表（不管哪个链表）中移除该节点
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// 在 e 插入到 list 中（查到头部）
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  // 转换成
  return reinterpret_cast<Cache::Handle*>(e);
}

// (根据Cache::Handle) 释放 LRUHandler
// 为什么不直接调用Unref ？  因为涉及到 强制类型 转换
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

// 在Cache中插入数据，并且注册释放函数（当元素不被使用的时候Unref，调用deleter）
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);
  // 申请内存，存储缓存数据
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    // 将新元素插入到in_use_链表中
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    // 当前Cache的内存占用
    usage_ += charge;
    // 插入会返回旧的元素，然后使用 FinishErase进行回收
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  // 当前Cache使用量超过了capacity_，回收lru中的旧记录
  // 缓存淘汰策略
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
//
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}
// 删除 lru_ 链上的记录（用于清理空间，不是缓存淘汰策略噢）
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  // 从 lru 中删除
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

// 为何写成这种格式？？？
// 为了下面便于求路由的分片
static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

// 默认的LRU Cache
// 内部包含多个 LRUCache
class ShardedLRUCache : public Cache {
 private:
  // 16 个 shard
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  // 求Slice的Hash
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // 分片路由（求位于哪个分片）
  // 取最高的 kNumShardBits 位
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;  // 向上取整
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
