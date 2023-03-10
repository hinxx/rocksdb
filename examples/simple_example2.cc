// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
// #include "rocksdb/coding.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif


// example of comparator function that handles "int:int" type keys
class TwoPartComparator : public rocksdb::Comparator {
  public:
  // Three-way comparison function:
  // if a < b: negative result
  // if a > b: positive result
  // else: zero result
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, &a1, &a2);
    ParseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  void ParseKey(const rocksdb::Slice &key, int *a, int *b) const {
    int n = sscanf(key.data(), "%d:%d", a, b);
    assert(n == 2);
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
  void FindShortSuccessor(std::string*) const { }
};


// inline void EncodeFixed64(char* buf, uint64_t value) {
//   if (port::kLittleEndian) {
//     memcpy(buf, &value, sizeof(value));
//   } else {
//     buf[0] = value & 0xff;
//     buf[1] = (value >> 8) & 0xff;
//     buf[2] = (value >> 16) & 0xff;
//     buf[3] = (value >> 24) & 0xff;
//     buf[4] = (value >> 32) & 0xff;
//     buf[5] = (value >> 40) & 0xff;
//     buf[6] = (value >> 48) & 0xff;
//     buf[7] = (value >> 56) & 0xff;
//   }
// }

inline void PutFixed64(std::string* dst, uint64_t value) {
  // if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  // } else {
  //   char buf[sizeof(value)];
  //   EncodeFixed64(buf, value);
  //   dst->append(buf, sizeof(buf));
  // }
}

inline uint64_t GetFixed64(const char* ptr) {
  // if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  // } else {
  //   uint64_t lo = DecodeFixed32(ptr);
  //   uint64_t hi = DecodeFixed32(ptr + 4);
  //   return (hi << 32) | lo;
  // }
}

int main(int argc, char **argv) {
  DB* db;
  Options options;
  Status s;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // set comparator with timestamp
  rocksdb::ConfigOptions config_options;
  const rocksdb::Comparator* user_comparator = nullptr;
  s = rocksdb::Comparator::CreateFromString(
      config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
  assert(user_comparator != nullptr);
  options.comparator = user_comparator;

  // open DB
  s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  if (argc > 1 && argv[1][0] == 'w') {
    // Put key-value
    // If sync is not used here then nothing is persisted in the SSTable on exit..
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    // s = db->Put(write_options, "key1", "value11");
    // std::string ts("1234");
    std::string ts;
    PutFixed64(&ts, 1111);
    s = db->Put(write_options, "key1", ts, "value11");
    printf("err: %s\n", s.ToString().c_str());
    assert(s.ok());
    ts.clear();
    PutFixed64(&ts, 2222);
    s = db->Put(write_options, "key2", ts, "value22");
    printf("err: %s\n", s.ToString().c_str());
    assert(s.ok());
    ts.clear();
    PutFixed64(&ts, 3333);
    s = db->Put(write_options, "key3", ts, "value33");
    printf("err: %s\n", s.ToString().c_str());
    assert(s.ok());
    // s = db->Put(write_options, "key6", "value66");
    // assert(s.ok());
    // s = db->Put(write_options, "key5", "value55");
    // assert(s.ok());
    // s = db->Put(write_options, "key4", "value44");
    // assert(s.ok());
  }

  // get value
  // std::string key("key1");
  // std::string value;
  // std::string ts;
  // PutFixed64(&ts, 100);
  // rocksdb::Slice tss = rocksdb::Slice(ts);
  // ReadOptions read_opts;
  // read_opts.timestamp = &tss;
  // s = db->Get(ReadOptions(), key, &value, &ts);
  // printf("err: %s\n", s.ToString().c_str());
  // err: Invalid argument: cannot call this method on column family default that enables timestamp
  // assert(s.ok());
  // assert(value == "value11");
  // printf("single key,value: %s = %s, timestamp %s\n", key.c_str(), value.c_str(), ts.c_str());

  // The following example demonstrates how to print all (key, value) pairs in a database.
  // rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  std::string startts;
  PutFixed64(&startts, 1800);
  std::string endts;
  PutFixed64(&endts, 4000);
  rocksdb::Slice starttss = rocksdb::Slice(startts);
  rocksdb::Slice endtss = rocksdb::Slice(endts);
  rocksdb::ReadOptions read_opts1;
  read_opts1.timestamp = &endtss;
  read_opts1.iter_start_ts = &starttss;
  rocksdb::Iterator* it = db->NewIterator(read_opts1);
  // for (it->SeekToFirst(); it->Valid(); it->Next()) {
  for (it->Seek("key2"); it->Valid(); it->Next()) {
    printf("iter all %s = %s, ts %ld\n", it->key().ToString().c_str(), it->value().ToString().c_str(), GetFixed64(it->timestamp().ToString().c_str()));
  }
  printf("err: %s\n", it->status().ToString().c_str());
  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;


  // it = db->NewIterator(rocksdb::ReadOptions());
  // // The following variation shows how to process just the keys in the range [start, limit):
  // for (it->Seek("key2");
  //      it->Valid() && it->key().ToString() < "key6";
  //      it->Next()) {
  //   printf("iter range %s = %s\n", it->key().ToString().c_str(), it->value().ToString().c_str());
  // }
  // assert(it->status().ok()); // Check for any errors found during the scan
  // delete it;




  s = db->Close();
  if (!s.ok()) {
    printf("Close() failed: %s\n", s.ToString().c_str());
  }

/*
  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point
*/

  delete db;

  return 0;
}
