// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <stdint.h>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/convenience.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
// using ROCKSDB_NAMESPACE::ConfigOptions;
// using ROCKSDB_NAMESPACE::Comparator;
using ROCKSDB_NAMESPACE::FlushOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

class TwoPartComparator : public rocksdb::Comparator {
  public:
  // Three-way comparison function:
  // if a < b: negative result
  // if a > b: positive result
  // else: zero result
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
    // int a1, a2, b1, b2;
    // ParseKey(a, &a1, &a2);
    // ParseKey(b, &b1, &b2);
    // if (a1 < b1) return -1;
    // if (a1 > b1) return +1;
    // if (a2 < b2) return -1;
    // if (a2 > b2) return +1;

    uint64_t ts1, ts2;
    // char *pv1, *pv2;
    memcpy(&ts1, a.data(), sizeof(uint64_t));
    memcpy(&ts2, b.data(), sizeof(uint64_t));
    const char *pv1 = a.data() + sizeof(uint64_t);
    const char *pv2 = b.data() + sizeof(uint64_t);
    if (ts1 < ts2) return -1;
    if (ts1 > ts2) return +1;
    // if (ts1 == ts2) return 0;

    // a or b key is only timestamp (no pv name)
    if (sizeof(uint64_t) == a.size()) {
      return -1;
    }
    if (sizeof(uint64_t) == b.size()) {
      return +1;
    }
    if ((sizeof(uint64_t) == a.size()) && (sizeof(uint64_t) == b.size())) {
      return 0;
    }
    if (pv1 < pv2) return -1;
    if (pv1 > pv2) return +1;

    return 0;
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
  void FindShortSuccessor(std::string*) const { }
};

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // use timestamps in keys internally
  // NOTE: The timestamp needs to be specified in Get() ReadOptions() and does not
  //       seem to be returned as part of the "value"?! Not really what I want!
  // ConfigOptions config_options;
  // const Comparator* user_comparator = nullptr;
  // Status s = Comparator::CreateFromString(
  //     config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
  // options.comparator = user_comparator;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  WriteOptions write_options;
  write_options.sync = true;

  // Put key-value
  // char buf[32];
  // uint64_t value64;
  // value64 = 1234456;
  // memcpy(buf, &value64, sizeof(value64));
  // s = db->Put(write_options, "key1", buf, "value");
  s = db->Put(write_options, "key1", "value11");
  assert(s.ok());
  s = db->Put(write_options, "key2", "value22");
  assert(s.ok());
  s = db->Put(write_options, "key3", "value33");
  assert(s.ok());
  s = db->Put(write_options, "key4", "value44");
  assert(s.ok());
  s = db->Put(write_options, "key5", "value55");
  assert(s.ok());
  s = db->Put(write_options, "key6", "value66");
  assert(s.ok());
  s = db->Put(write_options, "key7", "value77");
  assert(s.ok());



  std::string value;

  // get value
  // ReadOptions read_opts;
  // read_opts.timestamp = &ts;
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value11");

  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
  }
  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;
  
  db->Flush(FlushOptions());
  db->Close();
/*
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
