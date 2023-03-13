// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

// std::string kDBPath = "/tmp/rocksdb_simple_example_ts";
std::string kDBPath = "/tmp/rocksdb_simple_example_my_ts";

inline void PutFixed64(std::string* dst, uint64_t value) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)), sizeof(value));
}

inline uint64_t GetFixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

#define MAX_KEY_SIZE    512

class MyComparator : public rocksdb::Comparator {
 public:
  static const char* kClassName() {
    // static std::string class_name = kClassNameInternal();
    // return class_name.c_str();
    return "MyComparator";
  }
  const char* Name() const override { return kClassName(); }

  void FindShortSuccessor(std::string*) const override {}
  void FindShortestSeparator(std::string*, const rocksdb::Slice&) const override {}
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    // printf("---------------------------------------------------------------------------------\n");
    char abuf[MAX_KEY_SIZE] = {0};
    char bbuf[MAX_KEY_SIZE] = {0};
    size_t ts_sz = sizeof(uint64_t);

    sprintf(abuf, "'%8ld%s'", htobe64(*(uint64_t *)a.data()), a.data() + ts_sz);
    sprintf(bbuf, "'%8ld%s'", htobe64(*(uint64_t *)b.data()), b.data() + ts_sz);

    // printf("A [%3ld]: ", a.size());
    // hexdump((const void *)a.data(), a.size());
    // printf("B [%3ld]: ", b.size());
    // hexdump((const void *)b.data(), b.size());

    // assert(a.size() == b.size());
    const size_t min_len = (a.size() < b.size()) ? a.size() : b.size();

    int ret = memcmp(a.data(), b.data(), min_len);
    if (ret == 0) {
        // bytes up to min_len are the same, check sizes
        if (a.size() < b.size()) {
            // printf("%s < %s: -1 (len)\n", abuf, bbuf);
            return -1;
        } else if (a.size() > b.size()) {
            // printf("%s > %s: +1 (len)\n", abuf, bbuf);
            return +1;
        }
        // fall through for equal keys
    } else {
        // bytes up to min_len differ
        if (ret < 0) {
            // printf("%s < %s: -1 (cmp)\n", abuf, bbuf);
            return -1;
        }
        if (ret > 0) {
            // printf("%s > %s: +1 (cmp)\n", abuf, bbuf);
            return +1;
        }
        // should not get here!
        assert(1 == 0);
    }

    // printf("%s == %s: 0 (cmp)\n", abuf, bbuf);
    return 0;

  }

 private:
//   static std::string kClassNameInternal() {
//     std::stringstream ss;
//     ss << TComparator::kClassName() << "MyComparator";
//     return ss.str();
//   }
};



char slice_buf[MAX_KEY_SIZE] = {0};
rocksdb::Slice make_slice(uint64_t value, const char *pv, size_t pv_len) {
    size_t n = 0;
    
    // convert the value to big endian so that it can be compared with memcmp()
    *(uint64_t *)slice_buf = htobe64(value);
    n += sizeof(uint64_t);
    if (pv_len > 0) {
        memcpy(slice_buf + n, ":", 1);
        n += 1;
        memcpy(slice_buf + n, pv, pv_len);
        n += pv_len;
    }

    // create a fixed len key size, with trailing \0; could be faster to compare
    // with memcmp(), but then we do not know the actual length of key and that
    // affects ordering of some key values. We could run strlen() on the key, but
    // that would degrade the performance, likely..
    // use comparator2() with these slices.
    // return MAX_KEY_SIZE;
    
    // create arbitrary long key
    return rocksdb::Slice(slice_buf, n);
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
    
    // set comparator with timestamp support
    // rocksdb::ConfigOptions config_options;
    // const rocksdb::Comparator* user_comparator = nullptr;
    // s = rocksdb::Comparator::CreateFromString(config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
    // assert(user_comparator != nullptr);
    // options.comparator = user_comparator;
    
    // For the same user key with different timestamps, larger (newer) timestamp comes first.
    // options.comparator = rocksdb::BytewiseComparatorWithU64Ts();
    
    // For the same user key with different timestamps, smaller (older) timestamp comes first.
    // This comparator /should/ better fit our needs, but it seems that it is causing some ordering issues
    // when getting the data with Get()..
    // options.comparator = rocksdb::BytewiseComparatorWithU64Ts2();
    MyComparator my_comparator = MyComparator();
    options.comparator = &my_comparator;

    // see examples/simple_example_my_ts.txt
    
    // open DB
    s = DB::Open(options, kDBPath, &db);
    if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
    assert(s.ok());

    if (argc > 1 && argv[1][0] == 'w') {
        std::cout << "-------- PUT ----------------------" << std::endl;
        // If sync is not used here then nothing is persisted in the SSTable on exit..
        rocksdb::WriteOptions write_options;
        write_options.sync = true;
        {
            rocksdb::Slice key = make_slice(10, "a1", 2);
            rocksdb::Slice val = rocksdb::Slice("value1-a1");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(10, "b2", 2);
            rocksdb::Slice val = rocksdb::Slice("value1-b2");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(10, "d1", 2);
            rocksdb::Slice val = rocksdb::Slice("value1-d1");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(10, "z9", 2);
            rocksdb::Slice val = rocksdb::Slice("value1-z9");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(11, "z9", 2);
            rocksdb::Slice val = rocksdb::Slice("value2-z9");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(11, "a1", 2);
            rocksdb::Slice val = rocksdb::Slice("value2-a1");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(11, "x3", 2);
            rocksdb::Slice val = rocksdb::Slice("value1-x3");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }
        {
            rocksdb::Slice key = make_slice(11, "d1", 2);
            rocksdb::Slice val = rocksdb::Slice("value2-d1");
            s = db->Put(write_options, key, val);
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: " << key.ToString(true) << " = " << val.ToString() << std::endl;
        }

    }

    // DB::Get() with a timestamp ts specified in ReadOptions will return the most recent key/value whose timestamp
    // is smaller than or equal to ts, in addition to the visibility based on sequence numbers.
    // Note that if the database enables timestamp, then caller of DB::Get() should always set ReadOptions.timestamp.
    std::cout << "-------- GET ----------------------" << std::endl;
    {
        rocksdb::Slice key = make_slice(11, "x3", 2);
        std::string value;
        ReadOptions read_opts;
        s = db->Get(read_opts, key, &value);
        if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
        assert(s.ok());
        std::cout << "get: " << key.ToString(true) << " = " << value << std::endl;
    }

    std::cout << "-------- GET ----------------------" << std::endl;
    {
        rocksdb::Slice key = make_slice(10, "x3", 2);
        std::string value;
        ReadOptions read_opts;
        s = db->Get(read_opts, key, &value);
        if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
        assert(s.IsNotFound());
        // std::cout << "get: " << key.ToString(true) << " = " << value << std::endl;
    }

    std::cout << "-------- GET ----------------------" << std::endl;
    {
        rocksdb::Slice key = make_slice(12, "x3", 2);
        std::string value;
        ReadOptions read_opts;
        s = db->Get(read_opts, key, &value);
        if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
        assert(s.IsNotFound());
        // std::cout << "get: " << key.ToString(true) << " = " << value << std::endl;
    }

    // The following example demonstrates how to print all (key, value) pairs in a database.
    std::cout << "-------- ITER ----------------------" << std::endl;
    {
        rocksdb::ReadOptions read_opts;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::cout << "it : " << it->key().ToString(true) << " = " << it->value().ToString() << std::endl;
            if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        }
        if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }

    std::cout << "-------- ITER ----------------------" << std::endl;
    {
        rocksdb::ReadOptions read_opts;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        rocksdb::Slice key = make_slice(10, "d1", 2);
        for (it->Seek(key); it->Valid(); it->Next()) {
            std::cout << "it : " << it->key().ToString(true) << " = " << it->value().ToString() << std::endl;
            if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        }
        if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }

#if 0
    std::cout << "-------- ITER ----------------------" << std::endl;
    {
        std::string ts;
        PutFixed64(&ts, 9999);
        rocksdb::Slice ts_slice = rocksdb::Slice(ts);
        rocksdb::ReadOptions read_opts;
        read_opts.timestamp = &ts_slice;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        std::cout << "ts " << GetFixed64(ts.c_str()) << std::endl;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::cout << "iter all: " << it->key().ToString() << " = " << it->value().ToString() << ", ts " << GetFixed64(it->timestamp().ToString().c_str()) << std::endl;
            if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        }
        if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }
    {
        std::cout << "-------- ITER ----------------------" << std::endl;
        std::string ts;
        PutFixed64(&ts, 9999);
        std::string ts0;
        PutFixed64(&ts0, 0);
        rocksdb::Slice ts_slice = rocksdb::Slice(ts);
        rocksdb::Slice ts_slice0 = rocksdb::Slice(ts0);
        rocksdb::ReadOptions read_opts;
        read_opts.timestamp = &ts_slice;
        read_opts.iter_start_ts = &ts_slice0;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        std::cout << "ts0 " << GetFixed64(ts0.c_str()) << std::endl;
        std::cout << "ts " << GetFixed64(ts.c_str()) << std::endl;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            // we get garbled key output!
            // std::cout << "iter all: " << it->key().ToString() << " = " << it->value().ToString() << ", ts " << GetFixed64(it->timestamp().ToString().c_str()) << std::endl;
            
            // https://groups.google.com/g/rocksdb/c/RcOmb5Mvi9w/m/8EZ3ve1FAAAJ
            // Given the internal key returned by it->key() when you set both read_opts.timestamp and read_opts.iter_start_ts,
            // you can get user key with timestamp via:
            rocksdb::Slice key = it->key();
            key.remove_suffix(8); // strip seqno and op-type, and you get user key + timestamp in key
            rocksdb::Slice ts = rocksdb::Slice(key.data() + key.size() - sizeof(uint64_t), sizeof(uint64_t));
            key.remove_suffix(sizeof(uint64_t));  // remaining part is user key

            std::cout << "iter all: " << key.ToString() << " = " << it->value().ToString() << ", ts " << GetFixed64(it->timestamp().ToString().c_str()) << std::endl;
            if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        }
        if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }


    {
        std::cout << "-------- ITER ----------------------" << std::endl;
        std::string ts;
        PutFixed64(&ts, 9999);
        rocksdb::Slice ts_slice = rocksdb::Slice(ts);
        rocksdb::ReadOptions read_opts;
        read_opts.timestamp = &ts_slice;
        rocksdb::Iterator* it = db->NewIterator(read_opts);
        std::cout << "key key1" << std::endl;
        std::cout << "ts " << GetFixed64(ts.c_str()) << std::endl;
        for (it->Seek("key1"); it->Valid() && it->key().ToString() < "key3"; it->Next()) {
            std::cout << "iter all: " << it->key().ToString() << " = " << it->value().ToString() << ", ts " << GetFixed64(it->timestamp().ToString().c_str()) << std::endl;
            if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        }
        if (! it->status().ok()) std::cout << "err: " << it->status().ToString() << std::endl;
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }
#endif


    s = db->Close();
    if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;

    delete db;

    return 0;
}
