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
std::string kDBPath = "/tmp/rocksdb_simple_example_ts2";

inline void PutFixed64(std::string* dst, uint64_t value) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)), sizeof(value));
}

inline uint64_t GetFixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
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
    options.comparator = rocksdb::BytewiseComparatorWithU64Ts2();

    // see examples/simple_example_ts.txt
    
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
            std::string ts;
            PutFixed64(&ts, 1111);
            s = db->Put(write_options, "key1", ts, "value11");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key1 = value11, ts 1111" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 1144);
            s = db->Put(write_options, "key1", ts, "value1144");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key1 = value1144, ts 1144" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 1122);
            s = db->Put(write_options, "key1", ts, "value1122");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key1 = value1122, ts 1122" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 2222);
            s = db->Put(write_options, "key2", ts, "value22");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key2 = value22, ts 2222" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 3333);
            s = db->Put(write_options, "key3", ts, "value33");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key3 = value33, ts 3333" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 5555);
            s = db->Put(write_options, "key5", ts, "value55");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key5 = value55, ts 5555" << std::endl;
        }
        {
            std::string ts;
            PutFixed64(&ts, 7777);
            s = db->Put(write_options, "key7", ts, "value77");
            if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
            assert(s.ok());
            std::cout << "new: key7 = value77, ts 7777" << std::endl;
        }
    }

    // DB::Get() with a timestamp ts specified in ReadOptions will return the most recent key/value whose timestamp
    // is smaller than or equal to ts, in addition to the visibility based on sequence numbers.
    // Note that if the database enables timestamp, then caller of DB::Get() should always set ReadOptions.timestamp.
    std::cout << "-------- GET ----------------------" << std::endl;
    {
        std::string key("key1");
        std::string value;
        std::string ts, ts2;
        PutFixed64(&ts, 1123);
        rocksdb::Slice ts_slice = rocksdb::Slice(ts);
        ReadOptions read_opts;
        read_opts.timestamp = &ts_slice;
        std::cout << "ts " << GetFixed64(ts.c_str()) << std::endl;
        s = db->Get(read_opts, key, &value, &ts2);
        if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
        assert(s.ok());
        std::cout << "single: " << key << " = " << value << ", ts " << GetFixed64(ts2.c_str()) << std::endl;
    }
    std::cout << "-------- GET ----------------------" << std::endl;
    {
        std::string key("key1");
        std::string value;
        std::string ts, ts2;
        PutFixed64(&ts, 1121);
        rocksdb::Slice ts_slice = rocksdb::Slice(ts);
        ReadOptions read_opts;
        read_opts.timestamp = &ts_slice;
        std::cout << "ts " << GetFixed64(ts.c_str()) << std::endl;
        s = db->Get(read_opts, key, &value, &ts2);
        if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;
        assert(s.ok());
        std::cout << "single: " << key << " = " << value << ", ts " << GetFixed64(ts2.c_str()) << std::endl;
    }

    // The following example demonstrates how to print all (key, value) pairs in a database.
    // DB::NewIterator() with a timestamp ts specified in ReadOptions will return an iterator.
    // Accessing keys via the iterator will return data whose timestamp is smaller than or equal to ts,
    // in addition to the visibility based on sequence numbers.
    // The application should always call DB::NewIterator() with read_options.timestamp set.
    std::cout << "-------- ITER ----------------------" << std::endl;
    {
        std::string ts;
        PutFixed64(&ts, 0);
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



    s = db->Close();
    if (! s.ok()) std::cout << "err: " << s.ToString() << std::endl;

    delete db;

    return 0;
}
