//
// Created by dantas on 01/11/20.
//


#ifndef THESIS_MEMORY_BUFFER_DRIVER_H
#define THESIS_MEMORY_BUFFER_DRIVER_H

#include <boost/functional/hash.hpp>
#include "data_storage_driver.h"
#include <iostream>
#if defined BAZEL_BUILD || defined TF_BAZEL_BUILD
#include "third_party/tbb/include/concurrent_hash_map.h"
#else
#include "tbb/concurrent_hash_map.h"
#endif

template<typename K>
struct HashCompare {
    static size_t hash( const K& key )                  { return boost::hash_value(key); }
    static bool   equal( const K& key1, const K& key2 ) { return ( key1 == key2 ); }
};

typedef tbb::concurrent_hash_map<std::string, File*, HashCompare<std::string>> content_hash_map;

//Thread safe
class MemoryBufferDriver : public BaseDataStorageDriver {
    content_hash_map buffer;

public:
    Status read(File* f) override;
    Status write(File* f) override;
    Status remove(FileInfo* fi) override;
    ssize_t sizeof_content(FileInfo* fi) override;
    bool in_memory_type() override;
    void create_environment(std::vector<std::string>& dirs) override;
};

#endif //THESIS_MEMORY_BUFFER_DRIVER_H
