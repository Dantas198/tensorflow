//
// Created by dantas on 19/10/20.
//

#ifndef DATA_STORAGE_DRIVER_H
#define DATA_STORAGE_DRIVER_H

#include <iostream>
#include <unistd.h>
#include <string>
#include <mutex>
#include <functional>
#include <vector>
#include <condition_variable>
#include "../../metadata/file_info.h"
#include "../../metadata/file.h"
#include "utils/status.h"

class DataStorageDriverBuilder;
enum DriverType : unsigned int;

class DataStorageDriver {
public:
    virtual Status read(File* f) = 0;
    virtual Status write(File* f) = 0;
    virtual Status remove(FileInfo* fi) = 0;
    virtual bool alloc_type() = 0;
    virtual bool in_memory_type() = 0;
    virtual void create_environment(std::vector<std::string>& dirs) = 0;
    virtual std::vector<std::string> configs() = 0;
    virtual std::string prefix() = 0;
    static DataStorageDriverBuilder create(DriverType type);
};

class BaseDataStorageDriver : public DataStorageDriver {
protected:
    size_t block_size;
    std::string storage_prefix;

public:
    friend class DataStorageDriverBuilder;
    virtual ssize_t sizeof_content(FileInfo* fi) = 0;
    bool alloc_type() override;
    std::vector<std::string> configs() override;
    std::string prefix() override;
};

class AllocableDataStorageDriver : public DataStorageDriver {
protected:
    BaseDataStorageDriver* base_storage_driver;
    size_t current_size = 0;
    size_t max_storage_size;
    float max_storage_occupation_threshold = 0.8;
    bool auto_storage_management = false;
public:
    friend class DataStorageDriverBuilder;
    AllocableDataStorageDriver(BaseDataStorageDriver* base_storage_driver);
    Status allocate_storage(FileInfo* fi);
    Status free_storage(FileInfo* fi);
    size_t resize(size_t new_size);
    [[nodiscard]] size_t current_storage_size() const;
    [[nodiscard]] size_t get_max_storage_size() const;
    Status read(File* f) override;
    Status write(File* f) override;
    Status remove(FileInfo* fi) override;
    bool becomesFull(FileInfo* fi);
    bool occupation_threshold_reached() const;
    ssize_t sizeof_content(FileInfo* fi);
    bool in_memory_type() override;
    bool alloc_type() override {return true;};
    std::vector<std::string> configs() override;
    void create_environment(std::vector<std::string>& dirs) override;
    std::string prefix() override;
};

//TODO batch write_operation driver. Stores write and remove in a queue to execute them in a batch like manner
#endif //DATA_STORAGE_DRIVER_H