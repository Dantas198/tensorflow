//
// Created by dantas on 06/11/20.
//

#include "data_storage_driver.h"
#include "data_storage_driver_builder.h"
#include <iostream>

DataStorageDriverBuilder DataStorageDriver::create(DriverType type){
    return DataStorageDriverBuilder{type};
}

bool BaseDataStorageDriver::alloc_type(){
    return false;
}

std::vector<std::string> BaseDataStorageDriver::configs(){
    std::vector<std::string> configs;
    configs.push_back("block_size: " + std::to_string(block_size) + "\n");
    configs.push_back("storage_prefix: " + storage_prefix + "\n");
    return configs;
}

std::string BaseDataStorageDriver::prefix(){
    return storage_prefix;
}

AllocableDataStorageDriver::AllocableDataStorageDriver(BaseDataStorageDriver* base_storage_driver){
    AllocableDataStorageDriver::base_storage_driver = base_storage_driver;
}

Status AllocableDataStorageDriver::allocate_storage(FileInfo* fi){
    ssize_t bytes_to_alloc = base_storage_driver->sizeof_content(fi);

    if (bytes_to_alloc + current_size >= max_storage_size)
        return {STORAGE_FULL};

    current_size += bytes_to_alloc;
    float current_occupation = current_size / max_storage_size;

    if (current_occupation >= max_storage_occupation_threshold)
        return {SUCCESS, OCCUPATION_THRESHOLD_REACHED, bytes_to_alloc};

    return {bytes_to_alloc};
}

Status AllocableDataStorageDriver::free_storage(FileInfo* fi){
    ssize_t bytes_to_alloc = base_storage_driver->sizeof_content(fi);
    current_size -= bytes_to_alloc;
    return {bytes_to_alloc};
}

size_t AllocableDataStorageDriver::resize(size_t new_size){
    max_storage_size = new_size;
    return new_size;
}

bool AllocableDataStorageDriver::becomesFull(FileInfo* fi){
    return base_storage_driver->sizeof_content(fi) + current_size >= max_storage_size;
}

bool AllocableDataStorageDriver::occupation_threshold_reached() const{
    return current_size >= max_storage_size * max_storage_occupation_threshold;
}

size_t AllocableDataStorageDriver::current_storage_size() const{
    return current_size;
}

size_t AllocableDataStorageDriver::get_max_storage_size() const{
    return max_storage_size;
}

Status AllocableDataStorageDriver::read(File* f){
    return base_storage_driver->read(f);
}

Status AllocableDataStorageDriver::write(File* f){
    if (auto_storage_management){
        Status s = allocate_storage(f->get_info());
        if (s.state == STORAGE_FULL)
            return s;
    }
    return base_storage_driver->write(f);
}

Status AllocableDataStorageDriver::remove(FileInfo* fi){
    Status res = base_storage_driver->remove(fi);
    if(auto_storage_management)
        free_storage(fi);
    return res;
}


ssize_t AllocableDataStorageDriver::sizeof_content(FileInfo* fi){
    return base_storage_driver->sizeof_content(fi);
}

bool AllocableDataStorageDriver::in_memory_type(){
    return base_storage_driver->in_memory_type();
}

void AllocableDataStorageDriver::create_environment(std::vector<std::string>& dirs){
    base_storage_driver->create_environment(dirs);
}

std::vector<std::string> AllocableDataStorageDriver::configs(){
    auto configs = base_storage_driver->configs();
    configs.push_back("max_storage_size: " + std::to_string(max_storage_size) + "\n");
    configs.push_back("max_storage_occupation_threshold: " + std::to_string(max_storage_occupation_threshold) + "\n");
    std::string b = auto_storage_management ? "true" : "false";
    configs.push_back("auto_storage_management: " + b + "\n");
    return configs;
}

std::string AllocableDataStorageDriver::prefix() {
    return base_storage_driver->prefix();
}