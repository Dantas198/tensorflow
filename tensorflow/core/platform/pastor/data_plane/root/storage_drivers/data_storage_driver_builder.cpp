//
// Created by dantas on 04/11/20.
//

#include "data_storage_driver_builder.h"


DataStorageDriver* DataStorageDriverBuilder::build(){
    if (max_storage_size > 0){
        auto* adsd = new AllocableDataStorageDriver(data_storage_driver);
        adsd->max_storage_size = max_storage_size;
        adsd->max_storage_occupation_threshold = threshold;
        adsd->auto_storage_management = auto_storage_management;
        return adsd;
    }
    else
        return data_storage_driver;
}

DataStorageDriverBuilder& DataStorageDriverBuilder::with_block_size(size_t bs){
    data_storage_driver->block_size = bs;
    return *this;
}

DataStorageDriverBuilder& DataStorageDriverBuilder::with_storage_prefix(const std::string& prefix){
    data_storage_driver->storage_prefix = prefix;
    return *this;
}


DataStorageDriverBuilder& DataStorageDriverBuilder::with_allocation_capabilities(size_t mss, float t){
    max_storage_size = mss;
    threshold = t;
    return *this;
}

DataStorageDriverBuilder& DataStorageDriverBuilder::with_allocation_capabilities(size_t mss){
    max_storage_size = mss;
    return *this;
}

DataStorageDriverBuilder& DataStorageDriverBuilder::with_auto_storage_management(){
    auto_storage_management = true;
    return *this;
}