//
// Created by dantas on 04/11/20.
//

#ifndef THESIS_DATA_STORAGE_DRIVER_BUILDER_H
#define THESIS_DATA_STORAGE_DRIVER_BUILDER_H

#include "data_storage_driver.h"
#include "disk_driver.h"
#include "memory_buffer_driver.h"

enum DriverType : unsigned int {
    DISK = 1,
    BLOCKING_MEMORY_BUFFER = 2,
};

class DataStorageDriverBuilder {
    BaseDataStorageDriver* data_storage_driver;
    size_t max_storage_size = 0;
    float threshold = 1;
    bool auto_storage_management = false;

public:
    explicit DataStorageDriverBuilder (DriverType dt) {
        switch (dt) {
            case DISK:
                data_storage_driver = new DiskDriver();
                break;
            default:
                data_storage_driver = new MemoryBufferDriver();
                break;
        }
    };

    operator DataStorageDriver*() {return build();}

    DataStorageDriver* build();
    DataStorageDriverBuilder& with_allocation_capabilities(size_t max_storage_size, float threshold);
    DataStorageDriverBuilder& with_allocation_capabilities(size_t max_storage_size);
    DataStorageDriverBuilder& with_block_size(size_t block_size);
    DataStorageDriverBuilder& with_storage_prefix(const std::string& prefix);
    DataStorageDriverBuilder& with_auto_storage_management();
};

#endif //THESIS_DATA_STORAGE_DRIVER_BUILDER_H
