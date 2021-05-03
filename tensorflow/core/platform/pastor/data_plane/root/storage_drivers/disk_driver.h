//
// Created by dantas on 19/10/20.
//

#ifndef THESIS_DISK_DRIVER_H
#define THESIS_DISK_DRIVER_H

#include <atomic>
#include "../../metadata/file.h"
#include "data_storage_driver.h"

class DiskDriver: public BaseDataStorageDriver{
private:
    ssize_t read(File* f, int fd);
    ssize_t read_block(int fd, char* result, size_t n, uint64_t off);
    ssize_t write_block(int fd, char* buf, size_t n, uint64_t off);
    std::string get_full_path(const std::string& filename);
    static void create_dir(const std::string& path);
public:
    Status read(File* f) override;
    Status write(File* f) override;
    Status remove(FileInfo* fi) override;
    ssize_t sizeof_content(FileInfo* fi) override;
    bool in_memory_type() override;
    void create_environment(std::vector<std::string>& dirs) override;
};

#endif //THESIS_DISK_DRIVER_H
