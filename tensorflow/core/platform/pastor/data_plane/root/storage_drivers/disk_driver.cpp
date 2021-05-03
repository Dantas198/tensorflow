//
// Created by dantas on 19/10/20.
//

#include <fcntl.h>
#include <bits/stdc++.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>
#include <unistd.h>
#include <cstring>
#include <filesystem>

#include "disk_driver.h"

//TODO warning: comparison of integer expressions of different signedness: 'size_t' {aka 'long unsigned int'} and 'int' [-Wsign-compare]
 //  65 |     if(usable_block_size > file_size)

std::string DiskDriver::get_full_path(const std::string& filename){
    std::string file;
    return file.append(storage_prefix).append("/").append(filename);
}

ssize_t DiskDriver::read_block(int fd, char* result, size_t n, uint64_t off) {
    int error = 0;
    char* dst = result;
    ssize_t bytes_read = 0;

    while (n > 0 && error == 0) {
        // Some platforms, notably macs, throw EINVAL if pread is asked to read
        // more than fits in a 32-bit integer.
        size_t requested_read_length;
        if (n > INT32_MAX) {
            requested_read_length = INT32_MAX;
        } else {
            requested_read_length = n;
        }

        ssize_t r = pread(fd, dst, requested_read_length, static_cast<off_t>(off));

        if (r > 0) {
            dst += r;
            n -= r;
            bytes_read += r;
            off += r;
        } else if (r == 0) {
            std::cerr << "Read less bytes than requested." << std::endl;
            error = 1;
        } else if (errno == EINTR || errno == EAGAIN) {
            // Retry
        } else {
            std::cerr << "IOError (off:" << off << ", size:" << requested_read_length
                      << "): " << std::strerror(errno) << std::endl;
            error = 1;
        }
    }

    return bytes_read;
}


ssize_t DiskDriver::read(File* f, int fd){
    ssize_t bytes_read;
    ssize_t total_bytes_read = 0;
    size_t file_size = f->get_requested_size();
    uint64_t offset = f->get_offset();
    uint64_t buffer_offset = 0;
    size_t usable_block_size = BaseDataStorageDriver::block_size;
    // Update block size to prevent errors
    if(usable_block_size > file_size) {
        usable_block_size = file_size;
    }
    //std::mutex* l = fi->get_mutex().get();

    // Read file
    while(total_bytes_read < file_size) {
        // Read block
        bytes_read = read_block(fd, f->get_content() + buffer_offset, usable_block_size, offset);
        total_bytes_read += bytes_read;
        offset += bytes_read;
        buffer_offset += bytes_read;
        if(total_bytes_read + usable_block_size > file_size) {
            usable_block_size = file_size - total_bytes_read;
        }
    }

    return total_bytes_read;
}

Status DiskDriver::read(File* f){
    int fd;
    // Open file
    std::string file_path = get_full_path(f->get_filename());
    fd = open(file_path.c_str(), O_RDONLY);

    if(fd < 0)
        return {NOT_FOUND};

    ssize_t bytes_read = read(f, fd);
    close(fd);

    // Validate result
    return {bytes_read};
}


ssize_t DiskDriver::write_block(int fd, char* buf, size_t n, uint64_t off) {
    int error = 0;
    ssize_t bytes_written = 0;

    while (n > 0 && error == 0) {
        // Some platforms, notably macs, throw EINVAL if pread is asked to read
        // more than fits in a 32-bit integer.
        size_t requested_write_length;
        if (n > INT32_MAX) {
            requested_write_length = INT32_MAX;
        } else {
            requested_write_length = n;
        }

        ssize_t r = pwrite(fd, buf, requested_write_length, static_cast<off_t>(off));

        if (r > 0) {
            buf += r;
            n -= r;
            bytes_written += r;
            off += r;
        } else if (r == 0) {
            std::cerr << "Wrote less bytes than requested." << std::endl;
            error = 1;
        } else if (errno == EINTR || errno == EAGAIN) {
            // Retry
        } else {
            std::cerr << "Write IOError (off:" << off << ", size:" << requested_write_length
                      << "): " << std::strerror(errno) << std::endl;
            error = 1;
        }
    }

    return bytes_written;
}


// File here has content
Status DiskDriver::write(File* f) {
    std::string file_path = get_full_path(f->get_filename());
    //std::cout << "writting " << file_path << "\n";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);

    if(fd < 0)
        return {NOT_FOUND};

    uint64_t offset = f->get_offset();
    uint64_t buffer_offset = 0;
    ssize_t total_bytes_written = 0;
    ssize_t bytes_written;
    size_t size = f->get_requested_size();
    size_t usable_block_size = BaseDataStorageDriver::block_size;

    if(usable_block_size > size) {
        usable_block_size = size;
    }

    while(total_bytes_written < size ) {
        // Write block
        bytes_written = write_block(fd, f->get_content() + buffer_offset, usable_block_size, static_cast<off_t>(offset));

        buffer_offset += bytes_written;
        offset += bytes_written;
        total_bytes_written += bytes_written;

        if(total_bytes_written + usable_block_size > size) {
            usable_block_size = size - total_bytes_written;
        }
    }
    close(fd);
    return {total_bytes_written};
}

Status DiskDriver::remove(FileInfo* fi){
   if (std::remove(get_full_path(fi->get_name()).c_str()) == 0) {
       return {SUCCESS};
   }
   return {NOT_FOUND};
}

ssize_t DiskDriver::sizeof_content(FileInfo* fi){
    return fi->_get_size();
}

bool DiskDriver::in_memory_type(){
    return false;
}

void DiskDriver::create_dir(const std::string& path){
    std::string cmd = "mkdir -p " + path;
    int status = std::system(cmd.c_str());
    if (status == -1)
        std::cerr << "Error : " << strerror(errno) << std::endl;
    else
        std::cout << "Created " << path << std::endl;
}

void DiskDriver::create_environment(std::vector<std::string>& dirs){
    //needed since dirs can be empty;
    create_dir(storage_prefix);
    for(auto& dir : dirs){
        create_dir(storage_prefix + dir);
    }
}