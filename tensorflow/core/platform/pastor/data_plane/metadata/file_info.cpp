//
// Created by dantas on 19/10/20.
//

#include <utility>
#include <cstring>
#include <unistd.h>
#include "file_info.h"

FileInfo::FileInfo(std::string f, size_t file_size) {
    filename = std::move(f);
    size = file_size;
}

FileInfo::FileInfo(std::string f, size_t s, int t){
    filename = std::move(f);
    size = s;
    target = t;
}

const std::string &FileInfo::get_name() const {
    return filename;
}

size_t FileInfo::_get_size() const {
    return size;
}

int FileInfo::get_storage_level() {
    return storage_level;
}

void FileInfo::set_storage_level(int level) {
    storage_level = level;
}

int FileInfo::get_target(){
    return target;
}
void FileInfo::set_target(int t){
    target = t;
}
