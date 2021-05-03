//
// Created by dantas on 28/10/20.
//

#include "mutex_file_info.h"
#include <memory>
#include <utility>

MutexFileInfo::MutexFileInfo(std::string f, size_t size) : FileInfo(std::move(f), size) {
    loaded = false;
    unstable = false;
    n_reads = 0;
    consumed_size = 0;
}

MutexFileInfo::MutexFileInfo(std::string f, size_t size, int target) : FileInfo(std::move(f), size, target) {
    loaded = false;
    unstable = false;
    n_reads = 0;
    consumed_size = 0;
}

void MutexFileInfo::loaded_to(int level) {
    std::unique_lock<std::mutex> ul(mutex);
    if(level == 0){
        loaded = true;
        loaded_condition.notify_one();
    }
    unstable = false;
    unstable_condition.notify_all();
    set_storage_level(level);
}

void MutexFileInfo::unloaded_to(int level) {
    std::unique_lock<std::mutex> ul(mutex);
    loaded = false;
    unstable = false;
    set_storage_level(level);
    ul.unlock();
    unstable_condition.notify_all();
}

//true leads to a placeholder
bool MutexFileInfo::init_prefetch(){
    std::unique_lock<std::mutex> ul(mutex);
    while(unstable)
        unstable_condition.wait(ul);
    bool res = ++n_reads > 1 || storage_level == 0;
    return res;
}


bool MutexFileInfo::wait_on_data_transfer() {
    bool waited = false;
    std::unique_lock<std::mutex> ul(mutex);
    while (!loaded){
        waited = true;
        loaded_condition.wait(ul);
    }
    return waited;
}


int MutexFileInfo::finish_read(){
    std::unique_lock<std::mutex> ul(mutex);
    int res = --n_reads;
    if(res == 0)
        unstable = true;
    return res;
}

bool MutexFileInfo::consume(size_t consumed){
    consumed_size += consumed;
    return consumed_size == FileInfo::_get_size();
}

void MutexFileInfo::reset_consumed_size(){
    consumed_size = 0;
}