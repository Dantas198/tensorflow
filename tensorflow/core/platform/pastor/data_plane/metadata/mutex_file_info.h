//
// Created by dantas on 28/10/20.
//

#ifndef THESIS_MUTEX_FILE_INFO_H
#define THESIS_MUTEX_FILE_INFO_H

#include "file_info.h"

#include <mutex>
#include <condition_variable>
#include <atomic>

/*Notes:
 * If the file associated to this info is being prefetch, then no other thread is modifying this object
 * Object is readable after the prefetching is done and further modifications like finish_read are thread safe.
*/

class MutexFileInfo : public FileInfo{
    std::mutex mutex;
    std::condition_variable loaded_condition;
    std::condition_variable unstable_condition;
    // content is already cached on first level?
    bool loaded;
    // number of expected reads, also used to prevent multiple prefetching of the same item
    int n_reads;
    //possibly being evicted
    bool unstable;
    //for tfrecords. Increased by clients and reseted by prefetch loop
    std::atomic<size_t> consumed_size{0};

public:
    void unloaded_to(int level);
    void loaded_to(int level);
    bool wait_on_data_transfer();
    bool init_prefetch();
    int finish_read();
    //if true can evict
    bool consume(size_t consumed);
    void reset_consumed_size();
    MutexFileInfo(std::string f, size_t size);
    MutexFileInfo(std::string f, size_t size, int target);

};

#endif //THESIS_MUTEX_FILE_INFO_H
