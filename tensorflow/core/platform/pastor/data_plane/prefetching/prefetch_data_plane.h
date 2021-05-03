//
// Created by dantas on 26/10/20.
//

#ifndef THESIS_PREFETCH_DATA_PLANE_H
#define THESIS_PREFETCH_DATA_PLANE_H

#define CR "cycle_root"
#define PD "push_down"

#include <utility>
#include "../data_plane.h"
#include "../metadata/mutex_file_info.h"
#include "../root/hierarchical_data_plane.h"
#include "../../helpers/logger.h"
#include "../metadata/file.h"
#include "../metadata/prefetched_file.h"
#include <atomic>
#include <queue>

class PrefetchDataPlaneBuilder;

class PrefetchDataPlane : public HierarchicalDataPlane {
    //TODO separate into two pools? one for source and another for local storage
    ctpl::thread_pool* prefetch_thread_pool;

    std::vector<PrefetchedFile*> placement_queue;
    std::queue<PrefetchedFile*> postponed_queue;
    int allocated_samples_index;

    std::string placement_strategy;
    bool becomes_full;
    float eviction_percentage;

    PrefetchDataPlane(HierarchicalDataPlane* root_data_plane);
    Status place(PrefetchedFile* f);
    void start_prefetching();
    bool in_order(PrefetchedFile* f) const;
    Status update_queue_and_place();
    void evict(MutexFileInfo* mfi);
    void flush_postponed();
    void async_write(PrefetchedFile* f, int level);
    void async_remove(MutexFileInfo* mfi, int level, bool unload);
    void debug_write(const std::string& msg) override;
    ssize_t read(MutexFileInfo* mfi, char* result, uint64_t offset, size_t n);
public:
    friend class PrefetchDataPlaneBuilder;
    static PrefetchDataPlaneBuilder create(HierarchicalDataPlane* root_data_plane);
    static PrefetchDataPlaneBuilder* heap_create(HierarchicalDataPlane* root_data_plane);
    Status handle_placement(PrefetchedFile* f);
    ssize_t read(const std::string &filename, char* result, uint64_t offset, size_t n) override;
    ssize_t read_from_id(int file_id, char* result, uint64_t offset, size_t n) override;
    ssize_t read_from_id(int file_id, char* result) override;
    void init() override;
    std::vector<std::string> configs() override;
    void print() override;
    void start() override;
};

//@deprecated
class OperationSync {
    std::mutex mutex;
    std::condition_variable sync_condition;
    int num_operations;
    int current_operations;
public:
    OperationSync(int num_op);
    void incr();
    void wait();
};

#endif //THESIS_PREFETCH_DATA_PLANE_H
