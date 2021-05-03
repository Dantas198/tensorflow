//
// Created by dantas on 19/10/20.
//

#include <iostream>
#include <cstring>
#include <cmath>
#include <cstdlib>
#include <chrono>

#include "hierarchical_data_plane.h"
#include "hierarchical_data_plane_builder.h"
#include "../metadata/placed_file_info.h"

HierarchicalDataPlaneBuilder* HierarchicalDataPlane::create(int instance_id_, int world_size_, int number_of_workers, int hierarchy_size){
    return new HierarchicalDataPlaneBuilder{instance_id_, world_size_, number_of_workers, hierarchy_size};
};

HierarchicalDataPlane::HierarchicalDataPlane(int id, int ws, int nw, int hierarchy_size) {
    neighbours_index = 0;
    placed_samples = 0;
    instance_id = id;
    rank = -1;
    world_size = ws;
    worker_id = -1;
    num_workers = nw;
    storage_sync_timeout = 15;
    reached_stability = false;
    housekeeper_thread_pool = new ctpl::thread_pool(1);
    synchronization_thread_pool = new ctpl::thread_pool(1);
    storage_hierarchy_size = hierarchy_size;
    total_used_threads = 0;
    shared_thread_pool = false;
    debug_logger = new Logger();
    rate_limiter = nullptr;
    profiler = nullptr;
}

HierarchicalDataPlane::HierarchicalDataPlane(HierarchicalDataPlane* hdp){
    neighbours_index = 0;
    placed_samples = 0;
    storage_sync_timeout = hdp->storage_sync_timeout;
    instance_id = hdp->instance_id;
    matrix_index = hdp->matrix_index;
    rank = hdp->rank;
    world_size = hdp->world_size;
    worker_id = hdp->worker_id;
    num_workers = hdp->num_workers;
    reached_stability = false;
    housekeeper_thread_pool = hdp->housekeeper_thread_pool;
    synchronization_thread_pool = hdp->synchronization_thread_pool;
    storage_hierarchical_matrix = hdp->storage_hierarchical_matrix;
    storage_hierarchy_size = hdp->storage_hierarchy_size;
    metadata_container = hdp->metadata_container;
    storage_hierarchy_thread_pools = hdp->storage_hierarchy_thread_pools;
    total_used_threads = hdp->total_used_threads;
    shared_thread_pool = hdp->shared_thread_pool;
    debug_logger = hdp->debug_logger;
    rate_limiter = hdp->rate_limiter;
    profiler = hdp->profiler;
    type = hdp->type;
}

void HierarchicalDataPlane::start_sync_loop(int offset){
    if (world_size == 1 && num_workers == 1)
        return;
    while(placed_samples != metadata_container->get_iter_size()) {
        sleep(storage_sync_timeout);
        synchronize_storages(offset);
    }
}

ctpl::thread_pool* HierarchicalDataPlane::t_pool(int storage_index){
    if (!shared_thread_pool)
        return storage_hierarchy_thread_pools[storage_index];
    return storage_hierarchy_thread_pools[0];
}

int HierarchicalDataPlane::get_storage_hierarchical_matrix_index(int rank_, int worker_id_){
    return worker_id_ + rank_ * num_workers;
}

//ignores worker_id for now
//TODO stability should be reached when all storage level are 99.99% full and not related to index.
//THe if is kind of strange, but for now does no harm
int HierarchicalDataPlane::get_list_index(int rank_, int worker_id_, int local_index){
    if(local_index == metadata_container->get_file_count() - 1)
        reached_stability = true;
    return metadata_container->get_id(rank_, local_index);
}

bool HierarchicalDataPlane::is_allocable(int rank_, int worker_id_, int level){
    int line = get_storage_hierarchical_matrix_index(rank_, worker_id_);
    return storage_hierarchical_matrix[line][level]->alloc_type();
}

bool HierarchicalDataPlane::is_allocable(int level){
    return storage_hierarchical_matrix[matrix_index][level]->alloc_type();
}

AllocableDataStorageDriver* HierarchicalDataPlane::get_alloc_driver(int rank_, int worker_id_, int level){
    int line = get_storage_hierarchical_matrix_index(rank_, worker_id_);
    return (AllocableDataStorageDriver*)storage_hierarchical_matrix[line][level];
}

AllocableDataStorageDriver* HierarchicalDataPlane::get_alloc_driver(int level){
    return (AllocableDataStorageDriver*)storage_hierarchical_matrix[matrix_index][level];
}

DataStorageDriver* HierarchicalDataPlane::get_driver(int rank_, int worker_id_, int level){
    int line = get_storage_hierarchical_matrix_index(rank_, worker_id_);
    return storage_hierarchical_matrix[line][level];
}

DataStorageDriver* HierarchicalDataPlane::get_driver(int level){
    return storage_hierarchical_matrix[matrix_index][level];
}

//TODO does not count level 0 if it's in-memory type with prefetch
int HierarchicalDataPlane::free_level(int rank_, int worker_id_, FileInfo* fi, int offset){
    for(int i = offset; i < storage_hierarchy_size - 1; i++)
        if(is_allocable(rank_, worker_id_, i)){
            auto* driver = get_alloc_driver(rank_, worker_id_, i);
            if((type != "prefetch_enabled" || !driver->in_memory_type()) && !driver->becomesFull(fi))
                return i;
        }
    return -1;
}

int HierarchicalDataPlane::free_level(FileInfo* fi, int offset){
    for(int i = offset; i < storage_hierarchy_size - 1; i++) {
        if (is_allocable(i) && !get_alloc_driver(i)->becomesFull(fi)) {
            return i;
        }
    }
    return -1;
}

//TODO doesn't support in-memory type due to eviction policy
//Its thread since the housekeeper does not interact with the neighbours storages
void HierarchicalDataPlane::synchronize_storages(int offset){
    if(!reached_stability){
        for(int i = 0; i < placed_samples; i++){
            for(int r = 0; r < world_size; r++){
                for(int w = 0; w < num_workers; w++){
                    //dont update your own storage
                    if(get_storage_hierarchical_matrix_index(r, w) != matrix_index) {
                        int index = get_list_index(r, w, neighbours_index);
                        auto *fi = metadata_container->get_metadata_from_ordered_id(r, index);
                        if (fi->get_storage_level() == storage_hierarchy_size - 1) {
                            int level = free_level(r, w, fi, offset);
                            if (level > 0) {
                                get_alloc_driver(r, w, level)->allocate_storage(fi);
                                fi->set_storage_level(level);
                            }
                        }
                    }
                }
            }
            neighbours_index++;
        }
        debug_write("During synchronization neighbours_index was updated to:" + std::to_string(neighbours_index));
    }
}

void HierarchicalDataPlane::handle_placement(File* f){

    int storage_level = f->get_info()->get_storage_level();
    // f came from source
    if (storage_level == storage_hierarchy_size - 1){
        // search for a storage level with empty space except the last level
        int level = free_level(rank, worker_id, f->get_info(), 0);
        if (level >= 0){
            debug_write(f->get_info()->get_name() + " came from source. Found storage space on level: " +
                        std::to_string(level));
            get_alloc_driver(rank, worker_id, level)->allocate_storage(f->get_info());
            t_pool(level)->push([this, level, f](int id){
                //get info so that drivers can safely delete f
                auto* fi = f->get_info();
                if(f->get_requested_size() < fi->_get_size()){
                    debug_write(f->get_info()->get_name() + " is a partial request. Reshaping to full request for placement");
                    f->reshape_to_full_request();
                    read(f, fi->get_storage_level());
                }
                Status status = write(f, level);
                placed_samples++;
                if(!get_alloc_driver(rank, worker_id, level)->in_memory_type())
                    delete f;

                if (status.state == SUCCESS) {
                    debug_write(fi->get_name() + " placed on level: " + std::to_string(level));
                    fi->set_storage_level(level);
                }
            });
        }
        else{
            debug_write(f->get_info()->get_name() + " came from source. No storage space in any level for " +
                        f->get_info()->get_name());
            delete f;
        }
    }else
        delete f;
}

std::string HierarchicalDataPlane::decode_filename(std::string full_path){
    return full_path.erase(0, get_driver(storage_hierarchy_size -1)->prefix().length() + 1);
}

ssize_t HierarchicalDataPlane::read(FileInfo* fi, char* result, uint64_t offset, size_t n){
    auto* read_submission = new ReadSubmission();
    read_submission->r_start = std::chrono::high_resolution_clock::now();

    if(offset >= fi->_get_size()) {
        debug_write("Tried to read from offset: " + std::to_string(offset) + " which goes beond file size: " +
                    std::to_string(fi->_get_size()) + " name: " + fi->get_name());
        return 0;
    }

    auto* f = new File(fi, offset, n);
    auto* pfi = (PlacedFileInfo*)fi;
    debug_write("reading file " + pfi->get_name() + " from level " +
                std::to_string(pfi->get_storage_level()));

    Status read_status = read(f, pfi->get_storage_level());
    if(read_status.state == SUCCESS) {
        memcpy(result, f->get_content(), f->get_requested_size());
        //result is in buffer, writter thread will manage the created file deletion
        if(pfi->get_storage_level() == storage_hierarchy_size - 1 && pfi->begin_placement()) {
            housekeeper_thread_pool->push([this, f](int id) {
                handle_placement(f);
            });
        } else {
            delete f;
        }
    }
    read_submission->r_end = std::chrono::high_resolution_clock::now();
    read_submission->n = n;
    profiler->submit_client_read(read_submission);
    return read_status.bytes_size;
}

ssize_t HierarchicalDataPlane::read(const std::string& filename, char* result, uint64_t offset, size_t n) {
    auto* fi = metadata_container->get_metadata(filename);
    return read(fi, result, offset, n);
}

ssize_t HierarchicalDataPlane::read_from_id(int file_id, char* result, uint64_t offset, size_t n) {
    auto* fi = metadata_container->get_metadata(file_id);
    return read(fi, result, offset, n);
}

ssize_t HierarchicalDataPlane::read_from_id(int file_id, char* result){
    auto* fi = metadata_container->get_metadata(file_id);
    return read(fi, result, 0, fi->_get_size());
}

int HierarchicalDataPlane::get_target_class_from_id(int id) {
    return metadata_container->get_metadata(id)->get_target();
}

size_t HierarchicalDataPlane::get_file_size(const std::string &filename){
    return metadata_container->get_metadata(filename)->_get_size();
}

size_t HierarchicalDataPlane::get_file_size_from_id(int id){
    return metadata_container->get_metadata(id)->_get_size();
}

int HierarchicalDataPlane::get_target_class(const std::string &filename){
    return metadata_container->get_metadata(filename)->get_target();
}

//TODO encapsulate inside other member function to enable frequency

Status HierarchicalDataPlane::write(File* f, int level){
    auto st = std::chrono::high_resolution_clock::now();
    auto status = storage_hierarchical_matrix[matrix_index][level]->write(f);
    auto et = std::chrono::high_resolution_clock::now();
    profiler->submit_write_on_storage(st, et, level, f->get_requested_size());
    return status;
}

//TODO if not found go to lower level
Status HierarchicalDataPlane::read(File* f, int level){
    auto st = std::chrono::high_resolution_clock::now();
    Status s = NOT_FOUND;
    int found_level = level;
    while(s.state == NOT_FOUND && found_level < storage_hierarchy_size){
        s = storage_hierarchical_matrix[matrix_index][level]->read(f);
        if(s.state == NOT_FOUND)
            found_level += 1;
    }
    if(s.state == NOT_FOUND){
        std::cout << "File could not be found anywhere exiting...\n";
        exit(1);
    }

    if (found_level != level) {
        s = MISS;
        f->get_info()->set_storage_level(found_level);
        debug_write("Cache miss. Target level: " + std::to_string(level) + " Actual level: " + std::to_string(found_level));
    }
    auto et = std::chrono::high_resolution_clock::now();
    profiler->submit_read_on_storage(st, et, level, f->get_requested_size());
    return s;
}

Status HierarchicalDataPlane::remove(FileInfo* fi, int level){
    return storage_hierarchical_matrix[matrix_index][level]->remove(fi);
}

bool HierarchicalDataPlane::enforce_rate_limit(){
    if(rate_limiter != nullptr)
        return rate_limiter->rate_limit();
    return false;
}

void HierarchicalDataPlane::enforce_rate_brake(int new_brake_id){
    if(rate_limiter != nullptr) {
        rate_limiter->pull_brake(new_brake_id);
    }
}

void HierarchicalDataPlane::enforce_rate_continuation(int new_brake_release_id){
    if(rate_limiter != nullptr)
        rate_limiter->release_brake(new_brake_release_id);
}

void HierarchicalDataPlane::apply_job_termination() {
    if(rate_limiter != nullptr)
        rate_limiter->apply_job_termination();
}

void HierarchicalDataPlane::await_termination(){
    if(rate_limiter != nullptr)
        rate_limiter->await_termination();
}

void HierarchicalDataPlane::set_total_jobs(int iter_size){
    if(rate_limiter != nullptr)
        rate_limiter->set_total_jobs(iter_size);
}


void HierarchicalDataPlane::init(){
    debug_write("Setting metadata container instance id...");
    if(world_size > 1 || num_workers > 1) {
        metadata_container->make_partition(rank, world_size, worker_id, num_workers);
        debug_write("Metadatada container has partitioned file count set to: " + std::to_string(metadata_container->get_file_count()));

        for(int w = 0; w < num_workers; w++){
            for(int r = 0; r < world_size; r++){
                for(int i = 0; i < storage_hierarchy_size - 1; i++){
                    if (is_allocable(r, w, i)) {
                        auto *driver = get_alloc_driver(r, w, i);
                        size_t total_size = driver->get_max_storage_size();
                        size_t new_size = std::floor(total_size / (num_workers * world_size));
                        driver->resize(new_size);
                    }
                }
            }
        }
        debug_write("Allocable drivers max_storage_size have been resized according to the given partition");
    }
    if(instance_id == 0){
        //ignore source level
        auto dirs = metadata_container->get_dirs_for_environment();
        for(int i = 0; i < storage_hierarchy_size - 1; i++){
            (storage_hierarchical_matrix[matrix_index][i])->create_environment(dirs);
        }
        debug_write("Instance 0 created storage environment");
    }
}

void HierarchicalDataPlane::start(){
    if(rank == -1) {
        debug_write("Adjusting rank...");
        rank = 0;
    }
    if(worker_id == -1){
        debug_write("Adjusting worker_id");
        worker_id = 0;
    }

    synchronization_thread_pool->push([this](int id){start_sync_loop(0);});
}

//TODO use decent method -> stats.h
std::vector<std::string> HierarchicalDataPlane::configs() {
    std::vector<std::string> res;
    res.push_back("- HierarchicalModule\n");
    res.push_back("\t- hierarchy " + std::to_string(storage_hierarchy_size) + "\n");
    res.push_back("\t\thierarchy_size: " + std::to_string(storage_hierarchy_size) + "\n");
    std::string b = shared_thread_pool ? "true" : "false";
    res.push_back("\t\tshared_pool: " + b + "\n");
    res.push_back("\t\ttotal_used_threads: " + std::to_string(total_used_threads) + "\n");

    for(int i = 0; i < storage_hierarchy_size; i++){
        res.push_back("\t\t- storage_driver_" + std::to_string(i) + ":\n");
        if(!shared_thread_pool){
            res.push_back("\t\t\tthread_pool_size: " + std::to_string(storage_hierarchy_thread_pools[i]->size()) + "\n");
        }

        auto* driver = storage_hierarchical_matrix[matrix_index][i];
        for(const auto& str : driver->configs())
            res.push_back("\t\t\t" + str);
    }

    //for(const auto& str : metadata_container->configs())
      //  res.push_back("\t" + str);

    return res;
}

void HierarchicalDataPlane::print(){
    if(instance_id == 0)
        for (const auto &str : configs())
            std::cout << str;
}

int HierarchicalDataPlane::get_file_count(){
    return metadata_container->get_file_count();
}

CollectedStats* HierarchicalDataPlane::collect_statistics(){
    return profiler->collect();
}

void HierarchicalDataPlane::debug_write(const std::string& msg){
    debug_logger->_write("[HierarchicalDataPlane] " + msg);
}

void HierarchicalDataPlane::set_distributed_params(int rank_, int worker_id_){
    HierarchicalDataPlane::rank = rank_;
    HierarchicalDataPlane::worker_id = worker_id_;
    HierarchicalDataPlane::matrix_index = get_storage_hierarchical_matrix_index(rank, worker_id);
}

/*//TODO only possible if char** is passed as argument
 * if(read_status.state == SUCCESS) {
        int level = free_level(f->get_info());
        if (level >= 0){
            memcpy(result, f->get_content(), f->get_requested_size());
            //result is in buffer, writter thread will manage the created file deletion
            housekeeper_thread_pool->push([this, f](int id) { handle_placement(f); });
        }else
            result = f->get_content();
    }
 */