//
// Created by dantas on 26/10/20.
//

#include <thread>
#include <cstring>
#include <iostream>
#include "prefetch_data_plane.h"
#include "prefetch_data_plane_builder.h"
#include <unordered_map>
#include <cmath>

PrefetchDataPlane::PrefetchDataPlane(HierarchicalDataPlane* rdp) : HierarchicalDataPlane(rdp){
    eviction_percentage = 0.1;
    allocated_samples_index = 0;
}

void PrefetchDataPlane::flush_postponed(){
    if(!postponed_queue.empty()){
        debug_write("Handling postponed requests");
        long res = postponed_queue.size();
        while(res > 0) {
            auto* f = postponed_queue.front();
            debug_write("On handling postponed requests retrieved request with id: " +std::to_string(f->get_request_id()));
            Status s = place(f);
            if(s.state == DELAYED)
                break;
            else{
                res--;
                postponed_queue.pop();
            }
        }
        if(res == 0) {
            debug_write("All postponed requests are treated...resuming delayed requests treatment");
            Status s = update_queue_and_place();
            if(s.state == SUCCESS) {
                debug_write("After treating postponed requests update queue and place did not return DELAYED. Enforcing rate continuation");
                enforce_rate_continuation(allocated_samples_index);
            }
        }
    }
}
//Not async. TODO
void PrefetchDataPlane::async_remove(MutexFileInfo* mfi, int level, bool unload){
    remove(mfi, level);
    if(unload){
        mfi->unloaded_to(storage_hierarchy_size - 1);
    }
    debug_write("Removed from level: " + std::to_string(level) + " file: " + mfi->get_name());
    housekeeper_thread_pool->push([this, mfi, level](int id){
        get_alloc_driver(level)->free_storage(mfi);
        debug_write("Current storage size: " + std::to_string(get_alloc_driver(level)->current_storage_size()));
        flush_postponed();
    });
}

//use to write to level 0
void PrefetchDataPlane::async_write(PrefetchedFile* f, int level){
    t_pool(level)->push([this, f, level, name = f->get_filename()](int id){
        write(f, level);
        MutexFileInfo* mfi = f->get_info();
        mfi->loaded_to(level);
        apply_job_termination();
        HierarchicalDataPlane::placed_samples++;
        //debug_write("Data written to level: " + std::to_string(level) + " file: " + mfi->get_name() + " id: " + std::to_string(f->get_request_id()));
        if(!get_alloc_driver(level)->in_memory_type())
            delete f;
    });
}

void PrefetchDataPlane::evict(MutexFileInfo* mfi){
    int fl = HierarchicalDataPlane::free_level(mfi, 1);
    if(mfi->finish_read() == 0){
        if(fl > 0){
            debug_write("Evicting file: " + mfi->get_name() + " to storage level: " + std::to_string(fl));
            get_alloc_driver(fl)->allocate_storage(mfi);
            t_pool(0)->push([this, mfi, fl](int id){
                auto* f = new PrefetchedFile(mfi, -1);
                HierarchicalDataPlane::read(f, 0);
                async_remove(mfi, mfi->get_storage_level(), false);
                async_write(f, fl);
            });
        }
        else{
            debug_write("No storage space in any level. Fully discarding file: " + mfi->get_name());
            t_pool(0)->push([this, mfi](int id){
                async_remove(mfi, mfi->get_storage_level(), true);
            });
        }
    }
    else{
        debug_write("Cannot evict. File will be read again: " + mfi->get_name());
    }
}

Status PrefetchDataPlane::place(PrefetchedFile* f){
    if(!f->is_placeholder()) {
        int to_level = 0;
        if(placement_strategy == PD)
            to_level = HierarchicalDataPlane::free_level(f->get_info(), 0);
        if (is_allocable(to_level) && to_level >= 0) {
            auto *driver = get_alloc_driver(to_level);
            //edge case
            if (driver->becomesFull(f->get_info()) && placement_strategy == CR) {
                debug_write("Intended driver becomes full with file: " + std::to_string(f->get_request_id())
                + " current size: " + std::to_string(get_alloc_driver(to_level)->current_storage_size()));
                //std::cout << "storage size: " << driver->current_storage_size() << std::endl;
                return {DELAYED};
            }
            else {
                debug_write("Proceeding with storage allocation for file: " + std::to_string(f->get_request_id()));
                driver->allocate_storage(f->get_info());
                allocated_samples_index++;
                debug_write("Updating allocated index to: " + std::to_string(allocated_samples_index));
                async_write(f, to_level);
            }
        } else {
            debug_write("Cannot place file. Fully discarding it");
        }
    }else {
        apply_job_termination();
        allocated_samples_index++;
        debug_write("Updating allocated index to: " + std::to_string(allocated_samples_index));
    }
    return {SUCCESS};
}


bool PrefetchDataPlane::in_order(PrefetchedFile* f) const{
    return f->get_request_id() == allocated_samples_index;
}

Status PrefetchDataPlane::update_queue_and_place(){
    for(int i = 0; i < placement_queue.size(); i++){
        auto* f = placement_queue[i];
        if(in_order(f)){
            debug_write("Placement queue is being updated: found an in order request with id: " + std::to_string(f->get_request_id()));
            Status s = place(f);
            placement_queue.erase(placement_queue.begin() + i);
            if(s.state == SUCCESS) {
                debug_write("Delayed element was successfuly added: his timestamp: " + std::to_string(f->get_request_id()));
                return update_queue_and_place();
            }else{
                debug_write("Delayed element will be postponed timestamp: " + std::to_string(f->get_request_id()));
                postponed_queue.push(f);
                return {DELAYED};
            }
        }
    }
    //Success represents an empty queue or no new ordered files
    return {SUCCESS};
}

//We know already that any f to handle here is not on the level 0
Status PrefetchDataPlane::handle_placement(PrefetchedFile* f){
    if(in_order(f)){
        debug_write("Found an in order file with timestamp: " + std::to_string(f->get_request_id()));
        Status s = place(f);
        if(s.state == SUCCESS){
            s = update_queue_and_place();
            if(s.state == DELAYED){
                debug_write("Update queue and place returned DELAYED. Enforcing rate brake!");
                enforce_rate_brake(allocated_samples_index);
            }
        }
        else{
            debug_write("Postponing placement request for file with timestamp: " + std::to_string(f->get_request_id()) + ". Enforcing rate brake!");
            postponed_queue.push(f);
            enforce_rate_brake(allocated_samples_index);
        }
        return s;
    }
    else {
        debug_write("Found an out of order placement request, timestamp: " + std::to_string(f->get_request_id()) + " current index is: " + std::to_string(allocated_samples_index) );
        placement_queue.push_back(f);
        return {DELAYED};
    }
}

void PrefetchDataPlane::start_prefetching() {
    int iter_size = HierarchicalDataPlane::metadata_container->get_iter_size();
    HierarchicalDataPlane::set_total_jobs(iter_size);
    debug_write("Instance with rank "  + std::to_string(rank) + " starting prefetching with iter size: " + std::to_string(iter_size));
    for (int i = 0; i < iter_size; i++){
        int sample_id  = HierarchicalDataPlane::get_list_index(rank, worker_id, i);
        auto* mfi = (MutexFileInfo*)metadata_container->get_metadata(sample_id);
        if(enforce_rate_limit())
            debug_write("Rate limit was enforced");
        debug_write("Prefetching local_index: " + std::to_string(i) + ", sample_id: " + std::to_string(sample_id));

        prefetch_thread_pool->push([this, mfi, i](int id){
            mfi->reset_consumed_size();
            auto* f = new PrefetchedFile(mfi, i);

            if(mfi->init_prefetch()){
                debug_write("File already dealt with or is in the upper_level id: " + std::to_string(i));
                f->set_as_placeholder();
            }else {
                debug_write("Prefetching id: " + std::to_string(i) + " from level " + std::to_string(f->get_info()->get_storage_level()));
                HierarchicalDataPlane::read(f, f->get_info()->get_storage_level());
            }
            housekeeper_thread_pool->push([this, f](int id){
                handle_placement(f);
            });
        });
    }
    HierarchicalDataPlane::await_termination();
}


ssize_t PrefetchDataPlane::read(MutexFileInfo* mfi, char* result, uint64_t offset, size_t n){
    if(offset >= mfi->_get_size()) {
        debug_write("Tried to read from offset: " + std::to_string(offset) + " which goes beond file size: " +
                    std::to_string(mfi->_get_size()) + " name: " + mfi->get_name());
        return 0;
    }else{
        debug_write("Read from offset: " + std::to_string(offset) + " n: " + std::to_string(n) +
            " name: " + mfi->get_name());
    }
    if(placement_strategy == CR) {
        bool waited = mfi->wait_on_data_transfer();
        if (waited)
            debug_logger->_write("Client waited for file: " + mfi->get_name());
    }
    File* f = new File(mfi, offset, n);
    Status s = HierarchicalDataPlane::read(f, mfi->get_storage_level());
    memcpy(result, f->get_content(), f->get_requested_size());
    delete f;
    if(becomes_full && placement_strategy == CR) {
        if(mfi->consume(s.bytes_size)) {
            housekeeper_thread_pool->push([this, mfi](int id) { evict(mfi); });
        }
    }
    return s.bytes_size;
}

ssize_t PrefetchDataPlane::read(const std::string& filename, char* result, uint64_t offset, size_t n){
    auto* mfi = (MutexFileInfo*)(HierarchicalDataPlane::metadata_container->get_metadata(filename));
    return read(mfi, result, offset, n);
}

ssize_t PrefetchDataPlane::read_from_id(int file_id, char* result, uint64_t offset, size_t n){
    auto* mfi = (MutexFileInfo*)(HierarchicalDataPlane::metadata_container->get_metadata(file_id));
    return read(mfi, result, offset, n);
}

ssize_t PrefetchDataPlane::read_from_id(int file_id, char* result){
    auto* mfi = (MutexFileInfo*)(HierarchicalDataPlane::metadata_container->get_metadata(file_id));
    return read(mfi, result, 0, mfi->_get_size());
}

PrefetchDataPlaneBuilder PrefetchDataPlane::create(HierarchicalDataPlane* rdp) {
    return PrefetchDataPlaneBuilder(rdp);
}

PrefetchDataPlaneBuilder* PrefetchDataPlane::heap_create(HierarchicalDataPlane* rdp) {
    return new PrefetchDataPlaneBuilder{rdp};
}

void PrefetchDataPlane::init(){
    HierarchicalDataPlane::init();
    if(is_allocable(0)){
        auto* driver = get_alloc_driver(0);
        becomes_full = metadata_container->get_full_size() >= driver->get_max_storage_size();
    }
    std::string b_f = becomes_full ? "true" : "false";
    debug_write("First storage level becomes full: " + b_f );
}

void PrefetchDataPlane::start(){
    std::thread producer_thread(&PrefetchDataPlane::start_prefetching, this);
    producer_thread.detach();
    synchronization_thread_pool->push([this](int id){
        if(placement_strategy == CR)
            HierarchicalDataPlane::start_sync_loop(1);
        else
            HierarchicalDataPlane::start_sync_loop(0);
    });
}

std::vector<std::string> PrefetchDataPlane::configs(){
    std::vector<std::string> res;
    res.push_back("- PrefetchModule");
    res.push_back("\tprefetch_thread_pool_size: " + std::to_string(prefetch_thread_pool->size()) );
    res.push_back("\teviction_percentage: " + std::to_string(eviction_percentage) );
    for(const auto& str : HierarchicalDataPlane::configs())
        res.push_back("\t" + str);
    return res;
}

void PrefetchDataPlane::print(){
    if(instance_id == 0)
        for (const auto& str : configs())
            std::cout << str;
}

void PrefetchDataPlane::debug_write(const std::string& msg){
    debug_logger->_write("[PrefetchDataPlane] " + msg);
}
