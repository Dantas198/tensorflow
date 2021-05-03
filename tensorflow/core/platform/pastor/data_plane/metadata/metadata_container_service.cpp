//
// Created by dantas on 26/03/21.
//
#include <iostream>
#include <random>
#include <algorithm>
#include <utility>
#include <cmath>

#include "metadata_container_service.h"

//TODO warning: moving a local object in a return statement prevents copy elision [-Wpessimizing-move]
//  162 |     return std::move(res);


MetadataContainerService::MetadataContainerService(){
    current_epoch = 0;
    file_index = 0;
    partition_file_count = 0;
    world_size = 1;
    erroneous_state = false;
}

FileInfo* MetadataContainerService::get_metadata(const std::string& name){
    return name_to_info[name];
}

FileInfo* MetadataContainerService::get_metadata(int id){
    return id_to_info[id];
}

FileInfo* MetadataContainerService::get_metadata_from_ordered_id(int rank, int id){
    return id_to_info[partitioned_samples_ordered_ids[rank][id]];
}

FileInfo* MetadataContainerService::get_file(int index){
    return id_to_info[index];
}

int MetadataContainerService::get_iter_size(){
    return partition_file_count * epochs;
}

int MetadataContainerService::get_current_epoch(){
    return current_epoch;
}

size_t MetadataContainerService::get_full_size(){
    return full_size;
}

int MetadataContainerService::get_file_count(){
    return file_count;
}

void MetadataContainerService::set_distributed_id(int id){
    distributed_id = id;
}

void MetadataContainerService::set_world_size(int world_size_){
    world_size = world_size_;
}

void MetadataContainerService::set_file_count(int fc){
    MetadataContainerService::file_count = fc;
    MetadataContainerService::partition_file_count = fc;
}

void MetadataContainerService::set_full_size(size_t full_size){
    MetadataContainerService::full_size = full_size;
}

void MetadataContainerService::set_epochs(int epochs){
    MetadataContainerService::epochs = epochs;
}

void MetadataContainerService::set_storage_source_level(int level){
    MetadataContainerService::storage_source_level = level;
}

void MetadataContainerService::add_dir_file_count(const std::string& name, size_t count){
    dirs_file_count.emplace_back(name, count);
}

void MetadataContainerService::add_target_class_to_id(const std::string& name, int id){
    target_class_to_id.insert(std::make_pair(name, id));
}

void MetadataContainerService::generate_samples_ordered_ids(){
    if(epochs <= 0)
        std::cerr << "Instance: " << distributed_id << " generate samples ordered ids: No epochs provided\n";
    else {
        if (distributed_id == 0) {
            std::cout << "Instance: " << distributed_id << " received train_length: " << std::get<1>(dirs_file_count[0])
                      << std::endl;
            if(dirs_file_count.size() > 1)
                std::cout << "Instance: " << distributed_id << " received val_length: " << std::get<1>(dirs_file_count[1])
                          << std::endl;
        }
        // i = rank
        if(world_size > 1)
            for (int i = 0; i < world_size; i++)
                partitioned_samples_ordered_ids.push_back(make_list(i, world_size, dirs_file_count, epochs));
        else
            partitioned_samples_ordered_ids.push_back(make_list(-1, 1, dirs_file_count, epochs));
    }
}

void MetadataContainerService::generate_samples_ordered_ids(const std::vector<int>& shuffling_seeds){
    if(epochs <= 0)
        std::cerr << "Instance: " << distributed_id << " generate samples ordered ids: No epochs provided\n";
    else{
        if(distributed_id == 0) {
            std::cout << "Instance: " << distributed_id << " received train_length: " << std::get<1>(dirs_file_count[0])
                      << std::endl;
            if(dirs_file_count.size() > 1)
                std::cout << "Instance: " << distributed_id << " received val_length: " << std::get<1>(dirs_file_count[1])
                          << std::endl;
        }
        // i = rank
        if(world_size > 1)
            for (int i = 0; i < world_size; i++)
                partitioned_samples_ordered_ids.push_back(make_shuffled_list(i, world_size, dirs_file_count, shuffling_seeds));
        else
            partitioned_samples_ordered_ids.push_back(make_shuffled_list(-1, 1, dirs_file_count, shuffling_seeds));
    }
}


std::vector<int> MetadataContainerService::generate_ids_list(int start, int end){
    std::vector<int> res;
    int length = end - start;
    res.reserve(length);
    for(int i = start; i < end; i++)
        res.push_back(i);

    return res;
}

std::vector<std::string> MetadataContainerService::generate_filenames_list(std::vector<std::string>& filenames, int start, int end){
    std::vector<std::string> res;
    int length = end - start;
    res.reserve(length);
    for(int i = start; i < end; i++)
        res.push_back(filenames[i]);

    return res;
}

std::tuple<int, int> MetadataContainerService::get_sizes(int original_size, int world_size){
    int partitioned_size = std::ceil(original_size / world_size);
    if(partitioned_size % world_size != 0)
        partitioned_size = std::ceil((original_size - world_size) / world_size);
    int drop = original_size - partitioned_size * world_size;
    return {partitioned_size, drop};
}

template<typename T>
void MetadataContainerService::expand_list(int rank, int world_size_, std::vector<T>& res, std::vector<T> &l, std::tuple<int, int> l_info){
    int l_p_size = std::get<0>(l_info);
    int drop = std::get<1>(l_info);
    auto l_end = (rank < world_size_ - 1 && rank > -1) ? l.begin() + (l_p_size * (rank + 1)) : l.end() - drop;
    int real_rank = (rank > -1) ? rank : 0;
    res.insert(res.end(), l.begin() + (l_p_size * real_rank), l_end);
}

template<typename T>
std::vector<T> MetadataContainerService::concatenate_and_expand_list(int rank, int world_size,
                                                                     std::vector<T>& l1, std::vector<T>& l2,
                                                                     int expand_size){
    auto train_info = get_sizes(l1.size(), world_size);
    int train_p_size = std::get<0>(train_info);

    auto val_info = get_sizes(l2.size(), world_size);
    int val_p_size = std::get<0>(val_info);

    std::vector<T> res;
    res.reserve((train_p_size + val_p_size) * expand_size);

    for(int i = 0; i < expand_size; i++) {
        expand_list(rank, world_size, res, l1, train_info);
        expand_list(rank, world_size, res, l2, val_info);
    }
    return res;
}

template<typename T>
std::vector<T> MetadataContainerService::concatenate_and_expand_list(int rank, int world_size,
                                                                     std::vector<T>& l1, std::vector<T>& l2,
                                                                     const std::vector<int>& shuffling_seeds){

    auto train_info = get_sizes(l1.size(), world_size);
    int train_p_size = std::get<0>(train_info);

    auto val_info = get_sizes(l2.size(), world_size);
    int val_p_size = std::get<0>(val_info);
    std::vector<T> res;
    res.reserve((train_p_size + val_p_size) * shuffling_seeds.size());

    for(auto seed : shuffling_seeds) {
        std::shuffle(l1.begin(), l1.end() - 1, std::default_random_engine(seed));
        std::shuffle(l2.begin(), l2.end() - 1, std::default_random_engine(seed));

        expand_list(rank, world_size, res, l1, train_info);
        expand_list(rank, world_size, res, l2, val_info);
    }

    return res;
}

template<typename T>
std::vector<T> MetadataContainerService::expand_list(int rank, int world_size, std::vector<T>& l, int expand_size){
    auto train_info = get_sizes(l.size(), world_size);
    int train_p_size = std::get<0>(train_info);

    std::vector<T> res;
    res.reserve(train_p_size * expand_size);

    for(int i = 0; i < expand_size; i++) {
        expand_list(rank, world_size, res, l, train_info);
    }
    return res;
}
template<typename T>
std::vector<T> MetadataContainerService::expand_list(int rank, int world_size, std::vector<T>& l, const std::vector<int>& shuffling_seeds){
    auto train_info = get_sizes(l.size(), world_size);
    int train_p_size = std::get<0>(train_info);

    std::vector<T> res;
    res.reserve(train_p_size  * shuffling_seeds.size());

    for(auto seed : shuffling_seeds) {
        std::shuffle(l.begin(), l.end() - 1, std::default_random_engine(seed));
        expand_list(rank, world_size, res, l, train_info);
    }

    return res;
}



std::vector<int> MetadataContainerService::make_list(int rank,
                                                     int world_size,
                                                     std::vector<std::tuple<std::string, int>>& dirs_file_count,
                                                     int epochs){
    std::vector<int> res;
    std::vector<int> train;
    std::vector<int> val;

    int train_l = std::get<1>(dirs_file_count[0]);
    int total = train_l;
    train = generate_ids_list(0, train_l);
    if(dirs_file_count.size() > 1){
        int val_l = std::get<1>(dirs_file_count[1]);
        total += val_l;
        res.reserve(total);
        val = generate_ids_list(train_l, total);
        return concatenate_and_expand_list<int>(rank, world_size, train, val, epochs);
    }
    res.reserve(total);
    return expand_list<int>(rank, world_size, train, epochs);
}

std::vector<int> MetadataContainerService::make_shuffled_list(int rank, int world_size,
                                                              std::vector<std::tuple<std::string,
                                                              int>>& dirs_file_count,
                                                              const std::vector<int>& shuffling_seeds){
    std::vector<int> res;
    std::vector<int> train;
    std::vector<int> val;

    int train_l = std::get<1>(dirs_file_count[0]);
    int total = train_l;
    train = generate_ids_list(0, train_l);
    if(dirs_file_count.size() > 1){
        int val_l = std::get<1>(dirs_file_count[1]);
        total += val_l;
        res.reserve(total);
        val = generate_ids_list(train_l, total);
        return concatenate_and_expand_list<int>(rank, world_size, train, val, shuffling_seeds);
    }
    res.reserve(total);
    return expand_list<int>(rank, world_size, train, shuffling_seeds);
}

std::vector<std::string> MetadataContainerService::make_list(std::vector<std::tuple<std::string, int>>& dirs_file_count,
                                                             std::vector<std::string> filenames, int epochs){
    std::vector<std::string> res;
    std::vector<std::string> train;
    std::vector<std::string> val;

    int train_l = std::get<1>(dirs_file_count[0]);
    int total = train_l;
    train = generate_filenames_list(filenames, 0, train_l);
    if(dirs_file_count.size() > 1){
        int val_l = std::get<1>(dirs_file_count[1]);
        total += val_l;
        res.reserve(total);
        val = generate_filenames_list(filenames, train_l, total);
        return concatenate_and_expand_list<std::string>(-1, 1, train, val, epochs);
    }
    res.reserve(total);
    return expand_list<std::string>(-1, 1, train, epochs);
}

// Assumes root
std::vector<std::string> MetadataContainerService::make_shuffled_list(std::vector<std::tuple<std::string, int>>& dirs_file_count,
                                                                      std::vector<std::string> filenames,
                                                                      const std::vector<int>& shuffling_seeds){
    std::vector<std::string> res;
    std::vector<std::string> train;
    std::vector<std::string> val;

    int train_l = std::get<1>(dirs_file_count[0]);
    int total = train_l;
    train = generate_filenames_list(filenames, 0, train_l);
    if(dirs_file_count.size() > 1){
        int val_l = std::get<1>(dirs_file_count[1]);
        total += val_l;
        res.reserve(total);
        val = generate_filenames_list(filenames, train_l, total);
        return concatenate_and_expand_list<std::string>(-1, 1, train, val, shuffling_seeds);
    }
    res.reserve(total);
    return expand_list<std::string>(-1, 1, train, shuffling_seeds);
}

void MetadataContainerService::add_entry(int id, FileInfo* fi){
    fi->set_storage_level(storage_source_level);
    name_to_info.insert(std::make_pair(fi->get_name(), fi));
    id_to_info.insert(std::make_pair(id, fi));
}

std::vector<std::string> MetadataContainerService::get_dirs_for_environment(){
    std::vector<std::string> res;
    if(dirs_file_count.size() > 1) {
        for (auto entry1 : dirs_file_count) {
            for (auto &entry2 : target_class_to_id) {
                res.push_back("/" + std::get<0>(entry1) + "/" + entry2.first);
            }
        }
    }
    return res;
}

int MetadataContainerService::get_id(int rank, int index){
 return partitioned_samples_ordered_ids[rank][index];
}

//Transforms file_count into partitioned size
void MetadataContainerService::make_partition(int rank_, int world_size_, int worker_id_, int num_workers_){
    int train_size = std::get<1>(dirs_file_count[0]);
    auto train_partition_info = get_sizes(train_size, world_size_);
    int partition_train_size = std::get<0>(train_partition_info);
    //update state with new training size
    dirs_file_count[0] = std::make_pair(std::get<0>(dirs_file_count[0]), partition_train_size);
    partition_file_count = partition_train_size;

    if(dirs_file_count.size() > 1){
        int val_size = std::get<1>(dirs_file_count[1]);
        auto val_partition_info = get_sizes(val_size, world_size_);
        int partition_val_size = std::get<0>(val_partition_info);
        //update state with new val size
        dirs_file_count[1] = std::make_pair(std::get<0>(dirs_file_count[1]), partition_val_size);
        partition_file_count += partition_val_size;
    }
    /*
        //generates imbalaced load on the last rank
        file_count = std::ceil(size_per_world / num_workers_);
        if (size_per_world % num_workers_ != 0){
            std::cout << "Total number of samples per world is not evenly divisible by num_workers_. Droping last" <<std::endl;
            file_count = std::ceil((size_per_world - num_workers_) / num_workers_);
        }
    */
}

/*
 * int MetadataContainerService::calculate_offset(int rank, int world_size, int local_index){
    if(dirs_file_count.size() == 1)
        return rank * file_count + local_index;

    int train_l = std::get<1>(dirs_file_count[0]);
    int val_l = std::get<1>(dirs_file_count[1]);
    int epoch = std::floor(local_index / partition_file_count);
    int train_offset = rank * train_l + epoch * file_count;

    int index = local_index - partition_file_count * epoch;
    int is_val = index > train_l - 1 ? 1 : 0;

    int val_offset = ((world_size - rank - 1) * train_l + rank * val_l + 1) * is_val;
    return train_offset + val_offset + index;
}
 */