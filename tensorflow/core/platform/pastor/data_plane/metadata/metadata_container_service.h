//
// Created by dantas on 26/03/21.
//

#ifndef THESIS_METADATA_CONTAINER_SERVICE_H
#define THESIS_METADATA_CONTAINER_SERVICE_H

#include <string>
#include <unordered_map>
#include <vector>

#include "file_info.h"

//TODO swap names with the one in the controller

class MetadataContainerService {
    int distributed_id;
    std::unordered_map<std::string, int> target_class_to_id;
    std::unordered_map<std::string, FileInfo*> name_to_info;
    std::unordered_map<int, FileInfo*> id_to_info;
    std::vector<std::tuple<std::string, int>> dirs_file_count;
    int world_size;
    int file_count;
    int partition_file_count;
    size_t full_size;
    int epochs;
    std::vector<std::vector<int>> partitioned_samples_ordered_ids;
    int storage_source_level;
    int current_epoch;
    int file_index;
    bool erroneous_state;

    template<typename T>
    static void expand_list(int rank, int world_size, std::vector<T>& res, std::vector<T> &l, std::tuple<int, int> l_info);
    static std::vector<int> generate_ids_list(int start, int end);
    static std::vector<std::string> generate_filenames_list(std::vector<std::string>& filenames, int start, int end);
    template<typename T>
    static std::vector<T> expand_list(int rank, int world_size, std::vector<T>& l, int expand_size);
    template<typename T>
    static std::vector<T> expand_list(int rank, int world_size, std::vector<T>& l, const std::vector<int>& shuffling_seeds);

    template<typename T>
    static std::vector<T> concatenate_and_expand_list(int rank, int world_size, std::vector<T>& l1, std::vector<T>& l2, int expand_size);
    template<typename T>
    static std::vector<T> concatenate_and_expand_list(int rank, int world_size, std::vector<T>& l1, std::vector<T>& l2, const std::vector<int>& shuffling_seeds);

public:
    MetadataContainerService();
    void set_distributed_id(int id);
    void set_world_size(int world_size_);
    void set_file_count(int file_count);
    void set_full_size(size_t full_size);
    void set_epochs(int epochs);
    void set_storage_source_level(int level);
    void add_dir_file_count(const std::string& name, size_t count);
    void add_target_class_to_id(const std::string& name, int id);
    void add_entry(int id, FileInfo* fi);

    void make_partition(int rank_, int world_size_, int worker_id_, int num_workers_);
    //called once
    void generate_samples_ordered_ids();
    //called for each epoch
    void generate_samples_ordered_ids(const std::vector<int>& shuffling_seeds);

    static std::tuple<int, int> get_sizes(int original_size, int world_size);
    static std::vector<int> make_list(int rank, int world_size, std::vector<std::tuple<std::string, int>>& dirs_file_count, int epochs);
    static std::vector<int> make_shuffled_list(int rank, int world_size, std::vector<std::tuple<std::string, int>>& dirs_file_count, const std::vector<int>& shuffling_seeds);
    static std::vector<std::string> make_list(std::vector<std::tuple<std::string, int>>& dirs_file_count, std::vector<std::string> filenames, int epochs);
    static std::vector<std::string> make_shuffled_list(std::vector<std::tuple<std::string, int>>& dirs_file_count, std::vector<std::string> filenames, const std::vector<int>& shuffling_seeds);

    FileInfo* get_metadata(const std::string& name);
    FileInfo* get_metadata(int id);
    FileInfo* get_metadata_from_ordered_id(int rank, int id);
    int get_id(int rank, int index);
    FileInfo* get_file(int index);
    int get_current_epoch();
    int get_iter_size();
    size_t get_full_size();
    int get_file_count();
    std::vector<std::string> get_dirs_for_environment();
};


#endif //THESIS_METADATA_CONTAINER_SERVICE_H
