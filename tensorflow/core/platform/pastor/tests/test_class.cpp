//
// Created by dantas on 10/11/20.
//

#include "test_class.h"
#include "../data_plane/root/hierarchical_data_plane.h"
#include "../data_plane/prefetching/prefetch_data_plane.h"
#include "../data_plane/root/storage_drivers/data_storage_driver_builder.h"
#include "../data_plane/data_plane_instance.h"
#include <iostream>


void TestClass::run_instance(BootstrapClient* ec, const std::string &c_server_addr, const std::string &dp_server_addr){
    auto* dpi = new DataPlaneInstance(1,1);
    dpi->bind(ec->get_group(), c_server_addr, dp_server_addr);
    dpi->start();
    std::vector<std::string> values = ec->get_filenames();
    int i = 0;
    for (const std::string& v : values){
        int size = dpi->get_file_size(v);
        std::cout << "reading: " << v << " ,size: " << size << " ,i: " << i << "\n";
        char buff[size];
        dpi->read(v, buff, 0, size);
        i++;
    }
}

void TestClass::run_instance(int rank, BootstrapClient* ec, const std::string &c_server_addr, const std::string &dp_server_addr){
    auto* dpi = new DataPlaneInstance(rank, 0);
    dpi->bind(ec->get_group(), c_server_addr, dp_server_addr);
    dpi->start();
    std::vector<int> values = ec->get_ids_from_rank(rank);
    int i = 0;
    for (auto v : values){
        long size = dpi->get_file_size_from_id(v);
        int target = dpi->get_target_class_from_id(v);
        std::cout << "reading: " << v << " ,size: " << size << " ,i: " << i << " target:" << target << "\n";
        char buff[size];
        dpi->read_from_id(v, buff);
        i++;
    }
}

BootstrapClient* TestClass::run_ephemeral_client(const std::string& c_server_addr, const std::string& type, int number_of_workers, int world_size, bool return_names){

    auto* ec = new BootstrapClient(c_server_addr);
    ec->request_session(type, world_size, number_of_workers, return_names);

    std::string group = ec->get_group();

    std::cout << "EphemeralClient retrieved group = " << group << std::endl;

    std::vector<std::tuple<std::string, int>> dsi = ec->get_data_source_infos();

    std::cout << "EphemeralClient retrieved data_source_infos:" << std::endl;
    for(auto& tuple : dsi)
        std::cout << "dir: " << std::get<0>(tuple) << " cout: " << std::get<1>(tuple) << std::endl;

    std::cout << "EphemeralClient retrieved ids" << std::endl;
    auto idx = ec->get_ids();
    for(auto& index : idx)
        std::cout << index << ",";
    std::cout << std::endl;

    for(int i = 0; i < world_size; i++) {
        auto idx0 = ec->get_ids_from_rank(i);
        std::cout << "Printing rank " << i << " list" << std::endl;
        for (auto &index : idx0)
            std::cout << index << ",";
        std::cout << std::endl;
    }

    std::cout << "EphemeralClient retrieved filenames" << std::endl;
    auto filenames = ec->get_filenames();
    for(auto& str : filenames)
        std::cout << str << ",";
    std::cout << std::endl;

    return ec;
}

/*
DataPlane* TestClass::build_data_plane(int option){
    switch (option) {
        case 1:
            return build_data_plane_opt1();
        case 2:
            return build_data_plane_opt2();
        case 3:
            return build_data_plane_opt3();
        default:
            return build_data_plane_opt1();
    }
}

DataPlane* TestClass::build_data_plane_opt1(){

    return HierarchicalDataPlane::create(2);
        .hierarchy()
            .with_storage(DataStorageDriver::create(DISK)
                .with_block_size(8192)
                .with_storage_prefix("/home/dantas/thesis/ssd")
            .with_storage(DataStorageDriver::create(DISK)
                .with_block_size(8192)
                .with_storage_prefix("/home/dantas/thesis/hdd")
                .with_max_storage_size(1000))
        .policies()
            .with_cache_insertion_policy(FIRST_LEVEL_ONLY)
            .with_cache_eviction_policy(FORWARD_DEMOTION);
}

DataPlane* TestClass::build_data_plane_opt2(){
    //std::string train_files_dir = "/home/dantas/thesis/hdd";

    std::string out_filename = "../aux_files/shuffle_filenames.txt";
    //FilenamesHandler::create_filenames_files(train_files_dir , out_filename, 1, ".*.txt");

    RootDataPlane* root_data_plane = HierarchicalDataPlane::create(2)
        .hierarchy()
            .with_storage(DataStorageDriver::create(DISK)
                .with_block_size(8192)
                .with_storage_prefix("/home/dantas/thesis/ssd")
                .with_max_storage_size(100))
            .with_storage(DataStorageDriver::create(DISK)
                .with_block_size(8192)
                .with_storage_prefix("/home/dantas/thesis/hdd")
                .with_max_storage_size(1000))
        .policies()
            .with_cache_insertion_policy(FIRST_LEVEL_ONLY)
            .with_cache_eviction_policy(FORWARD_DEMOTION);

    return PrefetchDataPlane::create(root_data_plane)
        .policies()
            .with_data_transfer_policy(LAST_TO_FIRST_LEVEL);
}

DataPlane* TestClass::build_data_plane_opt3(){
    //std::string train_files_dir = "/home/dantas/thesis/hdd";

    std::string out_filename = "../aux_files/shuffle_filenames.txt";
    //FilenamesHandler::create_filenames_files(train_files_dir , out_filename, 1, ".*.txt");

    RootDataPlane* root_data_plane = HierarchicalDataPlane::create(2)
        .hierarchy()
            .with_storage(DataStorageDriver::create(BLOCKING_MEMORY_BUFFER)
                .with_max_storage_size(100)
                .with_storage_access_policy(THREAD_SYNC_ON_WRITE))
            .with_storage(DataStorageDriver::create(DISK)
                .with_block_size(8192)
                .with_storage_prefix("/home/dantas/thesis/hdd")
                .with_max_storage_size(1000))
        .policies()
            .with_cache_insertion_policy(FIRST_LEVEL_ONLY)
            .with_cache_eviction_policy(FORWARD_DEMOTION);

    return PrefetchDataPlane::create(root_data_plane)
            .policies()
                .with_data_transfer_policy(LAST_TO_FIRST_LEVEL);
}
 */