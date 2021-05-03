//
// Created by dantas on 25/10/20.
//

#include <iostream>
#include <cstdio>
#include <sys/stat.h>

#include "configuration_parser.h"
#include "../root/storage_drivers/data_storage_driver_builder.h"
#include "../remote/remote_stage_builder.h"

#define DEBUG_DIR "/debugger"
#define PROFILER_DIR "/profiler"

DataStorageDriver* ConfigurationParser::parseStorageDriver(YAML::Node driver){
    //default is disk
    DataStorageDriverBuilder storage_builder = DataStorageDriver::create(DISK);

    auto storage_type = driver["type"].as<std::string>();

    if (storage_type == "blocking_memory_buffer")
        storage_builder = DataStorageDriver::create(BLOCKING_MEMORY_BUFFER);

    else if (storage_type == "disk"){
        if(!driver["block_size"].IsNull())
            storage_builder.with_block_size(driver["block_size"].as<int>());
        storage_builder.with_storage_prefix(driver["prefix"].as<std::string>());
    }
    else
        std::cerr << "storage driver type not supported\n";

    YAML::Node mss = driver["max_storage_size"];
    YAML::Node msot = driver["max_storage_occupation_threshold"];

    if(mss && msot){
        storage_builder.with_allocation_capabilities(mss.as<size_t>(), msot.as<float>());
    }else if (mss)
        storage_builder.with_allocation_capabilities(mss.as<size_t>());

    if(driver["auto"] && driver["auto"].as<std::string>() == "true")
        storage_builder.with_auto_storage_management();

    return storage_builder.build();
}

ProfilerProxy* ConfigurationParser::parseProfiler(YAML::Node root_configs){
    YAML::Node configs = root_configs["data_plane"];
    YAML::Node hierarchy = configs["hierarchy"];
    int epochs = root_configs["metadata_container"]["epochs"].as<int>();
    ProfilerProxy* p;

    if(configs["profiler"] && configs["profiler"]["active"].as<std::string>() == "true"){

        YAML::Node frequency = configs["profiler"]["frequency"];
        YAML::Node warmup = configs["profiler"]["warmup"];

        if(frequency && warmup)
            p = new ProfilerProxy(hierarchy.size(), epochs, frequency.as<int>(), warmup.as<int>());
        else if(frequency)
            p = new ProfilerProxy(hierarchy.size(), epochs, frequency.as<int>());
        else
            p = new ProfilerProxy(hierarchy.size(), epochs);

        p->configure_service(root_configs["home_dir"].as<std::string>() + PROFILER_DIR);
    }else
        p = new ProfilerProxy();

    return p;
}

HierarchicalDataPlaneBuilder* ConfigurationParser::parseHierarchicalDataPlaneBuilder(RemoteStageBuilder* rbuilder, YAML::Node root_configs){
    YAML::Node configs = root_configs["data_plane"];
    YAML::Node hierarchy = configs["hierarchy"];
    HierarchicalDataPlaneBuilder* builder = HierarchicalDataPlane::create(rbuilder->get_instance_identifier(),
                                                                          rbuilder->get_world_size(),
                                                                          rbuilder->get_number_of_workers(),
                                                                          hierarchy.size());

    if(configs["storage_sync_timeout"]){
        builder->with_storage_synchronization_timeout(configs["storage_sync_timeout"].as<int>());
    }

    bool shared_tpool = false;
    auto tpool_size = configs["shared_tpool_size"];

    if(tpool_size){
        shared_tpool = true;
        builder->with_shared_thread_pool(tpool_size.as<int>());
    }

    std::vector<std::vector<DataStorageDriver*>> matrix;
    for (int j = 0; j < rbuilder->get_world_size() * rbuilder->get_number_of_workers(); j++) {
        std::vector<DataStorageDriver*> drivers;
        matrix.push_back(drivers);
        for(int i = 0; i < hierarchy.size(); i++) {
            auto driver = hierarchy[i];
            matrix[j].push_back(parseStorageDriver(driver));
        }
    }
    builder->with_storage_hierarchy(matrix);

    if (!shared_tpool) {
        std::vector<ctpl::thread_pool*> pools;
        for(int i = 0; i < hierarchy.size(); i++){
            auto driver = hierarchy[i];
            if (!driver["tpool_size"]) {
                std::cerr << "define a thread pool size for every storage level if shared_tpool is not used\n";
                exit(1);
            }
            pools.push_back( new ctpl::thread_pool(driver["tpool_size"].as<int>()));
        }
        builder->with_storage_hierarchy_pools(pools);
    }

    auto r_limiter = configs["rate_limiter"];
    if(r_limiter){
        auto type = r_limiter["type"].as<std::string>();
        if(type == "batch"){
            auto b_size = r_limiter["batch_size"];
            if(!b_size)
                std::cerr << "define a batch size for the batch rate limiter\n";
            else
                builder->with_batch_rate_limit(b_size.as<int>());
        }
        else
            std::cerr << "rate limiter type not supported\n";
    }

    if(root_configs["debug"].as<std::string>() == "true")
        builder->with_debug_enabled(root_configs["home_dir"].as<std::string>() + DEBUG_DIR, rbuilder->get_instance_identifier());

    auto type = configs["type"].as<std::string>();
    builder->with_metadata_container(rbuilder->get_metadata_container(type));
    builder->with_type(type);
    builder->with_profiling_enabled(parseProfiler(root_configs));
    return builder;
}


PrefetchDataPlaneBuilder* ConfigurationParser::parsePrefetchDataPlaneBuilder(HierarchicalDataPlane *root_data_plane, YAML::Node configs){
    PrefetchDataPlaneBuilder* builder = PrefetchDataPlane::heap_create(root_data_plane);

    YAML::Node t_configs = configs["type_configs"];

    if(t_configs["eviction_percentage"])
        builder->with_eviction_percentage(t_configs["eviction_percentage"].as<float>());

    if(t_configs["prefetch_tpool_size"])
        builder->with_prefetch_thread_pool_size(t_configs["prefetch_tpool_size"].as<int>());

    if(t_configs["placement_strategy"])
        builder->with_placement_strategy(t_configs["placement_strategy"].as<std::string>());

    return builder;
}


DataPlane* ConfigurationParser::parseDataPlane(HierarchicalDataPlaneBuilder* hierarchical_data_plane_builder, YAML::Node root_configs){
    YAML::Node configs = root_configs["data_plane"];
    auto data_plane_type = configs["type"].as<std::string>();
    if (data_plane_type == "prefetch_enabled")
        return parsePrefetchDataPlaneBuilder(hierarchical_data_plane_builder->build(), configs)->build();
    else if (data_plane_type == "root_standalone")
        return hierarchical_data_plane_builder->build();
    else {
        std::cerr << "data plane type not supported\n";
        return nullptr;
    }
}

DataPlane* ConfigurationParser::parse(RemoteStageBuilder* rbuilder){
    std::string dp_configuration = rbuilder->get_configuration();
    std::istringstream ss(dp_configuration);
    YAML::Node configs = YAML::Load(dp_configuration);

    if(configs["home_dir"]){
        int dir_res = mkdir(configs["home_dir"].as<std::string>().c_str(), 0777);
        if (dir_res == 0) {
            std::cout << "home directory created." << std::endl;
        }
    }else{
        std::cerr << "Please define a home directory for your data_plane instance.";
        exit(1);
    }

    auto* hierarchical_data_plane_builder = parseHierarchicalDataPlaneBuilder(rbuilder, configs);
    auto* data_plane = parseDataPlane(hierarchical_data_plane_builder, configs);
    return data_plane;
}


