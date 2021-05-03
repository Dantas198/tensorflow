//
// Created by dantas on 25/10/20.
//

#ifndef THESIS_HIERARCHICAL_DATA_PLANE_BUILDER_H
#define THESIS_HIERARCHICAL_DATA_PLANE_BUILDER_H

#include "hierarchical_data_plane.h"

enum CacheEvictionPolicy {
    FORWARD_DEMOTION = 0x01,
};

enum CacheInsertionPolicy {
    FIRST_LEVEL_ONLY = 0x01,
    FREE_STORAGE = 0x02,
};

class HierarchicalDataPlaneBuilder {
    HierarchicalDataPlane* data_plane;

public:
    explicit HierarchicalDataPlaneBuilder(int instance_id, int world_size, int number_of_workers, int hierarchy_size){
        data_plane = new HierarchicalDataPlane(instance_id, world_size, number_of_workers, hierarchy_size);
    };

    operator HierarchicalDataPlane*() const {return data_plane;}

    HierarchicalDataPlane* build();
    HierarchicalDataPlaneBuilder& hierarchy();
    HierarchicalDataPlaneBuilder& configurations();
    HierarchicalDataPlaneBuilder& policies();
    HierarchicalDataPlaneBuilder& with_storage_hierarchy(std::vector<std::vector<DataStorageDriver*>> matrix);
    HierarchicalDataPlaneBuilder& with_storage_hierarchy_pools(std::vector<ctpl::thread_pool*> pools);
    HierarchicalDataPlaneBuilder& with_debug_enabled(const std::string& dir_path, int unique_id);
    HierarchicalDataPlaneBuilder& with_metadata_container(MetadataContainerService* mdc);
    HierarchicalDataPlaneBuilder& with_shared_thread_pool(int pool_size);
    HierarchicalDataPlaneBuilder& with_batch_rate_limit(int size);
    HierarchicalDataPlaneBuilder& with_profiling_enabled(ProfilerProxy* p);
    HierarchicalDataPlaneBuilder& with_type(const std::string& type);
    HierarchicalDataPlaneBuilder& with_storage_synchronization_timeout(int timeout);
};

#endif //THESIS_HIERARCHICAL_DATA_PLANE_BUILDER_H
