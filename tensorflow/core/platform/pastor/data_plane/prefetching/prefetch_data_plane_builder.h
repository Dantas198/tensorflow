//
// Created by dantas on 29/10/20.
//

#ifndef THESIS_PREFETCH_DATA_PLANE_BUILDER_H
#define THESIS_PREFETCH_DATA_PLANE_BUILDER_H

#include "prefetch_data_plane.h"

class PrefetchDataPlaneBuilder  {
    PrefetchDataPlane* data_plane;

public:
    explicit PrefetchDataPlaneBuilder(HierarchicalDataPlane* root_data_plane){
        data_plane = new PrefetchDataPlane(root_data_plane);
    };

    operator DataPlane*() const {return data_plane;}

    DataPlane* build();
    PrefetchDataPlaneBuilder& with_eviction_percentage(float percentage);
    PrefetchDataPlaneBuilder& with_prefetch_thread_pool_size(int size);
    PrefetchDataPlaneBuilder& with_placement_strategy(std::string type);
    PrefetchDataPlaneBuilder& configurations();
    PrefetchDataPlaneBuilder& policies();
};


#endif //THESIS_PREFETCH_DATA_PLANE_BUILDER_H
