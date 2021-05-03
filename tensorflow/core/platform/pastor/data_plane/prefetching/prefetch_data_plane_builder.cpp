//
// Created by dantas on 29/10/20.
//

#include <iostream>
#include <utility>
#include "prefetch_data_plane_builder.h"

DataPlane* PrefetchDataPlaneBuilder::build(){
    if(data_plane->prefetch_thread_pool == nullptr)
        data_plane->prefetch_thread_pool = new ctpl::thread_pool(2);
    return data_plane;
}
PrefetchDataPlaneBuilder& PrefetchDataPlaneBuilder::configurations(){return *this;}

PrefetchDataPlaneBuilder& PrefetchDataPlaneBuilder::policies(){return *this;}

PrefetchDataPlaneBuilder &PrefetchDataPlaneBuilder::with_eviction_percentage(float percentage) {
    data_plane->eviction_percentage = percentage;
    return *this;
}

PrefetchDataPlaneBuilder &PrefetchDataPlaneBuilder::with_prefetch_thread_pool_size(int size) {
    data_plane->prefetch_thread_pool = new ctpl::thread_pool(size);
    return *this;
}

PrefetchDataPlaneBuilder& PrefetchDataPlaneBuilder::with_placement_strategy(std::string type){
    data_plane->placement_strategy = std::move(type);
    return *this;
}