//
// Created by dantas on 10/11/20.
//

#ifndef THESIS_TEST_CLASS_H
#define THESIS_TEST_CLASS_H

#include "../data_plane/data_plane.h"
#include <vector>
#include "../data_plane/remote/bootstrap_client.h"

class TestClass {
    DataPlane* data_plane;
public:
    void run_instance(BootstrapClient* ec, const std::string &c_server_addr, const std::string &dp_server_addr);
    BootstrapClient* run_ephemeral_client(const std::string& c_server_addr, const std::string& type, int number_of_workers, int world_size, bool return_names);
    static std::vector<std::string> get_filenames(const std::string& prefetch_file_path);
    void run_instance(int rank, BootstrapClient* ec, const std::string &c_server_addr, const std::string &dp_server_addr);
};

#endif //THESIS_TEST_CLASS_H
