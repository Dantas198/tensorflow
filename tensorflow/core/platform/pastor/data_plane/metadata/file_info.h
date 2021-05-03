//
// Created by dantas on 19/10/20.
//

#ifndef THESIS_FILE_INFO_H
#define THESIS_FILE_INFO_H

#include <string>
#include <atomic>

/*
 * File id might change during epochs, but that behavior it's not harmful.
 */

class FileInfo {
private:
    size_t size;

protected:
    int target;
    std::string filename;
    std::atomic<int> storage_level;

public:
    FileInfo(std::string f, size_t size);
    FileInfo(std::string f, size_t size, int t);
    virtual ~FileInfo()= default;

    const std::string &get_name() const;
    size_t _get_size() const;
    int get_storage_level();
    void set_storage_level(int level);
    int get_target();
    void set_target(int t);
};

#endif //THESIS_FILE_INFO_H
