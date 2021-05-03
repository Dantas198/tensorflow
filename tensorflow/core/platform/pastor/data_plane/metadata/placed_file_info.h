//
// Created by dantas on 29/04/21.
//

#ifndef THESIS_PLACED_FILE_INFO_H
#define THESIS_PLACED_FILE_INFO_H

#include "file_info.h"

#include <atomic>

class PlacedFileInfo : public FileInfo {
    std::atomic<bool> placed;

public:
    PlacedFileInfo(std::string f, size_t size) : FileInfo(std::move(f), size), placed(false){}
    PlacedFileInfo(std::string f, size_t size, int target): FileInfo(std::move(f), size, target), placed(false){}
    bool begin_placement(){
        bool expected = false;
        return placed.compare_exchange_strong(expected, true);
    }
};

#endif //THESIS_PLACED_FILE_INFO_H
