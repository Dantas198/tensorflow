//
// Created by dantas on 07/03/21.
//

#ifndef THESIS_PREFETCHED_FILE_H
#define THESIS_PREFETCHED_FILE_H

#include "file.h"
#include "mutex_file_info.h"

class PrefetchedFile : public File {
    int request_id;
    bool placeholder;

public:
    PrefetchedFile(MutexFileInfo* file_info, int id);
    int get_request_id();
    void set_as_placeholder();
    bool is_placeholder();
    MutexFileInfo* get_info() override;

};


#endif //THESIS_PREFETCHED_FILE_H
