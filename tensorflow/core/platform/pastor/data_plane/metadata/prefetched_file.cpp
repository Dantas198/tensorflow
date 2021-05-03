//
// Created by dantas on 07/03/21.
//

#include "prefetched_file.h"

PrefetchedFile::PrefetchedFile(MutexFileInfo *file_info, int id) : File(file_info) {
    PrefetchedFile::request_id = id;
    PrefetchedFile::placeholder = false;
}


int PrefetchedFile::get_request_id() {
    return request_id;
}

void PrefetchedFile::set_as_placeholder(){
    placeholder = true;
}

bool PrefetchedFile::is_placeholder(){
    return placeholder;
}

MutexFileInfo* PrefetchedFile::get_info(){
    return dynamic_cast<MutexFileInfo*>(info);
}