//
// Created by dantas on 01/11/20.
//

#include <cstring>
#include "memory_buffer_driver.h"

Status MemoryBufferDriver::read(File* f){
    content_hash_map::const_accessor a;
    bool file_in_buffer = buffer.find(a, f->get_filename());

    if(file_in_buffer) {
        File* fc = a->second;

        // Release lock for this filename
        a.release();

        // TODO see if this copy can be removed
        memcpy(f->get_content(), fc->get_content() + f->get_offset(), f->get_requested_size());
        return {static_cast<ssize_t>(f->get_requested_size())};
    }
    else {
        // Release lock for this filename
        a.release();

        return {NOT_FOUND};
    }
}


//TODO warning: deleting object of polymorphic class type 'File' which has non-virtual destructor might cause undefined behavior
Status MemoryBufferDriver::remove(FileInfo* fi){
    content_hash_map::const_accessor a;
    bool file_in_buffer = buffer.find(a, fi->get_name());
    if(file_in_buffer) {
        File* f = a->second;
        // Release lock for this filename
        a.release();
        bool res = buffer.erase(fi->get_name());
        delete f;
        if (res)
            return {SUCCESS};
        else
            return {NOT_FOUND};
    }
    return {NOT_FOUND};
}

Status MemoryBufferDriver::write(File* f){
    // Check if buffer contains file
    content_hash_map::accessor a;
    bool new_file = buffer.insert(a, f->get_filename());

    if (!new_file) {
        // Release lock for this filename
        a.release();
    } else {
        // Add file with no content to buffer
        a->second = f;

        // Release lock for this filename
        a.release();
        return {sizeof_content(f->get_info())};
    }
    return {SUCCESS};
}

ssize_t MemoryBufferDriver::sizeof_content(FileInfo* fi){
    return fi->_get_size() + fi->get_name().size();
}

bool MemoryBufferDriver::in_memory_type(){
    return true;
}

void MemoryBufferDriver::create_environment(std::vector<std::string>& dirs){}