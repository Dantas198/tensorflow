/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef TENSORFLOW_CORE_PLATFORM_PASTOR_PASTOR_FILE_SYSTEM_H_
#define TENSORFLOW_CORE_PLATFORM_PASTOR_PASTOR_FILE_SYSTEM_H_

#include "tensorflow/core/platform/env.h"

namespace tensorflow {

    class PastorFileSystem : public FileSystem {
    public:
        PastorFileSystem() {}

        ~PastorFileSystem() {}

        Status NewRandomAccessFile(
                const string& filename,
                std::unique_ptr<RandomAccessFile>* result) override;

        Status NewWritableFile(const string& fname,
                               std::unique_ptr<WritableFile>* result) override;

        Status NewAppendableFile(const string& fname,
                                 std::unique_ptr<WritableFile>* result) override;

        Status NewReadOnlyMemoryRegionFromFile(
                const string& filename,
                std::unique_ptr<ReadOnlyMemoryRegion>* result) override;

        Status FileExists(const string& fname) override;

        Status GetChildren(const string& dir, std::vector<string>* result) override;

        Status Stat(const string& fname, FileStatistics* stats) override;

        Status GetMatchingPaths(const string& pattern,
                                std::vector<string>* results) override;

        Status DeleteFile(const string& fname) override;

        Status CreateDir(const string& name) override;

        Status DeleteDir(const string& name) override;

        Status GetFileSize(const string& fname, uint64* size) override;

        Status RenameFile(const string& src, const string& target) override;

        Status CopyFile(const string& src, const string& target) override;

        Status ParsePastorPath(const string &fname, string* server_addr, string* path);

        Status ParsePastorPathForScheme(const string &fname, string scheme,
                                           string* server_addr, string* path) ;
        
        string TranslateName(const string& name) const override;
    };

}  // namespace tensorflow

#endif  // TENSORFLOW_CORE_PLATFORM_PASTOR_PASTOR_FILE_SYSTEM_H_
