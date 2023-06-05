/* Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.

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

#ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_CREATOR_H_
#define GRAPHLEARN_CORE_GRAPH_STORAGE_CREATOR_H_

#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/node_storage.h"
#include "vineyard/graph/grin/src/predefine.h"

namespace graphlearn {

io::GraphStorage* CreateGraphStorage(
    const std::string& type,
    const std::string& view_type,
    const std::string &use_attrs);
io::NodeStorage* CreateNodeStorage(
    const std::string& type,
    const std::string& view_type,
    const std::string &use_attrs);

}  // namespace graphlearn

#endif  // GRAPHLEARN_CORE_GRAPH_STORAGE_CREATOR_H_
