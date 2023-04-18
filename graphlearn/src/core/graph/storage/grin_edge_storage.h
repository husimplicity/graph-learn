/* Copyright 2020-2022 Alibaba Group Holding Limited. All Rights Reserved.

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

#ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_
#define GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_

#include <memory>

#include "vineyard/graph/grin/predefine.h"
#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/topology/structure.h"
#include "vineyard/graph/grin/include/topology/vertexlist.h"
#include "vineyard/graph/grin/include/topology/edgelist.h"
#include "vineyard/graph/grin/include/property/topology.h"
#include "vineyard/graph/grin/include/index/order.h"
#include "vineyard/graph/grin/include/property/type.h"
#include "vineyard/graph/grin/include/partition/partition.h"

#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/grin_storage_utils.h"

namespace graphlearn {

namespace io {

};

};

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_
