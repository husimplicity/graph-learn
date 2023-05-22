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

#ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_STORAGE_UTILS_H_
#define GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_STORAGE_UTILS_H_


extern "C" {
#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/partition/partition.h"
#include "vineyard/graph/grin/include/property/propertytable.h"
#include "vineyard/graph/grin/include/property/property.h"
#include "vineyard/graph/grin/include/property/propertylist.h"
#include "vineyard/graph/grin/include/property/topology.h"
}
#include "vineyard/graph/grin/src/predefine.h"

#include "core/graph/storage/edge_storage.h"
#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/node_storage.h"
#include "core/graph/storage/topo_storage.h"
#include "include/config.h"

namespace graphlearn {
namespace io {

using graphlearn::io::EdgeStorage;
using graphlearn::io::GraphStorage;
using graphlearn::io::NodeStorage;
using graphlearn::io::TopoStorage;

using graphlearn::io::Attribute;
using graphlearn::io::AttributeValue;
using graphlearn::io::EdgeValue;
using graphlearn::io::IdList;
using graphlearn::io::IndexType;
using graphlearn::io::IndexArray;
using graphlearn::io::NewDataHeldAttributeValue;
using graphlearn::io::SideInfo;

// GraphStorage* NewGrinGraphStorage(GRIN_PARTITIONED_GRAPH partitioned_graph,
//                                   GRIN_PARTITION partition,
//                                   const std::string& edge_type_name,
//                                   const std::set<std::string>& attrs);

GRIN_VERTEX_LIST GetVertexListByType(GRIN_GRAPH graph, GRIN_VERTEX_TYPE vtype);

SideInfo* init_edge_side_info(const GRIN_PARTITIONED_GRAPH& partitioned_graph,
                              const GRIN_PARTITION& partition,
                              const GRIN_GRAPH& graph,
                              const std::set<std::string>& attrs,
                              const std::string& edge_type_name,
                              const std::string& src_type_name,
                              const std::string& dst_type_name);

SideInfo* init_node_side_info(const GRIN_PARTITIONED_GRAPH& partitioned_graph,
                              const GRIN_PARTITION& partition,
                              const GRIN_GRAPH& graph,
                              const std::set<std::string>& attrs,
                              const std::string& node_type_name);


}

}

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_STORAGE_UTILS_H_
// #endif