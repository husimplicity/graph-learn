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


#include "core/graph/storage/grin_edge_storage.h"
#include "core/graph/storage/grin_graph_storage.h"
#include "core/graph/storage/grin_node_storage.h"
#include "core/graph/storage/grin_topo_storage.h"

#include "core/graph/storage/edge_storage.h"
#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/node_storage.h"
#include "core/graph/storage/topo_storage.h"

#include "vineyard/graph/grin/include/property/propertytable.h"
#include "vineyard/graph/grin/include/property/property.h"
#include "vineyard/graph/grin/include/property/propertylist.h"
#include "vineyard/graph/grin/include/property/topology.h"
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

SideInfo* init_edge_side_info(const GRIN_PARTITIONED_GRAPH& partitioned_graph,
                              const GRIN_PARTITION& partition,
                              const std::set<std::string>& attrs,
                              const std::string& edge_type_name,
                              const std::string& src_type_name,
                              const std::string& dst_type_name) {
  auto pid = grin_get_partition_id(partitioned_graph, partition);
  auto graph = grin_get_local_graph_from_partition(partitioned_graph, partition);
  auto edge_type = grin_get_edge_type_by_name(graph, edge_type_name.c_str());

  static std::map<GRIN_PARTITION_ID,
                  std::map<std::string, std::shared_ptr<SideInfo>>>
      side_info_cache;
  static std::mutex mutex;
  std::lock_guard<std::mutex> lexical_scope_lock(mutex);
  auto cache_entry = side_info_cache[pid][edge_type_name];
  if (cache_entry) {
    return cache_entry.get();
  }
  auto side_info = std::make_shared<SideInfo>();

  auto edge_table = grin_get_edge_property_table_by_type(graph, edge_type);
  auto fields = grin_get_edge_property_list_by_type(graph, edge_type);
  size_t field_size = grin_get_edge_property_list_size(graph, fields);
  side_info->format = kDefault;
  for (size_t idx = 0; idx < field_size; ++idx) {
    auto field = grin_get_edge_property_from_list(graph, fields, idx);
    std::string field_name = grin_get_edge_property_name(graph, field);
    if (attrs.find(field_name) == attrs.end()) {
      continue;
    }

    GRIN_DATATYPE field_type = grin_get_edge_property_data_type(graph, field);
    switch (field_type) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
      side_info->i_num += 1;
      break;
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      side_info->f_num += 1;
    case GRIN_DATATYPE::String:
      side_info->s_num += 1;
    default:
      break;
    }

    if (field_name == "label") {
      side_info->format |= kLabeled;
    } else if (field_name == "weight") {
      side_info->format |= kWeighted;
    }
    side_info->format |= kAttributed;
  }
  side_info->type = edge_type_name;
  side_info->src_type = src_type_name;
  side_info->dst_type = dst_type_name;

  side_info_cache[pid][edge_type_name] = side_info;
  return side_info.get();
}

SideInfo* init_node_side_info(const GRIN_PARTITIONED_GRAPH& partitioned_graph,
                              const GRIN_PARTITION& partition,
                              const std::set<std::string>& attrs,
                              const std::string& node_type_name) {
  auto pid = grin_get_partition_id(partitioned_graph, partition);
  auto graph = grin_get_local_graph_from_partition(partitioned_graph, partition);
  auto node_type = grin_get_edge_type_by_name(graph, node_type_name.c_str());

  static std::map<GRIN_PARTITION_ID,
                  std::map<std::string, std::shared_ptr<SideInfo>>>
      side_info_cache;

  static std::mutex mutex;
  std::lock_guard<std::mutex> lexical_scope_lock(mutex);
  auto cache_entry = side_info_cache[pid][node_type_name];
  if (cache_entry) {
    return cache_entry.get();
  }
  auto side_info = std::make_shared<SideInfo>();

  auto vertex_table = grin_get_vertex_property_table_by_type(graph, node_type);
  auto fields = grin_get_vertex_property_list_by_type(graph, node_type);
  size_t field_size = grin_get_vertex_property_list_size(graph, fields);
  side_info->format = kDefault;
  for (size_t idx = 0; idx < field_size; ++idx) {
    auto field = grin_get_vertex_property_from_list(graph, fields, idx);
    std::string field_name = grin_get_vertex_property_name(graph, field);
    if (attrs.find(field_name) == attrs.end()) {
      continue;
    }

    GRIN_DATATYPE field_type = grin_get_vertex_property_data_type(graph, field);
    switch (field_type) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
      side_info->i_num += 1;
      break;
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      side_info->f_num += 1;
    case GRIN_DATATYPE::String:
      side_info->s_num += 1;
    default:
      break;
    }

    if (field_name == "label") {
      side_info->format |= kLabeled;
    } else if (field_name == "weight") {
      side_info->format |= kWeighted;
    }
    side_info->format |= kAttributed;
  }
  side_info->type = node_type_name;

  side_info_cache[pid][node_type_name] = side_info;
  return side_info.get();
}

}
}

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_STORAGE_UTILS_H_