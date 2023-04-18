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

// #ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_
// #define GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_

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
#include "vineyard/graph/grin/include/property/propertytable.h"
#include "vineyard/graph/grin/include/property/property.h"

#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/grin_storage_utils.h"

namespace graphlearn {

namespace io {

class GrinGraphStorage : public GraphStorage {
public:
  explicit GrinGraphStorage(
    GRIN_PARTITIONED_GRAPH partitioned_graph, GRIN_PARTITION partition,
    const std::string& edge_type_name, const std::set<std::string>& attrs):
      partitioned_graph_(partitioned_graph),
      partition_(partition),
      attrs_(attrs) {
    graph_ = grin_get_local_graph_from_partition(partitioned_graph_, partition_);
    edge_type_ = grin_get_edge_type_by_name(graph_, edge_type_name.c_str());
    src_type_ = grin_get_vertex_type_from_list(
      graph_, grin_get_src_types_from_edge_type(graph_, edge_type_), 0);
    dst_type_ = grin_get_vertex_type_from_list(
      graph_, grin_get_dst_types_from_edge_type(graph_, edge_type_), 0);

    num_vertices_ = grin_get_vertex_num_by_type(graph_, src_type_);
    indptr_.resize(num_vertices_ + 1);
    auto src_vertex_list = GetVertexListByType(src_type_);

    for (size_t i = 0; i < num_vertices_; ++i) {
      auto v = grin_get_vertex_from_list(graph_, src_vertex_list, i);
      // grin_vertex_list_.push_back(v);
      auto adj_list = grin_select_edge_type_for_adjacent_list(
        graph_, edge_type_,
        grin_get_adjacent_list(graph_, GRIN_DIRECTION::OUT, v)
      );
      indptr_[i + 1] = grin_get_adjacent_list_size(graph_, adj_list) + indptr_[i];
      auto it = grin_get_adjacent_list_begin(graph_, adj_list);
      while (grin_is_adjacent_list_end(graph_, it) == false) {
        auto e = grin_get_edge_from_adjacent_list_iter(graph_, it);
        edge_list_.push_back(e);
        grin_get_next_adjacent_list_iter(graph_, it);
      }
    }

    side_info_ = init_edge_side_info(
      partitioned_graph_, partition_, attrs, edge_type_name,
      grin_get_vertex_type_name(graph_, src_type_),
      grin_get_vertex_type_name(graph_, dst_type_)
    );

  }

  virtual ~GrinGraphStorage() = default;

  virtual void Lock() override {}
  virtual void Unlock() override {}

  virtual void SetSideInfo(const SideInfo *info) override {}
  virtual const SideInfo *GetSideInfo() const override {
    return side_info_;
  }

  virtual void Add(EdgeValue *value) override {}
  virtual void Build() override {}

  IdType GetEdgeCount() const override {
    return edge_list_.size();
  }

  IdType GetSrcId(IdType edge_id) const override {
    auto src_vertex_list = GetVertexListByType(src_type_);
    auto src = grin_get_edge_src(graph_, edge_list_[edge_id]);
    return grin_get_position_of_vertex_from_sorted_list(graph_, src_vertex_list, src);
  }

  IdType GetDstId(IdType edge_id) const override {
    auto dst_vertex_list = GetVertexListByType(dst_type_);
    auto dst = grin_get_edge_dst(graph_, edge_list_[edge_id]);
    return grin_get_position_of_vertex_from_sorted_list(
      graph_, dst_vertex_list, dst);
  }

  float GetEdgeWeight(IdType edge_id) const override {
    if (!side_info_->IsWeighted() || edge_id >= GetEdgeCount()) {
      return -1;
    }
    GRIN_EDGE e = edge_list_[edge_id];
    auto edge_property = grin_get_edge_property_by_name(
      graph_, edge_type_, std::string("weight").c_str());
    auto edge_dtype = grin_get_edge_property_data_type(graph_, edge_property);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    auto weight = grin_get_value_from_edge_property_table(
      graph_, edge_table, e, edge_property);
    
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      return *static_cast<const float*>(weight);
    
    default:
      return -1;
    }
  }

  int32_t GetEdgeLabel(IdType edge_id) const override {
    if (!side_info_->IsLabeled() || edge_id >= GetEdgeCount()) {
      return -1;
    }

    GRIN_EDGE e = edge_list_[edge_id];
    auto edge_property = grin_get_edge_property_by_name(
      graph_, edge_type_, std::string("label").c_str());
    auto edge_dtype = grin_get_edge_property_data_type(graph_, edge_property);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    auto label = grin_get_value_from_edge_property_table(
      graph_, edge_table, e, edge_property);
    
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
      return *static_cast<const int32_t*>(label);
    
    default:
      return -1;
    }
  }

  virtual Attribute GetEdgeAttribute(IdType edge_id) const = 0;

  virtual Array<IdType> GetNeighbors(IdType src_id) const = 0;
  virtual Array<IdType> GetOutEdges(IdType src_id) const = 0;

  virtual IndexType GetInDegree(IdType dst_id) const = 0;
  virtual IndexType GetOutDegree(IdType src_id) const = 0;
  virtual const IndexArray GetAllInDegrees() const = 0;
  virtual const IndexArray GetAllOutDegrees() const = 0;
  virtual const IdArray GetAllSrcIds() const = 0;
  virtual const IdArray GetAllDstIds() const = 0;

private:
  GRIN_PARTITIONED_GRAPH partitioned_graph_;
  GRIN_PARTITION partition_;
  GRIN_GRAPH graph_;
  GRIN_EDGE_TYPE edge_type_; // optional
  GRIN_VERTEX_TYPE src_type_; // optional
  GRIN_VERTEX_TYPE dst_type_; // optional
  std::vector<IdType> indptr_;
  std::vector<GRIN_EDGE> edge_list_;
  size_t num_vertices_;

  std::set<std::string> attrs_;

  SideInfo *side_info_ = nullptr;

  GRIN_VERTEX_LIST GetVertexListByType(GRIN_VERTEX_TYPE vtype) const {
    return grin_select_type_for_vertex_list(
      graph_, vtype, grin_get_vertex_list(graph_)
    );
  }
};

};

}

// #endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_