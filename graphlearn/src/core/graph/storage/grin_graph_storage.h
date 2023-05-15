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
#include <numeric>

#include "vineyard/graph/grin/predefine.h"
#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/topology/structure.h"
#include "vineyard/graph/grin/include/topology/vertexlist.h"
#include "vineyard/graph/grin/include/topology/edgelist.h"
#include "vineyard/graph/grin/include/property/topology.h"
#include "vineyard/graph/grin/include/index/order.h"
#include "vineyard/graph/grin/include/property/type.h"
#include "vineyard/graph/grin/include/property/propertytable.h"
#include "vineyard/graph/grin/include/property/property.h"
#include "vineyard/graph/grin/include/property/propertylist.h"

#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "include/config.h"

namespace graphlearn {

namespace io {

class GrinEdgeStorage;

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
    auto src_vertex_list = GetVertexListByType(graph_, src_type_);

    for (size_t i = 0; i < num_vertices_; ++i) {
      auto v = grin_get_vertex_from_list(graph_, src_vertex_list, i);
      auto adj_all = grin_get_adjacent_list(graph_, GRIN_DIRECTION::OUT, v);
      auto adj_list = grin_select_edge_type_for_adjacent_list(
        graph_, edge_type_, adj_all);
      indptr_[i + 1] = grin_get_adjacent_list_size(graph_, adj_list) + indptr_[i];
      auto it = grin_get_adjacent_list_begin(graph_, adj_list);
      while (grin_is_adjacent_list_end(graph_, it) == false) {
        auto e = grin_get_edge_from_adjacent_list_iter(graph_, it);
        edge_list_.emplace_back(e);
        grin_get_next_adjacent_list_iter(graph_, it);
      }
      grin_destroy_adjacent_list_iter(graph_, it);
      grin_destroy_adjacent_list(graph_, adj_list);
      grin_destroy_adjacent_list(graph_, adj_all);
      grin_destroy_vertex(graph_, v);
    }

    auto src_type_name = grin_get_vertex_type_name(graph_, src_type_);
    auto dst_type_name = grin_get_vertex_type_name(graph_, dst_type_);
    side_info_ = init_edge_side_info(
      partitioned_graph_, partition_, attrs, 
      edge_type_name, src_type_name, dst_type_name
    );

    grin_destroy_vertex_list(graph_, src_vertex_list);
    delete src_type_name;
    delete dst_type_name;
  }

  virtual ~GrinGraphStorage() {
    delete side_info_;

    for (auto e : edge_list_) {
      grin_destroy_edge(graph_, e);
    }
    grin_destroy_edge_type(graph_, edge_type_);
    grin_destroy_vertex_type(graph_, src_type_);
    grin_destroy_vertex_type(graph_, dst_type_);
    grin_destroy_graph(graph_);
    grin_destroy_partition(partitioned_graph_, partition_);
    grin_destroy_partitioned_graph(partitioned_graph_);
  }

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
    auto src_vertex_list = GetVertexListByType(graph_, src_type_);
    auto src = grin_get_edge_src(graph_, edge_list_[edge_id]);
    
    auto src_id = grin_get_position_of_vertex_from_sorted_list(
      graph_, src_vertex_list, src);
    grin_destroy_vertex(graph_, src);
    grin_destroy_vertex_list(graph_, src_vertex_list);
    return src_id;
  }

  IdType GetDstId(IdType edge_id) const override {
    auto dst_vertex_list = GetVertexListByType(graph_, dst_type_);
    auto dst = grin_get_edge_dst(graph_, edge_list_[edge_id]);
    auto dst_id = grin_get_position_of_vertex_from_sorted_list(
      graph_, dst_vertex_list, dst);
    grin_destroy_vertex(graph_, dst);
    grin_destroy_vertex_list(graph_, dst_vertex_list);
    return dst_id;
  }

  virtual IdType GetEdgeId(IdType edge_index) const override {
    // TODO: edge_index is for temporal graphs and possibly not supported by
    //       some storages. Therefore, in general, we treat edge_index the same
    //       as edge_id in GRIN.
    return edge_index;
  }

  float GetEdgeWeight(IdType edge_id) const override {
    if (!side_info_->IsWeighted() || edge_id >= GetEdgeCount()) {
      return -1;
    }

    auto edge_property = grin_get_edge_property_by_name(
      graph_, edge_type_, std::string("weight").c_str());
    if (edge_property == GRIN_NULL_EDGE_PROPERTY) {
      return -1;
    }

    auto edge_dtype = grin_get_edge_property_data_type(graph_, edge_property);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    auto weight_val = grin_get_value_from_edge_property_table(
      graph_, edge_table, edge_list_[edge_id], edge_property);
    
    float weight;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      weight = *static_cast<const float*>(weight_val);

    default:
      weight = -1;
    }

    if (weight_val != NULL) {
      grin_destroy_value(graph_, edge_dtype, weight_val);
    }
    grin_destroy_edge_property_table(graph_, edge_table);
    grin_destroy_edge_property(graph_, edge_property);

    return weight;
  }

  int32_t GetEdgeLabel(IdType edge_id) const override {
    if (!side_info_->IsLabeled() || edge_id >= GetEdgeCount()) {
      return -1;
    }

    auto edge_property = grin_get_edge_property_by_name(
      graph_, edge_type_, std::string("label").c_str());
    if (edge_property == GRIN_NULL_EDGE_PROPERTY) {
      return -1;
    }

    auto edge_dtype = grin_get_edge_property_data_type(graph_, edge_property);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    auto label_val = grin_get_value_from_edge_property_table(
      graph_, edge_table, edge_list_[edge_id], edge_property);

    int32_t label;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
      label = *static_cast<const int32_t*>(label_val);
    
    default:
      label = -1;
    }

    if (label_val != NULL) {
      grin_destroy_value(graph_, edge_dtype, label_val);
    }
    grin_destroy_edge_property_table(graph_, edge_table);
    grin_destroy_edge_property(graph_, edge_property);

    return label;
  }

  virtual int64_t GetEdgeTimestamp(IdType edge_id) const override {
    if (!side_info_->IsTimestamped() || edge_id >= edge_list_.size()) {
      return -1;
    }

    auto edge_property = grin_get_edge_property_by_name(
      graph_, edge_type_, std::string("timestamp").c_str());
    if (edge_property == GRIN_NULL_EDGE_PROPERTY) {
      return -1;
    }

    auto edge_dtype = grin_get_edge_property_data_type(graph_, edge_property);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    auto timestamp_val = grin_get_value_from_edge_property_table(
      graph_, edge_table, edge_list_[edge_id], edge_property);
    
    int64_t timestamp;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      timestamp = *static_cast<const int64_t*>(timestamp_val);
      
    
    default:
      timestamp = -1;
    }

    if (timestamp_val != NULL) {
      grin_destroy_value(graph_, edge_dtype, timestamp_val);
    }
    grin_destroy_edge_property_table(graph_, edge_table);
    grin_destroy_edge_property(graph_, edge_property);

    return timestamp;
  }

  virtual Attribute GetEdgeAttribute(IdType edge_id) const override {
    if (!side_info_->IsAttributed()) {
      return Attribute();
    }
    if (edge_id >= edge_list_.size()) {
      return Attribute(AttributeValue::Default(side_info_), false);
    }

    auto attr = NewDataHeldAttributeValue();

    auto properties = grin_get_edge_property_list_by_type(graph_, edge_type_);
    auto edge_table = grin_get_edge_property_table_by_type(graph_, edge_type_);
    GRIN_ROW row = grin_get_row_from_edge_property_table(
      graph_, edge_table, edge_list_[edge_id], properties);

    auto property_size = grin_get_edge_property_list_size(graph_, properties);
    for (size_t i = 0; i < property_size; ++i) {
      auto property = grin_get_edge_property_from_list(graph_, properties, i);
      auto dtype = grin_get_edge_property_data_type(graph_, property);
      auto value = grin_get_value_from_row(graph_, row, dtype, i);
      switch(dtype) {
      case GRIN_DATATYPE::Int32:
      case GRIN_DATATYPE::UInt32:
      case GRIN_DATATYPE::Int64:
      case GRIN_DATATYPE::UInt64:
        if (side_info_->i_num > 0) {
          attr->Add(*static_cast<const int64_t*>(value));
        }
        break;
      case GRIN_DATATYPE::Float:
      case GRIN_DATATYPE::Double:
        if (side_info_->f_num > 0) {
          attr->Add(*static_cast<const float*>(value));
        }
        break;
      
      case GRIN_DATATYPE::String:
        if (side_info_->s_num > 0) {
          attr->Add(*static_cast<const std::string*>(value));
        }
        break;
      
      default:
        break;
      }

      grin_destroy_value(graph_, dtype, value);
      grin_destroy_edge_property(graph_, property);
    }

    grin_destroy_row(graph_, row);
    grin_destroy_edge_property_table(graph_, edge_table);
    grin_destroy_edge_property_list(graph_, properties);

    return Attribute(attr, true);
  }

  virtual Array<IdType> GetNeighbors(IdType src_id) const override {
    auto sz = indptr_[src_id + 1] - indptr_[src_id];
    std::vector<IdType> neighbors(sz);
    for (auto i = indptr_[src_id]; i < indptr_[src_id + 1]; ++i) {
      neighbors.emplace_back(GetDstId(i));
    }
    return IdArray(neighbors);
  }

  virtual Array<IdType> GetOutEdges(IdType src_id) const override {
    auto sz = indptr_[src_id + 1] - indptr_[src_id];
    std::vector<IdType> out_edges(sz);
    std::iota(out_edges.begin(), out_edges.end(), indptr_[src_id]);
    return IdArray(out_edges);
  }

  virtual IndexType GetInDegree(IdType dst_id) const override {
    auto dst_vertex_list = GetVertexListByType(graph_, dst_type_);
    auto v = grin_get_vertex_from_list(graph_, dst_vertex_list, dst_id);
    auto adj_in = grin_get_adjacent_list(graph_, GRIN_DIRECTION::IN, v);
    auto in_list = grin_select_edge_type_for_adjacent_list(
      graph_, edge_type_, adj_in);
    size_t deg = grin_get_adjacent_list_size(graph_, in_list);
    grin_destroy_adjacent_list(graph_, in_list);
    grin_destroy_adjacent_list(graph_, adj_in);
    grin_destroy_vertex(graph_, v);
    grin_destroy_vertex_list(graph_, dst_vertex_list);

    return deg;
  }

  virtual IndexType GetOutDegree(IdType src_id) const override {
    return indptr_[src_id + 1] - indptr_[src_id];
  }

  virtual const IndexArray GetAllInDegrees() const override {
    auto dst_vertex_list = GetVertexListByType(graph_, dst_type_);
    auto num_dst = grin_get_vertex_num_by_type(graph_, dst_type_);
    std::vector<IndexType> in_degrees(num_dst);
    for (size_t i = 0; i < num_dst; ++i) {
      auto v = grin_get_vertex_from_list(graph_, dst_vertex_list, i);
      auto adj_in = grin_get_adjacent_list(graph_, GRIN_DIRECTION::IN, v);
      auto in_list = grin_select_edge_type_for_adjacent_list(
        graph_, edge_type_, adj_in);
      in_degrees[i] = grin_get_adjacent_list_size(graph_, in_list);
      grin_destroy_adjacent_list(graph_, in_list);
      grin_destroy_adjacent_list(graph_, adj_in);
      grin_destroy_vertex(graph_, v);
    }
    grin_destroy_vertex_list(graph_, dst_vertex_list);

    return IndexArray(in_degrees);
  }

  virtual const IndexArray GetAllOutDegrees() const override {
    if (indptr_.size() <= 1)
      return IndexArray();

    std::vector<IndexType> out_degrees(indptr_.size());
    std::adjacent_difference(indptr_.begin(), indptr_.end(), out_degrees.begin());
    return IndexArray(
      std::vector<IndexType>(out_degrees.begin() + 1, out_degrees.end()));
  }

  virtual const IdArray GetAllSrcIds() const override {
    std::vector<IdType> srcs(indptr_.back());
    for (IdType i = 0; i < indptr_.size() - 1; ++i) {
      std::fill(srcs.begin() + indptr_[i], srcs.begin() + indptr_[i+1], i);
    }

    return IdArray(srcs);
  }

  virtual const IdArray GetAllDstIds() const override {
    auto num_dst = GetEdgeCount();
    std::vector<IdType> dst_ids(num_dst);
    for (IdType i = 0; i < num_dst; ++i) {
      dst_ids[i] = GetDstId(i);
    }

    return IdArray(dst_ids);
  }

private:
  friend class GrinEdgeStorage;  

  GRIN_PARTITIONED_GRAPH partitioned_graph_;
  GRIN_PARTITION partition_;
  GRIN_GRAPH graph_;
  GRIN_EDGE_TYPE edge_type_;
  GRIN_VERTEX_TYPE src_type_;
  GRIN_VERTEX_TYPE dst_type_;
  std::vector<IdType> indptr_;
  std::vector<GRIN_EDGE> edge_list_;
  size_t num_vertices_;

  std::set<std::string> attrs_;

  SideInfo *side_info_ = nullptr;

};

};

}

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_