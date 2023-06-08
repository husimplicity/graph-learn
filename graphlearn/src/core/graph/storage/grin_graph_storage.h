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

#include <iostream>
#include <memory>
#include <numeric>
#include <cstdio>

extern "C" {

#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/topology/structure.h"
#include "vineyard/graph/grin/include/topology/vertexlist.h"
#include "vineyard/graph/grin/include/topology/edgelist.h"
#include "vineyard/graph/grin/include/property/topology.h"
#include "vineyard/graph/grin/include/partition/partition.h"
#include "vineyard/graph/grin/include/index/order.h"
#include "vineyard/graph/grin/include/property/type.h"
#include "vineyard/graph/grin/include/property/row.h"
#include "vineyard/graph/grin/include/property/property.h"
#include "vineyard/graph/grin/include/property/propertylist.h"
}
#include "vineyard/graph/grin/src/predefine.h"

#include "core/graph/storage/graph_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "include/config.h"

namespace graphlearn {

namespace io {

class GrinEdgeStorage;

class GrinGraphStorage : public GraphStorage {
public:
  explicit GrinGraphStorage(
    const std::string& edge_label,
    const std::string& use_attrs=""){

    auto edge_type_name = edge_label;
    boost::algorithm::split(attrs_names_, use_attrs, boost::is_any_of(";"));
    std::cout << "edge_type_name: " << edge_type_name << std::endl;

    char** argv = new char*[2];
    argv[0] = new char[GLOBAL_FLAG(VineyardIPCSocket).size()];
    std::strcpy(argv[0], GLOBAL_FLAG(VineyardIPCSocket).c_str());
    argv[1] = new char[std::to_string(GLOBAL_FLAG(VineyardGraphID)).size()];
    std::strcpy(argv[1], std::to_string(GLOBAL_FLAG(VineyardGraphID)).c_str());
    int argc = sizeof(argv) / sizeof(char*);
    std::cout << "argc: " << 2 << " argv: " << argv[0] << " " << argv[1] <<std::endl;
    partitioned_graph_ = grin_get_partitioned_graph_from_storage(2, argv);
    local_partitions_ = grin_get_local_partition_list(partitioned_graph_);
    partition_ = grin_get_partition_from_list(
      partitioned_graph_, local_partitions_, 0);
    std::cout << "Get part done!" << std::endl;
    graph_ = grin_get_local_graph_by_partition(partitioned_graph_, partition_);
    edge_type_ = grin_get_edge_type_by_name(graph_, edge_type_name.c_str());
    for (auto attr_name : attrs_names_) {
      auto property = grin_get_edge_property_by_name(
        graph_, edge_type_, attr_name.c_str());
      if (property) {
        attrs_.insert(property);
      }
    }
    auto src_types = grin_get_src_types_by_edge_type(graph_, edge_type_);
    src_type_ = grin_get_vertex_type_from_list(graph_, src_types, 0);
    auto dst_types = grin_get_dst_types_by_edge_type(graph_, edge_type_);
    dst_type_ = grin_get_vertex_type_from_list(graph_, dst_types, 0);

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
      partitioned_graph_, partition_, graph_, attrs_names_, 
      edge_type_name, src_type_name, dst_type_name);

    grin_destroy_vertex_type_list(graph_, src_types);
    grin_destroy_vertex_type_list(graph_, dst_types);
    grin_destroy_vertex_list(graph_, src_vertex_list);
    delete[] argv;
    LOG(INFO) << "Create GrinGraphStorage Done.";
  }

  virtual ~GrinGraphStorage() {
    // delete side_info_;

    for (auto e : edge_list_) {
      grin_destroy_edge(graph_, e);
    }
    grin_destroy_edge_type(graph_, edge_type_);
    grin_destroy_vertex_type(graph_, src_type_);
    grin_destroy_vertex_type(graph_, dst_type_);
    grin_destroy_graph(graph_);
    grin_destroy_partition(partitioned_graph_, partition_);
    grin_destroy_partition_list(partitioned_graph_, local_partitions_);
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
    auto src = grin_get_src_vertex_from_edge(graph_, edge_list_[edge_id]);
    
    auto src_id = grin_get_position_of_vertex_from_sorted_list(
      graph_, src_vertex_list, src);
    grin_destroy_vertex(graph_, src);
    grin_destroy_vertex_list(graph_, src_vertex_list);
    return src_id;
  }

  IdType GetDstId(IdType edge_id) const override {
    auto dst_vertex_list = GetVertexListByType(graph_, dst_type_);
    auto dst = grin_get_dst_vertex_from_edge(graph_, edge_list_[edge_id]);
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

    auto edge_dtype = grin_get_edge_property_datatype(graph_, edge_property);
    
    float weight;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
      weight = grin_get_edge_property_value_of_int32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Int64:
      weight = grin_get_edge_property_value_of_int64(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt32:
      weight = grin_get_edge_property_value_of_uint32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt64:
      weight = grin_get_edge_property_value_of_uint64(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Float:
      weight = grin_get_edge_property_value_of_float(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Double:
      weight = grin_get_edge_property_value_of_double(
        graph_, edge_list_[edge_id], edge_property);
      break;

    default:
      weight = -1;
      break;
    }

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

    auto edge_dtype = grin_get_edge_property_datatype(graph_, edge_property);

    int32_t label;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
      label = grin_get_edge_property_value_of_int32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Int64:
      label = grin_get_edge_property_value_of_int64(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt32:
      label = grin_get_edge_property_value_of_uint32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt64:
      label = grin_get_edge_property_value_of_uint64(
        graph_, edge_list_[edge_id], edge_property);
      break;

    default:
      label = -1;
      break;
    }

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

    auto edge_dtype = grin_get_edge_property_datatype(graph_, edge_property);

    int64_t timestamp;
    switch (edge_dtype) {
    case GRIN_DATATYPE::Int32:
      timestamp = grin_get_edge_property_value_of_int32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Int64:
      timestamp = grin_get_edge_property_value_of_int64(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt32:
      timestamp = grin_get_edge_property_value_of_uint32(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::UInt64:
      timestamp = grin_get_edge_property_value_of_uint64(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Float:
      timestamp = grin_get_edge_property_value_of_float(
        graph_, edge_list_[edge_id], edge_property);
      break;
    case GRIN_DATATYPE::Double:
      timestamp = grin_get_edge_property_value_of_double(
        graph_, edge_list_[edge_id], edge_property);
      break;
    default:
      timestamp = -1;
      break;
    }

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

    for (auto property : attrs_) {
      auto dtype = grin_get_edge_property_datatype(graph_, property);
      switch(dtype) {
      case GRIN_DATATYPE::Int32:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_edge_property_value_of_int32(graph_, edge_list_[edge_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::UInt32:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_edge_property_value_of_uint32(graph_, edge_list_[edge_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::Int64:
        if (side_info_->i_num > 0) {
          attr->Add((int64_t)grin_get_edge_property_value_of_int64(graph_, edge_list_[edge_id], property));
        }
        break;
      case GRIN_DATATYPE::UInt64:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_edge_property_value_of_uint64(graph_, edge_list_[edge_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::Float:
        if (side_info_->f_num > 0) {
          attr->Add(grin_get_edge_property_value_of_float(graph_, edge_list_[edge_id], property));
        }
        break;
      case GRIN_DATATYPE::Double:
        if (side_info_->f_num > 0) {
          float v = grin_get_edge_property_value_of_double(graph_, edge_list_[edge_id], property);
          attr->Add(v);
        }
        break;
      
      case GRIN_DATATYPE::String:
        if (side_info_->s_num > 0) {
          std::string s = grin_get_edge_property_value_of_string(graph_, edge_list_[edge_id], property);
          attr->Add(s);
        }
        break;
      
      default:
        break;
      }
    }

    return Attribute(attr, true);
  }

  virtual Array<IdType> GetNeighbors(IdType src_id) const override {
    auto sz = indptr_[src_id + 1] - indptr_[src_id];
    std::shared_ptr<IdType> neighbors(new IdType[sz], std::default_delete<IdType[]>());
    IdType* nbr_ptr = neighbors.get();

    for (auto i = 0; i < sz; ++i) {
      nbr_ptr[i] = GetDstId(i + indptr_[src_id]);
    }

    return IdArray(neighbors.get(), sz, neighbors);
  }

  virtual Array<IdType> GetOutEdges(IdType src_id) const override {
    auto sz = indptr_[src_id + 1] - indptr_[src_id];
    std::shared_ptr<IdType> out_edges(new IdType[sz], std::default_delete<IdType[]>());
    IdType* oes_ptr = out_edges.get();
    std::iota(oes_ptr, oes_ptr + sz, indptr_[src_id]);
    return IdArray(oes_ptr, sz, out_edges);
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
    std::shared_ptr<IndexType> in_degrees(new IndexType[num_dst], 
                                          std::default_delete<IndexType[]>());
    IndexType* in_degrees_ptr = in_degrees.get();
    for (size_t i = 0; i < num_dst; ++i) {
      auto v = grin_get_vertex_from_list(graph_, dst_vertex_list, i);
      auto adj_in = grin_get_adjacent_list(graph_, GRIN_DIRECTION::IN, v);
      auto in_list = grin_select_edge_type_for_adjacent_list(
        graph_, edge_type_, adj_in);
      in_degrees_ptr[i] = grin_get_adjacent_list_size(graph_, in_list);
      grin_destroy_adjacent_list(graph_, in_list);
      grin_destroy_adjacent_list(graph_, adj_in);
      grin_destroy_vertex(graph_, v);
    }
    grin_destroy_vertex_list(graph_, dst_vertex_list);

    return IndexArray(in_degrees.get(), num_dst, in_degrees);
  }

  virtual const IndexArray GetAllOutDegrees() const override {
    if (indptr_.size() <= 1)
      return IndexArray();

    std::shared_ptr<IndexType> out_degrees(new IndexType[indptr_.size()],
                                           std::default_delete<IndexType[]>());
    IndexType* out_degrees_ptr = out_degrees.get();
    std::adjacent_difference(indptr_.begin(), indptr_.end(), out_degrees_ptr);
    return IndexArray(out_degrees.get() + 1, indptr_.size() - 1, out_degrees);
  }

  virtual const IdArray GetAllSrcIds() const override {
    std::shared_ptr<IdType> srcs(new IdType[indptr_.back()], 
                                 std::default_delete<IdType[]>());
    IdType* srcs_ptr = srcs.get();
    for (IdType i = 0; i < indptr_.size() - 1; ++i) {
      std::fill(srcs_ptr + indptr_[i], srcs_ptr + indptr_[i + 1], i);
    }

    return IdArray(srcs.get(), indptr_.back(), srcs);
  }

  virtual const IdArray GetAllDstIds() const override {
    auto num_dst = GetEdgeCount();
    std::shared_ptr<IdType> dst_ids(new IdType[num_dst], 
                                    std::default_delete<IdType[]>());
    IdType* dst_ids_ptr = dst_ids.get();
    for (IdType i = 0; i < num_dst; ++i) {
      dst_ids_ptr[i] = GetDstId(i);
    }

    return IdArray(dst_ids.get(), num_dst, dst_ids);
  }

private:
  friend class GrinEdgeStorage;  
  GRIN_PARTITIONED_GRAPH partitioned_graph_;
  GRIN_PARTITION_LIST local_partitions_;
  GRIN_PARTITION partition_;
  GRIN_GRAPH graph_;
  GRIN_EDGE_TYPE edge_type_;
  GRIN_VERTEX_TYPE src_type_;
  GRIN_VERTEX_TYPE dst_type_;
  std::vector<IdType> indptr_;
  std::vector<GRIN_EDGE> edge_list_;
  size_t num_vertices_;

  std::set<std::string> attrs_names_;
  std::set<uint64_t> attrs_;

  SideInfo *side_info_ = nullptr;

};

};

}

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_
