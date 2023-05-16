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

#ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_NODE_STORAGE_H_
#define GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_NODE_STORAGE_H_

#include <cstdint>
#include <vector>
#include "core/graph/storage/types.h"

extern "C" {

#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/partition/partition.h"
}
#include "vineyard/graph/grin/src/predefine.h"

#include "core/graph/storage/node_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "include/config.h"

namespace graphlearn {
namespace io {

class GrinNodeStorage : public graphlearn::io::NodeStorage {
public:
  explicit GrinNodeStorage(GRIN_PARTITIONED_GRAPH partitioned_graph,
                           GRIN_PARTITION partition,
                           const std::string& node_type_name,
                           const std::set<std::string>& attrs):
      partitioned_graph_(partitioned_graph),
      partition_(partition),
      attrs_(attrs) {
    graph_ = grin_get_local_graph_from_partition(partitioned_graph_, partition_);
    side_info_ = init_node_side_info(
      partitioned_graph_, partition_, attrs_, node_type_name);
    vertex_type_ = grin_get_vertex_type_by_name(graph_, node_type_name.c_str());
    auto vl = GetVertexListByType(graph_, vertex_type_);
    num_vertices_ = grin_get_vertex_num_by_type(graph_, vertex_type_);
    vertex_list_.reserve(num_vertices_);

    for (size_t i = 0; i < num_vertices_; ++i) {
      auto v = grin_get_vertex_from_list(graph_, vl, i);
      vertex_list_.emplace_back(v);
    }
    grin_destroy_vertex_list(graph_, vl);
  }

  virtual ~GrinNodeStorage() {
    for (auto v : vertex_list_) {
      grin_destroy_vertex(graph_, v);
    }
    grin_destroy_vertex_type(graph_, vertex_type_);
    grin_destroy_graph(graph_);
    grin_destroy_partitioned_graph(partitioned_graph_);
    delete side_info_;
  }

  virtual void Lock() override {}
  virtual void Unlock() override {}

  virtual void SetSideInfo(const SideInfo* info) override {}
  virtual const SideInfo* GetSideInfo() const override {
    return side_info_;
  }

  /// Do some re-organization after data fixed.
  virtual void Build() override {}

  /// Get the total node count after data fixed.
  virtual IdType Size() const override {
    return num_vertices_;
  }

  /// A NODE is made up of [ id, weight, label, timestamp, attributes ].
  /// Insert a node. If a node with the same id existed, just ignore.
  virtual void Add(NodeValue* value) override {}

  /// Lookup node infos by node_id, including
  ///    node weight,
  ///    node label,
  ///    node timestamp,
  ///    node attributes
  virtual float GetWeight(IdType node_id) const override {
    if (!side_info_->IsWeighted()) {
      return -1;
    }

    auto node_property = grin_get_vertex_property_by_name(
      graph_, vertex_type_, std::string("weight").c_str());
    auto node_dtype = grin_get_vertex_property_data_type(graph_, node_property);
    auto node_table = grin_get_vertex_property_table_by_type(graph_, vertex_type_);
    auto weight_val = grin_get_value_from_vertex_property_table(
      graph_, node_table, vertex_list_[node_id], node_property);
    
    float weight;
    switch (node_dtype) {
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
      grin_destroy_value(graph_, node_dtype, weight_val);
    }
    grin_destroy_vertex_property_table(graph_, node_table);
    grin_destroy_vertex_property(graph_, node_property);

    return weight;
  }

  virtual int32_t GetLabel(IdType node_id) const override {
    if (!side_info_->IsLabeled()) {
      return -1;
    }

    auto node_property = grin_get_vertex_property_by_name(
      graph_, vertex_type_, std::string("label").c_str());
    auto node_dtype = grin_get_vertex_property_data_type(graph_, node_property);
    auto node_table = grin_get_vertex_property_table_by_type(graph_, vertex_type_);
    auto label_val = grin_get_value_from_vertex_property_table(
      graph_, node_table, vertex_list_[node_id], node_property);
    
    int32_t label;
    switch (node_dtype) {
    case GRIN_DATATYPE::Int32:
    case GRIN_DATATYPE::Int64:
    case GRIN_DATATYPE::UInt32:
    case GRIN_DATATYPE::UInt64:
    case GRIN_DATATYPE::Float:
    case GRIN_DATATYPE::Double:
      label = *static_cast<const int32_t*>(label_val);
    
    default:
      label = -1;
    }

    if (label_val != NULL) {
      grin_destroy_value(graph_, node_dtype, label_val);
    }
    grin_destroy_vertex_property_table(graph_, node_table);
    grin_destroy_vertex_property(graph_, node_property);

    return label;    
  }

  virtual int64_t GetTimestamp(IdType node_id) const override {
    if (!side_info_->IsTimestamped()) {
      return -1;
    }

    auto node_property = grin_get_vertex_property_by_name(
      graph_, vertex_type_, std::string("timestamp").c_str());
    auto node_dtype = grin_get_vertex_property_data_type(graph_, node_property);
    auto node_table = grin_get_vertex_property_table_by_type(graph_, vertex_type_);
    auto timestamp_val = grin_get_value_from_vertex_property_table(
      graph_, node_table, vertex_list_[node_id], node_property);
    
    int64_t timestamp;
    switch (node_dtype) {
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
      grin_destroy_value(graph_, node_dtype, timestamp_val);
    }
    grin_destroy_vertex_property_table(graph_, node_table);
    grin_destroy_vertex_property(graph_, node_property);

    return timestamp;
  }

  virtual Attribute GetAttribute(IdType node_id) const override {
    if (!side_info_->IsAttributed()) {
      return Attribute();
    }
    if (node_id >= vertex_list_.size()) {
      return Attribute(AttributeValue::Default(side_info_), false);
    }

    auto attr = NewDataHeldAttributeValue();

    auto properties = grin_get_vertex_property_list_by_type(
      graph_, vertex_type_);
    auto node_table = grin_get_vertex_property_table_by_type(
      graph_, vertex_type_);
    GRIN_ROW row = grin_get_row_from_vertex_property_table(
      graph_, node_table, vertex_list_[node_id], properties);

    auto property_size = grin_get_vertex_property_list_size(graph_, properties);
    for (size_t i = 0; i < property_size; ++i) {
      auto property = grin_get_vertex_property_from_list(graph_, properties, i);
      auto dtype = grin_get_vertex_property_data_type(graph_, property);
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
      grin_destroy_vertex_property(graph_, property);
    }

    grin_destroy_row(graph_, row);
    grin_destroy_vertex_property_table(graph_, node_table);
    grin_destroy_vertex_property_list(graph_, properties);

    return Attribute(attr, true);
  }

  /// For the needs of traversal and sampling, the data distribution is
  /// helpful. The interface should make it convenient to get the global data.
  ///
  /// Get all the node ids, the count of which is the same with Size().
  /// These ids are distinct.
  virtual const IdArray GetIds() const override {
    std::vector<IdType> ids(num_vertices_);
    std::iota(ids.begin(), ids.end(), 0);
    return IdArray(ids);
  }

  /// Get all weights if existed, the count of which is the same with Size().
  virtual const Array<float> GetWeights() const override {
    if (!side_info_->IsWeighted()) {
      return Array<float>();
    }

    std::vector<float> weights(Size());
    std::generate(weights.begin(), weights.end(), [this, i = 0] () mutable {
      return GetWeight(i++);
    });
    return Array<float>(weights);
  }

  /// Get all labels if existed, the count of which is the same with Size().
  virtual const Array<int32_t> GetLabels() const override {
    if (!side_info_->IsLabeled()) {
      return Array<int32_t>();
    }

    std::vector<int32_t> labels(Size());
    std::generate(labels.begin(), labels.end(), [this, i = 0] () mutable {
      return GetLabel(i++);
    });
    return Array<int32_t>(labels);
  }

  /// Get all timestamps if existed, the count of which is the same with Size().
  virtual const Array<int64_t> GetTimestamps() const override {
    if (!side_info_->IsTimestamped()) {
      return Array<int64_t>();
    }

    std::vector<int64_t> timestamps(Size());
    std::generate(timestamps.begin(), timestamps.end(), [this, i = 0] () mutable {
      return GetTimestamp(i++);
    });
    return Array<int64_t>(timestamps);
  }

  /// Get all attributes if existed, the count of which is the same with Size().
  virtual const std::vector<Attribute>* GetAttributes() const override {
    if (!side_info_->IsAttributed()) {
      return nullptr;
    }

    auto attributes = new std::vector<Attribute>();
    attributes->reserve(Size());
    for (int32_t i = 0; i < Size(); ++i) {
      attributes->emplace_back(GetAttribute(i));
    }
    return attributes;
  }

private:
  GRIN_PARTITIONED_GRAPH partitioned_graph_;
  GRIN_PARTITION partition_;
  GRIN_GRAPH graph_;

  GRIN_VERTEX_TYPE vertex_type_;
  std::vector<GRIN_VERTEX> vertex_list_;
  size_t num_vertices_;

  std::set<std::string> attrs_;

  SideInfo *side_info_ = nullptr;

};

}
}

#endif  // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_NODE_STORAGE_H_