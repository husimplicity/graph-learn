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

#include <iostream>
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
  explicit GrinNodeStorage(
    const std::string& node_type="",
    const std::string& use_attrs="") {

    auto node_type_name = node_type;
    boost::algorithm::split(attrs_, use_attrs, boost::is_any_of(","));

    // char* socket = new char[GLOBAL_FLAG(VineyardIPCSocket).size()];
    // std::strcpy(socket, GLOBAL_FLAG(VineyardIPCSocket).c_str());
    // char* gid = new char[std::to_string(GLOBAL_FLAG(VineyardGraphID)).size()];
    // std::strcpy(gid, std::to_string(GLOBAL_FLAG(VineyardGraphID)).c_str());

    char** argv = new char*[2];
    argv[0] = new char[GLOBAL_FLAG(VineyardIPCSocket).size()];
    std::strcpy(argv[0], GLOBAL_FLAG(VineyardIPCSocket).c_str());
    argv[1] = new char[std::to_string(GLOBAL_FLAG(VineyardGraphID)).size()];
    std::strcpy(argv[1], std::to_string(GLOBAL_FLAG(VineyardGraphID)).c_str());
    int argc = sizeof(argv) / sizeof(char*);
    std::cout << "argc: " << argc << " argv: " << argv[0] << " " << argv[1] <<std::endl;
    partitioned_graph_ = grin_get_partitioned_graph_from_storage(2, argv);
    local_partitions_ = grin_get_local_partition_list(partitioned_graph_);
    partition_ = grin_get_partition_from_list(
      partitioned_graph_, local_partitions_, 0);

    graph_ = grin_get_local_graph_by_partition(partitioned_graph_, partition_);
    side_info_ = init_node_side_info(
      partitioned_graph_, partition_, graph_, attrs_, node_type_name);
    vertex_type_ = grin_get_vertex_type_by_name(graph_, node_type_name.c_str());
    auto vl = GetVertexListByType(graph_, vertex_type_);
    num_vertices_ = grin_get_vertex_num_by_type(graph_, vertex_type_);
    vertex_list_.reserve(num_vertices_);

    for (size_t i = 0; i < num_vertices_; ++i) {
      auto v = grin_get_vertex_from_list(graph_, vl, i);
      vertex_list_.emplace_back(v);
    }
    grin_destroy_vertex_list(graph_, vl);
    delete[] argv;
    LOG(INFO) << "Create GrinNodeStorage Done." << Size();
  }

  virtual ~GrinNodeStorage() {
    // delete side_info_;
    for (auto v : vertex_list_) {
      grin_destroy_vertex(graph_, v);
    }
    grin_destroy_vertex_type(graph_, vertex_type_);
    grin_destroy_graph(graph_);
    // grin_destroy_partition(partitioned_graph_, partition_);
    // grin_destroy_partition_list(partitioned_graph_, local_partitions_);
    // grin_destroy_partitioned_graph(partitioned_graph_);
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
    auto node_dtype = grin_get_vertex_property_datatype(graph_, node_property);

    float weight;
    switch (node_dtype) {
    case GRIN_DATATYPE::Int32:
      weight = grin_get_vertex_property_value_of_int32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Int64:
      weight = grin_get_vertex_property_value_of_int64(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt32:
      weight = grin_get_vertex_property_value_of_uint32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt64:
      weight = grin_get_vertex_property_value_of_uint64(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Float:
      weight = grin_get_vertex_property_value_of_float(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Double:
      weight = grin_get_vertex_property_value_of_double(
        graph_, vertex_list_[node_id], node_property);
      break;

    default:
      weight = -1;
      break;
    }

    grin_destroy_vertex_property(graph_, node_property);

    return weight;
  }

  virtual int32_t GetLabel(IdType node_id) const override {
    if (!side_info_->IsLabeled()) {
      return -1;
    }

    auto node_property = grin_get_vertex_property_by_name(
      graph_, vertex_type_, std::string("label").c_str());
    auto node_dtype = grin_get_vertex_property_datatype(graph_, node_property);
    
    int32_t label;
    switch (node_dtype) {
    case GRIN_DATATYPE::Int32:
      label = grin_get_vertex_property_value_of_int32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Int64:
      label = grin_get_vertex_property_value_of_int64(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt32:
      label = grin_get_vertex_property_value_of_uint32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt64:
      label = grin_get_vertex_property_value_of_uint64(
        graph_, vertex_list_[node_id], node_property);
      break;

    default:
      label = -1;
      break;
    }

    grin_destroy_vertex_property(graph_, node_property);
    return label;    
  }

  virtual int64_t GetTimestamp(IdType node_id) const override {
    if (!side_info_->IsTimestamped()) {
      return -1;
    }

    auto node_property = grin_get_vertex_property_by_name(
      graph_, vertex_type_, std::string("timestamp").c_str());
    auto node_dtype = grin_get_vertex_property_datatype(graph_, node_property);
    
    int64_t timestamp;
    switch (node_dtype) {
    case GRIN_DATATYPE::Int32:
      timestamp = grin_get_vertex_property_value_of_int32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Int64:
      timestamp = grin_get_vertex_property_value_of_int64(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt32:
      timestamp = grin_get_vertex_property_value_of_uint32(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::UInt64:
      timestamp = grin_get_vertex_property_value_of_uint64(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Float:
      timestamp = grin_get_vertex_property_value_of_float(
        graph_, vertex_list_[node_id], node_property);
      break;
    case GRIN_DATATYPE::Double:
      timestamp = grin_get_vertex_property_value_of_double(
        graph_, vertex_list_[node_id], node_property);
      break;

    
    default:
      timestamp = -1;
      break;
    }

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

    auto property_size = grin_get_vertex_property_list_size(graph_, properties);
    for (size_t i = 0; i < property_size; ++i) {
      auto property = grin_get_vertex_property_from_list(graph_, properties, i);
      auto dtype = grin_get_vertex_property_datatype(graph_, property);
      switch(dtype) {
      case GRIN_DATATYPE::Int32:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_vertex_property_value_of_int32(graph_, vertex_list_[node_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::UInt32:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_vertex_property_value_of_uint32(graph_, vertex_list_[node_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::Int64:
        if (side_info_->i_num > 0) {
          attr->Add((int64_t)grin_get_vertex_property_value_of_int64(graph_, vertex_list_[node_id], property));
        }
        break;
      case GRIN_DATATYPE::UInt64:
        if (side_info_->i_num > 0) {
          int64_t v = grin_get_vertex_property_value_of_uint64(graph_, vertex_list_[node_id], property);
          attr->Add(v);
        }
        break;
      case GRIN_DATATYPE::Float:
        if (side_info_->f_num > 0) {
          attr->Add(grin_get_vertex_property_value_of_float(graph_, vertex_list_[node_id], property));
        }
        break;
      case GRIN_DATATYPE::Double:
        if (side_info_->f_num > 0) {
          float v = grin_get_vertex_property_value_of_double(graph_, vertex_list_[node_id], property);
          attr->Add(v);
        }
        break;
      
      case GRIN_DATATYPE::String:
        if (side_info_->s_num > 0) {
          std::string s = grin_get_vertex_property_value_of_string(graph_, vertex_list_[node_id], property);
          attr->Add(s);
        }
        break;
      
      default:
        break;
      }

      grin_destroy_vertex_property(graph_, property);
    }

    grin_destroy_vertex_property_list(graph_, properties);

    return Attribute(attr, true);
  }

  /// For the needs of traversal and sampling, the data distribution is
  /// helpful. The interface should make it convenient to get the global data.
  ///
  /// Get all the node ids, the count of which is the same with Size().
  /// These ids are distinct.
  virtual const IdArray GetIds() const override {
    std::shared_ptr<IdType> ids(new IdType[Size()],
                                std::default_delete<IdType[]>());
    IdType* ids_ptr = ids.get();
    std::iota(ids_ptr, ids_ptr + Size(), 0);
    return IdArray(ids.get(), Size(), ids);
  }

  /// Get all weights if existed, the count of which is the same with Size().
  virtual const Array<float> GetWeights() const override {
    if (!side_info_->IsWeighted()) {
      return Array<float>();
    }

    std::shared_ptr<float> weights(new float[Size()],
                                   std::default_delete<float[]>());
    auto weights_ptr = weights.get();
    std::generate(weights_ptr, weights_ptr + Size(), [this, i = 0] () mutable {
      return GetWeight(i++);
    });
    return Array<float>(weights_ptr, Size(), weights);
  }

  /// Get all labels if existed, the count of which is the same with Size().
  virtual const Array<int32_t> GetLabels() const override {
    if (!side_info_->IsLabeled()) {
      return Array<int32_t>();
    }

    std::shared_ptr<int32_t> labels(new int32_t[Size()],
                                    std::default_delete<int32_t[]>());
    auto labels_ptr = labels.get();
    std::generate(labels_ptr, labels_ptr + Size(), [this, i = 0] () mutable {
      return GetLabel(i++);
    });
    return Array<int32_t>(labels_ptr, Size(), labels);
  }

  /// Get all timestamps if existed, the count of which is the same with Size().
  virtual const Array<int64_t> GetTimestamps() const override {
    if (!side_info_->IsTimestamped()) {
      return Array<int64_t>();
    }

    std::shared_ptr<int64_t> timestamps(new int64_t[Size()],
                                        std::default_delete<int64_t[]>());
    auto timestamps_ptr = timestamps.get();  
    std::generate(timestamps_ptr, timestamps_ptr + Size(), [this, i = 0] () mutable {
      return GetTimestamp(i++);
    });
    return Array<int64_t>(timestamps_ptr, Size(), timestamps);
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
  GRIN_PARTITION_LIST local_partitions_;
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