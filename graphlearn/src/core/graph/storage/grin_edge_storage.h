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

#ifndef GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_EDGE_STORAGE_H_
#define GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_EDGE_STORAGE_H_

#include <memory>

extern "C" {

#include "vineyard/graph/grin/include/topology/adjacentlist.h"
#include "vineyard/graph/grin/include/topology/structure.h"
#include "vineyard/graph/grin/include/topology/vertexlist.h"
#include "vineyard/graph/grin/include/topology/edgelist.h"
#include "vineyard/graph/grin/include/property/topology.h"
#include "vineyard/graph/grin/include/index/order.h"
#include "vineyard/graph/grin/include/property/type.h"
#include "vineyard/graph/grin/include/partition/partition.h"
}
#include "vineyard/graph/grin/src/predefine.h"

#include "core/graph/storage/edge_storage.h"
#include "core/graph/storage/grin_graph_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "include/config.h"

namespace graphlearn {

namespace io {

class GrinEdgeStorage : public EdgeStorage {
public:
  explicit GrinEdgeStorage(
    GRIN_PARTITIONED_GRAPH partitioned_graph, GRIN_PARTITION partition,
    const std::string& edge_type_name, const std::set<std::string>& attrs=std::set<std::string>()) {
      graph_ = new GrinGraphStorage(
        partitioned_graph, partition, edge_type_name, attrs);
  }

  virtual ~GrinEdgeStorage() = default;

  virtual void SetSideInfo(const SideInfo* info) override {}
  virtual const SideInfo* GetSideInfo() const override {
    return graph_->GetSideInfo();
  }

  /// Do some re-organization after data fixed.
  virtual void Build() override {}

  /// Get the total edge count after data fixed.
  virtual IdType Size() const override {
    return graph_->GetEdgeCount();
  }

  /// An EDGE is made up of [ src_id, dst_id, weight, label, timestamp, attributes ].
  /// Insert the value to get an unique id.
  /// If the value is invalid, return -1.
  virtual IdType Add(EdgeValue* value) {
    throw std::runtime_error("Not implemented");
  }

  /// Lookup edge infos by edge_id, including
  ///    source node id,
  ///    destination node id,
  ///    edge weight,
  ///    edge label,
  ///    edge timestamp,
  ///    edge attributes
  virtual IdType GetSrcId(IdType edge_id) const override {
    return graph_->GetSrcId(edge_id);
  }
  virtual IdType GetDstId(IdType edge_id) const override {
    return graph_->GetDstId(edge_id);
  }
  virtual float GetWeight(IdType edge_id) const override {
    return graph_->GetEdgeWeight(edge_id);
  }
  virtual int32_t GetLabel(IdType edge_id) const override {
    return graph_->GetEdgeLabel(edge_id);
  }
  virtual int64_t GetTimestamp(IdType edge_id) const override {
    return graph_->GetEdgeTimestamp(edge_id);
  }
  virtual Attribute GetAttribute(IdType edge_id) const override {
    return graph_->GetEdgeAttribute(edge_id);
  }

  /// For the needs of traversal and sampling, the data distribution is
  /// helpful. The interface should make it convenient to get the global data.
  ///
  /// Get all the source node ids, the count of which is the same with Size().
  /// These ids are not distinct.
  virtual const IdArray GetSrcIds() const override {
    return graph_->GetAllSrcIds();
  }

  /// Get all the destination node ids, the count of which is the same with
  /// Size(). These ids are not distinct.
  virtual const IdArray GetDstIds() const override {
    return graph_->GetAllDstIds();
  }
  /// Get all weights if existed, the count of which is the same with Size().
  virtual const Array<float> GetWeights() const {
    if (!graph_->side_info_->IsWeighted()) {
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
  virtual const Array<int32_t> GetLabels() const {
    if (!graph_->side_info_->IsLabeled()) {
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
  virtual const Array<int64_t> GetTimestamps() const {
    if (!graph_->side_info_->IsTimestamped()) {
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
  virtual const std::vector<Attribute>* GetAttributes() const {
    if (!graph_->side_info_->IsAttributed()) {
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
  GrinGraphStorage *graph_ = nullptr;
};
};

};

#endif // GRAPHLEARN_CORE_GRAPH_STORAGE_GRIN_GRAPH_STORAGE_H_
