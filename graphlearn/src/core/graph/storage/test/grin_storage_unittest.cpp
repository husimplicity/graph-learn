#include <stdio.h>

#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"

extern "C" {
#include "vineyard/graph/grin/include/partition/partition.h"
}
#include "vineyard/graph/grin/src/predefine.h"

#include "core/graph/storage/grin_edge_storage.h"
#include "core/graph/storage/grin_graph_storage.h"
#include "core/graph/storage/grin_node_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "core/graph/storage/grin_topo_storage.h"

#include "core/graph/graph_store.h"
#include "include/config.h"
using namespace graphlearn::io;

int main(int argc, char **argv) {
  
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);
  std::string obj_id = std::string(argv[index++]);
  char* argv2[] = {argv[1], argv[2]};

  grape::InitMPIComm();

  LOG(INFO) << argc << " " << ipc_socket << " " << obj_id << std::endl;
  LOG(INFO) << "Note: This grin test is for ogbn_mag_small dataset.";
  graphlearn::SetGlobalFlagVineyardGraphID(std::stoll(obj_id));
  graphlearn::SetGlobalFlagVineyardIPCSocket(ipc_socket);

  {
    LOG(INFO) << "Topo tests";
    auto edge_store = std::make_shared<GrinEdgeStorage>("writes");

    CHECK_EQ(edge_store->Size(), 394980);
    CHECK_EQ(edge_store->GetSrcId(0), 0);
    CHECK_EQ(edge_store->GetSrcId(2), 1);
    CHECK_EQ(edge_store->GetSrcId(7), edge_store->GetSrcId(8));
    CHECK_EQ(edge_store->GetDstId(0), edge_store->GetDstId(1433));

    auto topo_store = std::make_shared<GrinTopoStorage>("writes");
    CHECK_EQ(topo_store->GetInDegree(edge_store->GetDstId(0)), 5);
    CHECK_EQ(topo_store->GetInDegree(edge_store->GetDstId(6)), 6);
    CHECK_EQ(topo_store->GetOutDegree(edge_store->GetSrcId(1)), 2);
    CHECK_EQ(topo_store->GetOutDegree(edge_store->GetSrcId(12)), 4);
    CHECK_EQ(topo_store->GetNeighbors(edge_store->GetSrcId(1)).Size(), 2);
    auto nbrs = topo_store->GetNeighbors(edge_store->GetSrcId(12));
    for (int i = 0; i < nbrs.Size(); ++i) {
      CHECK_EQ(nbrs[i], edge_store->GetDstId(12 + i));
    }
    auto outEdges = topo_store->GetOutEdges(edge_store->GetSrcId(12));
    for (int i = 0; i < outEdges.Size(); ++i) {
      CHECK_EQ(outEdges[i], 12 + i);
    }

    CHECK_EQ(topo_store->GetAllInDegrees()[edge_store->GetDstId(6)], 6);
    CHECK_EQ(topo_store->GetAllInDegrees()[edge_store->GetDstId(0)], 5);

    CHECK_EQ(topo_store->GetAllOutDegrees()[edge_store->GetSrcId(12)], 4);
    CHECK_EQ(topo_store->GetAllOutDegrees()[edge_store->GetSrcId(1)], 2);

    CHECK_EQ(topo_store->GetAllSrcIds().Size(), edge_store->Size());
    LOG(INFO) << "Done!";
  
    LOG(INFO) << "Feature tests";
    auto node_store = std::make_shared<GrinNodeStorage>("paper", "feat_0,feat_1,feat_2,label");
    CHECK_EQ(node_store->Size(), 40000);
    CHECK_EQ(node_store->GetLabel(4), 55);
    CHECK_EQ(node_store->GetLabel(0), 95);

    CHECK_EQ(node_store->GetLabels().Size(), node_store->Size());
    CHECK_EQ(node_store->GetLabels()[4], 55);
  
    CHECK_NEAR(
      node_store->GetAttribute(5)->GetFloats(nullptr)[0], 0.005732, 0.000001);
    CHECK_NEAR(
      node_store->GetAttribute(0)->GetFloats(nullptr)[1], -0.080044, 0.000001);
    CHECK_EQ(node_store->GetAttributes()->size(), node_store->Size());
    CHECK_NEAR(
      node_store->GetAttributes()->at(5)->GetFloats(nullptr)[0], 0.005732, 0.000001);

    LOG(INFO) << "Done!";
  }

  grape::FinalizeMPIComm();
  
  return 0;
}