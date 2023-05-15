#include <stdio.h>

#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "include/config.h"
#include "core/graph/graph_store.h"


#include "core/graph/storage/grin_edge_storage.h"
#include "core/graph/storage/grin_graph_storage.h"
#include "core/graph/storage/grin_node_storage.h"
#include "core/graph/storage/grin_storage_utils.h"
#include "core/graph/storage/grin_topo_storage.h"

using namespace graphlearn::io;

int main(int argc, char **argv) {

#if defined(WITH_VINEYARD)
  return 1;
#else
  return 0;
#endif
  
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);
  std::string obj_id = std::string(argv[index++]);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  grape::InitMPIComm();

  {

  auto pg = grin_get_partitioned_graph_from_storage(2, argv);
  GRIN_PARTITION_LIST local_partitions = grin_get_local_partition_list(pg);
  auto partition = grin_get_partition_from_list(pg, local_partitions, 0);
  GRIN_GRAPH g = grin_get_local_graph_from_partition(pg, partition);

  }

  grape::FinalizeMPIComm();
  
  return 0;
}