#!/bin/bash

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/cluster-env.sh

echo "Creating nodes in zookeeper..."
/opt/zookeeper/bin/zkCli.sh -server localhost:2181 <<EOF
create /vitess vitess_base
create /vitess/global vitess_global
create /vitess/$cell vitess_$cell
quit
EOF

echo "Adding cell info in zookeeper..."
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/$cell \
  -server_address $ZK_SERVER \
  $cell || /bin/true
