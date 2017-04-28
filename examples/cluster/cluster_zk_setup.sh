source $script_root/cluster-env.sh
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/test \
  -server_address $ZK_SERVER \
  test || /bin/true
  