package main

import (
	"flag"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/vtgr"
)

func main() {
	clustersToWatch := flag.String("clusters_to_watch", "", "Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: \"ks1,ks2/-80\"")
	flag.Parse()

	// openTabletDiscovery will open up a connection to topo server
	// and populate the tablets in memory
	vtgr := vtgr.OpenTabletDiscovery(context.Background(), nil, strings.Split(*clustersToWatch, ","))
	vtgr.RefreshCluster()
	vtgr.ScanAndRepair()

	// block here so that we don't exit directly
	select {}
}
