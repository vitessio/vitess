/*
Copyright 2021 The ApeCloud.
*/

package main

import (
	"context"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus"
)

func main() {
	var clustersToWatch []string
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		// vtconsensus --clusters_to_watch="commerce/-0". Only one ks/shard currently.
		fs.StringSliceVar(&clustersToWatch, "clusters_to_watch", nil, `Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: "ks1,ks2/-80"`)

		acl.RegisterFlags(fs)
	})
	servenv.ParseFlags("vtconsensus")

	// openTabletDiscovery will open up a connection to topo server
	// and populate the tablets in memory,
	// include mysql instance hostname and port in every tablet.
	vtconsensus := vtconsensus.OpenTabletDiscovery(context.Background(), []string{"zone1"}, clustersToWatch)
	// get the latest tablets from topo server
	vtconsensus.RefreshCluster()
	// starts the scanAndFix tablet status.
	vtconsensus.ScanAndRepair()

	// block here so that we don't exit directly
	select {}
}
