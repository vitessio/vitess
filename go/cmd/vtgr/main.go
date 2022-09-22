/*
Copyright 2021 The Vitess Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgr"
)

func main() {
	var clustersToWatch []string
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.StringSliceVar(&clustersToWatch, "clusters_to_watch", nil, `Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: "ks1,ks2/-80"`)
	})

	servenv.ParseFlags("vtgr")

	// openTabletDiscovery will open up a connection to topo server
	// and populate the tablets in memory
	vtgr := vtgr.OpenTabletDiscovery(context.Background(), nil, clustersToWatch)
	vtgr.RefreshCluster()
	vtgr.ScanAndRepair()

	// block here so that we don't exit directly
	select {}
}
