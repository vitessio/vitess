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

package vtconsensus

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtconsensus/controller"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	refreshInterval       = 10 * time.Second
	scanInterval          = 3 * time.Second
	scanAndRepairTimeout  = 3 * time.Second
	vtconsensusConfigFile string

	localDbPort int
)

func init() {
	// vtconsensus --refresh_interval=10 --scan_interval=3 --scan_repair_timeout=3 --vtconsensus_config="config.file" --db_port=3306
	// vtconsensus_config is required.
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.DurationVar(&refreshInterval, "refresh_interval", 10*time.Second, "Refresh interval to load tablets.")
		fs.DurationVar(&scanInterval, "scan_interval", 3*time.Second, "Scan interval to diagnose and repair.")
		fs.DurationVar(&scanAndRepairTimeout, "scan_repair_timeout", 3*time.Second, "Time to wait for a Diagnose and repair operation.")
		fs.StringVar(&vtconsensusConfigFile, "vtconsensus_config", "", "Config file for vtconsensus.")
		fs.IntVar(&localDbPort, "db_port", 0, "Local mysql port, set this to enable local fast check.")
	})
}

// VTConsensus is the interface to manage the component to set up ApeCloud MySQL with Vitess.
// The main goal of it is to reconcile ApeCloud MySQL and the Vitess topology.
// Caller should use OpenTabletDiscovery to create the VTConsensus instance.
// ApeCloud MySQL Only one shard.
type VTConsensus struct {
	// Shards are all the shards that a VTConsenus is monitoring.
	// Caller can choose to iterate the shards to scan and repair for more granular control (e.g., stats report)
	// instead of calling ScanAndRepair() directly.
	Shards []*controller.ConsensusShard
	topo   controller.ConsensusTopo
	tmc    tmclient.TabletManagerClient
	ctx    context.Context

	stopped sync2.AtomicBool
}

func newVTConsensus(ctx context.Context, ts controller.ConsensusTopo, tmc tmclient.TabletManagerClient) *VTConsensus {
	return &VTConsensus{
		topo: ts,
		tmc:  tmc,
		ctx:  ctx,
	}
}

// OpenTabletDiscovery calls OpenTabletDiscoveryWithAcitve and set the shard to be active
// it opens connection with topo server
// and triggers the first round of controller based on specified cells and keyspace/shards.
func OpenTabletDiscovery(ctx context.Context, cellsToWatch, clustersToWatch []string) *VTConsensus {
	return OpenTabletDiscoveryWithAcitve(ctx, cellsToWatch, clustersToWatch, true)
}

// OpenTabletDiscoveryWithAcitve opens connection with topo server
// and triggers the first round of controller based on parameter
func OpenTabletDiscoveryWithAcitve(ctx context.Context, cellsToWatch, clustersToWatch []string, active bool) *VTConsensus {
	// if vtconsensusConfigFile == "" {
	// 	log.Fatal("vtconsensus_config is required")
	// }
	// config, err := config.ReadVTConsensusConfig(vtconsensusConfigFile)
	// if err != nil {
	//	log.Fatalf("Cannot load vtconsensus config file: %v", err)
	// }

	vtconsensus := newVTConsensus(
		ctx,
		topo.Open(),
		tmclient.NewTabletManagerClient(),
	)
	var shards []*controller.ConsensusShard
	ctx, cancel := context.WithTimeout(vtconsensus.ctx, topo.RemoteOperationTimeout)
	defer cancel()
	for _, ks := range clustersToWatch {
		if strings.Contains(ks, "/") {
			// This is a keyspace/shard specification
			input := strings.Split(ks, "/")
			// input[0] is commerce, input[1] is 0, if ks is commerce and shard is 0.
			shards = append(shards, controller.NewConsensusShard(
				input[0],
				input[1],
				cellsToWatch,
				vtconsensus.tmc,
				vtconsensus.topo,
				db.NewVTConsensusSqlAgent(),
				nil,
				localDbPort,
				active))
		} else {
			// Assume this is a keyspace and find all shards in keyspace
			shardNames, err := vtconsensus.topo.GetShardNames(ctx, ks)
			if err != nil {
				// Log the error and continue
				log.Errorf("Error fetching shards for keyspace %v: %v", ks, err)
				continue
			}
			if len(shardNames) == 0 {
				log.Errorf("Topo has no shards for ks: %v", ks)
				continue
			}
			for _, s := range shardNames {
				shards = append(shards, controller.NewConsensusShard(ks, s, cellsToWatch, vtconsensus.tmc, vtconsensus.topo, db.NewVTConsensusSqlAgent(), nil, localDbPort, active))
			}
		}
	}
	vtconsensus.handleSignal(os.Exit)
	vtconsensus.Shards = shards
	log.Infof("Monitoring shards size %v", len(vtconsensus.Shards))
	// Force refresh all tablet here to populate data for vtconsensus
	var wg sync.WaitGroup
	for _, shard := range vtconsensus.Shards {
		wg.Add(1)
		go func(shard *controller.ConsensusShard) {
			defer wg.Done()
			shard.UpdateTabletsInShardWithLock(ctx)
		}(shard)
	}
	wg.Wait()
	log.Info("Ready to start VTConsensus")
	return vtconsensus
}

// RefreshCluster get the latest tablets from topo server
func (vtconsensus *VTConsensus) RefreshCluster() {
	for _, shard := range vtconsensus.Shards {
		// start thread
		go func(shard *controller.ConsensusShard) {
			// period timer task, refresh tablet info in shard.
			ticker := time.Tick(refreshInterval)
			for range ticker {
				ctx, cancel := context.WithTimeout(vtconsensus.ctx, refreshInterval)
				shard.UpdateTabletsInShardWithLock(ctx)
				cancel()
			}
		}(shard)
	}
}

// ScanAndRepair starts the scanAndFix routine
func (vtconsensus *VTConsensus) ScanAndRepair() {
	for _, shard := range vtconsensus.Shards {
		go func(shard *controller.ConsensusShard) {
			ticker := time.Tick(scanInterval)
			for range ticker {
				func() {
					ctx, cancel := context.WithTimeout(vtconsensus.ctx, scanAndRepairTimeout)
					defer cancel()
					if !vtconsensus.stopped.Get() {
						log.Infof("Start scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
						shard.ScanAndRepairShard(ctx)
						log.Infof("Finished scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
					}
				}()
			}
		}(shard)
	}
}

func (vtconsensus *VTConsensus) handleSignal(action func(int)) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		// block until the signal is received
		<-sigChan
		log.Infof("Handling SIGHUP")
		// Set stopped to true so that following repair call won't do anything
		// For the ongoing repairs, checkShardLocked will abort if needed
		vtconsensus.stopped.Set(true)
		for _, shard := range vtconsensus.Shards {
			shard.UnlockShard()
		}
		action(1)
	}()
}
