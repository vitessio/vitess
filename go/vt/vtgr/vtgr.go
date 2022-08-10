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

package vtgr

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgr/config"
	"vitess.io/vitess/go/vt/vtgr/controller"
	"vitess.io/vitess/go/vt/vtgr/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	refreshInterval      = 10 * time.Second
	scanInterval         = 3 * time.Second
	scanAndRepairTimeout = 3 * time.Second
	vtgrConfigFile       string

	localDbPort int
)

func init() {
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.DurationVar(&refreshInterval, "refresh_interval", 10*time.Second, "Refresh interval to load tablets.")
		fs.DurationVar(&scanInterval, "scan_interval", 3*time.Second, "Scan interval to diagnose and repair.")
		fs.DurationVar(&scanAndRepairTimeout, "scan_repair_timeout", 3*time.Second, "Time to wait for a Diagnose and repair operation.")
		fs.StringVar(&vtgrConfigFile, "vtgr_config", "", "Config file for vtgr.")
		fs.IntVar(&localDbPort, "db_port", 0, "Local mysql port, set this to enable local fast check.")
	})
}

// VTGR is the interface to manage the component to set up group replication with Vitess.
// The main goal of it is to reconcile MySQL group and the Vitess topology.
// Caller should use OpenTabletDiscovery to create the VTGR instance.
type VTGR struct {
	// Shards are all the shards that a VTGR is monitoring.
	// Caller can choose to iterate the shards to scan and repair for more granular control (e.g., stats report)
	// instead of calling ScanAndRepair() directly.
	Shards []*controller.GRShard
	topo   controller.GRTopo
	tmc    tmclient.TabletManagerClient
	ctx    context.Context

	stopped sync2.AtomicBool
}

func newVTGR(ctx context.Context, ts controller.GRTopo, tmc tmclient.TabletManagerClient) *VTGR {
	return &VTGR{
		topo: ts,
		tmc:  tmc,
		ctx:  ctx,
	}
}

// OpenTabletDiscovery calls OpenTabletDiscoveryWithAcitve and set the shard to be active
// it opens connection with topo server
// and triggers the first round of controller based on specified cells and keyspace/shards.
func OpenTabletDiscovery(ctx context.Context, cellsToWatch, clustersToWatch []string) *VTGR {
	return OpenTabletDiscoveryWithAcitve(ctx, cellsToWatch, clustersToWatch, true)
}

// OpenTabletDiscoveryWithAcitve opens connection with topo server
// and triggers the first round of controller based on parameter
func OpenTabletDiscoveryWithAcitve(ctx context.Context, cellsToWatch, clustersToWatch []string, active bool) *VTGR {
	if vtgrConfigFile == "" {
		log.Fatal("vtgr_config is required")
	}
	config, err := config.ReadVTGRConfig(vtgrConfigFile)
	if err != nil {
		log.Fatalf("Cannot load vtgr config file: %v", err)
	}
	vtgr := newVTGR(
		ctx,
		topo.Open(),
		tmclient.NewTabletManagerClient(),
	)
	var shards []*controller.GRShard
	ctx, cancel := context.WithTimeout(vtgr.ctx, *topo.RemoteOperationTimeout)
	defer cancel()
	for _, ks := range clustersToWatch {
		if strings.Contains(ks, "/") {
			// This is a keyspace/shard specification
			input := strings.Split(ks, "/")
			shards = append(shards, controller.NewGRShard(input[0], input[1], cellsToWatch, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, localDbPort, active))
		} else {
			// Assume this is a keyspace and find all shards in keyspace
			shardNames, err := vtgr.topo.GetShardNames(ctx, ks)
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
				shards = append(shards, controller.NewGRShard(ks, s, cellsToWatch, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, localDbPort, active))
			}
		}
	}
	vtgr.handleSignal(os.Exit)
	vtgr.Shards = shards
	log.Infof("Monitoring shards size %v", len(vtgr.Shards))
	// Force refresh all tablet here to populate data for vtgr
	var wg sync.WaitGroup
	for _, shard := range vtgr.Shards {
		wg.Add(1)
		go func(shard *controller.GRShard) {
			defer wg.Done()
			shard.UpdateTabletsInShardWithLock(ctx)
		}(shard)
	}
	wg.Wait()
	log.Info("Ready to start VTGR")
	return vtgr
}

// RefreshCluster get the latest tablets from topo server
func (vtgr *VTGR) RefreshCluster() {
	for _, shard := range vtgr.Shards {
		go func(shard *controller.GRShard) {
			ticker := time.Tick(refreshInterval)
			for range ticker {
				ctx, cancel := context.WithTimeout(vtgr.ctx, refreshInterval)
				shard.UpdateTabletsInShardWithLock(ctx)
				cancel()
			}
		}(shard)
	}
}

// ScanAndRepair starts the scanAndFix routine
func (vtgr *VTGR) ScanAndRepair() {
	for _, shard := range vtgr.Shards {
		go func(shard *controller.GRShard) {
			ticker := time.Tick(scanInterval)
			for range ticker {
				func() {
					ctx, cancel := context.WithTimeout(vtgr.ctx, scanAndRepairTimeout)
					defer cancel()
					if !vtgr.stopped.Get() {
						log.Infof("Start scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
						shard.ScanAndRepairShard(ctx)
						log.Infof("Finished scan and repair %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
					}
				}()
			}
		}(shard)
	}
}

// Diagnose exposes the endpoint to diagnose a particular shard
func (vtgr *VTGR) Diagnose(ctx context.Context, shard *controller.GRShard) (controller.DiagnoseType, error) {
	return shard.Diagnose(ctx)
}

// Repair exposes the endpoint to repair a particular shard
func (vtgr *VTGR) Repair(ctx context.Context, shard *controller.GRShard, diagnose controller.DiagnoseType) (controller.RepairResultCode, error) {
	if vtgr.stopped.Get() {
		return controller.Fail, errors.New("VTGR is stopped")
	}
	return shard.Repair(ctx, diagnose)
}

// GetCurrentShardStatuses is used when we want to know what VTGR observes
// it contains information about a list of instances and primary tablet
func (vtgr *VTGR) GetCurrentShardStatuses() []controller.ShardStatus {
	var result []controller.ShardStatus
	for _, shard := range vtgr.Shards {
		status := shard.GetCurrentShardStatuses()
		result = append(result, status)
	}
	return result
}

// OverrideRebootstrapGroupSize forces an override the group size used in safety check for rebootstrap
func (vtgr *VTGR) OverrideRebootstrapGroupSize(groupSize int) error {
	errorRecord := concurrency.AllErrorRecorder{}
	for _, shard := range vtgr.Shards {
		err := shard.OverrideRebootstrapGroupSize(groupSize)
		if err != nil {
			errorRecord.RecordError(err)
		}
	}
	return errorRecord.Error()
}

func (vtgr *VTGR) handleSignal(action func(int)) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		// block until the signal is received
		<-sigChan
		log.Infof("Handling SIGHUP")
		// Set stopped to true so that following repair call won't do anything
		// For the ongoing repairs, checkShardLocked will abort if needed
		vtgr.stopped.Set(true)
		for _, shard := range vtgr.Shards {
			shard.UnlockShard()
		}
		action(1)
	}()
}
