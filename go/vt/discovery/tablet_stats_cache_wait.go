package discovery

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// how much to sleep between each check
	waitAvailableTabletInterval = 100 * time.Millisecond
)

// keyspaceShard is a helper structure used internally
type keyspaceShard struct {
	keyspace string
	shard    string
}

// WaitForTablets waits for at least one tablet in the given cell /
// keyspace / shard before returning. The tablets do not have to be healthy.
// It will return ctx.Err() if the context is canceled.
func (tc *TabletStatsCache) WaitForTablets(ctx context.Context, cell, keyspace, shard string, types []topodatapb.TabletType) error {
	keyspaceShards := map[keyspaceShard]bool{
		keyspaceShard{
			keyspace: keyspace,
			shard:    shard,
		}: true,
	}
	return tc.waitForTablets(ctx, keyspaceShards, types, false)
}

// WaitForAllServingTablets waits for at least one healthy serving tablet in
// the given cell for all keyspaces / shards before returning.
// It will return ctx.Err() if the context is canceled.
func (tc *TabletStatsCache) WaitForAllServingTablets(ctx context.Context, ts topo.SrvTopoServer, cell string, types []topodatapb.TabletType) error {
	keyspaceShards, err := findAllKeyspaceShards(ctx, ts, cell)
	if err != nil {
		return err
	}

	return tc.waitForTablets(ctx, keyspaceShards, types, true)
}

// findAllKeyspaceShards goes through all serving shards in the topology
func findAllKeyspaceShards(ctx context.Context, ts topo.SrvTopoServer, cell string) (map[keyspaceShard]bool, error) {
	ksNames, err := ts.GetSrvKeyspaceNames(ctx, cell)
	if err != nil {
		return nil, err
	}

	keyspaceShards := make(map[keyspaceShard]bool)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errRecorder concurrency.AllErrorRecorder
	for _, ksName := range ksNames {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			// get SrvKeyspace for cell/keyspace
			ks, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				errRecorder.RecordError(err)
				return
			}

			// get all shard names that are used for serving
			mu.Lock()
			for _, ksPartition := range ks.Partitions {
				for _, shard := range ksPartition.ShardReferences {
					keyspaceShards[keyspaceShard{
						keyspace: keyspace,
						shard:    shard.Name,
					}] = true
				}
			}
			mu.Unlock()
		}(ksName)
	}
	wg.Wait()
	if errRecorder.HasErrors() {
		return nil, errRecorder.Error()
	}

	return keyspaceShards, nil
}

// waitForTablets is the internal method that polls for tablets
func (tc *TabletStatsCache) waitForTablets(ctx context.Context, keyspaceShards map[keyspaceShard]bool, types []topodatapb.TabletType, requireServing bool) error {
	for {
		for ks := range keyspaceShards {
			allPresent := true
			for _, tt := range types {
				var stats []TabletStats
				if requireServing {
					stats = tc.GetHealthyTabletStats(ks.keyspace, ks.shard, tt)
				} else {
					stats = tc.GetTabletStats(ks.keyspace, ks.shard, tt)
				}
				if len(stats) == 0 {
					allPresent = false
					break
				}
			}

			if allPresent {
				delete(keyspaceShards, ks)
			}
		}

		if len(keyspaceShards) == 0 {
			// we found everything we needed
			return nil
		}

		// Unblock after the sleep or when the context has expired.
		timer := time.NewTimer(waitAvailableTabletInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
