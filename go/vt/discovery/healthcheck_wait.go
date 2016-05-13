package discovery

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// ErrWaitForEndPointsTimeout is returned if we cannot get the endpoints in time
	ErrWaitForEndPointsTimeout = errors.New("timeout waiting for endpoints")

	// how much to sleep between each check
	waitAvailableEndPointInterval = 100 * time.Millisecond
)

// keyspaceShard is a helper structure used internally
type keyspaceShard struct {
	keyspace string
	shard    string
}

// WaitForEndPoints waits for at least one endpoint in the given cell /
// keyspace / shard before returning.
func WaitForEndPoints(ctx context.Context, hc HealthCheck, cell, keyspace, shard string, types []topodatapb.TabletType) error {
	keyspaceShards := map[keyspaceShard]bool{
		keyspaceShard{
			keyspace: keyspace,
			shard:    shard,
		}: true,
	}
	return waitForEndPoints(ctx, hc, keyspaceShards, types, false)
}

// WaitForAllServingEndPoints waits for at least one serving endpoint in the given cell
// for all keyspaces / shards before returning.
func WaitForAllServingEndPoints(ctx context.Context, hc HealthCheck, ts topo.SrvTopoServer, cell string, types []topodatapb.TabletType) error {
	keyspaceShards, err := findAllKeyspaceShards(ctx, ts, cell)
	if err != nil {
		return err
	}

	return waitForEndPoints(ctx, hc, keyspaceShards, types, true)
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

// waitForEndPoints is the internal method that polls for endpoints
func waitForEndPoints(ctx context.Context, hc HealthCheck, keyspaceShards map[keyspaceShard]bool, types []topodatapb.TabletType, requireServing bool) error {
RetryLoop:
	for {
		select {
		case <-ctx.Done():
			break RetryLoop
		default:
			// Context is still valid. Move on.
		}

		for ks := range keyspaceShards {
			allPresent := true
			for _, tt := range types {
				epl := hc.GetEndPointStatsFromTarget(ks.keyspace, ks.shard, tt)
				if requireServing {
					hasServingEP := false
					for _, eps := range epl {
						if eps.LastError == nil && eps.Serving {
							hasServingEP = true
							break
						}
					}
					if !hasServingEP {
						allPresent = false
						break
					}
				} else {
					if len(epl) == 0 {
						allPresent = false
						break
					}
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
		timer := time.NewTimer(waitAvailableEndPointInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	if ctx.Err() == context.DeadlineExceeded {
		log.Warningf("waitForEndPoints timeout for %v (context error: %v)", keyspaceShards, ctx.Err())
		return ErrWaitForEndPointsTimeout
	}
	err := fmt.Errorf("waitForEndPoints failed for %v (context error: %v)", keyspaceShards, ctx.Err())
	log.Error(err)
	return err
}
