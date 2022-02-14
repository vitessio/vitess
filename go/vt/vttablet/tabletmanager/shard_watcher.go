package tabletmanager

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

type shardWatcher struct {
	watchChan   <-chan *topo.WatchShardData
	watchCancel topo.CancelFunc
}

func (sw *shardWatcher) active() bool {
	return sw.watchChan != nil
}

func (sw *shardWatcher) start(ctx context.Context, ts *topo.Server, keyspace, shard string) error {
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	log.Infof("Starting shard watch of %v/%v", keyspace, shard)

	event, c, watchCancel := ts.WatchShard(ctx, keyspace, shard)
	if event.Err != nil {
		return event.Err
	}

	sw.watchChan = c
	sw.watchCancel = watchCancel
	return nil
}

func (sw *shardWatcher) stop() {
	if !sw.active() {
		return
	}

	log.Infof("Stopping shard watch...")
	sw.watchCancel()

	// Drain all remaining watch events.
	for range sw.watchChan {
	}
	log.Infof("Shard watch stopped.")

	sw.watchChan = nil
	sw.watchCancel = nil
}
