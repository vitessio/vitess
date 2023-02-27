/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

type shardWatcher struct {
	watchChan   <-chan *topo.WatchShardData
	watchCancel context.CancelFunc
}

func (sw *shardWatcher) active() bool {
	return sw.watchChan != nil
}

func (sw *shardWatcher) start(ts *topo.Server, keyspace, shard string) error {
	log.Infof("Starting shard watch of %v/%v", keyspace, shard)

	ctx, cancel := context.WithCancel(context.Background())
	_, c, err := ts.WatchShard(ctx, keyspace, shard)
	if err != nil {
		cancel()
		return err
	}

	sw.watchChan = c
	sw.watchCancel = cancel
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
