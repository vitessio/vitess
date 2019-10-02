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
	"golang.org/x/net/context"

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

	sw.watchCancel()

	// Drain all remaining watch events.
	log.Infof("Stopping shard watch...")
	for range sw.watchChan {
	}

	sw.watchChan = nil
	sw.watchCancel = nil
}
