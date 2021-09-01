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

package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type KeyspaceEventWatcher struct {
	ts        srvtopo.Server
	hc        HealthCheck
	localCell string

	mu        sync.Mutex
	keyspaces map[string]*keyspaceState

	subsMu sync.Mutex
	subs   map[chan *KeyspaceEvent]struct{}
}

type KeyspaceEvent struct {
	Cell     string
	Keyspace string
	Shards   []ShardEvent
}

type ShardEvent struct {
	Tablet  *topodatapb.TabletAlias
	Target  *query.Target
	Serving bool
}

func NewKeyspaceEventWatcher(ctx context.Context, topoServer srvtopo.Server, hc HealthCheck, localCell string) *KeyspaceEventWatcher {
	kew := &KeyspaceEventWatcher{
		hc:        hc,
		ts:        topoServer,
		localCell: localCell,
		keyspaces: make(map[string]*keyspaceState),
		subs:      make(map[chan *KeyspaceEvent]struct{}),
	}
	kew.run(ctx)
	log.Infof("started watching keyspace events in %q", localCell)
	return kew
}

type keyspaceState struct {
	kew      *KeyspaceEventWatcher
	keyspace string

	mu         sync.Mutex
	deleted    bool
	consistent bool

	lastError    error
	lastKeyspace *topodatapb.SrvKeyspace
	shards       map[string]*shardState
}

func (kss *keyspaceState) Format(f fmt.State, verb rune) {
	kss.mu.Lock()
	defer kss.mu.Unlock()

	fmt.Fprintf(f, "Keyspace(%s) = deleted: %v, consistent: %v, shards: [\n", kss.keyspace, kss.deleted, kss.consistent)
	for shard, ss := range kss.shards {
		fmt.Fprintf(f, "  Shard(%s) = target: [%s/%s %v], serving: %v, externally_reparented: %d, current_primary: %s\n",
			shard,
			ss.target.Keyspace, ss.target.Shard, ss.target.TabletType,
			ss.serving, ss.externallyReparented,
			ss.currentPrimary.String(),
		)
	}
	fmt.Fprintf(f, "]\n")
}

func (kss *keyspaceState) beingResharded(currentShard string) bool {
	kss.mu.Lock()
	defer kss.mu.Unlock()

	if kss.deleted || kss.consistent {
		return false
	}

	for shard, sstate := range kss.shards {
		if shard != currentShard && sstate.serving {
			return true
		}
	}

	return false
}

type shardState struct {
	target               *query.Target
	serving              bool
	externallyReparented int64
	currentPrimary       *topodatapb.TabletAlias
}

func (kew *KeyspaceEventWatcher) Subscribe() chan *KeyspaceEvent {
	kew.subsMu.Lock()
	defer kew.subsMu.Unlock()
	c := make(chan *KeyspaceEvent, 2)
	kew.subs[c] = struct{}{}
	return c
}

// Unsubscribe removes a listener.
func (kew *KeyspaceEventWatcher) Unsubscribe(c chan *KeyspaceEvent) {
	kew.subsMu.Lock()
	defer kew.subsMu.Unlock()
	delete(kew.subs, c)
}

func (kew *KeyspaceEventWatcher) broadcast(th *KeyspaceEvent) {
	kew.subsMu.Lock()
	defer kew.subsMu.Unlock()
	for c := range kew.subs {
		select {
		case c <- th:
		default:
		}
	}
}

func (kew *KeyspaceEventWatcher) run(ctx context.Context) {
	hcChan := kew.hc.Subscribe()
	bufferCtx, bufferCancel := context.WithCancel(ctx)

	go func() {
		defer bufferCancel()

		for {
			select {
			case <-bufferCtx.Done():
				return
			case result := <-hcChan:
				if result == nil {
					return
				}
				kew.processHealthCheck(result)
			}
		}
	}()

	go func() {
		defer bufferCancel()

		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-bufferCtx.Done():
				return
			case <-tick.C:
				keyspaces, err := kew.ts.GetSrvKeyspaceNames(ctx, kew.localCell, true)
				if err != nil {
					log.Errorf("CEM: initialize failed cell %q: %v", kew.localCell, err)
					continue
				}
				for _, ks := range keyspaces {
					kew.getKeyspaceStatus(ks)
				}
			}
		}
	}()
}

func (kss *keyspaceState) ensureConsistentLocked() {
	if kss.consistent {
		return
	}

	primary := topoproto.SrvKeyspaceGetPartition(kss.lastKeyspace, topodatapb.TabletType_PRIMARY)
	if primary == nil || len(primary.ShardTabletControls) > 0 {
		return
	}

	activeShardsInPartition := make(map[string]bool)
	for _, shard := range primary.ShardReferences {
		sstate := kss.shards[shard.Name]
		if sstate == nil || !sstate.serving {
			return
		}
		activeShardsInPartition[shard.Name] = true
	}

	for shard, sstate := range kss.shards {
		if sstate.serving && !activeShardsInPartition[shard] {
			return
		}
	}

	kss.consistent = true

	ksevent := &KeyspaceEvent{
		Cell:     kss.kew.localCell,
		Keyspace: kss.keyspace,
		Shards:   make([]ShardEvent, 0, len(kss.shards)),
	}

	for shard, sstate := range kss.shards {
		ksevent.Shards = append(ksevent.Shards, ShardEvent{
			Tablet:  sstate.currentPrimary,
			Target:  sstate.target,
			Serving: sstate.serving,
		})

		log.Infof("keyspace event resolved: %s/%s is now consistent (serving: %v)",
			sstate.target.Keyspace, sstate.target.Keyspace,
			sstate.serving,
		)

		if !sstate.serving {
			delete(kss.shards, shard)
		}
	}

	kss.kew.broadcast(ksevent)
}

func (kss *keyspaceState) onHealthCheck(th *TabletHealth) {
	if th.Target.TabletType != topodatapb.TabletType_PRIMARY {
		return
	}

	kss.mu.Lock()
	defer kss.mu.Unlock()

	sstate := kss.shards[th.Target.Shard]
	if sstate == nil {
		if !th.Serving {
			return
		}

		sstate = &shardState{target: th.Target}
		kss.shards[th.Target.Shard] = sstate
	}

	if sstate.serving != th.Serving {
		sstate.serving = th.Serving
		kss.consistent = false
	}

	if th.PrimaryTermStartTime != 0 && th.PrimaryTermStartTime > sstate.externallyReparented {
		sstate.externallyReparented = th.PrimaryTermStartTime
		sstate.currentPrimary = th.Tablet.Alias
		kss.consistent = false
	}

	kss.ensureConsistentLocked()
}

func (kss *keyspaceState) onSrvKeyspace(newKeyspace *topodatapb.SrvKeyspace, newError error) bool {
	kss.mu.Lock()
	defer kss.mu.Unlock()

	if topo.IsErrType(newError, topo.NoNode) {
		kss.deleted = true
		log.Infof("keyspace %q deleted", kss.keyspace)
		return false
	}

	if newError != nil {
		kss.lastError = newError
		log.Errorf("error while watching keyspace %q: %v", kss.keyspace, newError)
		return true
	}

	if proto.Equal(kss.lastKeyspace, newKeyspace) {
		// no changes
		return true
	}

	var oldPrimary, newPrimary *topodatapb.SrvKeyspace_KeyspacePartition
	if kss.lastKeyspace != nil {
		oldPrimary = topoproto.SrvKeyspaceGetPartition(kss.lastKeyspace, topodatapb.TabletType_PRIMARY)
	}
	if newKeyspace != nil {
		newPrimary = topoproto.SrvKeyspaceGetPartition(newKeyspace, topodatapb.TabletType_PRIMARY)
	}
	if !proto.Equal(oldPrimary, newPrimary) {
		kss.consistent = false
	}

	kss.lastKeyspace = newKeyspace
	kss.ensureConsistentLocked()
	return true
}

func newKeyspaceState(kew *KeyspaceEventWatcher, cell, keyspace string) *keyspaceState {
	log.Infof("created dedicated watcher for keyspace %s/%s", cell, keyspace)
	kss := &keyspaceState{
		kew:      kew,
		keyspace: keyspace,
		shards:   make(map[string]*shardState),
	}
	kew.ts.WatchSrvKeyspace(context.Background(), cell, keyspace, kss.onSrvKeyspace)
	return kss
}

func (kew *KeyspaceEventWatcher) processHealthCheck(th *TabletHealth) {
	kss := kew.getKeyspaceStatus(th.Target.Keyspace)
	if kss == nil {
		return
	}

	kss.onHealthCheck(th)
}

func (kew *KeyspaceEventWatcher) getKeyspaceStatus(keyspace string) *keyspaceState {
	kew.mu.Lock()
	defer kew.mu.Unlock()

	kss := kew.keyspaces[keyspace]
	if kss == nil {
		kss = newKeyspaceState(kew, kew.localCell, keyspace)
		kew.keyspaces[keyspace] = kss
	}
	if kss.deleted {
		kss = nil
		delete(kew.keyspaces, keyspace)
	}
	return kss
}

func (kew *KeyspaceEventWatcher) TargetIsBeingResharded(keyspace, shard string) bool {
	ks := kew.getKeyspaceStatus(keyspace)
	if ks == nil {
		return false
	}
	return ks.beingResharded(shard)
}
