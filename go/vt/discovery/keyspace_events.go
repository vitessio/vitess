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

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// KeyspaceEventWatcher is an auxiliary watcher that watches all availability incidents
// for all keyspaces in a Vitess cell and notifies listeners when the events have been resolved.
// Right now this is capable of detecting the end of failovers, both planned and unplanned,
// and the end of resharding operations.
//
// The KeyspaceEventWatcher works by consolidating TabletHealth events from a HealthCheck stream,
// which is a peer-to-peer check between nodes via GRPC, with events from a Topology Server, which
// are global to the cluster and stored in an external system like etcd.
type KeyspaceEventWatcher struct {
	ts        srvtopo.Server
	hc        HealthCheck
	localCell string

	mu        sync.Mutex
	keyspaces map[string]*keyspaceState

	subsMu sync.Mutex
	subs   map[chan *KeyspaceEvent]struct{}
}

// KeyspaceEvent is yielded to all watchers when an availability event for a keyspace has been resolved
type KeyspaceEvent struct {
	// Cell is the cell where the keyspace lives
	Cell string

	// Keyspace is the name of the keyspace which was (partially) unavailable and is now fully healthy
	Keyspace string

	// Shards is a list of all the shards in the keyspace, including their state after the event is resolved
	Shards []ShardEvent
}

type ShardEvent struct {
	Tablet  *topodatapb.TabletAlias
	Target  *query.Target
	Serving bool
}

// NewKeyspaceEventWatcher returns a new watcher for all keyspace events in the given cell.
// It requires access to a topology server, and an existing HealthCheck implementation which
// will be used to detect unhealthy nodes.
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

// keyspaceState is the internal state for all the keyspaces that the KEW is
// currently watching
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

// Format prints the internal state for this keyspace for debug purposes
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

// beingResharded returns whether this keyspace is thought to be in the middle of a resharding
// operation. currentShard is the name of the shard that belongs to this keyspace and which
// we are trying to access. currentShard can _only_ be a primary shard.
func (kss *keyspaceState) beingResharded(currentShard string) bool {
	kss.mu.Lock()
	defer kss.mu.Unlock()

	// if the keyspace is gone, or if it has no known availability events, the keyspace
	// cannot be in the middle of a resharding operation
	if kss.deleted || kss.consistent {
		return false
	}

	// for all the known shards, try to find a primary shard besides the one we're trying to access
	// and which is currently healthy. if there are other healthy primaries in the keyspace, it means
	// we're in the middle of a resharding operation
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

// Subscribe returns a channel that will receive any KeyspaceEvents for all keyspaces in the current cell
func (kew *KeyspaceEventWatcher) Subscribe() chan *KeyspaceEvent {
	kew.subsMu.Lock()
	defer kew.subsMu.Unlock()
	c := make(chan *KeyspaceEvent, 2)
	kew.subs[c] = struct{}{}
	return c
}

// Unsubscribe removes a listener previously returned from Subscribe
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
				//log.Info("getCurrentValue: kew.processHealthCheck(result)")
				if result == nil {
					return
				}
				kew.processHealthCheck(result)
			}
		}
	}()

	go func() {
		// Seed the keyspace statuses once at startup
		//log.Info("getCurrentValue: kew.ts.GetSrvKeyspaceNames")
		keyspaces, err := kew.ts.GetSrvKeyspaceNames(ctx, kew.localCell, true)
		if err != nil {
			log.Errorf("CEM: initialize failed for cell %q: %v", kew.localCell, err)
			return
		}
		for _, ks := range keyspaces {
			kew.getKeyspaceStatus(ks)
		}
	}()
}

// ensureConsistentLocked checks if the current keyspace has recovered from an availability
// event, and if so, returns information about the availability event to all subscribers
func (kss *keyspaceState) ensureConsistentLocked() {
	// if this keyspace is consistent, there's no ongoing availability event
	if kss.consistent {
		return
	}

	// get the topology metadata for our primary from `lastKeyspace`; this value is refreshed
	// from our topology watcher whenever a change is detected, so it should always be up to date
	primary := topoproto.SrvKeyspaceGetPartition(kss.lastKeyspace, topodatapb.TabletType_PRIMARY)

	// if there's no primary, the keyspace is unhealthy;
	// if there are ShardTabletControls active, the keyspace is undergoing a topology change;
	// either way, the availability event is still ongoing
	if primary == nil || len(primary.ShardTabletControls) > 0 {
		return
	}

	activeShardsInPartition := make(map[string]bool)

	// iterate through all the primary shards that the topology server knows about;
	// for each shard, if our HealthCheck stream hasn't found the shard yet, or
	// if the HealthCheck stream still thinks the shard is unhealthy, this
	// means the availability event is still ongoing
	for _, shard := range primary.ShardReferences {
		sstate := kss.shards[shard.Name]
		if sstate == nil || !sstate.serving {
			return
		}
		activeShardsInPartition[shard.Name] = true
	}

	// iterate through all the shards as seen by our HealthCheck stream. if there are any
	// shards that HealthCheck thinks are healthy, and they haven't been seen by the topology
	// watcher, it means the keyspace is not fully consistent yet
	for shard, sstate := range kss.shards {
		if sstate.serving && !activeShardsInPartition[shard] {
			return
		}
	}

	// we haven't found any inconsistencies between the HealthCheck stream and the topology
	// watcher. this means the ongoing availability event has been resolved, so we can broadcast
	// a resolution event to all listeners
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

// onHealthCheck is the callback that updates this keyspace with event data from the HealthCheck stream.
// the HealthCheck stream applies to all the keyspaces in the cluster and emits TabletHealth events to our
// parent KeyspaceWatcher, which will mux them into their corresponding keyspaceState
func (kss *keyspaceState) onHealthCheck(th *TabletHealth) {
	// we only care about health events on the primary
	if th.Target.TabletType != topodatapb.TabletType_PRIMARY {
		return
	}

	kss.mu.Lock()
	defer kss.mu.Unlock()

	sstate := kss.shards[th.Target.Shard]

	// if we've never seen this shard before, we need to allocate a shardState for it, unless
	// we've received a _not serving_ shard event for a shard which we don't know about yet,
	// in which case we don't need to keep track of it. we'll start tracking it if/when the
	// shard becomes healthy again
	if sstate == nil {
		if !th.Serving {
			return
		}

		sstate = &shardState{target: th.Target}
		kss.shards[th.Target.Shard] = sstate
	}

	// if the shard went from serving to not serving, or the other way around, the keyspace
	// is undergoing an availability event
	if sstate.serving != th.Serving {
		sstate.serving = th.Serving
		kss.consistent = false
	}

	// if the primary for this shard has been externally reparented, we're undergoing a failover,
	// which is considered an availability event. update this shard to point it to the new tablet
	// that acts as primary now
	if th.PrimaryTermStartTime != 0 && th.PrimaryTermStartTime > sstate.externallyReparented {
		sstate.externallyReparented = th.PrimaryTermStartTime
		sstate.currentPrimary = th.Tablet.Alias
		kss.consistent = false
	}

	kss.ensureConsistentLocked()
}

// onSrvKeyspace is the callback that updates this keyspace with fresh topology data from our topology server.
// this callback is called from a Watcher in the topo server whenever a change to the topology for this keyspace
// occurs. this watcher is dedicated to this keyspace, and will only yield topology metadata changes for as
// long as we're interested on this keyspace.
func (kss *keyspaceState) onSrvKeyspace(newKeyspace *topodatapb.SrvKeyspace, newError error) bool {
	kss.mu.Lock()
	defer kss.mu.Unlock()

	// if the topology watcher has seen a NoNode while watching this keyspace, it means the keyspace
	// has been deleted from the cluster. we mark it for eventual cleanup here, as we no longer need
	// to keep watching for events in this keyspace.
	if topo.IsErrType(newError, topo.NoNode) {
		kss.deleted = true
		log.Infof("keyspace %q deleted", kss.keyspace)
		return false
	}

	// if there's another kind of error while watching this keyspace, we assume it's temporary and related
	// to the topology server, not to the keyspace itself. we'll keep waiting for more topology events.
	if newError != nil {
		kss.lastError = newError
		log.Errorf("error while watching keyspace %q: %v", kss.keyspace, newError)
		return true
	}

	// if the topology metadata for our keyspace is identical to the last one we saw there's nothing to do
	// here. this is a side-effect of the way ETCD watchers work.
	if proto.Equal(kss.lastKeyspace, newKeyspace) {
		// no changes
		return true
	}

	// we only mark this keyspace as inconsistent if there has been a topology change in the PRIMARY for
	// this keyspace, but we store the topology metadata for both primary and replicas for future-proofing.
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

// newKeyspaceState allocates the internal state required to keep track of availability incidents
// in this keyspace, and starts up a SrvKeyspace watcher on our topology server which will update
// our keyspaceState with any topology changes in real time.
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

// processHealthCheck is the callback that is called by the global HealthCheck stream that was initiated
// by this KeyspaceEventWatcher. it redirects the TabletHealth event to the corresponding keyspaceState
func (kew *KeyspaceEventWatcher) processHealthCheck(th *TabletHealth) {
	kss := kew.getKeyspaceStatus(th.Target.Keyspace)
	if kss == nil {
		return
	}

	kss.onHealthCheck(th)
}

// getKeyspaceStatus returns the keyspaceState object for the corresponding keyspace, allocating it
// if we've never seen the keyspace before.
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

// TargetIsBeingResharded checks if the reason why the given target is not accessible right now
// is because the keyspace where it resides is (potentially) undergoing a resharding operation.
// This is not a fully accurate heuristic, but it's good enough that we'd want to buffer the
// request for the given target under the assumption that the reason why it cannot be completed
// right now is transitory.
func (kew *KeyspaceEventWatcher) TargetIsBeingResharded(target *query.Target) bool {
	if target.TabletType != topodatapb.TabletType_PRIMARY {
		return false
	}
	ks := kew.getKeyspaceStatus(target.Keyspace)
	if ks == nil {
		return false
	}
	return ks.beingResharded(target.Shard)
}

// PrimaryIsNotServing checks if the reason why the given target is not accessible right now is
// that the primary tablet for that shard is not serving. This is possible during a Planned Reparent Shard
// operation. Just as the operation completes, a new primary will be elected, and it will send its own healthcheck
// stating that it is serving. We should buffer requests until that point.
// There are use cases where people do not run with a Primary server at all, so we must verify that
// we only start buffering when a primary was present, and it went not serving.
// The shard state keeps track of the current primary and the last externally reparented time, which we can use
// to determine that there was a serving primary which now became non serving. This is only possible in a DemotePrimary
// RPC which are only called from ERS and PRS. So buffering will stop when these operations succeed.
func (kew *KeyspaceEventWatcher) PrimaryIsNotServing(target *query.Target) bool {
	if target.TabletType != topodatapb.TabletType_PRIMARY {
		return false
	}
	ks := kew.getKeyspaceStatus(target.Keyspace)
	if ks == nil {
		return false
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if state, ok := ks.shards[target.Shard]; ok {
		// If the primary tablet was present then externallyReparented will be non-zero and currentPrimary will be not nil
		return !state.serving && !ks.consistent && state.externallyReparented != 0 && state.currentPrimary != nil
	}
	return false
}
