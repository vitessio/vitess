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

package wrangler

/*
This file handles the reparenting operations.
*/

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)

	wr.logger.Infof("Gathering tablet replication status for: %v", tablets)
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	result := make([]*replicationdatapb.Status, len(tablets))

	for i, ti := range tablets {
		// Don't scan tablets that won't return something
		// useful. Otherwise, you'll end up waiting for a timeout.
		if ti.Type == topodatapb.TabletType_PRIMARY {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				pos, err := wr.tmc.MasterPosition(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("MasterPosition(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = &replicationdatapb.Status{
					Position: pos,
				}
			}(i, ti)
		} else if ti.IsReplicaType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := wr.tmc.ReplicationStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("ReplicationStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return tablets, result, rec.Error()
}

// ReparentTablet tells a tablet to reparent this tablet to the current
// primary, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	_, err := wr.vtctld.ReparentTablet(ctx, &vtctldatapb.ReparentTabletRequest{
		Tablet: tabletAlias,
	})
	return err
}

// InitShardPrimary will make the provided tablet the primary for the shard.
func (wr *Wrangler) InitShardPrimary(ctx context.Context, keyspace, shard string, primaryElectTabletAlias *topodatapb.TabletAlias, force bool, waitReplicasTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("InitShardPrimary(%v)", topoproto.TabletAliasString(primaryElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = grpcvtctldserver.NewVtctldServer(wr.ts).InitShardPrimaryLocked(ctx, ev, &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                keyspace,
		Shard:                   shard,
		PrimaryElectTabletAlias: primaryElectTabletAlias,
		Force:                   force,
	}, waitReplicasTimeout, wr.tmc, wr.logger)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardPrimary: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardPrimary")
	}
	return err
}

// PlannedReparentShard will make the provided tablet the primary for the shard,
// when both the current and new primary are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, primaryElectTabletAlias, avoidTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) (err error) {
	_, err = reparentutil.NewPlannedReparenter(wr.ts, wr.tmc, wr.logger).ReparentShard(
		ctx,
		keyspace,
		shard,
		reparentutil.PlannedReparentOptions{
			AvoidPrimaryAlias:   avoidTabletAlias,
			NewPrimaryAlias:     primaryElectTabletAlias,
			WaitReplicasTimeout: waitReplicasTimeout,
		},
	)

	return err
}

// EmergencyReparentShard will make the provided tablet the primary for
// the shard, when the old primary is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, primaryElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration, ignoredTablets sets.String, preventCrossCellPromotion bool) (err error) {
	_, err = reparentutil.NewEmergencyReparenter(wr.ts, wr.tmc, wr.logger).ReparentShard(
		ctx,
		keyspace,
		shard,
		reparentutil.EmergencyReparentOptions{
			NewPrimaryAlias:           primaryElectTabletAlias,
			WaitReplicasTimeout:       waitReplicasTimeout,
			IgnoreReplicas:            ignoredTablets,
			PreventCrossCellPromotion: preventCrossCellPromotion,
		},
	)

	return err
}

// TabletExternallyReparented changes the type of new primary for this shard to PRIMARY
// and updates it's tablet record in the topo. Updating the shard record is handled
// by the new primary tablet
func (wr *Wrangler) TabletExternallyReparented(ctx context.Context, newPrimaryAlias *topodatapb.TabletAlias) error {

	tabletInfo, err := wr.ts.GetTablet(ctx, newPrimaryAlias)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read tablet record for %v: %v", newPrimaryAlias, err)
		return err
	}

	// Check the global shard record.
	tablet := tabletInfo.Tablet
	si, err := wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// We update the tablet only if it is not currently primary
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		log.Infof("TabletExternallyReparented: executing tablet type change to PRIMARY")

		// Create a reusable Reparent event with available info.
		ev := &events.Reparent{
			ShardInfo:  *si,
			NewPrimary: tablet,
			OldPrimary: &topodatapb.Tablet{
				Alias: si.PrimaryAlias,
				Type:  topodatapb.TabletType_PRIMARY,
			},
		}
		defer func() {
			if err != nil {
				event.DispatchUpdate(ev, "failed: "+err.Error())
			}
		}()
		event.DispatchUpdate(ev, "starting external reparent")

		if err := wr.tmc.ChangeType(ctx, tablet, topodatapb.TabletType_PRIMARY); err != nil {
			log.Warningf("Error calling ChangeType on new primary %v: %v", topoproto.TabletAliasString(newPrimaryAlias), err)
			return err
		}
		event.DispatchUpdate(ev, "finished")
	}
	return nil
}
