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

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"context"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Tablet related methods for wrangler

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a replica type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// allowUpdate is true, and a tablet with the same ID exists, just update it.
// If a tablet is created as primary, and there is already a different
// primary in the shard, allowPrimaryOverride must be set.
func (wr *Wrangler) InitTablet(ctx context.Context, tablet *topodatapb.Tablet, allowPrimaryOverride, createShardAndKeyspace, allowUpdate bool) error {
	shard, kr, err := topo.ValidateShardName(tablet.Shard)
	if err != nil {
		return err
	}
	tablet.Shard = shard
	tablet.KeyRange = kr

	// get the shard, possibly creating it
	var si *topo.ShardInfo

	if createShardAndKeyspace {
		// create the parent keyspace and shard if needed
		si, err = wr.ts.GetOrCreateShard(ctx, tablet.Keyspace, tablet.Shard)
	} else {
		si, err = wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
		if topo.IsErrType(err, topo.NoNode) {
			return fmt.Errorf("missing parent shard, use -parent option to create it, or CreateKeyspace / CreateShard")
		}
	}

	// get the shard, checks a couple things
	if err != nil {
		return fmt.Errorf("cannot get (or create) shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
	}
	if !key.KeyRangeEqual(si.KeyRange, tablet.KeyRange) {
		return fmt.Errorf("shard %v/%v has a different KeyRange: %v != %v", tablet.Keyspace, tablet.Shard, si.KeyRange, tablet.KeyRange)
	}
	if tablet.Type == topodatapb.TabletType_PRIMARY && si.HasPrimary() && !topoproto.TabletAliasEqual(si.PrimaryAlias, tablet.Alias) && !allowPrimaryOverride {
		// InitTablet is deprecated, so the flag has not been renamed
		return fmt.Errorf("creating this tablet would override old primary %v in shard %v/%v, use allow_master_override flag", topoproto.TabletAliasString(si.PrimaryAlias), tablet.Keyspace, tablet.Shard)
	}

	if tablet.Type == topodatapb.TabletType_PRIMARY {
		// we update primary_term_start_time even if the primary hasn't changed
		// because that means a new primary term with the same primary
		tablet.PrimaryTermStartTime = logutil.TimeToProto(time.Now())
	}

	err = wr.ts.CreateTablet(ctx, tablet)
	if topo.IsErrType(err, topo.NodeExists) && allowUpdate {
		// Try to update then
		oldTablet, err := wr.ts.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return fmt.Errorf("failed reading existing tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}

		// Check we have the same keyspace / shard, and if not,
		// require the allowDifferentShard flag.
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("old tablet has shard %v/%v. Cannot override with shard %v/%v. Delete and re-add tablet if you want to change the tablet's keyspace/shard", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}
		oldTablet.Tablet = proto.Clone(tablet).(*topodatapb.Tablet)
		if err := wr.ts.UpdateTablet(ctx, oldTablet); err != nil {
			return fmt.Errorf("failed updating tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}
		return nil
	}
	return err
}

// DeleteTablet removes a tablet from a shard.
// - if allowPrimary is set, we can Delete a primary tablet (and clear
// its record from the Shard record if it was the primary).
func (wr *Wrangler) DeleteTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias, allowPrimary bool) (err error) {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	wasPrimary, err := wr.isPrimaryTablet(ctx, ti)
	if err != nil {
		return err
	}

	if wasPrimary && !allowPrimary {
		return fmt.Errorf("cannot delete tablet %v as it is a primary, use allow_primary flag", topoproto.TabletAliasString(tabletAlias))
	}

	// update the Shard object if the primary was scrapped.
	// we do this before calling DeleteTablet so that the operation can be retried in case of failure.
	if wasPrimary {
		// We lock the shard to not conflict with reparent operations.
		ctx, unlock, lockErr := wr.ts.LockShard(ctx, ti.Keyspace, ti.Shard, fmt.Sprintf("DeleteTablet(%v)", topoproto.TabletAliasString(tabletAlias)))
		if lockErr != nil {
			return lockErr
		}
		defer unlock(&err)

		// update the shard record's primary
		if _, err := wr.ts.UpdateShardFields(ctx, ti.Keyspace, ti.Shard, func(si *topo.ShardInfo) error {
			if !topoproto.TabletAliasEqual(si.PrimaryAlias, tabletAlias) {
				wr.Logger().Warningf("Deleting primary %v from shard %v/%v but primary in Shard object was %v", topoproto.TabletAliasString(tabletAlias), ti.Keyspace, ti.Shard, topoproto.TabletAliasString(si.PrimaryAlias))
				return topo.NewError(topo.NoUpdateNeeded, si.Keyspace()+"/"+si.ShardName())
			}
			si.PrimaryAlias = nil
			si.SetPrimaryTermStartTime(time.Now())
			return nil
		}); err != nil {
			return err
		}
	}

	// remove the record and its replication graph entry
	if err := topotools.DeleteTablet(ctx, wr.ts, ti.Tablet); err != nil {
		return err
	}

	return nil
}

// ChangeTabletType changes the type of tablet and recomputes all
// necessary derived paths in the serving graph, if necessary.
//
// Note we don't update the primary record in the Shard here, as we
// can't ChangeType from and out of primary anyway.
func (wr *Wrangler) ChangeTabletType(ctx context.Context, tabletAlias *topodatapb.TabletAlias, tabletType topodatapb.TabletType) error {
	// Load tablet to find endpoint, and keyspace and shard assignment.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if !topo.IsTrivialTypeChange(ti.Type, tabletType) {
		return fmt.Errorf("tablet %v type change %v -> %v is not an allowed transition for ChangeTabletType", tabletAlias, ti.Type, tabletType)
	}

	// We should clone the tablet and change its type to the expected type before checking the durability rules
	// Since we want to check the durability rules for the desired state and not before we make that change
	expectedTablet := proto.Clone(ti.Tablet).(*topodatapb.Tablet)
	expectedTablet.Type = tabletType
	semiSync, err := wr.shouldSendSemiSyncAck(ctx, expectedTablet)

	if err != nil {
		return err
	}
	// and ask the tablet to make the change
	return wr.tmc.ChangeType(ctx, ti.Tablet, tabletType, semiSync)
}

// StartReplication is used to start replication on the specified tablet
// It also finds out if the tablet should be sending semi-sync ACKs or not.
func (wr *Wrangler) StartReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	semiSync, err := wr.shouldSendSemiSyncAck(ctx, tablet)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().StartReplication(ctx, tablet, semiSync)
}

func (wr *Wrangler) shouldSendSemiSyncAck(ctx context.Context, tablet *topodatapb.Tablet) (bool, error) {
	shard, err := wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return false, err
	}

	if !shard.HasPrimary() {
		return false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	shardPrimary, err := wr.ts.GetTablet(ctx, shard.PrimaryAlias)
	if err != nil {
		return false, fmt.Errorf("cannot lookup primary tablet %v for shard %v/%v: %w", topoproto.TabletAliasString(shard.PrimaryAlias), tablet.Keyspace, tablet.Shard, err)
	}

	if shardPrimary.Type != topodatapb.TabletType_PRIMARY {
		return false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "TopologyServer has incosistent state for shard primary %v", topoproto.TabletAliasString(shard.PrimaryAlias))
	}

	if shardPrimary.Keyspace != tablet.Keyspace || shardPrimary.Shard != tablet.Shard {
		return false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary %v and potential replica %v not in same keypace shard (%v/%v)", topoproto.TabletAliasString(shard.PrimaryAlias), topoproto.TabletAliasString(tablet.Alias), tablet.Keyspace, tablet.Shard)
	}

	return reparentutil.IsReplicaSemiSync(shardPrimary.Tablet, tablet), nil
}

// RefreshTabletState refreshes tablet state
func (wr *Wrangler) RefreshTabletState(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	// Load tablet to find endpoint, and keyspace and shard assignment.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	// and ask the tablet to refresh itself
	return wr.tmc.RefreshState(ctx, ti.Tablet)
}

// ExecuteFetchAsApp executes a query remotely using the App pool
func (wr *Wrangler) ExecuteFetchAsApp(ctx context.Context, tabletAlias *topodatapb.TabletAlias, usePool bool, query string, maxRows int) (*querypb.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.ExecuteFetchAsApp(ctx, ti.Tablet, usePool, []byte(query), maxRows)
}

// ExecuteFetchAsDba executes a query remotely using the DBA pool
func (wr *Wrangler) ExecuteFetchAsDba(ctx context.Context, tabletAlias *topodatapb.TabletAlias, query string, maxRows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.ExecuteFetchAsDba(ctx, ti.Tablet, false, []byte(query), maxRows, disableBinlogs, reloadSchema)
}

// VReplicationExec executes a query remotely using the DBA pool
func (wr *Wrangler) VReplicationExec(ctx context.Context, tabletAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.VReplicationExec(ctx, ti.Tablet, query)
}

// GenericVExec executes a query remotely using the DBA pool
func (wr *Wrangler) GenericVExec(ctx context.Context, tabletAlias *topodatapb.TabletAlias, query, workflow, keyspace string) (*querypb.QueryResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.VExec(ctx, ti.Tablet, query, workflow, keyspace)
}

// isPrimaryTablet is a shortcut way to determine whether the current tablet
// is a primary before we allow its tablet record to be deleted. The canonical
// way to determine the only true primary in a shard is to list all the tablets
// and find the one with the highest PrimaryTermStartTime among the ones that
// claim to be primary.
// We err on the side of caution here, i.e. we should never return false for
// a true primary tablet, but it is ok to return true for a tablet that isn't
// the true primary. This can occur if someone issues a DeleteTablet while
// the system is in transition (a reparenting event is in progress and parts of
// the topo have not yet been updated).
func (wr *Wrangler) isPrimaryTablet(ctx context.Context, ti *topo.TabletInfo) (bool, error) {
	return topotools.IsPrimaryTablet(ctx, wr.TopoServer(), ti)
}
