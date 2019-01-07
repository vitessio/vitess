/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Tablet related methods for wrangler

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a slave type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// allowUpdate is true, and a tablet with the same ID exists, just update it.
// If a tablet is created as master, and there is already a different
// master in the shard, allowMasterOverride must be set.
func (wr *Wrangler) InitTablet(ctx context.Context, tablet *topodatapb.Tablet, allowMasterOverride, createShardAndKeyspace, allowUpdate bool) error {
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
	if tablet.Type == topodatapb.TabletType_MASTER && si.HasMaster() && !topoproto.TabletAliasEqual(si.MasterAlias, tablet.Alias) && !allowMasterOverride {
		return fmt.Errorf("creating this tablet would override old master %v in shard %v/%v, use allow_master_override flag", topoproto.TabletAliasString(si.MasterAlias), tablet.Keyspace, tablet.Shard)
	}

	// update the shard record if needed
	if err := wr.updateShardMaster(ctx, si, tablet.Alias, tablet.Type, allowMasterOverride); err != nil {
		return err
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

		*(oldTablet.Tablet) = *tablet
		if err := wr.ts.UpdateTablet(ctx, oldTablet); err != nil {
			return fmt.Errorf("failed updating tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}
		return nil
	}
	return err
}

// DeleteTablet removes a tablet from a shard.
// - if allowMaster is set, we can Delete a master tablet (and clear
// its record from the Shard record if it was the master).
func (wr *Wrangler) DeleteTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias, allowMaster bool) (err error) {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	wasMaster := ti.Type == topodatapb.TabletType_MASTER
	if wasMaster && !allowMaster {
		return fmt.Errorf("cannot delete tablet %v as it is a master, use allow_master flag", topoproto.TabletAliasString(tabletAlias))
	}

	// remove the record and its replication graph entry
	if err := topotools.DeleteTablet(ctx, wr.ts, ti.Tablet); err != nil {
		return err
	}

	// update the Shard object if the master was scrapped.
	if wasMaster {
		// We lock the shard to not conflict with reparent operations.
		ctx, unlock, lockErr := wr.ts.LockShard(ctx, ti.Keyspace, ti.Shard, fmt.Sprintf("DeleteTablet(%v)", topoproto.TabletAliasString(tabletAlias)))
		if lockErr != nil {
			return lockErr
		}
		defer unlock(&err)

		// update the shard record's master
		_, err = wr.ts.UpdateShardFields(ctx, ti.Keyspace, ti.Shard, func(si *topo.ShardInfo) error {
			if !topoproto.TabletAliasEqual(si.MasterAlias, tabletAlias) {
				wr.Logger().Warningf("Deleting master %v from shard %v/%v but master in Shard object was %v", topoproto.TabletAliasString(tabletAlias), ti.Keyspace, ti.Shard, topoproto.TabletAliasString(si.MasterAlias))
				return topo.NewError(topo.NoUpdateNeeded, si.Keyspace()+"/"+si.ShardName())
			}
			si.MasterAlias = nil
			return nil
		})
		return err
	}

	return nil
}

// ChangeSlaveType changes the type of tablet and recomputes all
// necessary derived paths in the serving graph, if necessary.
//
// Note we don't update the master record in the Shard here, as we
// can't ChangeType from and out of master anyway.
func (wr *Wrangler) ChangeSlaveType(ctx context.Context, tabletAlias *topodatapb.TabletAlias, tabletType topodatapb.TabletType) error {
	// Load tablet to find endpoint, and keyspace and shard assignment.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if !topo.IsTrivialTypeChange(ti.Type, tabletType) {
		return fmt.Errorf("tablet %v type change %v -> %v is not an allowed transition for ChangeSlaveType", tabletAlias, ti.Type, tabletType)
	}

	// and ask the tablet to make the change
	return wr.tmc.ChangeType(ctx, ti.Tablet, tabletType)
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
