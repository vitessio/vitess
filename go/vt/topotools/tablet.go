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

/*
Package topotools contains high level functions based on vt/topo and
vt/actionnode. It should not depend on anything else that's higher
level. In particular, it cannot depend on:
- vt/wrangler: much higher level, wrangler depends on topotools.
- vt/tabletmanager/initiator: we don't want the various remote
  protocol dependencies here.

topotools is used by wrangler, so it ends up in all tools using
wrangler (vtctl, vtctld, ...). It is also included by vttablet, so it contains:
- most of the logic to create a shard / keyspace (tablet's init code)
- some of the logic to perform a TabletExternallyReparented (RPC call
  to master vttablet to let it know it's the master).

*/
package topotools

// This file contains utility functions for tablets

import (
	"errors"
	"fmt"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

// ConfigureTabletHook configures the right parameters for a hook
// running locally on a tablet.
func ConfigureTabletHook(hk *hook.Hook, tabletAlias *topodatapb.TabletAlias) {
	if hk.ExtraEnv == nil {
		hk.ExtraEnv = make(map[string]string, 1)
	}
	hk.ExtraEnv["TABLET_ALIAS"] = topoproto.TabletAliasString(tabletAlias)
}

// ChangeType changes the type of the tablet. Make this external, since these
// transitions need to be forced from time to time.
//
// If successful, the updated tablet record is returned.
func ChangeType(ctx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, newType topodatapb.TabletType, masterTermStartTime *vttime.Time) (*topodatapb.Tablet, error) {
	var result *topodatapb.Tablet
	// Always clear out the master timestamp if not master.
	if newType != topodatapb.TabletType_MASTER {
		masterTermStartTime = nil
	}
	_, err := ts.UpdateTabletFields(ctx, tabletAlias, func(tablet *topodatapb.Tablet) error {
		// Save the most recent tablet value so we can return it
		// either if the update succeeds or if no update is needed.
		result = tablet
		if tablet.Type == newType && proto.Equal(tablet.MasterTermStartTime, masterTermStartTime) {
			return topo.NewError(topo.NoUpdateNeeded, topoproto.TabletAliasString(tabletAlias))
		}
		tablet.Type = newType
		tablet.MasterTermStartTime = masterTermStartTime
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CheckOwnership returns nil iff the Hostname and port match on oldTablet and
// newTablet, which implies that no other tablet process has taken over the
// record.
func CheckOwnership(oldTablet, newTablet *topodatapb.Tablet) error {
	if oldTablet == nil || newTablet == nil {
		return errors.New("unable to verify ownership of tablet record")
	}
	if oldTablet.Hostname != newTablet.Hostname || oldTablet.PortMap["vt"] != newTablet.PortMap["vt"] {
		return fmt.Errorf(
			"tablet record was taken over by another process: "+
				"my address is %v:%v, but record is owned by %v:%v",
			oldTablet.Hostname, oldTablet.PortMap["vt"], newTablet.Hostname, newTablet.PortMap["vt"])
	}
	return nil
}

// IsPrimaryTablet is a helper function to determine whether the current tablet
// is a primary before we allow its tablet record to be deleted. The canonical
// way to determine the only true primary in a shard is to list all the tablets
// and find the one with the highest MasterTermStartTime among the ones that
// claim to be master.
//
// We err on the side of caution here, i.e. we should never return false for
// a true primary tablet, but it is okay to return true for a tablet that isn't
// the true primary. This can occur if someone issues a DeleteTablet while
// the system is in transition (a reparenting event is in progress and parts of
// the topo have not yet been updated).
func IsPrimaryTablet(ctx context.Context, ts *topo.Server, ti *topo.TabletInfo) (bool, error) {
	// Tablet record claims to be non-master, we believe it
	if ti.Type != topodatapb.TabletType_MASTER {
		return false, nil
	}

	si, err := ts.GetShard(ctx, ti.Keyspace, ti.Shard)
	if err != nil {
		// strictly speaking it isn't correct to return false here, the tablet
		// status is unknown
		return false, err
	}

	// Tablet record claims to be master, and shard record matches
	if topoproto.TabletAliasEqual(si.MasterAlias, ti.Tablet.Alias) {
		return true, nil
	}

	// Shard record has another tablet as master, so check MasterTermStartTime
	// If tablet record's MasterTermStartTime is later than the one in the shard
	// record, then the tablet is master
	tabletMTST := ti.GetMasterTermStartTime()
	shardMTST := si.GetMasterTermStartTime()

	return tabletMTST.After(shardMTST), nil
}

// DeleteTablet removes a tablet record from the topology:
// - the replication data record if any
// - the tablet record
func DeleteTablet(ctx context.Context, ts *topo.Server, tablet *topodatapb.Tablet) error {
	// try to remove replication data, no fatal if we fail
	if err := topo.DeleteTabletReplicationData(ctx, ts, tablet); err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			log.V(6).Infof("no ShardReplication object for cell %v", tablet.Alias.Cell)
			err = nil
		}
		if err != nil {
			log.Warningf("remove replication data for %v failed: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}
	}

	// then delete the tablet record
	return ts.DeleteTablet(ctx, tablet.Alias)
}

// TabletIdent returns a concise string representation of this tablet.
func TabletIdent(tablet *topodatapb.Tablet) string {
	tagStr := ""
	if tablet.Tags != nil {
		for key, val := range tablet.Tags {
			tagStr = tagStr + fmt.Sprintf(" %s=%s", key, val)
		}
	}

	return fmt.Sprintf("%s-%d (%s%s)", tablet.Alias.Cell, tablet.Alias.Uid, tablet.Hostname, tagStr)
}

// TargetIdent returns a concise string representation of a query target
func TargetIdent(target *querypb.Target) string {
	return fmt.Sprintf("%s/%s (%s)", target.Keyspace, target.Shard, target.TabletType)
}
