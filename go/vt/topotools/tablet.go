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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
func ChangeType(ctx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, newType topodatapb.TabletType) (*topodatapb.Tablet, error) {
	return ts.UpdateTabletFields(ctx, tabletAlias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = newType
		return nil
	})
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
