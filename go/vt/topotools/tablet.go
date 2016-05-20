// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
func ChangeType(ctx context.Context, ts topo.Server, tabletAlias *topodatapb.TabletAlias, newType topodatapb.TabletType) (*topodatapb.Tablet, error) {
	return ts.UpdateTabletFields(ctx, tabletAlias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = newType
		return nil
	})
}

// ChangeOwnType is like ChangeType, except it fails if you no longer own the
// tablet record, as determined by CheckOwnership().
//
// Note that oldTablet is only used for its Alias, and to call CheckOwnership().
// Other fields in oldTablet have no effect on the update, which will read the
// latest tablet record before setting the type (just like ChangeType() does).
//
// If successful, the updated tablet record is returned.
func ChangeOwnType(ctx context.Context, ts topo.Server, oldTablet *topodatapb.Tablet, newType topodatapb.TabletType) (*topodatapb.Tablet, error) {
	return ts.UpdateTabletFields(ctx, oldTablet.Alias, func(tablet *topodatapb.Tablet) error {
		if err := CheckOwnership(oldTablet, tablet); err != nil {
			return err
		}
		tablet.Type = newType
		return nil
	})
}

// CheckOwnership returns nil iff the IP and port match on oldTablet and
// newTablet, which implies that no other tablet process has taken over the
// record.
func CheckOwnership(oldTablet, newTablet *topodatapb.Tablet) error {
	if oldTablet == nil || newTablet == nil {
		return errors.New("unable to verify ownership of tablet record")
	}
	if oldTablet.Ip != newTablet.Ip || oldTablet.PortMap["vt"] != newTablet.PortMap["vt"] {
		return fmt.Errorf(
			"tablet record was taken over by another process: "+
				"my address is %v:%v, but record is owned by %v:%v",
			oldTablet.Ip, oldTablet.PortMap["vt"], newTablet.Ip, newTablet.PortMap["vt"])
	}
	return nil
}

// DeleteTablet removes a tablet record from the topology:
// - the replication data record if any
// - the tablet record
func DeleteTablet(ctx context.Context, ts topo.Server, tablet *topodatapb.Tablet) error {
	// try to remove replication data, no fatal if we fail
	if err := topo.DeleteTabletReplicationData(ctx, ts, tablet); err != nil {
		if err == topo.ErrNoNode {
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
