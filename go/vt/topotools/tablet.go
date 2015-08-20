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
- most of the logic to rebuild a shard serving graph (helthcheck module)
- some of the logic to perform a TabletExternallyReparented (RPC call
  to master vttablet to let it know it's the master).

*/
package topotools

// This file contains utility functions for tablets

import (
	"fmt"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ConfigureTabletHook configures the right parameters for a hook
// running locally on a tablet.
func ConfigureTabletHook(hk *hook.Hook, tabletAlias *pb.TabletAlias) {
	if hk.ExtraEnv == nil {
		hk.ExtraEnv = make(map[string]string, 1)
	}
	hk.ExtraEnv["TABLET_ALIAS"] = topoproto.TabletAliasString(tabletAlias)
}

// Scrap will update the tablet type to 'Scrap', and remove it from
// the serving graph.
//
// 'force' means we are not on the tablet being scrapped, so it is
// probably dead. So if 'force' is true, we will also remove pending
// remote actions.  And if 'force' is false, we also run an optional
// hook.
func Scrap(ctx context.Context, ts topo.Server, tabletAlias *pb.TabletAlias, force bool) error {
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	// If you are already scrap, skip updating replication data. It won't
	// be there anyway.
	wasAssigned := tablet.IsAssigned()
	tablet.Type = pb.TabletType_SCRAP
	// Update the tablet first, since that is canonical.
	err = ts.UpdateTablet(ctx, tablet)
	if err != nil {
		return err
	}

	if wasAssigned {
		err = topo.DeleteTabletReplicationData(ctx, ts, tablet.Tablet)
		if err != nil {
			if err == topo.ErrNoNode {
				log.V(6).Infof("no ShardReplication object for cell %v", tablet.Alias.Cell)
				err = nil
			}
			if err != nil {
				log.Warningf("remove replication data for %v failed: %v", tablet.Alias, err)
			}
		}
	}

	// run a hook for final cleanup, only in non-force mode.
	// (force mode executes on the vtctl side, not on the vttablet side)
	if !force {
		hk := hook.NewSimpleHook("postflight_scrap")
		ConfigureTabletHook(hk, tablet.Alias)
		if hookErr := hk.ExecuteOptional(); hookErr != nil {
			// we don't want to return an error, the server
			// is already in bad shape probably.
			log.Warningf("Scrap: postflight_scrap failed: %v", hookErr)
		}
	}

	return nil
}

// ChangeType changes the type of the tablet and possibly also updates
// the health informaton for it. Make this external, since these
// transitions need to be forced from time to time.
//
// - if health is nil, we don't touch the Tablet's Health record.
// - if health is an empty map, we clear the Tablet's Health record.
// - if health has values, we overwrite the Tablet's Health record.
func ChangeType(ctx context.Context, ts topo.Server, tabletAlias *pb.TabletAlias, newType pb.TabletType, health map[string]string) error {
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if !topo.IsTrivialTypeChange(tablet.Type, newType) {
		return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, tabletAlias)
	}

	tablet.Type = newType
	if newType == pb.TabletType_IDLE {
		tablet.Keyspace = ""
		tablet.Shard = ""
		tablet.KeyRange = nil
		tablet.HealthMap = health
	}
	if health != nil {
		if len(health) == 0 {
			tablet.HealthMap = nil
		} else {
			tablet.HealthMap = health
		}
	}
	return ts.UpdateTablet(ctx, tablet)
}
