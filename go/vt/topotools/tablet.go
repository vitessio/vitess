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

// ChangeType changes the type of the tablet and possibly also updates
// the health information for it. Make this external, since these
// transitions need to be forced from time to time.
//
// - if health is nil, we don't touch the Tablet's Health record.
// - if health is an empty map, we clear the Tablet's Health record.
// - if health has values, we overwrite the Tablet's Health record.
func ChangeType(ctx context.Context, ts topo.Server, tabletAlias *pb.TabletAlias, newType pb.TabletType, health map[string]string) error {
	return ts.UpdateTabletFields(ctx, tabletAlias, func(tablet *pb.Tablet) error {
		if !topo.IsTrivialTypeChange(tablet.Type, newType) {
			return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, tabletAlias)
		}

		tablet.Type = newType
		if health != nil {
			if len(health) == 0 {
				tablet.HealthMap = nil
			} else {
				tablet.HealthMap = health
			}
		}
		return nil
	})
}

// DeleteTablet removes a tablet record from the topology:
// - the replication data record if any
// - the tablet record
func DeleteTablet(ctx context.Context, ts topo.Server, tablet *pb.Tablet) error {
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
