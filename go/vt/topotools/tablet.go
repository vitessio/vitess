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
	"sync"

	"code.google.com/p/go.net/context"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/vt/concurrency"
	"github.com/henryanand/vitess/go/vt/hook"
	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/topo"
)

// ConfigureTabletHook configures the right parameters for a hook
// running locally on a tablet.
func ConfigureTabletHook(hk *hook.Hook, tabletAlias topo.TabletAlias) {
	if hk.ExtraEnv == nil {
		hk.ExtraEnv = make(map[string]string, 1)
	}
	hk.ExtraEnv["TABLET_ALIAS"] = tabletAlias.String()
}

// Scrap will update the tablet type to 'Scrap', and remove it from
// the serving graph.
//
// 'force' means we are not on the tablet being scrapped, so it is
// probably dead. So if 'force' is true, we will also remove pending
// remote actions.  And if 'force' is false, we also run an optional
// hook.
func Scrap(ts topo.Server, tabletAlias topo.TabletAlias, force bool) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	// If you are already scrap, skip updating replication data. It won't
	// be there anyway.
	wasAssigned := tablet.IsAssigned()
	tablet.Type = topo.TYPE_SCRAP
	tablet.Parent = topo.TabletAlias{}
	// Update the tablet first, since that is canonical.
	err = topo.UpdateTablet(context.TODO(), ts, tablet)
	if err != nil {
		return err
	}

	if wasAssigned {
		err = topo.DeleteTabletReplicationData(ts, tablet.Tablet)
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
func ChangeType(ts topo.Server, tabletAlias topo.TabletAlias, newType topo.TabletType, health map[string]string, runHooks bool) error {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	if !topo.IsTrivialTypeChange(tablet.Type, newType) || !topo.IsValidTypeChange(tablet.Type, newType) {
		return fmt.Errorf("cannot change tablet type %v -> %v %v", tablet.Type, newType, tabletAlias)
	}

	if runHooks {
		// Only run the preflight_serving_type hook when
		// transitioning from non-serving to serving.
		if !topo.IsInServingGraph(tablet.Type) && topo.IsInServingGraph(newType) {
			if err := hook.NewSimpleHook("preflight_serving_type").ExecuteOptional(); err != nil {
				return err
			}
		}
	}

	tablet.Type = newType
	if newType == topo.TYPE_IDLE {
		if tablet.Parent.IsZero() {
			si, err := ts.GetShard(tablet.Keyspace, tablet.Shard)
			if err != nil {
				return err
			}
			rec := concurrency.AllErrorRecorder{}
			wg := sync.WaitGroup{}
			for _, cell := range si.Cells {
				wg.Add(1)
				go func(cell string) {
					defer wg.Done()
					sri, err := ts.GetShardReplication(cell, tablet.Keyspace, tablet.Shard)
					if err != nil {
						log.Warningf("Cannot check cell %v for extra replication paths, assuming it's good", cell)
						return
					}
					for _, rl := range sri.ReplicationLinks {
						if rl.Parent == tabletAlias {
							rec.RecordError(fmt.Errorf("Still have a ReplicationLink in cell %v", cell))
						}
					}
				}(cell)
			}
			wg.Wait()
			if rec.HasErrors() {
				return rec.Error()
			}
		}
		tablet.Parent = topo.TabletAlias{}
		tablet.Keyspace = ""
		tablet.Shard = ""
		tablet.KeyRange = key.KeyRange{}
		tablet.Health = health
	}
	if health != nil {
		if len(health) == 0 {
			tablet.Health = nil
		} else {
			tablet.Health = health
		}
	}
	return topo.UpdateTablet(context.TODO(), ts, tablet)
}
