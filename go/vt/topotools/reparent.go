// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

// This file contains utility functions for reparenting. It is used by
// the new master tablet for TabletExternallyReparented.

import (
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// RestartSlavesExternal will tell all the slaves in the provided list
// that they have a new master, and also tell all the masters. The
// masters will be scrapped if they don't answer.
// We execute all the actions in parallel.
func RestartSlavesExternal(ts topo.Server, log logutil.Logger, slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTabletAlias topo.TabletAlias, slaveWasRestarted func(*topo.TabletInfo, *actionnode.SlaveWasRestartedArgs) error) {
	wg := sync.WaitGroup{}

	swrd := actionnode.SlaveWasRestartedArgs{
		Parent: masterElectTabletAlias,
	}

	log.Infof("Updating individual tablets with the right master...")

	// do all the slaves
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			if err := slaveWasRestarted(ti, &swrd); err != nil {
				log.Warningf("Slave %v had an error: %v", ti.Alias, err)
			}
			wg.Done()
		}(ti)
	}

	// and do the old master and any straggler, if possible.
	for _, ti := range masterTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			err := slaveWasRestarted(ti, &swrd)
			if err != nil {
				// the old master can be annoying if left
				// around in the replication graph, so if we
				// can't restart it, we just scrap it.
				// We don't rebuild the Shard just yet though.
				log.Warningf("Old master %v is not restarting in time, forcing it to spare: %v", ti.Alias, err)

				ti.Type = topo.TYPE_SPARE
				ti.Parent = masterElectTabletAlias
				if err := topo.UpdateTablet(context.TODO(), ts, ti); err != nil {
					log.Warningf("Failed to change old master %v to spare: %v", ti.Alias, err)
				}
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()
}
