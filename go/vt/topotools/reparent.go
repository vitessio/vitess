// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

// This file contains utility functions for reparenting. It is used by
// the new master tablet for TabletExternallyReparented.

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RestartSlavesExternal will tell all the slaves in the provided list
// that they have a new master, and also tell all the masters. The
// masters will be scrapped if they don't answer.
// We execute all the actions in parallel.
func RestartSlavesExternal(ts topo.Server, log logutil.Logger, slaveTabletMap, masterTabletMap map[topodatapb.TabletAlias]*topo.TabletInfo, masterElectTabletAlias *topodatapb.TabletAlias, slaveWasRestarted func(*topo.TabletInfo, *topodatapb.TabletAlias) error) {
	wg := sync.WaitGroup{}

	log.Infof("Updating individual tablets with the right master...")

	// do all the slaves
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			if err := slaveWasRestarted(ti, masterElectTabletAlias); err != nil {
				log.Warningf("Slave %v had an error: %v", ti.Alias, err)
			}
			wg.Done()
		}(ti)
	}

	// and do the old master and any straggler, if possible.
	for _, ti := range masterTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			err := slaveWasRestarted(ti, masterElectTabletAlias)
			if err != nil {
				// the old master can be annoying if left
				// around in the replication graph, so if we
				// can't restart it, we just make it spare.
				log.Warningf("Old master %v is not restarting in time, forcing it to spare: %v", ti.Alias, err)

				ti.Type = topodatapb.TabletType_SPARE
				if err := ts.UpdateTablet(context.TODO(), ti); err != nil {
					log.Warningf("Failed to change old master %v to spare: %v", ti.Alias, err)
				}
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()
}
