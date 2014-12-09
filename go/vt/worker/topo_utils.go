// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// findHealthyEndPoint returns a random healthy endpoint.
// Since we don't want to use them all, we require at least 2 servers
// are healthy.
func findHealthyEndPoint(wr *wrangler.Wrangler, cell, keyspace, shard string) (topo.TabletAlias, error) {
	endPoints, err := wr.TopoServer().GetEndPoints(cell, keyspace, shard, topo.TYPE_RDONLY)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("GetEndPoints(%v,%v,%v,rdonly) failed: %v", cell, keyspace, shard, err)
	}
	healthyEndpoints := make([]topo.EndPoint, 0, len(endPoints.Entries))
	for _, entry := range endPoints.Entries {
		if len(entry.Health) == 0 {
			healthyEndpoints = append(healthyEndpoints, entry)
		}
	}
	if len(healthyEndpoints) <= 1 {
		return topo.TabletAlias{}, fmt.Errorf("Not enough endpoints to chose from in (%v,%v/%v), have %v healthy ones", cell, keyspace, shard, len(healthyEndpoints))
	}

	// random server in the list is what we want
	index := rand.Intn(len(healthyEndpoints))
	return topo.TabletAlias{
		Cell: cell,
		Uid:  healthyEndpoints[index].Uid,
	}, nil
}

// findChecker:
// - find a rdonly instance in the keyspace / shard
// - mark it as checker
// - tag it with our worker process
func findChecker(wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, cell, keyspace, shard string) (topo.TabletAlias, error) {
	tabletAlias, err := findHealthyEndPoint(wr, cell, keyspace, shard)
	if err != nil {
		return topo.TabletAlias{}, err
	}

	// We add the tag before calling ChangeSlaveType, so the destination
	// vttablet reloads the worker URL when it reloads the tablet.
	ourURL := servenv.ListeningURL.String()
	wr.Logger().Infof("Adding tag[worker]=%v to tablet %v", ourURL, tabletAlias)
	if err := wr.TopoServer().UpdateTabletFields(tabletAlias, func(tablet *topo.Tablet) error {
		if tablet.Tags == nil {
			tablet.Tags = make(map[string]string)
		}
		tablet.Tags["worker"] = ourURL
		return nil
	}); err != nil {
		return topo.TabletAlias{}, err
	}
	// we remove the tag *before* calling ChangeSlaveType back, so
	// we need to record this tag change after the change slave
	// type change in the cleaner.
	defer wrangler.RecordTabletTagAction(cleaner, tabletAlias, "worker", "")

	wr.Logger().Infof("Changing tablet %v to 'checker'", tabletAlias)
	wr.ResetActionTimeout(30 * time.Second)
	if err := wr.ChangeType(tabletAlias, topo.TYPE_CHECKER, false /*force*/); err != nil {
		return topo.TabletAlias{}, err
	}

	// Record a clean-up action to take the tablet back to rdonly.
	// We will alter this one later on and let the tablet go back to
	// 'spare' if we have stopped replication for too long on it.
	wrangler.RecordChangeSlaveTypeAction(cleaner, tabletAlias, topo.TYPE_RDONLY)
	return tabletAlias, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
