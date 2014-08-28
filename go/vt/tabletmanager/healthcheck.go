// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the health check. It is enabled by passing a
// target_tablet_type command line parameter. The tablet will then go
// to the target tablet type if healthy, and to 'spare' if not.

import (
	"flag"
	"fmt"
	"reflect"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

var (
	healthCheckInterval = flag.Duration("health_check_interval", 20*time.Second, "Interval between health checks")
	targetTabletType    = flag.String("target_tablet_type", "", "The tablet type we are thriving to be when healthy. When not healthy, we'll go to spare.")
)

// HealthRecord records one run of the health checker
type HealthRecord struct {
	Error  error
	Result map[string]string
	Time   time.Time
}

// This returns a readable one word version of the health
func (r *HealthRecord) Class() string {
	switch {
	case r.Error != nil:
		return "unhealthy"
	case len(r.Result) > 0:
		return "unhappy"
	default:
		return "healthy"
	}
}

// IsDuplicate implements history.Deduplicable
func (r *HealthRecord) IsDuplicate(other interface{}) bool {
	rother, ok := other.(*HealthRecord)
	if !ok {
		return false
	}
	return reflect.DeepEqual(r.Error, rother.Error) && reflect.DeepEqual(r.Result, rother.Result)
}

func (agent *ActionAgent) IsRunningHealthCheck() bool {
	return *targetTabletType != ""
}

func (agent *ActionAgent) initHeathCheck() {
	if !agent.IsRunningHealthCheck() {
		log.Infof("No target_tablet_type specified, disabling any health check")
		return
	}

	log.Infof("Starting periodic health check every %v with target_tablet_type=%v", *healthCheckInterval, *targetTabletType)
	t := timer.NewTimer(*healthCheckInterval)
	servenv.OnTerm(func() {
		// When we enter lameduck mode, we want to not call
		// the health check any more. After this returns, we
		// are guaranteed to not call it.
		log.Info("Stopping periodic health check timer")
		t.Stop()

		// Now we can finish up and force ourselves to not healthy.
		agent.terminateHealthChecks(topo.TabletType(*targetTabletType))
	})
	t.Start(func() {
		agent.runHealthCheck(topo.TabletType(*targetTabletType))
	})
}

// runHealthCheck takes the action mutex, runs the health check,
// and if we need to change our state, do it.
// If we are the master, we don't change our type, healthy or not.
// If we are not the master, we change to spare if not healthy,
// or to the passed in targetTabletType if healthy.
//
// Note we only update the topo record if we need to, that is if our type or
// health details changed.
func (agent *ActionAgent) runHealthCheck(targetTabletType topo.TabletType) {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	// read the current tablet record
	agent.mutex.Lock()
	tablet := agent._tablet
	agent.mutex.Unlock()

	// run the health check
	typeForHealthCheck := targetTabletType
	if tablet.Type == topo.TYPE_MASTER {
		typeForHealthCheck = topo.TYPE_MASTER
	}
	health, err := health.Run(typeForHealthCheck)

	// Figure out if we should be running QueryService. If we should,
	// and we aren't, and we're otherwise healthy, try to start it.
	if err == nil && topo.IsRunningQueryService(targetTabletType) && agent.BinlogPlayerMap.size() == 0 {
		err = agent.allowQueries(tablet.Tablet)
	}

	// save the health record
	record := &HealthRecord{
		Error:  err,
		Result: health,
		Time:   time.Now(),
	}
	agent.History.Add(record)

	// Update our topo.Server state, start with no change
	newTabletType := tablet.Type
	if err != nil {
		// The tablet is not healthy, let's see what we need to do
		if tablet.Type != targetTabletType {
			if tablet.Type != topo.TYPE_SPARE {
				// we onyl log if we're not in spare,
				// as the spare state is normal for a
				// failed health check.
				log.Infof("Tablet not healthy and in state %v, not changing it: %v", tablet.Type, err)
			}
			return
		}

		// Note that if the query service is running, we may
		// need to stop it. The post-action callback will do
		// it, and it will be done after we change our state,
		// so it's the right order, let it do it.
		log.Infof("Tablet not healthy, converting it from %v to spare: %v", targetTabletType, err)
		newTabletType = topo.TYPE_SPARE
	} else {
		// We are healthy, maybe with health, see if we need
		// to update the record. We only change from spare to
		// our target type.
		if tablet.Type == topo.TYPE_SPARE {
			newTabletType = targetTabletType
		}
		if tablet.Type == newTabletType && tablet.IsHealthEqual(health) {
			// no change in health, not logging anything,
			// and we're done
			return
		}

		// we need to update our state
		log.Infof("Updating tablet record as healthy type %v -> %v with health details %v -> %v", tablet.Type, newTabletType, tablet.Health, health)
		agent.lastHealthMapCount.Set(int64(len(health)))
	}

	// Change the Type, update the health. Note we pass in a map
	// that's not nil, meaning if it's empty, we will clear it.
	if err := topotools.ChangeType(agent.TopoServer, tablet.Alias, newTabletType, health, true /*runHooks*/); err != nil {
		log.Infof("Error updating tablet record: %v", err)
		return
	}

	// Rebuild the serving graph in our cell, only if we're dealing with
	// a serving type
	if err := agent.rebuildShardIfNeeded(tablet, targetTabletType); err != nil {
		log.Warningf("rebuildShardIfNeeded failed, not running post action callbacks: %v", err)
		return
	}

	// run the post action callbacks
	agent.afterAction("healthcheck", false /* reloadSchema */)
}

// terminateHealthChecks is called when we enter lame duck mode.
// We will clean up our state, and shut down query service.
// We only do something if we are in targetTabletType state, and then
// we just go to spare.
func (agent *ActionAgent) terminateHealthChecks(targetTabletType topo.TabletType) {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()
	log.Info("agent.terminateHealthChecks is starting")

	// read the current tablet record
	agent.mutex.Lock()
	tablet := agent._tablet
	agent.mutex.Unlock()

	if tablet.Type != targetTabletType {
		log.Infof("Tablet in state %v, not changing it", tablet.Type)
		return
	}

	// Change the Type to spare, update the health. Note we pass in a map
	// that's not nil, meaning we will clear it.
	if err := topotools.ChangeType(agent.TopoServer, tablet.Alias, topo.TYPE_SPARE, make(map[string]string), true /*runHooks*/); err != nil {
		log.Infof("Error updating tablet record: %v", err)
		return
	}

	// Rebuild the serving graph in our cell, only if we're dealing with
	// a serving type
	if err := agent.rebuildShardIfNeeded(tablet, targetTabletType); err != nil {
		log.Warningf("rebuildShardIfNeeded failed, not running post action callbacks: %v", err)
		return
	}

	// Run the post action callbacks (let them shutdown the query service)
	agent.afterAction("terminatehealthcheck", false /* reloadSchema */)
}

// rebuildShardIfNeeded will rebuild the serving graph if we need to
func (agent *ActionAgent) rebuildShardIfNeeded(tablet *topo.TabletInfo, targetTabletType topo.TabletType) error {
	if topo.IsInServingGraph(targetTabletType) {
		// TODO: interrupted may need to be a global one closed when we exit
		interrupted := make(chan struct{})

		// no need to take the shard lock in this case
		if err := topotools.RebuildShard(logutil.NewConsoleLogger(), agent.TopoServer, tablet.Keyspace, tablet.Shard, []string{tablet.Alias.Cell}, *LockTimeout, interrupted); err != nil {
			return fmt.Errorf("topotools.RebuildShard returned an error: %v", err)
		}
	}
	return nil
}
