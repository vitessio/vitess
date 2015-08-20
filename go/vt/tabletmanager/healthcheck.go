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
	"html/template"
	"reflect"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	defaultDegradedThreshold  = time.Duration(30 * time.Second)
	defaultUnhealthyThreshold = time.Duration(2 * time.Hour)
)

var (
	healthCheckInterval = flag.Duration("health_check_interval", 20*time.Second, "Interval between health checks")
	targetTabletType    = flag.String("target_tablet_type", "", "The tablet type we are thriving to be when healthy. When not healthy, we'll go to spare.")
	degradedThreshold   = flag.Duration("degraded_threshold", defaultDegradedThreshold, "replication lag after which a replica is considered degraded")
	unhealthyThreshold  = flag.Duration("unhealthy_threshold", defaultUnhealthyThreshold, "replication lag  after which a replica is considered unhealthy")
)

// HealthRecord records one run of the health checker
type HealthRecord struct {
	Error            error
	ReplicationDelay time.Duration
	Time             time.Time
}

// Class returns a human-readable one word version of the health state.
func (r *HealthRecord) Class() string {
	switch {
	case r.Error != nil:
		return "unhealthy"
	case r.ReplicationDelay > *degradedThreshold:
		return "unhappy"
	default:
		return "healthy"
	}
}

// HTML returns a html version to be displayed on UIs
func (r *HealthRecord) HTML() template.HTML {
	switch {
	case r.Error != nil:
		return template.HTML(fmt.Sprintf("unhealthy: %v", r.Error))
	case r.ReplicationDelay > *degradedThreshold:
		return template.HTML(fmt.Sprintf("unhappy: %v behind on replication", r.ReplicationDelay))
	default:
		if r.ReplicationDelay > 0 {
			return template.HTML(fmt.Sprintf("healthy: only %v behind on replication", r.ReplicationDelay))
		}
		return template.HTML("healthy")
	}
}

// IsDuplicate implements history.Deduplicable
func (r *HealthRecord) IsDuplicate(other interface{}) bool {
	rother, ok := other.(*HealthRecord)
	if !ok {
		return false
	}
	if !reflect.DeepEqual(r.Error, rother.Error) {
		return false
	}
	unhealthy := r.ReplicationDelay > *degradedThreshold
	unhealthyOther := rother.ReplicationDelay > *degradedThreshold
	return (unhealthy && unhealthyOther) || (!unhealthy && !unhealthyOther)
}

// ConfigHTML returns a formatted summary of health checking config values.
func ConfigHTML() template.HTML {
	return template.HTML(fmt.Sprintf(
		"healthCheckInterval: %v; degradedThreshold: %v; unhealthyThreshold: %v",
		healthCheckInterval, degradedThreshold, unhealthyThreshold))
}

// IsRunningHealthCheck indicates if the agent is configured to run healthchecks.
func (agent *ActionAgent) IsRunningHealthCheck() bool {
	return *targetTabletType != ""
}

func (agent *ActionAgent) initHealthCheck() {
	if !agent.IsRunningHealthCheck() {
		log.Infof("No target_tablet_type specified, disabling any health check")
		return
	}

	tt, err := topoproto.ParseTabletType(*targetTabletType)
	if err != nil {
		log.Fatalf("Invalid target tablet type %v: %v", *targetTabletType, err)
	}

	log.Infof("Starting periodic health check every %v with target_tablet_type=%v", *healthCheckInterval, *targetTabletType)
	t := timer.NewTimer(*healthCheckInterval)
	servenv.OnTermSync(func() {
		// When we enter lameduck mode, we want to not call
		// the health check any more. After this returns, we
		// are guaranteed to not call it.
		log.Info("Stopping periodic health check timer")
		t.Stop()

		// Now we can finish up and force ourselves to not healthy.
		agent.terminateHealthChecks(tt)
	})
	t.Start(func() {
		agent.runHealthCheck(tt)
	})
	t.Trigger()
}

// runHealthCheck takes the action mutex, runs the health check,
// and if we need to change our state, do it.
// If we are the master, we don't change our type, healthy or not.
// If we are not the master, we change to spare if not healthy,
// or to the passed in targetTabletType if healthy.
//
// Note we only update the topo record if we need to, that is if our type or
// health details changed.
func (agent *ActionAgent) runHealthCheck(targetTabletType pbt.TabletType) {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	// read the current tablet record and tablet control
	agent.mutex.Lock()
	tablet := agent._tablet
	tabletControl := agent._tabletControl
	agent.mutex.Unlock()

	// figure out if we should be running the query service
	shouldQueryServiceBeRunning := false
	var blacklistedTables []string
	if topo.IsRunningQueryService(targetTabletType) && !agent.BinlogPlayerMap.isRunningFilteredReplication() {
		shouldQueryServiceBeRunning = true
		if tabletControl != nil {
			blacklistedTables = tabletControl.BlacklistedTables
			if tabletControl.DisableQueryService {
				shouldQueryServiceBeRunning = false
			}
		}
	}

	// run the health check
	typeForHealthCheck := targetTabletType
	if tablet.Type == pbt.TabletType_MASTER {
		typeForHealthCheck = pbt.TabletType_MASTER
	}
	replicationDelay, err := agent.HealthReporter.Report(topo.IsSlaveType(typeForHealthCheck), shouldQueryServiceBeRunning)
	health := make(map[string]string)
	if err == nil {
		if replicationDelay > *unhealthyThreshold {
			err = fmt.Errorf("reported replication lag: %v higher than unhealthy threshold: %v", replicationDelay.Seconds(), unhealthyThreshold.Seconds())
		} else if replicationDelay > *degradedThreshold {
			health[topo.ReplicationLag] = topo.ReplicationLagHigh
		}
	}

	// Figure out if we should be running QueryService, see if we are,
	// and reconcile.
	if err != nil {
		if tablet.Type != pbt.TabletType_WORKER {
			// We are not healthy and must shut down QueryService.
			// At the moment, the only exception to this are "worker" tablets which
			// still must serve queries e.g. as source tablet during a "SplitClone".
			shouldQueryServiceBeRunning = false
		}
	}
	isQueryServiceRunning := agent.QueryServiceControl.IsServing()
	if shouldQueryServiceBeRunning {
		if !isQueryServiceRunning {
			// send the type we want to be, not the type we are
			currentType := tablet.Type
			if tablet.Type == pbt.TabletType_SPARE {
				tablet.Type = targetTabletType
			}

			// we remember this new possible error
			err = agent.allowQueries(tablet.Tablet, blacklistedTables)

			// restore the current type
			tablet.Type = currentType
		}
	} else {
		if isQueryServiceRunning {
			// we are not healthy or should not be running the
			// query service, shut it down.
			agent.disallowQueries()
		}
	}

	// save the health record
	record := &HealthRecord{
		Error:            err,
		ReplicationDelay: replicationDelay,
		Time:             time.Now(),
	}
	agent.History.Add(record)

	// try to figure out the mysql port if we don't have it yet
	if _, ok := tablet.PortMap["mysql"]; !ok {
		// we don't know the port, try to get it from mysqld
		mysqlPort, err := agent.MysqlDaemon.GetMysqlPort()
		if err != nil {
			// Don't log if we're already in a waiting-for-mysql state.
			agent.mutex.Lock()
			if !agent._waitingForMysql {
				log.Warningf("Can't get mysql port, won't populate Tablet record in topology (will retry silently at healthcheck interval %v): %v", *healthCheckInterval, err)
				agent._waitingForMysql = true
			}
			agent.mutex.Unlock()
		} else {
			log.Infof("Updating tablet mysql port to %v", mysqlPort)
			if err := agent.TopoServer.UpdateTabletFields(agent.batchCtx, tablet.Alias, func(tablet *pbt.Tablet) error {
				tablet.PortMap["mysql"] = int32(mysqlPort)
				return nil
			}); err != nil {
				log.Infof("Error updating mysql port in tablet record: %v", err)
				return
			}

			// save the port so we don't update it again next time
			// we do the health check.
			agent.mutex.Lock()
			agent._tablet.PortMap["mysql"] = int32(mysqlPort)
			agent._waitingForMysql = false
			agent.mutex.Unlock()
		}
	}

	// remember our health status
	agent.mutex.Lock()
	agent._healthy = err
	agent._healthyTime = time.Now()
	agent._replicationDelay = replicationDelay
	terTime := agent._tabletExternallyReparentedTime
	agent.mutex.Unlock()

	// send it to our observers
	// (the Target has already been updated when restarting the
	// query service earlier)
	// FIXME(alainjobart,liguo) add CpuUsage
	stats := &pb.RealtimeStats{
		SecondsBehindMaster: uint32(replicationDelay.Seconds()),
	}
	stats.SecondsBehindMasterFilteredReplication, stats.BinlogPlayersCount = agent.BinlogPlayerMap.StatusSummary()
	if err != nil {
		stats.HealthError = err.Error()
	}
	defer func() {
		var ts int64
		if !terTime.IsZero() {
			ts = terTime.Unix()
		}
		agent.QueryServiceControl.BroadcastHealth(ts, stats)
	}()

	// Update our topo.Server state, start with no change
	newTabletType := tablet.Type
	if err != nil {
		// The tablet is not healthy, let's see what we need to do
		if tablet.Type != targetTabletType {
			if tablet.Type != pbt.TabletType_SPARE {
				// we only log if we're not in spare,
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
		newTabletType = pbt.TabletType_SPARE
	} else {
		// We are healthy, maybe with health, see if we need
		// to update the record. We only change from spare to
		// our target type.
		if tablet.Type == pbt.TabletType_SPARE {
			newTabletType = targetTabletType
		}
		if tablet.Type == newTabletType && topo.IsHealthEqual(health, tablet.HealthMap) {
			// no change in health, not logging anything,
			// and we're done
			return
		}

		// we need to update our state
		log.Infof("Updating tablet record as healthy type %v -> %v with health details %v -> %v", tablet.Type, newTabletType, tablet.HealthMap, health)
		agent.lastHealthMapCount.Set(int64(len(health)))
	}

	// Change the Type, update the health. Note we pass in a map
	// that's not nil, meaning if it's empty, we will clear it.
	if err := topotools.ChangeType(agent.batchCtx, agent.TopoServer, tablet.Alias, newTabletType, health); err != nil {
		log.Infof("Error updating tablet record: %v", err)
		return
	}
	tablet.HealthMap = health
	tablet.Type = newTabletType

	// Rebuild the serving graph in our cell, only if we're dealing with
	// a serving type
	if err := agent.updateServingGraph(tablet, targetTabletType); err != nil {
		log.Warningf("updateServingGraph failed (will still run post action callbacks, serving graph might be out of date): %v", err)
	}

	// run the post action callbacks, not much we can do with returned error
	if err := agent.refreshTablet(agent.batchCtx, "healthcheck"); err != nil {
		log.Warningf("refreshTablet failed: %v", err)
	}
}

// terminateHealthChecks is called when we enter lame duck mode.
// We will clean up our state, and shut down query service.
// We only do something if we are in targetTabletType state, and then
// we just go to spare.
func (agent *ActionAgent) terminateHealthChecks(targetTabletType pbt.TabletType) {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()
	log.Info("agent.terminateHealthChecks is starting")

	// read the current tablet record
	tablet := agent.Tablet()
	if tablet.Type != targetTabletType {
		log.Infof("Tablet in state %v, not changing it", tablet.Type)
		return
	}

	// Change the Type to spare, update the health. Note we pass in a map
	// that's not nil, meaning we will clear it.
	if err := topotools.ChangeType(agent.batchCtx, agent.TopoServer, tablet.Alias, pbt.TabletType_SPARE, make(map[string]string)); err != nil {
		log.Infof("Error updating tablet record: %v", err)
		return
	}

	// Update the serving graph in our cell, only if we're dealing with
	// a serving type
	if err := agent.updateServingGraph(tablet, targetTabletType); err != nil {
		log.Warningf("updateServingGraph failed (will still run post action callbacks, serving graph might be out of date): %v", err)
	}

	// We've already rebuilt the shard, which is the only reason we registered
	// ourself as OnTermSync (synchronous). The rest can be done asynchronously.
	go func() {
		// Run the post action callbacks (let them shutdown the query service)
		if err := agent.refreshTablet(agent.batchCtx, "terminatehealthcheck"); err != nil {
			log.Warningf("refreshTablet failed: %v", err)
		}
	}()
}

// updateServingGraph will update the serving graph if we need to.
func (agent *ActionAgent) updateServingGraph(tablet *topo.TabletInfo, targetTabletType pbt.TabletType) error {
	if topo.IsInServingGraph(targetTabletType) {
		if err := topotools.UpdateTabletEndpoints(agent.batchCtx, agent.TopoServer, tablet.Tablet); err != nil {
			return fmt.Errorf("UpdateTabletEndpoints failed: %v", err)
		}
	}
	return nil
}
