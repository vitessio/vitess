// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the health check. It is always enabled in production
// vttablets (but not in vtcombo, and not in unit tests by default).
// If we are unhealthy, we'll stop the query service. In any case,
// we report our replication delay so vtgate's discovery can use this tablet
// or not.
//
// Note: we used to go to SPARE when unhealthy, and back to the target
// tablet type when healhty. Now that we use the discovery module,
// health is handled by clients subscribing to the health stream, so
// we don't need to do that any more.

import (
	"flag"
	"fmt"
	"html/template"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	defaultDegradedThreshold  = time.Duration(30 * time.Second)
	defaultUnhealthyThreshold = time.Duration(2 * time.Hour)
)

var (
	healthCheckInterval = flag.Duration("health_check_interval", 20*time.Second, "Interval between health checks")
	targetTabletType    = flag.String("target_tablet_type", "", "DEPRECATED, use init_tablet_type now.")
	degradedThreshold   = flag.Duration("degraded_threshold", defaultDegradedThreshold, "replication lag after which a replica is considered degraded (only used in status UI)")
	unhealthyThreshold  = flag.Duration("unhealthy_threshold", defaultUnhealthyThreshold, "replication lag  after which a replica is considered unhealthy")
)

// HealthRecord records one run of the health checker.
type HealthRecord struct {
	Time             time.Time
	Error            error
	IgnoredError     error
	IgnoreErrorExpr  string
	ReplicationDelay time.Duration
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

// HTML returns an HTML version to be displayed on UIs.
func (r *HealthRecord) HTML() template.HTML {
	switch {
	case r.Error != nil:
		return template.HTML(fmt.Sprintf("unhealthy: %v", r.Error))
	case r.ReplicationDelay > *degradedThreshold:
		return template.HTML(fmt.Sprintf("unhappy: %v behind on replication", r.ReplicationDelay))
	default:
		html := "healthy"
		if r.ReplicationDelay > 0 {
			html += fmt.Sprintf(": only %v behind on replication", r.ReplicationDelay)
		}
		if r.IgnoredError != nil {
			html += fmt.Sprintf(" (ignored error: %v, matches expression: %v)", r.IgnoredError, r.IgnoreErrorExpr)
		}
		return template.HTML(html)
	}
}

// Degraded returns true if the replication delay is beyond degradedThreshold.
func (r *HealthRecord) Degraded() bool {
	return r.ReplicationDelay > *degradedThreshold
}

// ErrorString returns Error as a string.
func (r *HealthRecord) ErrorString() string {
	if r.Error == nil {
		return ""
	}
	return r.Error.Error()
}

// IgnoredErrorString returns IgnoredError as a string.
func (r *HealthRecord) IgnoredErrorString() string {
	if r.IgnoredError == nil {
		return ""
	}
	return r.IgnoredError.Error()
}

// IsDuplicate implements history.Deduplicable
func (r *HealthRecord) IsDuplicate(other interface{}) bool {
	rother, ok := other.(*HealthRecord)
	if !ok {
		return false
	}
	return r.ErrorString() == rother.ErrorString() &&
		r.IgnoredErrorString() == rother.IgnoredErrorString() &&
		r.IgnoreErrorExpr == rother.IgnoreErrorExpr &&
		r.Degraded() == rother.Degraded()
}

// ConfigHTML returns a formatted summary of health checking config values.
func ConfigHTML() template.HTML {
	return template.HTML(fmt.Sprintf(
		"healthCheckInterval: %v; degradedThreshold: %v; unhealthyThreshold: %v",
		healthCheckInterval, degradedThreshold, unhealthyThreshold))
}

// initHealthCheck will start the health check background go routine,
// and configure the healthcheck shutdown. It is only run by NewActionAgent
// for real vttablet agents (not by tests, nor vtcombo).
func (agent *ActionAgent) initHealthCheck() {
	log.Infof("Starting periodic health check every %v", *healthCheckInterval)
	t := timer.NewTimer(*healthCheckInterval)
	servenv.OnTermSync(func() {
		// When we enter lameduck mode, we want to not call
		// the health check any more. After this returns, we
		// are guaranteed to not call it.
		log.Info("Stopping periodic health check timer")
		t.Stop()

		// Now we can finish up and force ourselves to not healthy.
		agent.terminateHealthChecks()
	})
	t.Start(func() {
		agent.runHealthCheck()
	})
	t.Trigger()
}

// runHealthCheck takes the action mutex, runs the health check,
// and if we need to change our state, do it. We never change our type,
// just the health we report (so we do not change the topo server at all).
// We do not interact with topo server, we use cached values for everything.
//
// This will not change the BinlogPlayerMap, but if it is not empty,
// we will think we should not be running the query service.
//
// This will not change the TabletControl record, but will use it
// to see if we should be running the query service.
func (agent *ActionAgent) runHealthCheck() {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()

	agent.runHealthCheckProtected()
}

func (agent *ActionAgent) runHealthCheckProtected() {
	// read the current tablet record and tablet control
	agent.mutex.Lock()
	tablet := proto.Clone(agent._tablet).(*topodatapb.Tablet)
	tabletControl := proto.Clone(agent._tabletControl).(*topodatapb.Shard_TabletControl)
	ignoreErrorExpr := agent._ignoreHealthErrorExpr
	agent.mutex.Unlock()

	// figure out if we should be running the query service and update stream
	shouldBeServing := false
	runUpdateStream := true
	if topo.IsRunningQueryService(tablet.Type) && (agent.BinlogPlayerMap == nil || !agent.BinlogPlayerMap.isRunningFilteredReplication()) {
		shouldBeServing = true
		if tabletControl != nil {
			if tabletControl.DisableQueryService {
				shouldBeServing = false
			}
		}
	}

	// run the health check
	record := &HealthRecord{}
	isSlaveType := true
	if tablet.Type == topodatapb.TabletType_MASTER {
		isSlaveType = false
	}

	// Remember the health error as healthErr to be sure we don't
	// accidentally overwrite it with some other err.
	replicationDelay, healthErr := agent.HealthReporter.Report(isSlaveType, shouldBeServing)
	if healthErr != nil && ignoreErrorExpr != nil &&
		ignoreErrorExpr.MatchString(healthErr.Error()) {
		// we need to ignore this health error
		record.IgnoredError = healthErr
		record.IgnoreErrorExpr = ignoreErrorExpr.String()
		healthErr = nil
	}
	if healthErr == health.ErrSlaveNotRunning {
		// The slave is not running, so we just don't know the
		// delay.  Use a maximum delay, so we can let vtgate
		// find the right replica, instead of erroring out.
		// (this works as the check below is a strict > operator).
		replicationDelay = *unhealthyThreshold
		healthErr = nil
	}
	if healthErr == nil {
		if replicationDelay > *unhealthyThreshold {
			healthErr = fmt.Errorf("reported replication lag: %v higher than unhealthy threshold: %v", replicationDelay.Seconds(), unhealthyThreshold.Seconds())
		}
	}

	// Figure out if we should be running QueryService, see if we are,
	// and reconcile.
	if healthErr != nil {
		if tablet.Type != topodatapb.TabletType_WORKER {
			// We are not healthy and must shut down QueryService.
			// At the moment, the only exception to this are "worker" tablets which
			// still must serve queries e.g. as source tablet during a "SplitClone".
			shouldBeServing = false
		}
	}
	isServing := agent.QueryServiceControl.IsServing()
	if shouldBeServing {
		if !isServing {
			// If starting queryservice fails, that's our
			// new reason for being unhealthy.
			//
			// We don't care if the QueryService state actually
			// changed because we'll broadcast the latest health
			// status after this immediately anway.
			_ /* state changed */, healthErr = agent.allowQueries(tablet.Type)

			if healthErr == nil {
				// we were unhealthy, are now healthy,
				// make sure we have the right mysql port.
				if updatedTablet := agent.checkTabletMysqlPort(agent.batchCtx, tablet); updatedTablet != nil {
					agent.setTablet(updatedTablet)
					tablet = updatedTablet
				}
			}
		}
	} else {
		if isServing {
			// We are not healthy or should not be running
			// the query service.

			// First enter lameduck during gracePeriod to
			// limit client errors.
			if topo.IsSubjectToLameduck(tablet.Type) && *gracePeriod > 0 {
				// put query service in lameduck during gracePeriod.
				agent.enterLameduck("health check failed")
				agent.broadcastHealth()
				time.Sleep(*gracePeriod)
			}

			//
			// We don't care if the QueryService state actually
			// changed because we'll broadcast the latest health
			// status after this immediately anway.
			_ /* state changed */, err := agent.disallowQueries(tablet.Type,
				fmt.Sprintf("health-check failure(%v)", healthErr),
			)
			if err != nil {
				log.Errorf("disallowQueries failed: %v", err)
			}
		}
	}

	// change UpdateStream state if necessary
	if healthErr != nil {
		runUpdateStream = false
	}
	if topo.IsRunningUpdateStream(tablet.Type) && runUpdateStream {
		agent.UpdateStream.Enable()
	} else {
		agent.UpdateStream.Disable()
	}

	// save the health record
	record.Time = time.Now()
	record.Error = healthErr
	record.ReplicationDelay = replicationDelay
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
			_, err := agent.TopoServer.UpdateTabletFields(agent.batchCtx, tablet.Alias,
				func(tablet *topodatapb.Tablet) error {
					if err := topotools.CheckOwnership(agent.initialTablet, tablet); err != nil {
						return err
					}
					tablet.PortMap["mysql"] = mysqlPort
					return nil
				})
			if err != nil {
				log.Infof("Error updating mysql port in tablet record (will try again at healthcheck interval): %v", err)
			} else {
				// save the port so we don't update it again next time
				// we do the health check.
				agent.mutex.Lock()
				agent._tablet.PortMap["mysql"] = mysqlPort
				agent._waitingForMysql = false
				agent.mutex.Unlock()
			}
		}
	}

	// remember our health status
	agent.mutex.Lock()
	agent._healthy = healthErr
	agent._healthyTime = time.Now()
	agent._replicationDelay = replicationDelay
	agent.mutex.Unlock()

	// send it to our observers
	agent.broadcastHealth()
}

// terminateHealthChecks is called when we enter lame duck mode.
// We will clean up our state, and set query service to lame duck mode.
// We only do something if we are in a serving state, and not a master.
func (agent *ActionAgent) terminateHealthChecks() {
	agent.actionMutex.Lock()
	defer agent.actionMutex.Unlock()
	log.Info("agent.terminateHealthChecks is starting")

	// read the current tablet record
	tablet := agent.Tablet()
	if !topo.IsSubjectToLameduck(tablet.Type) {
		// If we're MASTER, SPARE, WORKER, etc. then we
		// shouldn't enter lameduck. We do lameduck to not
		// trigger errors on clients.
		log.Infof("Tablet in state %v, not entering lameduck", tablet.Type)
	} else if *gracePeriod == 0 {
		log.Infof("No serving_state_grace_period set, not entering lameduck")

	} else {
		// Go lameduck for gracePeriod.
		// We've already checked above that we're not MASTER.

		// Enter new lameduck mode for gracePeriod, then shut down
		// queryservice.  New lameduck mode means keep accepting
		// queries, but advertise unhealthy.  After we return from
		// this synchronous OnTermSync hook, servenv may decide to
		// wait even longer, for the rest of the time specified by its
		// own "-lameduck-period" flag. During that extra period,
		// queryservice will be in old lameduck mode, meaning stay
		// alive but reject new queries.
		agent.enterLameduck("terminating healthchecks")
		agent.broadcastHealth()
		time.Sleep(*gracePeriod)
	}

	// in any case, we're done now
	agent.disallowQueries(tablet.Type, "terminating healthchecks")
}
