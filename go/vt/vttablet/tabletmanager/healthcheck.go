/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/health"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	defaultDegradedThreshold  = time.Duration(30 * time.Second)
	defaultUnhealthyThreshold = time.Duration(2 * time.Hour)
)

var (
	healthCheckInterval = flag.Duration("health_check_interval", 20*time.Second, "Interval between health checks")
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
	registerReplicationReporter(agent)
	registerHeartbeatReporter(agent.QueryServiceControl)

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
	if err := agent.lock(agent.batchCtx); err != nil {
		log.Warningf("cannot lock actionMutex, not running HealthCheck")
		return
	}
	defer agent.unlock()

	agent.runHealthCheckLocked()
}

func (agent *ActionAgent) runHealthCheckLocked() {
	// read the current tablet record and tablet control
	agent.mutex.Lock()
	tablet := proto.Clone(agent._tablet).(*topodatapb.Tablet)
	shouldBeServing := agent._disallowQueryService == ""
	runUpdateStream := agent._enableUpdateStream
	ignoreErrorExpr := agent._ignoreHealthErrorExpr
	agent.mutex.Unlock()

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
		if tablet.Type != topodatapb.TabletType_DRAINED {
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
			_ /* state changed */, healthErr = agent.QueryServiceControl.SetServingType(tablet.Type, true, nil)

			if healthErr == nil {
				// We were unhealthy, are now healthy,
				// make sure we have the right mysql port.
				// Also make sure to display any error message.
				agent.gotMysqlPort = false
				agent.waitingForMysql = false
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
				agent.lameduck("health check failed")
			}

			// We don't care if the QueryService state actually
			// changed because we'll broadcast the latest health
			// status after this immediately anway.
			log.Infof("Disabling query service because of health-check failure: %v", healthErr)
			if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, false, nil); err != nil {
				log.Errorf("SetServingType(serving=false) failed: %v", err)
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

	// All master tablets have to run the VReplication engine.
	// There is no guarantee that VREngine was succesfully started when tabletmanager
	// came up. This is because the mysql could have been in read-only mode, etc.
	// So, start the engine if it's not already running.
	if tablet.Type == topodatapb.TabletType_MASTER && !agent.VREngine.IsOpen() {
		if err := agent.VREngine.Open(agent.batchCtx); err == nil {
			log.Info("VReplication engine successfully started")
		}
	}

	// save the health record
	record.Time = time.Now()
	record.Error = healthErr
	record.ReplicationDelay = replicationDelay
	agent.History.Add(record)

	// Try to figure out the mysql port if we don't have it yet.
	if !agent.gotMysqlPort {
		if updatedTablet := agent.checkTabletMysqlPort(agent.batchCtx, tablet); updatedTablet != nil {
			agent.setTablet(updatedTablet)
			tablet = updatedTablet
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
	// No need to check for error, only a canceled batchCtx would fail this.
	agent.lock(agent.batchCtx)
	defer agent.unlock()
	log.Info("agent.terminateHealthChecks is starting")

	// read the current tablet record
	tablet := agent.Tablet()
	if !topo.IsSubjectToLameduck(tablet.Type) {
		// If we're MASTER, SPARE, WORKER, etc. then we
		// shouldn't enter lameduck. We do lameduck to not
		// trigger errors on clients.
		log.Infof("Tablet in state %v, not entering lameduck", tablet.Type)
		return
	}

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
	agent.lameduck("terminating healthchecks")

	// Note we only do this now if we entered lameduck. In the
	// master case for instance, we want to keep serving until
	// vttablet dies entirely (where else is the client going to
	// go?).  After servenv lameduck, the queryservice is stopped
	// from a servenv.OnClose() hook anyway.
	log.Infof("Disabling query service after lameduck in terminating healthchecks")
	agent.QueryServiceControl.SetServingType(tablet.Type, false, nil)
}
