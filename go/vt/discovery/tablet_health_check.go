/*
Copyright 2020 The Vitess Authors.

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

package discovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// tabletHealthCheck maintains the health status of a tablet. A map of this
// structure is maintained in HealthCheck.
type tabletHealthCheck struct {
	ctx context.Context
	// cancelFunc must be called before discarding tabletHealthCheck.
	// This will ensure that the associated checkConn goroutine will terminate.
	cancelFunc context.CancelFunc
	// Tablet is the tablet object that was sent to HealthCheck.AddTablet.
	Tablet *topodata.Tablet
	// mutex to protect Conn
	connMu sync.Mutex
	// Conn is the connection associated with the tablet.
	Conn queryservice.QueryService
	// Target is the current target as returned by the streaming
	// StreamHealth RPC.
	Target *query.Target
	// Serving describes if the tablet can be serving traffic.
	Serving bool
	// MasterTermStartTime is the last time at which
	// this tablet was either elected the master, or received
	// a TabletExternallyReparented event. It is set to 0 if the
	// tablet doesn't think it's a master.
	MasterTermStartTime int64
	// Stats is the current health status, as received by the
	// StreamHealth RPC (replication lag, ...).
	Stats *query.RealtimeStats
	// LastError is the error we last saw when trying to get the
	// tablet's healthcheck.
	LastError error
	// possibly delete both these
	loggedServingState    bool
	lastResponseTimestamp time.Time // timestamp of the last healthcheck response
}

// String is defined because we want to print a []*tabletHealthCheck array nicely.
func (thc *tabletHealthCheck) String() string {
	return fmt.Sprintf("tabletHealthCheck{Tablet: %v,Target: %v,Serving: %v, MasterTermStartTime: %v, Stats: %v, LastError: %v",
		thc.Tablet, thc.Target, thc.Serving, thc.MasterTermStartTime, *thc.Stats, thc.LastError)
}

// SimpleCopy returns a TabletHealth with all the necessary fields copied from tabletHealthCheck.
// Note that this is not a deep copy because we point to the same underlying RealtimeStats.
// That is fine because the RealtimeStats object is never changed after creation.
func (thc *tabletHealthCheck) SimpleCopy() *TabletHealth {
	thc.connMu.Lock()
	defer thc.connMu.Unlock()
	return &TabletHealth{
		Conn:                thc.Conn,
		Tablet:              thc.Tablet,
		Target:              thc.Target,
		Stats:               thc.Stats,
		LastError:           thc.LastError,
		MasterTermStartTime: thc.MasterTermStartTime,
		Serving:             thc.Serving,
	}
}

// setServingState sets the tablet state to the given value.
//
// If the state changes, it logs the change so that failures
// from the health check connection are logged the first time,
// but don't continue to log if the connection stays down.
//
// thc.mu must be locked before calling this function
func (thc *tabletHealthCheck) setServingState(serving bool, reason string) {
	if !thc.loggedServingState || (serving != thc.Serving) {
		// Emit the log from a separate goroutine to avoid holding
		// the th lock while logging is happening
		log.Infof("HealthCheckUpdate(Serving State): tablet: %v serving %v => %v for %v/%v (%v) reason: %s",
			topotools.TabletIdent(thc.Tablet),
			thc.Serving,
			serving,
			thc.Tablet.GetKeyspace(),
			thc.Tablet.GetShard(),
			thc.Target.GetTabletType(),
			reason,
		)
		thc.loggedServingState = true
	}
	thc.Serving = serving
}

// stream streams healthcheck responses to callback.
func (thc *tabletHealthCheck) stream(ctx context.Context, callback func(*query.StreamHealthResponse) error) error {
	conn := thc.Connection()
	if conn == nil {
		// This signals the caller to retry
		return nil
	}
	err := conn.StreamHealth(ctx, callback)
	if err != nil {
		// Depending on the specific error the caller can take action
		thc.closeConnection(ctx, err)
	}
	return err
}

func (thc *tabletHealthCheck) Connection() queryservice.QueryService {
	thc.connMu.Lock()
	defer thc.connMu.Unlock()
	return thc.connectionLocked()
}

func (thc *tabletHealthCheck) connectionLocked() queryservice.QueryService {
	if thc.Conn == nil {
		conn, err := tabletconn.GetDialer()(thc.Tablet, grpcclient.FailFast(true))
		if err != nil {
			thc.LastError = err
			return nil
		}
		thc.Conn = conn
		thc.LastError = nil
	}
	return thc.Conn
}

// processResponse reads one health check response, and updates health
func (thc *tabletHealthCheck) processResponse(hc *HealthCheckImpl, shr *query.StreamHealthResponse) error {
	select {
	case <-thc.ctx.Done():
		return thc.ctx.Err()
	default:
	}

	// Check for invalid data, better than panicking.
	if shr.Target == nil || shr.RealtimeStats == nil {
		return fmt.Errorf("health stats is not valid: %v", shr)
	}

	// an app-level error from tablet, force serving state.
	var healthErr error
	serving := shr.Serving
	if shr.RealtimeStats.HealthError != "" {
		healthErr = fmt.Errorf("vttablet error: %v", shr.RealtimeStats.HealthError)
		serving = false
	}

	if shr.TabletAlias != nil && !proto.Equal(shr.TabletAlias, thc.Tablet.Alias) {
		// TabletAlias change means that the host:port has been taken over by another tablet
		// We cancel / exit the healthcheck for this tablet right away
		// With the next topo refresh we will get a new tablet with the new host/port
		return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, fmt.Sprintf("health stats mismatch, tablet %+v alias does not match response alias %v", thc.Tablet, shr.TabletAlias))
	}

	prevTarget := thc.Target
	// check whether this is a trivial update so as to update healthy map
	trivialUpdate := thc.LastError == nil && thc.Serving && shr.RealtimeStats.HealthError == "" && shr.Serving &&
		prevTarget.TabletType != topodata.TabletType_MASTER && prevTarget.TabletType == shr.Target.TabletType && thc.isTrivialReplagChange(shr.RealtimeStats)
	thc.lastResponseTimestamp = time.Now()
	thc.Target = shr.Target
	thc.MasterTermStartTime = shr.TabletExternallyReparentedTimestamp
	thc.Stats = shr.RealtimeStats
	thc.LastError = healthErr
	reason := "healthCheck update"
	if healthErr != nil {
		reason = "healthCheck update error: " + healthErr.Error()
	}
	thc.setServingState(serving, reason)

	// notify downstream for master change
	hc.updateHealth(thc.SimpleCopy(), prevTarget, trivialUpdate, true)
	return nil
}

// isTrivialReplagChange returns true iff the old and new RealtimeStats
// haven't changed enough to warrant re-calling FilterLegacyStatsByReplicationLag.
func (thc *tabletHealthCheck) isTrivialReplagChange(newStats *query.RealtimeStats) bool {
	// first time always return false
	if thc.Stats == nil {
		return false
	}
	// Skip replag filter when replag remains in the low rep lag range,
	// which should be the case majority of the time.
	lowRepLag := lowReplicationLag.Seconds()
	oldRepLag := float64(thc.Stats.SecondsBehindMaster)
	newRepLag := float64(newStats.SecondsBehindMaster)
	if oldRepLag <= lowRepLag && newRepLag <= lowRepLag {
		return true
	}
	// Skip replag filter when replag remains in the high rep lag range,
	// and did not change beyond +/- 10%.
	// when there is a high rep lag, it takes a long time for it to reduce,
	// so it is not necessary to re-calculate every time.
	// In that case, we won't save the new record, so we still
	// remember the original replication lag.
	if oldRepLag > lowRepLag && newRepLag > lowRepLag && newRepLag < oldRepLag*1.1 && newRepLag > oldRepLag*0.9 {
		return true
	}
	return false
}

// checkConn performs health checking on the given tablet.
func (thc *tabletHealthCheck) checkConn(hc *HealthCheckImpl) {
	defer func() {
		// TODO(deepthi): We should ensure any return from this func calls the equivalent of hc.deleteTablet
		thc.finalizeConn()
		hc.connsWG.Done()
	}()

	// Initialize error counter
	hcErrorCounters.Add([]string{thc.Target.Keyspace, thc.Target.Shard, topoproto.TabletTypeLString(thc.Target.TabletType)}, 0)

	retryDelay := hc.retryDelay
	for {
		streamCtx, streamCancel := context.WithCancel(thc.ctx)

		// Setup a watcher that restarts the timer every time an update is received.
		// If a timeout occurs for a serving tablet, we make it non-serving and send
		// a status update. The stream is also terminated so it can be retried.
		// servingStatus feeds into the serving var, which keeps track of the serving
		// status transmitted by the tablet.
		servingStatus := make(chan bool, 1)
		// timedout is accessed atomically because there could be a race
		// between the goroutine that sets it and the check for its value
		// later.
		timedout := sync2.NewAtomicBool(false)
		go func() {
			for {
				select {
				case <-servingStatus:
					continue
				case <-time.After(hc.healthCheckTimeout):
					timedout.Set(true)
					streamCancel()
					return
				case <-streamCtx.Done():
					// If the stream is done, stop watching.
					return
				}
			}
		}()

		// Read stream health responses.
		err := thc.stream(streamCtx, func(shr *query.StreamHealthResponse) error {
			// We received a message. Reset the back-off.
			retryDelay = hc.retryDelay
			// Don't block on send to avoid deadlocks.
			select {
			case servingStatus <- shr.Serving:
			default:
			}
			return thc.processResponse(hc, shr)
		})

		// streamCancel to make sure the watcher goroutine terminates.
		streamCancel()

		if err != nil {
			hcErrorCounters.Add([]string{thc.Target.Keyspace, thc.Target.Shard, topoproto.TabletTypeLString(thc.Target.TabletType)}, 1)
			if strings.Contains(err.Error(), "health stats mismatch") {
				hc.deleteTablet(thc.Tablet)
				return
			}
			// trivialUpdate = false because this is an error
			// isPrimaryUp = false because we did not get a healthy response
			hc.updateHealth(thc.SimpleCopy(), thc.Target, false, false)
		}
		// If there was a timeout send an error. We do this after stream has returned.
		// This will ensure that this update prevails over any previous message that
		// stream could have sent.
		if timedout.Get() {
			thc.LastError = fmt.Errorf("healthcheck timed out (latest %v)", thc.lastResponseTimestamp)
			thc.setServingState(false, thc.LastError.Error())
			hcErrorCounters.Add([]string{thc.Target.Keyspace, thc.Target.Shard, topoproto.TabletTypeLString(thc.Target.TabletType)}, 1)
			// trivialUpdate = false because this is an error
			// isPrimaryUp = false because we did not get a healthy response within the timeout
			hc.updateHealth(thc.SimpleCopy(), thc.Target, false, false)
		}

		// Streaming RPC failed e.g. because vttablet was restarted or took too long.
		// Sleep until the next retry is up or the context is done/canceled.
		select {
		case <-thc.ctx.Done():
			return
		case <-time.After(retryDelay):
			// Exponentially back-off to prevent tight-loop.
			retryDelay *= 2
			// Limit the retry delay backoff to the health check timeout
			if retryDelay > hc.healthCheckTimeout {
				retryDelay = hc.healthCheckTimeout
			}
		}
	}
}

func (thc *tabletHealthCheck) closeConnection(ctx context.Context, err error) {
	log.Warningf("tablet %v healthcheck stream error: %v", thc.Tablet.Alias, err)
	thc.setServingState(false, err.Error())
	thc.LastError = err
	_ = thc.Conn.Close(ctx)
	thc.Conn = nil
}

// finalizeConn closes the health checking connection.
// To be called only on exit from checkConn().
func (thc *tabletHealthCheck) finalizeConn() {
	thc.setServingState(false, "finalizeConn closing connection")
	// Note: checkConn() exits only when thc.ctx.Done() is closed. Thus it's
	// safe to simply get Err() value here and assign to LastError.
	thc.LastError = thc.ctx.Err()
	if thc.Conn != nil {
		// Don't use thc.ctx because it's already closed.
		// Use a separate context, and add a timeout to prevent unbounded waits.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = thc.Conn.Close(ctx)
		thc.Conn = nil
	}
}
