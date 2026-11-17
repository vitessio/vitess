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

package repltracker

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/heartbeat"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// HeartbeatWrites keeps a count of the number of heartbeats written over time.
	writes = stats.NewCounter("HeartbeatWrites", "Count of heartbeats written over time")
	// HeartbeatWriteErrors keeps a count of errors encountered while writing heartbeats.
	writeErrors = stats.NewCounter("HeartbeatWriteErrors", "Count of errors encountered while writing heartbeats")
	// HeartbeatReads keeps a count of the number of heartbeats read over time.
	reads = stats.NewCounter("HeartbeatReads", "Count of heartbeats read over time")
	// HeartbeatReadErrors keeps a count of errors encountered while reading heartbeats.
	readErrors = stats.NewCounter("HeartbeatReadErrors", "Count of errors encountered while reading heartbeats")
	// HeartbeatCumulativeLagNs is incremented by the current lag at each heartbeat read interval. Plotting this
	// over time allows calculating of a rolling average lag.
	cumulativeLagNs = stats.NewCounter("HeartbeatCumulativeLagNs", "Incremented by the current lag at each heartbeat read interval")
	// HeartbeatCurrentLagNs is a point-in-time calculation of the lag, updated at each heartbeat read interval.
	currentLagNs = stats.NewGauge("HeartbeatCurrentLagNs", "Point in time calculation of the heartbeat lag")
	// HeartbeatLagNsHistogram is a histogram of the lag values. Cutoffs are 0, 1ms, 10ms, 100ms, 1s, 10s, 100s, 1000s
	heartbeatLagNsHistogram = stats.NewGenericHistogram("HeartbeatLagNsHistogram",
		"Histogram of lag values in nanoseconds", []int64{0, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12},
		[]string{"0", "1ms", "10ms", "100ms", "1s", "10s", "100s", "1000s", ">1000s"}, "Count", "Total")
)

// ReplTracker tracks replication lag.
type ReplTracker struct {
	mode string

	mu        sync.Mutex
	isPrimary bool
	// writesSuppressed keeps the heartbeat writer closed while the tablet is PRIMARY (see
	// SetHeartbeatWritesSuppressed).
	writesSuppressed bool

	hw     *heartbeatWriter
	hr     *heartbeatReader
	poller *poller
}

// NewReplTracker creates a new ReplTracker.
func NewReplTracker(env tabletenv.Env, alias *topodatapb.TabletAlias) *ReplTracker {
	return &ReplTracker{
		mode:   env.Config().ReplicationTracker.Mode,
		hw:     newHeartbeatWriter(env, alias),
		hr:     newHeartbeatReader(env),
		poller: &poller{},
	}
}

// HeartbeatWriter returns the heartbeat writer used by this tracker
func (rt *ReplTracker) HeartbeatWriter() heartbeat.HeartbeatWriter {
	return rt.hw
}

// InitDBConfig initializes the target name.
func (rt *ReplTracker) InitDBConfig(target *querypb.Target, mysqld mysqlctl.MysqlDaemon) {
	rt.hw.InitDBConfig(target)
	rt.hr.InitDBConfig(target)
	rt.poller.InitDBConfig(mysqld)
}

// MakePrimary must be called if the tablet type becomes PRIMARY.
func (rt *ReplTracker) MakePrimary() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	log.Info("Replication Tracker: going into primary mode")

	rt.isPrimary = true
	if rt.mode == tabletenv.Heartbeat {
		rt.hr.Close()
	}
	if rt.writesSuppressed {
		log.Info("Replication Tracker: heartbeat writes are suppressed")
		rt.hw.Close()
	} else {
		rt.hw.Open()
	}
	replicationLagSeconds.Reset() // we are the primary, we have no lag
}

// SetHeartbeatWritesSuppressed keeps the heartbeat writer of a primary closed while suppressed is
// set, whatever its serving state: neither the periodic heartbeats (--heartbeat-enable) nor the
// on-demand ones are written. The tablet manager sets it while a Group Replication primary must
// not commit anything (its group lacks a majority of the shard's voters, or it is about to
// bootstrap a new group), because MySQL is still writable then: a heartbeat would be committed
// on a single voter. A primary that does not serve for another reason, a semi-sync primary for
// example, keeps writing heartbeats.
func (rt *ReplTracker) SetHeartbeatWritesSuppressed(suppressed bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.writesSuppressed == suppressed {
		return
	}
	rt.writesSuppressed = suppressed
	log.Info(fmt.Sprintf("Replication Tracker: heartbeat writes suppressed: %v", suppressed))
	if !rt.isPrimary {
		return
	}
	if suppressed {
		rt.hw.Close()
	} else {
		rt.hw.Open()
	}
}

// MakeNonPrimary must be called if the tablet type becomes non-PRIMARY.
func (rt *ReplTracker) MakeNonPrimary() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	log.Info("Replication Tracker: going into non-primary mode")

	rt.isPrimary = false
	switch rt.mode {
	case tabletenv.Heartbeat:
		rt.hw.Close()
		rt.hr.Open()
	case tabletenv.Polling:
		// Run the status once to pre-initialize values.
		rt.poller.Status()
	}
	rt.hw.Close()
}

// Close closes ReplTracker.
func (rt *ReplTracker) Close() {
	rt.hw.Close()
	rt.hr.Close()
	log.Info("Replication Tracker: closed")
}

// Status reports the replication status.
func (rt *ReplTracker) Status() (time.Duration, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	switch {
	case rt.isPrimary || rt.mode == tabletenv.Disable:
		replicationLagSeconds.Reset() // we are the primary, we have no lag
		return 0, nil
	case rt.mode == tabletenv.Heartbeat:
		return rt.hr.Status()
	}
	// rt.mode == tabletenv.Poller
	return rt.poller.Status()
}

// SetGroupReplicationVerdict records the tablet manager's verdict about the group membership of
// the tablet's MySQL, which the replication lag poller needs on a Group Replication member:
// healthy is set when MySQL is ONLINE in the shard's legitimate replication group, with a
// majority of the shard's voters ONLINE in its view, and viewID is the view the verdict is about.
// The tablet manager renews it on every run of its group replication sync loop.
func (rt *ReplTracker) SetGroupReplicationVerdict(healthy bool, viewID string) {
	rt.poller.SetGroupReplicationVerdict(healthy, viewID)
}

// EnableHeartbeat enables or disables writes of heartbeat. This functionality
// is only used by tests.
func (rt *ReplTracker) EnableHeartbeat(enable bool) {
	if enable {
		rt.hw.enableWrites()
	} else {
		rt.hw.disableWrites()
	}
}
