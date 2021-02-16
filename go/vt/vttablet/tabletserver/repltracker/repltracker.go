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
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
	mode           string
	forceHeartbeat bool

	mu       sync.Mutex
	isMaster bool

	hw     *heartbeatWriter
	hr     *heartbeatReader
	poller *poller
}

// NewReplTracker creates a new ReplTracker.
func NewReplTracker(env tabletenv.Env, alias topodatapb.TabletAlias) *ReplTracker {
	return &ReplTracker{
		mode:           env.Config().ReplicationTracker.Mode,
		forceHeartbeat: env.Config().EnableLagThrottler,
		hw:             newHeartbeatWriter(env, alias),
		hr:             newHeartbeatReader(env),
		poller:         &poller{},
	}
}

// InitDBConfig initializes the target name.
func (rt *ReplTracker) InitDBConfig(target querypb.Target, mysqld mysqlctl.MysqlDaemon) {
	rt.hw.InitDBConfig(target)
	rt.hr.InitDBConfig(target)
	rt.poller.InitDBConfig(mysqld)
}

// MakeMaster must be called if the tablet type becomes MASTER.
func (rt *ReplTracker) MakeMaster() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	log.Info("Replication Tracker: going into master mode")

	rt.isMaster = true
	if rt.mode == tabletenv.Heartbeat {
		rt.hr.Close()
		rt.hw.Open()
	}
	if rt.forceHeartbeat {
		rt.hw.Open()
	}
}

// MakeNonMaster must be called if the tablet type becomes non-MASTER.
func (rt *ReplTracker) MakeNonMaster() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	log.Info("Replication Tracker: going into non-master mode")

	rt.isMaster = false
	switch rt.mode {
	case tabletenv.Heartbeat:
		rt.hw.Close()
		rt.hr.Open()
	case tabletenv.Polling:
		// Run the status once to pre-initialize values.
		rt.poller.Status()
	}
	if rt.forceHeartbeat {
		rt.hw.Close()
	}
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
	case rt.isMaster || rt.mode == tabletenv.Disable:
		return 0, nil
	case rt.mode == tabletenv.Heartbeat:
		return rt.hr.Status()
	}
	// rt.mode == tabletenv.Poller
	return rt.poller.Status()
}

// EnableHeartbeat enables or disables writes of heartbeat. This functionality
// is only used by tests.
func (rt *ReplTracker) EnableHeartbeat(enable bool) {
	rt.hw.enableWrites(enable)
}
