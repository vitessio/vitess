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

	"vitess.io/vitess/go/stats"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type method int

const (
	none = method(iota)
	polling
	heartbeat
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
)

// ReplTracker tracks replication lag.
type ReplTracker struct {
	mu       sync.Mutex
	isMaster bool
	method   method

	hw *heartbeatWriter
	hr *heartbeatReader
}

// NewReplTracker creates a new ReplTracker.
func NewReplTracker(env tabletenv.Env, alias topodatapb.TabletAlias) *ReplTracker {
	return &ReplTracker{
		hw: newHeartbeatWriter(env, alias),
		hr: newHeartbeatReader(env),
	}
}

// InitDBConfig initializes the target name.
func (rt *ReplTracker) InitDBConfig(target querypb.Target) {
	rt.hw.InitDBConfig(target)
	rt.hr.InitDBConfig(target)
}
