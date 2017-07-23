/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package heartbeat contains a writer and reader of heartbeats for a master-slave cluster.
// This is similar to Percona's pt-heartbeat, and is meant to supplement the information
// returned from SHOW SLAVE STATUS. In some circumstances, lag returned from SHOW SLAVE STATUS
// is incorrect and is at best only at 1 second resolution. The heartbeat package directly
// tests replication by writing a record with a timestamp on the master, and comparing that
// timestamp after reading it on the slave. This happens at the interval defined by heartbeat_interval.
// Note: the lag reported will be affected by clock drift, so it is recommended to run ntpd or similar.
//
// The data collected by the heartbeat package is made available in /debug/vars in counters prefixed by Heartbeat*.
// It's additionally used as a source for healthchecks and will impact the serving state of a tablet, if enabled.
// The heartbeat interval is purposefully kept distinct from the health check interval because lag measurement
// requires more frequent polling that the healthcheck typically is configured for.
package heartbeat

import (
	"github.com/youtube/vitess/go/stats"
)

var (
	// HeartbeatWrites keeps a count of the number of heartbeats written over time.
	writes = stats.NewInt("HeartbeatWrites")
	// HeartbeatWriteErrors keeps a count of errors encountered while writing heartbeats.
	writeErrors = stats.NewInt("HeartbeatWriteErrors")
	// HeartbeatReads keeps a count of the number of heartbeats read over time.
	reads = stats.NewInt("HeartbeatReads")
	// HeartbeatReadErrors keeps a count of errors encountered while reading heartbeats.
	readErrors = stats.NewInt("HeartbeatReadErrors")
	// HeartbeatCumulativeLagNs is incremented by the current lag at each heartbeat read interval. Plotting this
	// over time allows calculating of a rolling average lag.
	lagNs = stats.NewInt("HeartbeatCumulativeLagNs")
)
