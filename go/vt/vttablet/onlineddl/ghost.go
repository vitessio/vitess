/*
Copyright 2019 The Vitess Authors.

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

// Package heartbeat contains a writer and reader of heartbeats for a master-replica cluster.
// This is similar to Percona's pt-heartbeat, and is meant to supplement the information
// returned from SHOW SLAVE STATUS. In some circumstances, lag returned from SHOW SLAVE STATUS
// is incorrect and is at best only at 1 second resolution. The heartbeat package directly
// tests replication by writing a record with a timestamp on the master, and comparing that
// timestamp after reading it on the replica. This happens at the interval defined by heartbeat_interval.
// Note: the lag reported will be affected by clock drift, so it is recommended to run ntpd or similar.
//
// The data collected by the heartbeat package is made available in /debug/vars in counters prefixed by Heartbeat*.
// It's additionally used as a source for healthchecks and will impact the serving state of a tablet, if enabled.
// The heartbeat interval is purposefully kept distinct from the health check interval because lag measurement
// requires more frequent polling that the healthcheck typically is configured for.
package onlineddl

import (
	"vitess.io/vitess/go/stats"
)

var (
	startedMigrations    = stats.NewCounter("StartedMigrations", "Count of initiated migrations")
	successfulMigrations = stats.NewCounter("SuccessfulMigrations", "Count of successful migrations, a subset of StartedMigrations")
	failedMigrations     = stats.NewCounter("FailedMigrations", "Count of failed migrations, a subset of StartedMigrations")
)
