/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	retryDelay          = 5 * time.Second
	maxTimeToRetryError time.Duration // Default behavior is to keep retrying, for backward compatibility

	tabletTypesStr = "in_order:REPLICA,PRIMARY" // Default value

	relayLogMaxSize  = 250000
	relayLogMaxItems = 5000

	replicaLagTolerance = 1 * time.Minute

	vreplicationHeartbeatUpdateInterval = 1

	vreplicationStoreCompressedGTID   = false
	vreplicationParallelInsertWorkers = 1
)

func registerVReplicationFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&retryDelay, "vreplication_retry_delay", retryDelay, "delay before retrying a failed workflow event in the replication phase")
	fs.DurationVar(&maxTimeToRetryError, "vreplication_max_time_to_retry_on_error", maxTimeToRetryError, "stop automatically retrying when we've had consecutive failures with the same error for this long after the first occurrence")

	fs.IntVar(&relayLogMaxSize, "relay_log_max_size", relayLogMaxSize, "Maximum buffer size (in bytes) for VReplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	fs.IntVar(&relayLogMaxItems, "relay_log_max_items", relayLogMaxItems, "Maximum number of rows for VReplication target buffering.")

	fs.DurationVar(&replicaLagTolerance, "vreplication_replica_lag_tolerance", replicaLagTolerance, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no real events on the source and the source
	// vstream is only sending heartbeats for this long. Keep this low if you expect high QPS and are monitoring this column to alert about potential
	// outages. Keep this high if
	// 		you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//		you have too many streams and/or a large source field (lot of participating tables) which generates unacceptable increase in your binlog size
	fs.IntVar(&vreplicationHeartbeatUpdateInterval, "vreplication_heartbeat_update_interval", vreplicationHeartbeatUpdateInterval, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	fs.BoolVar(&vreplicationStoreCompressedGTID, "vreplication_store_compressed_gtid", vreplicationStoreCompressedGTID, "Store compressed gtids in the pos column of the sidecar database's vreplication table")

	fs.IntVar(&vreplicationParallelInsertWorkers, "vreplication-parallel-insert-workers", vreplicationParallelInsertWorkers, "Number of parallel insertion workers to use during copy phase. Set <= 1 to disable parallelism, or > 1 to enable concurrent insertion during copy phase.")

	// Deprecated and ignored in v19.
	fs.String("vreplication_tablet_type", tabletTypesStr, "Comma-separated list of tablet types used as a source.")
	fs.MarkDeprecated("vreplication_tablet_type", "As of v19 this is ignored and will be removed in a future release.")
}

func init() {
	servenv.OnParseFor("vtcombo", registerVReplicationFlags)
	servenv.OnParseFor("vttablet", registerVReplicationFlags)
}
