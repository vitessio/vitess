/*
Copyright 2023 The Vitess Authors.

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

package vttablet

import (
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

const (
	// VReplicationExperimentalFlags is a bitmask of experimental features in vreplication.
	VReplicationExperimentalFlagOptimizeInserts           = int64(1)
	VReplicationExperimentalFlagAllowNoBlobBinlogRowImage = int64(2)
	VReplicationExperimentalFlagVPlayerBatching           = int64(4)
)

var (
	// Default flags.
	VReplicationExperimentalFlags   = VReplicationExperimentalFlagOptimizeInserts | VReplicationExperimentalFlagAllowNoBlobBinlogRowImage
	VReplicationNetReadTimeout      = 300
	VReplicationNetWriteTimeout     = 600
	VReplicationCopyPhaseDuration   = 1 * time.Hour
	VReplicationRetryDelay          = 5 * time.Second
	VReplicationMaxTimeToRetryError = 0 * time.Second // Default behavior is to keep retrying, for backward compatibility

	VReplicationTabletTypesStr = "in_order:REPLICA,PRIMARY" // Default value

	VReplicationRelayLogMaxSize  = 250000
	VReplicationRelayLogMaxItems = 5000

	VReplicationReplicaLagTolerance = 1 * time.Minute

	VReplicationHeartbeatUpdateInterval = 1

	VReplicationStoreCompressedGTID   = false
	VReplicationParallelInsertWorkers = 1
)

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)

}

func registerFlags(fs *pflag.FlagSet) {
	fs.Int64Var(&VReplicationExperimentalFlags, "vreplication_experimental_flags", VReplicationExperimentalFlags,
		"(Bitmask) of experimental features in vreplication to enable")
	fs.IntVar(&VReplicationNetReadTimeout, "vreplication_net_read_timeout", VReplicationNetReadTimeout, "Session value of net_read_timeout for vreplication, in seconds")
	fs.IntVar(&VReplicationNetWriteTimeout, "vreplication_net_write_timeout", VReplicationNetWriteTimeout, "Session value of net_write_timeout for vreplication, in seconds")
	fs.DurationVar(&VReplicationCopyPhaseDuration, "vreplication_copy_phase_duration", VReplicationCopyPhaseDuration, "Duration for each copy phase loop (before running the next catchup: default 1h)")
	fs.DurationVar(&VReplicationRetryDelay, "vreplication_retry_delay", VReplicationRetryDelay, "delay before retrying a failed workflow event in the replication phase")
	fs.DurationVar(&VReplicationMaxTimeToRetryError, "vreplication_max_time_to_retry_on_error", VReplicationMaxTimeToRetryError, "stop automatically retrying when we've had consecutive failures with the same error for this long after the first occurrence")

	fs.IntVar(&VReplicationRelayLogMaxSize, "relay_log_max_size", VReplicationRelayLogMaxSize, "Maximum buffer size (in bytes) for VReplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	fs.IntVar(&VReplicationRelayLogMaxItems, "relay_log_max_items", VReplicationRelayLogMaxItems, "Maximum number of rows for VReplication target buffering.")

	fs.DurationVar(&VReplicationReplicaLagTolerance, "vreplication_replica_lag_tolerance", VReplicationReplicaLagTolerance, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no real events on the source and the source
	// vstream is only sending heartbeats for this long. Keep this low if you expect high QPS and are monitoring this column to alert about potential
	// outages. Keep this high if
	// 		you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//		you have too many streams and/or a large source field (lot of participating tables) which generates unacceptable increase in your binlog size
	fs.IntVar(&VReplicationHeartbeatUpdateInterval, "vreplication_heartbeat_update_interval", VReplicationHeartbeatUpdateInterval, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	fs.BoolVar(&VReplicationStoreCompressedGTID, "vreplication_store_compressed_gtid", VReplicationStoreCompressedGTID, "Store compressed gtids in the pos column of the sidecar database's vreplication table")

	fs.IntVar(&VReplicationParallelInsertWorkers, "vreplication-parallel-insert-workers", VReplicationParallelInsertWorkers, "Number of parallel insertion workers to use during copy phase. Set <= 1 to disable parallelism, or > 1 to enable concurrent insertion during copy phase.")
}
