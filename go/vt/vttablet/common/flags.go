/*
Copyright 2024 The Vitess Authors.

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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/servenv"
)

const (
	// VReplicationExperimentalFlags is a bitmask of experimental features in vreplication.
	VReplicationExperimentalFlagOptimizeInserts           = int64(1)
	VReplicationExperimentalFlagAllowNoBlobBinlogRowImage = int64(2)
	VReplicationExperimentalFlagVPlayerBatching           = int64(4)
)

var (
	vreplicationExperimentalFlags   = VReplicationExperimentalFlagOptimizeInserts | VReplicationExperimentalFlagAllowNoBlobBinlogRowImage | VReplicationExperimentalFlagVPlayerBatching
	vreplicationNetReadTimeout      = 300
	vreplicationNetWriteTimeout     = 600
	vreplicationCopyPhaseDuration   = 1 * time.Hour
	vreplicationRetryDelay          = 5 * time.Second
	vreplicationMaxTimeToRetryError = 0 * time.Second // Default behavior is to keep retrying, for backward compatibility

	vreplicationTabletTypesStr = "in_order:REPLICA,PRIMARY" // Default value

	vreplicationRelayLogMaxSize  = 250000
	vreplicationRelayLogMaxItems = 5000

	vreplicationReplicaLagTolerance = 1 * time.Minute

	vreplicationHeartbeatUpdateInterval = 1

	vreplicationStoreCompressedGTID   = false
	vreplicationParallelInsertWorkers = 1

	// VStreamerBinlogRotationThreshold is the threshold, above which we rotate binlogs, before taking a GTID snapshot
	VStreamerBinlogRotationThreshold = int64(64 * 1024 * 1024) // 64MiB
	VStreamerDefaultPacketSize       = 250000
	VStreamerUseDynamicPacketSize    = true
)

func GetVReplicationNetReadTimeout() int {
	return vreplicationNetReadTimeout
}
func GetVReplicationNetWriteTimeout() int {
	return vreplicationNetWriteTimeout
}

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	fs.Int64Var(&vreplicationExperimentalFlags, "vreplication_experimental_flags", vreplicationExperimentalFlags,
		"(Bitmask) of experimental features in vreplication to enable")
	fs.IntVar(&vreplicationNetReadTimeout, "vreplication_net_read_timeout", vreplicationNetReadTimeout, "Session value of net_read_timeout for vreplication, in seconds")
	fs.IntVar(&vreplicationNetWriteTimeout, "vreplication_net_write_timeout", vreplicationNetWriteTimeout, "Session value of net_write_timeout for vreplication, in seconds")
	fs.DurationVar(&vreplicationCopyPhaseDuration, "vreplication_copy_phase_duration", vreplicationCopyPhaseDuration, "Duration for each copy phase loop (before running the next catchup: default 1h)")
	fs.DurationVar(&vreplicationRetryDelay, "vreplication_retry_delay", vreplicationRetryDelay, "delay before retrying a failed workflow event in the replication phase")
	fs.DurationVar(&vreplicationMaxTimeToRetryError, "vreplication_max_time_to_retry_on_error", vreplicationMaxTimeToRetryError, "stop automatically retrying when we've had consecutive failures with the same error for this long after the first occurrence")

	fs.IntVar(&vreplicationRelayLogMaxSize, "relay_log_max_size", vreplicationRelayLogMaxSize, "Maximum buffer size (in bytes) for vreplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	fs.IntVar(&vreplicationRelayLogMaxItems, "relay_log_max_items", vreplicationRelayLogMaxItems, "Maximum number of rows for vreplication target buffering.")

	fs.DurationVar(&vreplicationReplicaLagTolerance, "vreplication_replica_lag_tolerance", vreplicationReplicaLagTolerance, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no
	// real events on the source and the source vstream is only sending heartbeats for this long. Keep this low if you
	// expect high QPS and are monitoring this column to alert about potential outages. Keep this high if
	// 	 * you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//	 * you have too many streams and/or a large source field (lot of participating tables) which generates
	//     unacceptable increase in your binlog size
	fs.IntVar(&vreplicationHeartbeatUpdateInterval, "vreplication_heartbeat_update_interval", vreplicationHeartbeatUpdateInterval, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	fs.BoolVar(&vreplicationStoreCompressedGTID, "vreplication_store_compressed_gtid", vreplicationStoreCompressedGTID, "Store compressed gtids in the pos column of the sidecar database's vreplication table")

	fs.IntVar(&vreplicationParallelInsertWorkers, "vreplication-parallel-insert-workers", vreplicationParallelInsertWorkers, "Number of parallel insertion workers to use during copy phase. Set <= 1 to disable parallelism, or > 1 to enable concurrent insertion during copy phase.")

	fs.Uint64Var(&mysql.ZstdInMemoryDecompressorMaxSize, "binlog-in-memory-decompressor-max-size", mysql.ZstdInMemoryDecompressorMaxSize, "This value sets the uncompressed transaction payload size at which we switch from in-memory buffer based decompression to the slower streaming mode.")
}
