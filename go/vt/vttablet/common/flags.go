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
	"vitess.io/vitess/go/vt/utils"
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

	// Enable the /debug/vrlog HTTP endpoint.
	vreplicationEnableHttpLog = false
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
	utils.SetFlagInt64Var(fs, &vreplicationExperimentalFlags, "vreplication-experimental-flags", vreplicationExperimentalFlags,
		"(Bitmask) of experimental features in vreplication to enable")
	utils.SetFlagIntVar(fs, &vreplicationNetReadTimeout, "vreplication-net-read-timeout", vreplicationNetReadTimeout, "Session value of net_read_timeout for vreplication, in seconds")
	utils.SetFlagIntVar(fs, &vreplicationNetWriteTimeout, "vreplication-net-write-timeout", vreplicationNetWriteTimeout, "Session value of net_write_timeout for vreplication, in seconds")
	utils.SetFlagDurationVar(fs, &vreplicationCopyPhaseDuration, "vreplication-copy-phase-duration", vreplicationCopyPhaseDuration, "Duration for each copy phase loop (before running the next catchup: default 1h)")
	utils.SetFlagDurationVar(fs, &vreplicationRetryDelay, "vreplication-retry-delay", vreplicationRetryDelay, "delay before retrying a failed workflow event in the replication phase")
	utils.SetFlagDurationVar(fs, &vreplicationMaxTimeToRetryError, "vreplication-max-time-to-retry-on-error", vreplicationMaxTimeToRetryError, "stop automatically retrying when we've had consecutive failures with the same error for this long after the first occurrence")

	utils.SetFlagIntVar(fs, &vreplicationRelayLogMaxSize, "relay-log-max-size", vreplicationRelayLogMaxSize, "Maximum buffer size (in bytes) for vreplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	utils.SetFlagIntVar(fs, &vreplicationRelayLogMaxItems, "relay-log-max-items", vreplicationRelayLogMaxItems, "Maximum number of rows for vreplication target buffering.")

	utils.SetFlagDurationVar(fs, &vreplicationReplicaLagTolerance, "vreplication-replica-lag-tolerance", vreplicationReplicaLagTolerance, "Replica lag threshold duration: once lag is below this we switch from copy phase to the replication (streaming) phase")

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no
	// real events on the source and the source vstream is only sending heartbeats for this long. Keep this low if you
	// expect high QPS and are monitoring this column to alert about potential outages. Keep this high if
	// 	 * you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//	 * you have too many streams and/or a large source field (lot of participating tables) which generates
	//     unacceptable increase in your binlog size
	utils.SetFlagIntVar(fs, &vreplicationHeartbeatUpdateInterval, "vreplication-heartbeat-update-interval", vreplicationHeartbeatUpdateInterval, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	utils.SetFlagBoolVar(fs, &vreplicationStoreCompressedGTID, "vreplication-store-compressed-gtid", vreplicationStoreCompressedGTID, "Store compressed gtids in the pos column of the sidecar database's vreplication table")

	fs.IntVar(&vreplicationParallelInsertWorkers, "vreplication-parallel-insert-workers", vreplicationParallelInsertWorkers, "Number of parallel insertion workers to use during copy phase. Set <= 1 to disable parallelism, or > 1 to enable concurrent insertion during copy phase.")

	fs.Uint64Var(&mysql.ZstdInMemoryDecompressorMaxSize, "binlog-in-memory-decompressor-max-size", mysql.ZstdInMemoryDecompressorMaxSize, "This value sets the uncompressed transaction payload size at which we switch from in-memory buffer based decompression to the slower streaming mode.")

	fs.BoolVar(&vreplicationEnableHttpLog, "vreplication-enable-http-log", vreplicationEnableHttpLog, "Enable the /debug/vrlog HTTP endpoint, which will produce a log of the events replicated on primary tablets in the target keyspace by all VReplication workflows that are in the running/replicating phase.")
}
