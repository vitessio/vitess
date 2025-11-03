/*
Copyright 2021 The Vitess Authors.

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

package test

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type InfoForRecoveryAnalysis struct {
	TabletInfo                                *topodatapb.Tablet
	PrimaryTabletInfo                         *topodatapb.Tablet
	PrimaryTimestamp                          *time.Time
	Cell                                      string
	Keyspace                                  string
	Shard                                     string
	ShardPrimaryTermTimestamp                 string
	KeyspaceType                              int
	DurabilityPolicy                          string
	IsInvalid                                 int
	IsPrimary                                 int
	IsCoPrimary                               int
	Hostname                                  string
	Port                                      int
	LogFile                                   string
	LogPos                                    uint32
	IsStaleBinlogCoordinates                  int
	GTIDMode                                  string
	ReplicaNetTimeout                         int32
	HeartbeatInterval                         float64
	ErrantGTID                                string
	LastCheckValid                            int
	LastCheckPartialSuccess                   int
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountDowntimedReplicas                    uint
	ReplicationStopped                        int
	IsDowntimed                               int
	DowntimeEndTimestamp                      string
	DowntimeRemainingSeconds                  int
	CountValidOracleGTIDReplicas              uint
	CountValidBinlogServerReplicas            uint
	SemiSyncPrimaryEnabled                    int
	SemiSyncPrimaryStatus                     int
	SemiSyncBlocked                           int
	SemiSyncPrimaryWaitForReplicaCount        uint
	SemiSyncPrimaryClients                    uint
	SemiSyncReplicaEnabled                    int
	CurrentTabletType                         int
	CountSemiSyncReplicasEnabled              uint
	CountLoggingReplicas                      uint
	CountStatementBasedLoggingReplicas        uint
	CountMixedBasedLoggingReplicas            uint
	CountRowBasedLoggingReplicas              uint
	CountDistinctMajorVersionsLoggingReplicas uint
	CountDelayedReplicas                      uint
	CountLaggingReplicas                      uint
	MinReplicaGTIDMode                        string
	MaxReplicaGTIDMode                        string
	MaxReplicaGTIDErrant                      string
	ReadOnly                                  uint
	IsStalledDisk                             uint
}

func (info *InfoForRecoveryAnalysis) ConvertToRowMap() sqlutils.RowMap {
	rowMap := make(sqlutils.RowMap)
	rowMap["binary_log_file"] = sqlutils.CellData{String: info.LogFile, Valid: true}
	rowMap["binary_log_pos"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.LogPos), 10), Valid: true}
	rowMap["cell"] = sqlutils.CellData{String: info.Cell, Valid: true}
	rowMap["count_binlog_server_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_co_primary_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_delayed_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountDelayedReplicas), 10), Valid: true}
	rowMap["count_distinct_logging_major_versions"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountDistinctMajorVersionsLoggingReplicas), 10), Valid: true}
	rowMap["count_downtimed_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountDowntimedReplicas), 10), Valid: true}
	rowMap["count_lagging_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountLaggingReplicas), 10), Valid: true}
	rowMap["count_logging_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountLoggingReplicas), 10), Valid: true}
	rowMap["count_mixed_based_logging_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountMixedBasedLoggingReplicas), 10), Valid: true}
	rowMap["count_oracle_gtid_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountReplicas), 10), Valid: true}
	rowMap["count_row_based_logging_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountRowBasedLoggingReplicas), 10), Valid: true}
	rowMap["count_semi_sync_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountSemiSyncReplicasEnabled), 10), Valid: true}
	rowMap["count_statement_based_logging_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountStatementBasedLoggingReplicas), 10), Valid: true}
	rowMap["count_valid_binlog_server_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountValidBinlogServerReplicas), 10), Valid: true}
	rowMap["count_valid_oracle_gtid_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountValidOracleGTIDReplicas), 10), Valid: true}
	rowMap["count_valid_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountValidReplicas), 10), Valid: true}
	rowMap["count_valid_replicating_replicas"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.CountValidReplicatingReplicas), 10), Valid: true}
	rowMap["downtime_end_timestamp"] = sqlutils.CellData{String: info.DowntimeEndTimestamp, Valid: true}
	rowMap["downtime_remaining_seconds"] = sqlutils.CellData{String: strconv.Itoa(info.DowntimeRemainingSeconds), Valid: true}
	rowMap["durability_policy"] = sqlutils.CellData{String: info.DurabilityPolicy, Valid: true}
	rowMap["gtid_errant"] = sqlutils.CellData{String: info.ErrantGTID, Valid: true}
	rowMap["gtid_mode"] = sqlutils.CellData{String: info.GTIDMode, Valid: true}
	rowMap["hostname"] = sqlutils.CellData{String: info.Hostname, Valid: true}
	rowMap["is_co_primary"] = sqlutils.CellData{String: strconv.Itoa(info.IsCoPrimary), Valid: true}
	rowMap["is_downtimed"] = sqlutils.CellData{String: strconv.Itoa(info.IsDowntimed), Valid: true}
	rowMap["is_invalid"] = sqlutils.CellData{String: strconv.Itoa(info.IsInvalid), Valid: true}
	rowMap["is_last_check_valid"] = sqlutils.CellData{String: strconv.Itoa(info.LastCheckValid), Valid: true}
	rowMap["is_primary"] = sqlutils.CellData{String: strconv.Itoa(info.IsPrimary), Valid: true}
	rowMap["is_stale_binlog_coordinates"] = sqlutils.CellData{String: strconv.Itoa(info.IsStaleBinlogCoordinates), Valid: true}
	rowMap["keyspace_type"] = sqlutils.CellData{String: strconv.Itoa(info.KeyspaceType), Valid: true}
	rowMap["keyspace"] = sqlutils.CellData{String: info.Keyspace, Valid: true}
	rowMap["shard"] = sqlutils.CellData{String: info.Shard, Valid: true}
	rowMap["shard_primary_term_timestamp"] = sqlutils.CellData{String: info.ShardPrimaryTermTimestamp, Valid: true}
	rowMap["last_check_partial_success"] = sqlutils.CellData{String: strconv.Itoa(info.LastCheckPartialSuccess), Valid: true}
	rowMap["replica_net_timeout"] = sqlutils.CellData{String: strconv.Itoa(int(info.ReplicaNetTimeout)), Valid: true}
	rowMap["heartbeat_interval"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.HeartbeatInterval), Valid: true}
	rowMap["max_replica_gtid_errant"] = sqlutils.CellData{String: info.MaxReplicaGTIDErrant, Valid: true}
	rowMap["max_replica_gtid_mode"] = sqlutils.CellData{String: info.MaxReplicaGTIDMode, Valid: true}
	rowMap["min_replica_gtid_mode"] = sqlutils.CellData{String: info.MinReplicaGTIDMode, Valid: true}
	rowMap["port"] = sqlutils.CellData{String: strconv.Itoa(info.Port), Valid: true}
	if info.PrimaryTabletInfo == nil {
		rowMap["primary_tablet_info"] = sqlutils.CellData{Valid: false}
	} else {
		res, _ := prototext.Marshal(info.PrimaryTabletInfo)
		rowMap["primary_tablet_info"] = sqlutils.CellData{String: string(res), Valid: true}
	}
	rowMap["primary_timestamp"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.PrimaryTimestamp), Valid: true}
	rowMap["read_only"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.ReadOnly), 10), Valid: true}
	rowMap["replication_stopped"] = sqlutils.CellData{String: strconv.Itoa(info.ReplicationStopped), Valid: true}
	rowMap["semi_sync_primary_clients"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.SemiSyncPrimaryClients), 10), Valid: true}
	rowMap["semi_sync_primary_enabled"] = sqlutils.CellData{String: strconv.Itoa(info.SemiSyncPrimaryEnabled), Valid: true}
	rowMap["semi_sync_primary_status"] = sqlutils.CellData{String: strconv.Itoa(info.SemiSyncPrimaryStatus), Valid: true}
	rowMap["semi_sync_blocked"] = sqlutils.CellData{String: strconv.Itoa(info.SemiSyncBlocked), Valid: true}
	rowMap["semi_sync_primary_wait_for_replica_count"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.SemiSyncPrimaryWaitForReplicaCount), 10), Valid: true}
	rowMap["semi_sync_replica_enabled"] = sqlutils.CellData{String: strconv.Itoa(info.SemiSyncReplicaEnabled), Valid: true}
	res, _ := prototext.Marshal(info.TabletInfo)
	currentType := info.CurrentTabletType
	rowMap["current_tablet_type"] = sqlutils.CellData{String: strconv.Itoa(currentType), Valid: true}
	rowMap["tablet_info"] = sqlutils.CellData{String: string(res), Valid: true}
	rowMap["is_disk_stalled"] = sqlutils.CellData{String: strconv.FormatUint(uint64(info.IsStalledDisk), 10), Valid: true}
	return rowMap
}

func (info *InfoForRecoveryAnalysis) SetValuesFromTabletInfo() {
	info.Hostname = info.TabletInfo.MysqlHostname
	info.Port = int(info.TabletInfo.MysqlPort)
	info.Cell = info.TabletInfo.Alias.Cell
	info.Keyspace = info.TabletInfo.Keyspace
	info.Shard = info.TabletInfo.Shard
}
