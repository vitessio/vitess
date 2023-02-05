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
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type InfoForRecoveryAnalysis struct {
	TabletInfo                                *topodatapb.Tablet
	PrimaryTabletInfo                         *topodatapb.Tablet
	PrimaryTimestamp                          *time.Time
	Keyspace                                  string
	Shard                                     string
	KeyspaceType                              int
	DurabilityPolicy                          string
	IsInvalid                                 int
	IsPrimary                                 int
	IsCoPrimary                               int
	Hostname                                  string
	Port                                      int
	SourceHost                                string
	SourcePort                                int
	DataCenter                                string
	Region                                    string
	PhysicalEnvironment                       string
	LogFile                                   string
	LogPos                                    int64
	IsStaleBinlogCoordinates                  int
	GTIDMode                                  string
	LastCheckValid                            int
	LastCheckPartialSuccess                   int
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountReplicasFailingToConnectToPrimary    uint
	CountDowntimedReplicas                    uint
	ReplicationDepth                          uint
	IsFailingToConnectToPrimary               int
	ReplicationStopped                        int
	IsDowntimed                               int
	DowntimeEndTimestamp                      string
	DowntimeRemainingSeconds                  int
	IsBinlogServer                            int
	CountValidOracleGTIDReplicas              uint
	CountValidMariaDBGTIDReplicas             uint
	CountValidBinlogServerReplicas            uint
	SemiSyncPrimaryEnabled                    int
	SemiSyncPrimaryStatus                     int
	SemiSyncPrimaryWaitForReplicaCount        uint
	SemiSyncPrimaryClients                    uint
	SemiSyncReplicaEnabled                    int
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
}

func (info *InfoForRecoveryAnalysis) ConvertToRowMap() sqlutils.RowMap {
	rowMap := make(sqlutils.RowMap)
	rowMap["binary_log_file"] = sqlutils.CellData{String: info.LogFile, Valid: true}
	rowMap["binary_log_pos"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.LogPos), Valid: true}
	rowMap["count_binlog_server_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_co_primary_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_delayed_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountDelayedReplicas), Valid: true}
	rowMap["count_distinct_logging_major_versions"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountDistinctMajorVersionsLoggingReplicas), Valid: true}
	rowMap["count_downtimed_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountDowntimedReplicas), Valid: true}
	rowMap["count_lagging_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountLaggingReplicas), Valid: true}
	rowMap["count_logging_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountLoggingReplicas), Valid: true}
	rowMap["count_mariadb_gtid_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_mixed_based_logging_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountMixedBasedLoggingReplicas), Valid: true}
	rowMap["count_oracle_gtid_replicas"] = sqlutils.CellData{Valid: false}
	rowMap["count_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountReplicas), Valid: true}
	rowMap["count_replicas_failing_to_connect_to_primary"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountReplicasFailingToConnectToPrimary), Valid: true}
	rowMap["count_row_based_logging_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountRowBasedLoggingReplicas), Valid: true}
	rowMap["count_semi_sync_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountSemiSyncReplicasEnabled), Valid: true}
	rowMap["count_statement_based_logging_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountStatementBasedLoggingReplicas), Valid: true}
	rowMap["count_valid_binlog_server_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountValidBinlogServerReplicas), Valid: true}
	rowMap["count_valid_mariadb_gtid_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountValidMariaDBGTIDReplicas), Valid: true}
	rowMap["count_valid_oracle_gtid_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountValidOracleGTIDReplicas), Valid: true}
	rowMap["count_valid_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountValidReplicas), Valid: true}
	rowMap["count_valid_replicating_replicas"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.CountValidReplicatingReplicas), Valid: true}
	rowMap["data_center"] = sqlutils.CellData{String: info.DataCenter, Valid: true}
	rowMap["downtime_end_timestamp"] = sqlutils.CellData{String: info.DowntimeEndTimestamp, Valid: true}
	rowMap["downtime_remaining_seconds"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.DowntimeRemainingSeconds), Valid: true}
	rowMap["durability_policy"] = sqlutils.CellData{String: info.DurabilityPolicy, Valid: true}
	rowMap["gtid_mode"] = sqlutils.CellData{String: info.GTIDMode, Valid: true}
	rowMap["hostname"] = sqlutils.CellData{String: info.Hostname, Valid: true}
	rowMap["is_binlog_server"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsBinlogServer), Valid: true}
	rowMap["is_co_primary"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsCoPrimary), Valid: true}
	rowMap["is_downtimed"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsDowntimed), Valid: true}
	rowMap["is_failing_to_connect_to_primary"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsFailingToConnectToPrimary), Valid: true}
	rowMap["is_invalid"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsInvalid), Valid: true}
	rowMap["is_last_check_valid"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.LastCheckValid), Valid: true}
	rowMap["is_primary"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsPrimary), Valid: true}
	rowMap["is_stale_binlog_coordinates"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.IsStaleBinlogCoordinates), Valid: true}
	rowMap["keyspace_type"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.KeyspaceType), Valid: true}
	rowMap["keyspace"] = sqlutils.CellData{String: info.Keyspace, Valid: true}
	rowMap["shard"] = sqlutils.CellData{String: info.Shard, Valid: true}
	rowMap["last_check_partial_success"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.LastCheckPartialSuccess), Valid: true}
	rowMap["max_replica_gtid_errant"] = sqlutils.CellData{String: info.MaxReplicaGTIDErrant, Valid: true}
	rowMap["max_replica_gtid_mode"] = sqlutils.CellData{String: info.MaxReplicaGTIDMode, Valid: true}
	rowMap["min_replica_gtid_mode"] = sqlutils.CellData{String: info.MinReplicaGTIDMode, Valid: true}
	rowMap["physical_environment"] = sqlutils.CellData{String: info.PhysicalEnvironment, Valid: true}
	rowMap["port"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.Port), Valid: true}
	if info.PrimaryTabletInfo == nil {
		rowMap["primary_tablet_info"] = sqlutils.CellData{Valid: false}
	} else {
		res, _ := prototext.Marshal(info.PrimaryTabletInfo)
		rowMap["primary_tablet_info"] = sqlutils.CellData{String: string(res), Valid: true}
	}
	rowMap["primary_timestamp"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.PrimaryTimestamp), Valid: true}
	rowMap["read_only"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.ReadOnly), Valid: true}
	rowMap["region"] = sqlutils.CellData{String: info.Region, Valid: true}
	rowMap["replication_depth"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.ReplicationDepth), Valid: true}
	rowMap["replication_stopped"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.ReplicationStopped), Valid: true}
	rowMap["semi_sync_primary_clients"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SemiSyncPrimaryClients), Valid: true}
	rowMap["semi_sync_primary_enabled"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SemiSyncPrimaryEnabled), Valid: true}
	rowMap["semi_sync_primary_status"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SemiSyncPrimaryStatus), Valid: true}
	rowMap["semi_sync_primary_wait_for_replica_count"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SemiSyncPrimaryWaitForReplicaCount), Valid: true}
	rowMap["semi_sync_replica_enabled"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SemiSyncReplicaEnabled), Valid: true}
	rowMap["source_host"] = sqlutils.CellData{String: info.SourceHost, Valid: true}
	rowMap["source_port"] = sqlutils.CellData{String: fmt.Sprintf("%v", info.SourcePort), Valid: true}
	res, _ := prototext.Marshal(info.TabletInfo)
	rowMap["tablet_info"] = sqlutils.CellData{String: string(res), Valid: true}
	return rowMap
}

func (info *InfoForRecoveryAnalysis) SetValuesFromTabletInfo() {
	info.Hostname = info.TabletInfo.MysqlHostname
	info.Port = int(info.TabletInfo.MysqlPort)
	info.DataCenter = info.TabletInfo.Alias.Cell
	info.Keyspace = info.TabletInfo.Keyspace
	info.Shard = info.TabletInfo.Shard
}
