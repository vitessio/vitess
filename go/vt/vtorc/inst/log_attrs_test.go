/*
Copyright 2026 The Vitess Authors.

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

package inst

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// attrMap returns the attributes as a map from key to value. It fails the
// test if a key repeats.
func attrMap(t *testing.T, attrs []slog.Attr) map[string]any {
	m := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		_, dup := m[attr.Key]
		require.False(t, dup, "key %q repeats", attr.Key)
		m[attr.Key] = attr.Value.Any()
	}
	return m
}

func TestInstanceLogAttrs(t *testing.T) {
	instance := &Instance{
		Hostname:                           "mysql-host",
		Port:                               3306,
		InstanceAlias:                      &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		TabletType:                         topodatapb.TabletType_REPLICA,
		Cell:                               "zone1",
		ServerID:                           7,
		ServerUUID:                         "server-uuid",
		Version:                            "8.4.0",
		VersionComment:                     "comment",
		FlavorName:                         "MySQL",
		ReadOnly:                           true,
		BinlogFormat:                       "ROW",
		BinlogRowImage:                     "FULL",
		LogBinEnabled:                      true,
		LogReplicationUpdatesEnabled:       true,
		SelfBinlogCoordinates:              BinlogCoordinates{LogFile: "self-bin.000001", LogPos: 1},
		SourceHost:                         "source-host",
		SourcePort:                         3307,
		SourceUUID:                         "source-uuid",
		AncestryUUID:                       "source-uuid,server-uuid",
		ReplicaNetTimeout:                  8,
		HeartbeatInterval:                  4.5,
		ReplicationSQLThreadRuning:         true,
		ReplicationIOThreadRuning:          true,
		ReplicationSQLThreadState:          ReplicationThreadStateRunning,
		ReplicationIOThreadState:           ReplicationThreadStateOther,
		HasReplicationFilters:              true,
		GTIDMode:                           "ON",
		SupportsOracleGTID:                 true,
		UsingOracleGTID:                    true,
		ReadBinlogCoordinates:              BinlogCoordinates{LogFile: "source-bin.000002", LogPos: 2},
		ExecBinlogCoordinates:              BinlogCoordinates{LogFile: "source-bin.000002", LogPos: 3},
		IsDetached:                         true,
		RelaylogCoordinates:                BinlogCoordinates{LogFile: "relay-bin.000004", LogPos: 4, Type: RelayLog},
		LastSQLError:                       "sql error",
		LastIOError:                        "io error",
		SecondsBehindPrimary:               sql.NullInt64{Int64: 5, Valid: true},
		SQLDelay:                           6,
		ExecutedGtidSet:                    "executed-set",
		GtidPurged:                         "purged-set",
		GtidErrant:                         "errant-set",
		primaryExecutedGtidSet:             "primary-executed-set",
		ReplicationLagSeconds:              sql.NullInt64{Int64: 5, Valid: true},
		ReplicationDepth:                   1,
		IsCoPrimary:                        true,
		HasReplicationCredentials:          true,
		SemiSyncEnforced:                   true,
		SemiSyncPrimaryEnabled:             true,
		SemiSyncReplicaEnabled:             true,
		SemiSyncPrimaryTimeout:             10000,
		SemiSyncPrimaryWaitForReplicaCount: 2,
		SemiSyncPrimaryStatus:              true,
		SemiSyncPrimaryClients:             3,
		SemiSyncReplicaStatus:              true,
		SemiSyncBlocked:                    true,
		LastSeenTimestamp:                  "2026-01-01 00:00:00",
		IsLastCheckValid:                   true,
		IsUpToDate:                         true,
		IsRecentlyChecked:                  true,
		SecondsSinceLastSeen:               sql.NullInt64{Int64: 1, Valid: true},
		StalledDisk:                        true,
		AllowTLS:                           true,
		Problems:                           []string{"not_replicating"},
		LastDiscoveryLatency:               14 * time.Millisecond,
	}
	attrs := instance.LogAttrs(slog.String("tablet", "zone1-0000000101"))
	assert.Equal(t, "tablet", attrs[0].Key)
	assert.Equal(t, map[string]any{
		"tablet":                                   "zone1-0000000101",
		"mysql_hostname":                           "mysql-host",
		"mysql_port":                               int64(3306),
		"current_tablet_type":                      "REPLICA",
		"cell":                                     "zone1",
		"server_id":                                uint64(7),
		"server_uuid":                              "server-uuid",
		"mysql_version":                            "8.4.0",
		"version_comment":                          "comment",
		"read_only":                                true,
		"binlog_format":                            "ROW",
		"binlog_row_image":                         "FULL",
		"log_bin_enabled":                          true,
		"log_replication_updates_enabled":          true,
		"self_binlog_coordinates":                  "self-bin.000001:1",
		"source_host":                              "source-host",
		"source_port":                              int64(3307),
		"source_uuid":                              "source-uuid",
		"ancestry_uuid":                            "source-uuid,server-uuid",
		"replica_net_timeout":                      int64(8),
		"replication_heartbeat_interval":           4.5,
		"replication_sql_thread_running":           true,
		"replication_io_thread_running":            true,
		"replication_sql_thread_state":             int64(1),
		"replication_io_thread_state":              int64(2),
		"has_replication_filters":                  true,
		"gtid_mode":                                "ON",
		"supports_oracle_gtid":                     true,
		"using_oracle_gtid":                        true,
		"read_binlog_coordinates":                  "source-bin.000002:2",
		"exec_binlog_coordinates":                  "source-bin.000002:3",
		"is_detached":                              true,
		"relaylog_coordinates":                     "relay-bin.000004:4",
		"last_sql_error":                           "sql error",
		"last_io_error":                            "io error",
		"replication_lag_seconds":                  int64(5),
		"sql_delay":                                uint64(6),
		"executed_gtid_set":                        "executed-set",
		"gtid_purged":                              "purged-set",
		"errant_gtids":                             "errant-set",
		"primary_executed_gtid_set":                "primary-executed-set",
		"replication_depth":                        uint64(1),
		"is_co_primary":                            true,
		"has_replication_credentials":              true,
		"semi_sync_primary":                        true,
		"semi_sync_replica":                        true,
		"semi_sync_primary_timeout":                uint64(10000),
		"semi_sync_primary_wait_for_replica_count": uint64(2),
		"semi_sync_primary_status":                 true,
		"semi_sync_primary_clients":                uint64(3),
		"semi_sync_replica_status":                 true,
		"semi_sync_blocked":                        true,
		"allow_tls":                                true,
		"last_discovery_latency":                   14 * time.Millisecond,
	}, attrMap(t, attrs))
	assert.Len(t, attrs, instanceLogAttrCount+1)

	// Empty strings, empty coordinates and an unknown lag are left out.
	assert.Len(t, attrMap(t, (&Instance{}).LogAttrs()), 31)

	var nilInstance *Instance
	assert.Equal(t, []slog.Attr{slog.String("tablet", "t")}, nilInstance.LogAttrs(slog.String("tablet", "t")))
}

func TestDetectionAnalysisLogAttrs(t *testing.T) {
	evaluatedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	analysis := &DetectionAnalysis{
		AnalyzedInstanceAlias:        &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		AnalyzedInstancePrimaryAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		TabletType:                   topodatapb.TabletType_PRIMARY,
		IsTabletShutdown:             true,
		CurrentTabletType:            topodatapb.TabletType_REPLICA,
		PrimaryTimeStamp:             evaluatedAt,
		AnalyzedKeyspace:             "ks",
		AnalyzedShard:                "0",
		AnalyzedCell:                 "zone1",
		AnalyzedKeyspaceEmergencyReparentDisabled: true,
		AnalyzedShardEmergencyReparentDisabled:    true,
		ShardPrimaryTermTimestamp:                 evaluatedAt.Add(time.Hour),
		AnalyzedInstanceBinlogCoordinates:         BinlogCoordinates{LogFile: "bin.000001", LogPos: 9},
		IsPrimary:                                 true,
		IsClusterPrimary:                          true,
		LastCheckValid:                            true,
		PrimaryHealthUnhealthy:                    true,
		LastCheckPartialSuccess:                   true,
		CountReplicas:                             1,
		ShardEligibleObservers:                    2,
		CountValidReplicas:                        3,
		CountValidReplicatingReplicas:             4,
		CountValidSemiSyncReplicatingReplicas:     5,
		ReplicationStopped:                        true,
		ErrantGTID:                                "errant-set",
		ReplicaNetTimeout:                         8,
		HeartbeatInterval:                         4.5,
		Analysis:                                  PrimaryTabletUnreachableByQuorum,
		AnalysisMatchedProblems:                   []*DetectionAnalysisProblemMeta{{Analysis: PrimaryTabletUnreachableByQuorum}, {Analysis: DeadPrimary}},
		Description:                               "description",
		StructureAnalysis:                         []StructureAnalysisCode{NoFailoverSupportStructureWarning, NoWriteablePrimaryStructureWarning},
		OracleGTIDImmediateTopology:               true,
		BinlogServerImmediateTopology:             true,
		SemiSyncPrimaryEnabled:                    true,
		SemiSyncPrimaryStatus:                     true,
		SemiSyncPrimaryWaitForReplicaCount:        6,
		SemiSyncPrimaryClients:                    7,
		SemiSyncReplicaEnabled:                    true,
		SemiSyncBlocked:                           true,
		CountSemiSyncReplicasEnabled:              8,
		CountLoggingReplicas:                      9,
		CountStatementBasedLoggingReplicas:        10,
		CountMixedBasedLoggingReplicas:            11,
		CountRowBasedLoggingReplicas:              12,
		CountDistinctMajorVersionsLoggingReplicas: 13,
		CountDelayedReplicas:                      14,
		CountLaggingReplicas:                      15,
		IsActionableRecovery:                      true,
		RecoveryId:                                16,
		GTIDMode:                                  "ON",
		MinReplicaGTIDMode:                        "OFF",
		MaxReplicaGTIDMode:                        "ON_PERMISSIVE",
		MaxReplicaGTIDErrant:                      "max-errant-set",
		IsReadOnly:                                true,
		IsDiskStalled:                             true,
		QuorumDetail: &QuorumResult{
			PrimaryAlias:      "zone1-0000000100",
			Keyspace:          "ks",
			Shard:             "0",
			Down:              true,
			DownVotes:         2,
			TotalObservers:    3,
			EligibleObservers: 4,
			ExpectedObservers: 5,
			Fraction:          0.5,
			MinObservers:      2,
			Observers: []ObserverVote{
				{Alias: "zone1-0000000102", TabletType: "REPLICA", Fresh: true, ConsecutiveFailures: 3, Vote: voteDown},
				{Alias: "zone1-0000000103", TabletType: "RDONLY", ConsecutiveFailures: 1, Vote: voteStale},
			},
			EvaluatedAt: evaluatedAt,
		},
	}
	attrs := analysis.LogAttrs(slog.Int64("recovery_id", 1))
	assert.Equal(t, "recovery_id", attrs[0].Key)
	assert.Equal(t, map[string]any{
		"recovery_id":                          int64(1),
		"analysis":                             "PrimaryTabletUnreachableByQuorum",
		"description":                          "description",
		"tablet":                               "zone1-0000000100",
		"primary_tablet":                       "zone1-0000000101",
		"detection_id":                         int64(16),
		"keyspace":                             "ks",
		"shard":                                "0",
		"cell":                                 "zone1",
		"tablet_type":                          "PRIMARY",
		"current_tablet_type":                  "REPLICA",
		"is_tablet_shutdown":                   true,
		"primary_timestamp":                    evaluatedAt,
		"keyspace_emergency_reparent_disabled": true,
		"shard_emergency_reparent_disabled":    true,
		"shard_primary_term_timestamp":         evaluatedAt.Add(time.Hour),
		"binlog_coordinates":                   "bin.000001:9",
		"is_primary":                           true,
		"is_cluster_primary":                   true,
		"last_check_valid":                     true,
		"primary_health_unhealthy":             true,
		"last_check_partial_success":           true,
		"count_replicas":                       uint64(1),
		"shard_eligible_observers":             int64(2),
		"count_valid_replicas":                 uint64(3),
		"count_valid_replicating_replicas":     uint64(4),
		"count_valid_semi_sync_replicating_replicas":     uint64(5),
		"replication_stopped":                            true,
		"errant_gtids":                                   "errant-set",
		"replica_net_timeout":                            int64(8),
		"replication_heartbeat_interval":                 4.5,
		"matched_problems":                               []string{"PrimaryTabletUnreachableByQuorum", "DeadPrimary"},
		"structure_analysis":                             []string{"NoFailoverSupportStructureWarning", "NoWriteablePrimaryStructureWarning"},
		"oracle_gtid_immediate_topology":                 true,
		"binlog_server_immediate_topology":               true,
		"semi_sync_primary":                              true,
		"semi_sync_primary_status":                       true,
		"semi_sync_primary_wait_for_replica_count":       uint64(6),
		"semi_sync_primary_clients":                      uint64(7),
		"semi_sync_replica":                              true,
		"semi_sync_blocked":                              true,
		"count_semi_sync_replicas_enabled":               uint64(8),
		"count_logging_replicas":                         uint64(9),
		"count_statement_based_logging_replicas":         uint64(10),
		"count_mixed_based_logging_replicas":             uint64(11),
		"count_row_based_logging_replicas":               uint64(12),
		"count_distinct_major_versions_logging_replicas": uint64(13),
		"count_delayed_replicas":                         uint64(14),
		"count_lagging_replicas":                         uint64(15),
		"actionable":                                     true,
		"gtid_mode":                                      "ON",
		"min_replica_gtid_mode":                          "OFF",
		"max_replica_gtid_mode":                          "ON_PERMISSIVE",
		"max_replica_errant_gtids":                       "max-errant-set",
		"read_only":                                      true,
		"is_disk_stalled":                                true,
		"quorum_down":                                    true,
		"quorum_down_votes":                              int64(2),
		"quorum_total_observers":                         int64(3),
		"quorum_eligible_observers":                      int64(4),
		"quorum_expected_observers":                      int64(5),
		"quorum_fraction":                                0.5,
		"quorum_min_observers":                           int64(2),
		"quorum_observers":                               []string{"zone1-0000000102=REPLICA:down:3", "zone1-0000000103=RDONLY:stale:1"},
		"quorum_evaluated_at":                            evaluatedAt,
	}, attrMap(t, attrs))
	assert.Len(t, attrs, analysisLogAttrCount+1)

	// Empty strings, unset aliases and times, empty lists and a missing
	// quorum are left out.
	assert.Len(t, attrMap(t, (&DetectionAnalysis{}).LogAttrs()), analysisLogAttrCount-26)

	var nilAnalysis *DetectionAnalysis
	assert.Empty(t, nilAnalysis.LogAttrs())
}

func TestTabletLogAttrs(t *testing.T) {
	tablet := &topodatapb.Tablet{
		Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname:             "tablet-host",
		PortMap:              map[string]int32{"vt": 15100, "grpc": 16100},
		Keyspace:             "ks",
		Shard:                "-80",
		KeyRange:             &topodatapb.KeyRange{End: []byte{0x80}},
		Type:                 topodatapb.TabletType_REPLICA,
		DbNameOverride:       "vt_ks",
		Tags:                 map[string]string{"team": "a", "env": "test"},
		MysqlHostname:        "mysql-host",
		MysqlPort:            17100,
		PrimaryTermStartTime: &vttimepb.Time{Seconds: 1700000000},
		DefaultConnCollation: 255,
		TabletStartTime:      &vttimepb.Time{Seconds: 1700000100},
		TabletShutdownTime:   &vttimepb.Time{Seconds: 1700000200},
	}
	attrs := TabletLogAttrs(tablet, slog.Bool("force", true))
	assert.Equal(t, "force", attrs[0].Key)
	assert.Equal(t, map[string]any{
		"force":                   true,
		"tablet":                  "zone1-0000000100",
		"hostname":                "tablet-host",
		"port_map":                map[string]int32{"grpc": 16100, "vt": 15100},
		"keyspace":                "ks",
		"shard":                   "-80",
		"key_range":               "-80",
		"tablet_type":             "REPLICA",
		"db_name_override":        "vt_ks",
		"tablet_tags":             map[string]string{"env": "test", "team": "a"},
		"mysql_hostname":          "mysql-host",
		"mysql_port":              int64(17100),
		"primary_term_start_time": time.Unix(1700000000, 0),
		"default_conn_collation":  uint64(255),
		"tablet_start_time":       time.Unix(1700000100, 0),
		"tablet_shutdown_time":    time.Unix(1700000200, 0),
	}, attrMap(t, attrs))
	assert.Len(t, attrs, tabletLogAttrCount+1)

	// Unset fields are left out; the type is always logged.
	assert.Equal(t, map[string]any{"tablet_type": "UNKNOWN"}, attrMap(t, TabletLogAttrs(&topodatapb.Tablet{})))
	assert.Empty(t, TabletLogAttrs(nil))
}
