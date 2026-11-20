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
	"fmt"
	"log/slog"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// LogAttrs returns attrs followed by the instance's fields as log attributes,
// keyed by the field names in snake_case. Empty strings and unset values are
// left out. last_sql_error and last_io_error hold MySQL error text, which can
// quote row values.
func (instance *Instance) LogAttrs(attrs ...slog.Attr) []slog.Attr {
	if instance == nil {
		return attrs
	}
	attrs = append(make([]slog.Attr, 0, len(attrs)+instanceLogAttrCount), attrs...)
	attrs = appendString(attrs, "mysql_hostname", instance.Hostname)
	attrs = append(attrs, slog.Int("mysql_port", instance.Port))
	// The type the tablet reports for itself, as current_tablet_type is in
	// the analysis.
	attrs = append(attrs, slog.String("current_tablet_type", instance.TabletType.String()))
	attrs = appendString(attrs, "cell", instance.Cell)
	attrs = append(attrs, slog.Uint64("server_id", uint64(instance.ServerID)))
	attrs = appendString(attrs, "server_uuid", instance.ServerUUID)
	attrs = appendString(attrs, "mysql_version", instance.Version)
	attrs = appendString(attrs, "version_comment", instance.VersionComment)
	attrs = append(attrs, slog.Bool("read_only", instance.ReadOnly))
	attrs = appendString(attrs, "binlog_format", instance.BinlogFormat)
	attrs = appendString(attrs, "binlog_row_image", instance.BinlogRowImage)
	attrs = append(attrs,
		slog.Bool("log_bin_enabled", instance.LogBinEnabled),
		slog.Bool("log_replication_updates_enabled", instance.LogReplicationUpdatesEnabled))
	attrs = appendCoordinates(attrs, "self_binlog_coordinates", instance.SelfBinlogCoordinates)
	attrs = appendString(attrs, "source_host", instance.SourceHost)
	attrs = append(attrs, slog.Int("source_port", instance.SourcePort))
	attrs = appendString(attrs, "source_uuid", instance.SourceUUID)
	attrs = appendString(attrs, "ancestry_uuid", instance.AncestryUUID)
	attrs = append(attrs,
		slog.Int("replica_net_timeout", int(instance.ReplicaNetTimeout)),
		slog.Float64("replication_heartbeat_interval", instance.HeartbeatInterval),
		slog.Bool("replication_sql_thread_running", instance.ReplicationSQLThreadRuning),
		slog.Bool("replication_io_thread_running", instance.ReplicationIOThreadRuning),
		slog.Int("replication_sql_thread_state", int(instance.ReplicationSQLThreadState)),
		slog.Int("replication_io_thread_state", int(instance.ReplicationIOThreadState)),
		slog.Bool("has_replication_filters", instance.HasReplicationFilters))
	attrs = appendString(attrs, "gtid_mode", instance.GTIDMode)
	attrs = append(attrs,
		slog.Bool("supports_oracle_gtid", instance.SupportsOracleGTID),
		slog.Bool("using_oracle_gtid", instance.UsingOracleGTID))
	attrs = appendCoordinates(attrs, "read_binlog_coordinates", instance.ReadBinlogCoordinates)
	attrs = appendCoordinates(attrs, "exec_binlog_coordinates", instance.ExecBinlogCoordinates)
	attrs = append(attrs, slog.Bool("is_detached", instance.IsDetached))
	attrs = appendCoordinates(attrs, "relaylog_coordinates", instance.RelaylogCoordinates)
	attrs = appendString(attrs, "last_sql_error", instance.LastSQLError)
	attrs = appendString(attrs, "last_io_error", instance.LastIOError)
	if instance.SecondsBehindPrimary.Valid {
		attrs = append(attrs, slog.Int64("replication_lag_seconds", instance.SecondsBehindPrimary.Int64))
	}
	attrs = append(attrs, slog.Uint64("sql_delay", uint64(instance.SQLDelay)))
	attrs = appendString(attrs, "executed_gtid_set", instance.ExecutedGtidSet)
	attrs = appendString(attrs, "gtid_purged", instance.GtidPurged)
	attrs = appendString(attrs, "errant_gtids", instance.GtidErrant)
	attrs = appendString(attrs, "primary_executed_gtid_set", instance.primaryExecutedGtidSet)
	attrs = append(attrs,
		slog.Uint64("replication_depth", uint64(instance.ReplicationDepth)),
		slog.Bool("is_co_primary", instance.IsCoPrimary),
		slog.Bool("has_replication_credentials", instance.HasReplicationCredentials),
		slog.Bool("semi_sync_primary", instance.SemiSyncPrimaryEnabled),
		slog.Bool("semi_sync_replica", instance.SemiSyncReplicaEnabled),
		slog.Uint64("semi_sync_primary_timeout", instance.SemiSyncPrimaryTimeout),
		slog.Uint64("semi_sync_primary_wait_for_replica_count", uint64(instance.SemiSyncPrimaryWaitForReplicaCount)),
		slog.Bool("semi_sync_primary_status", instance.SemiSyncPrimaryStatus),
		slog.Uint64("semi_sync_primary_clients", uint64(instance.SemiSyncPrimaryClients)),
		slog.Bool("semi_sync_replica_status", instance.SemiSyncReplicaStatus),
		slog.Bool("semi_sync_blocked", instance.SemiSyncBlocked),
		slog.Bool("allow_tls", instance.AllowTLS),
		slog.Duration("last_discovery_latency", instance.LastDiscoveryLatency))
	return attrs
}

// LogAttrs returns attrs followed by the analysis's fields as log attributes,
// keyed by the field names in snake_case, with the analyzed tablet, keyspace,
// shard and cell under tablet, keyspace, shard and cell. Empty strings and
// unset values are left out.
func (a *DetectionAnalysis) LogAttrs(attrs ...slog.Attr) []slog.Attr {
	if a == nil {
		return attrs
	}
	attrs = append(make([]slog.Attr, 0, len(attrs)+analysisLogAttrCount), attrs...)
	attrs = appendString(attrs, "analysis", string(a.Analysis))
	attrs = appendString(attrs, "description", a.Description)
	attrs = appendAlias(attrs, "tablet", a.AnalyzedInstanceAlias)
	attrs = appendAlias(attrs, "primary_tablet", a.AnalyzedInstancePrimaryAlias)
	attrs = append(attrs, slog.Int64("detection_id", a.RecoveryId))
	attrs = appendString(attrs, "keyspace", a.AnalyzedKeyspace)
	attrs = appendString(attrs, "shard", a.AnalyzedShard)
	attrs = appendString(attrs, "cell", a.AnalyzedCell)
	attrs = append(attrs,
		slog.String("tablet_type", a.TabletType.String()),
		slog.String("current_tablet_type", a.CurrentTabletType.String()),
		slog.Bool("is_tablet_shutdown", a.IsTabletShutdown))
	attrs = appendTime(attrs, "primary_timestamp", a.PrimaryTimeStamp)
	attrs = append(attrs,
		slog.Bool("keyspace_emergency_reparent_disabled", a.AnalyzedKeyspaceEmergencyReparentDisabled),
		slog.Bool("shard_emergency_reparent_disabled", a.AnalyzedShardEmergencyReparentDisabled))
	attrs = appendTime(attrs, "shard_primary_term_timestamp", a.ShardPrimaryTermTimestamp)
	attrs = appendCoordinates(attrs, "binlog_coordinates", a.AnalyzedInstanceBinlogCoordinates)
	attrs = append(attrs,
		slog.Bool("is_primary", a.IsPrimary),
		slog.Bool("is_cluster_primary", a.IsClusterPrimary),
		slog.Bool("last_check_valid", a.LastCheckValid),
		slog.Bool("primary_health_unhealthy", a.PrimaryHealthUnhealthy),
		slog.Bool("last_check_partial_success", a.LastCheckPartialSuccess),
		slog.Uint64("count_replicas", uint64(a.CountReplicas)),
		slog.Int("shard_eligible_observers", a.ShardEligibleObservers),
		slog.Uint64("count_valid_replicas", uint64(a.CountValidReplicas)),
		slog.Uint64("count_valid_replicating_replicas", uint64(a.CountValidReplicatingReplicas)),
		slog.Uint64("count_valid_semi_sync_replicating_replicas", uint64(a.CountValidSemiSyncReplicatingReplicas)),
		slog.Bool("replication_stopped", a.ReplicationStopped))
	attrs = appendString(attrs, "errant_gtids", a.ErrantGTID)
	attrs = append(attrs,
		slog.Int("replica_net_timeout", int(a.ReplicaNetTimeout)),
		slog.Float64("replication_heartbeat_interval", a.HeartbeatInterval))
	problems := make([]string, 0, len(a.AnalysisMatchedProblems))
	for _, p := range a.AnalysisMatchedProblems {
		if p != nil {
			problems = append(problems, string(p.Analysis))
		}
	}
	attrs = appendStrings(attrs, "matched_problems", problems)
	structure := make([]string, 0, len(a.StructureAnalysis))
	for _, s := range a.StructureAnalysis {
		structure = append(structure, string(s))
	}
	attrs = appendStrings(attrs, "structure_analysis", structure)
	attrs = append(attrs,
		slog.Bool("oracle_gtid_immediate_topology", a.OracleGTIDImmediateTopology),
		slog.Bool("binlog_server_immediate_topology", a.BinlogServerImmediateTopology),
		slog.Bool("semi_sync_primary", a.SemiSyncPrimaryEnabled),
		slog.Bool("semi_sync_primary_status", a.SemiSyncPrimaryStatus),
		slog.Uint64("semi_sync_primary_wait_for_replica_count", uint64(a.SemiSyncPrimaryWaitForReplicaCount)),
		slog.Uint64("semi_sync_primary_clients", uint64(a.SemiSyncPrimaryClients)),
		slog.Bool("semi_sync_replica", a.SemiSyncReplicaEnabled),
		slog.Bool("semi_sync_blocked", a.SemiSyncBlocked),
		slog.Uint64("count_semi_sync_replicas_enabled", uint64(a.CountSemiSyncReplicasEnabled)),
		slog.Uint64("count_logging_replicas", uint64(a.CountLoggingReplicas)),
		slog.Uint64("count_statement_based_logging_replicas", uint64(a.CountStatementBasedLoggingReplicas)),
		slog.Uint64("count_mixed_based_logging_replicas", uint64(a.CountMixedBasedLoggingReplicas)),
		slog.Uint64("count_row_based_logging_replicas", uint64(a.CountRowBasedLoggingReplicas)),
		slog.Uint64("count_distinct_major_versions_logging_replicas", uint64(a.CountDistinctMajorVersionsLoggingReplicas)),
		slog.Uint64("count_delayed_replicas", uint64(a.CountDelayedReplicas)),
		slog.Uint64("count_lagging_replicas", uint64(a.CountLaggingReplicas)),
		slog.Bool("actionable", a.IsActionableRecovery))
	attrs = appendString(attrs, "gtid_mode", a.GTIDMode)
	attrs = appendString(attrs, "min_replica_gtid_mode", a.MinReplicaGTIDMode)
	attrs = appendString(attrs, "max_replica_gtid_mode", a.MaxReplicaGTIDMode)
	attrs = appendString(attrs, "max_replica_errant_gtids", a.MaxReplicaGTIDErrant)
	attrs = append(attrs,
		slog.Bool("read_only", a.IsReadOnly),
		slog.Bool("is_disk_stalled", a.IsDiskStalled))
	if q := a.QuorumDetail; q != nil {
		// The quorum's primary, keyspace and shard are the analysis's tablet,
		// keyspace and shard, and an observer is fresh when its vote isn't
		// stale.
		observers := make([]string, 0, len(q.Observers))
		for _, o := range q.Observers {
			observers = append(observers, fmt.Sprintf("%s=%s:%s:%d", o.Alias, o.TabletType, o.Vote, o.ConsecutiveFailures))
		}
		attrs = append(attrs,
			slog.Bool("quorum_down", q.Down),
			slog.Int("quorum_down_votes", q.DownVotes),
			slog.Int("quorum_total_observers", q.TotalObservers),
			slog.Int("quorum_eligible_observers", q.EligibleObservers),
			slog.Int("quorum_expected_observers", q.ExpectedObservers),
			slog.Float64("quorum_fraction", q.Fraction),
			slog.Int("quorum_min_observers", q.MinObservers))
		attrs = appendStrings(attrs, "quorum_observers", observers)
		attrs = appendTime(attrs, "quorum_evaluated_at", q.EvaluatedAt)
	}
	return attrs
}

// TabletLogAttrs returns attrs followed by the tablet record's fields as log
// attributes, keyed by the proto field names, with the alias under tablet, the
// type under tablet_type and the tags under tablet_tags. Unset fields are left
// out, as the proto text format does.
func TabletLogAttrs(tablet *topodatapb.Tablet, attrs ...slog.Attr) []slog.Attr {
	if tablet == nil {
		return attrs
	}
	attrs = append(make([]slog.Attr, 0, len(attrs)+tabletLogAttrCount), attrs...)
	attrs = appendAlias(attrs, "tablet", tablet.Alias)
	attrs = appendString(attrs, "hostname", tablet.Hostname)
	attrs = appendMap(attrs, "port_map", tablet.PortMap)
	attrs = appendString(attrs, "keyspace", tablet.Keyspace)
	attrs = appendString(attrs, "shard", tablet.Shard)
	if tablet.KeyRange != nil {
		attrs = append(attrs, slog.String("key_range", key.KeyRangeString(tablet.KeyRange)))
	}
	attrs = append(attrs, slog.String("tablet_type", tablet.Type.String()))
	attrs = appendString(attrs, "db_name_override", tablet.DbNameOverride)
	attrs = appendMap(attrs, "tablet_tags", tablet.Tags)
	attrs = appendString(attrs, "mysql_hostname", tablet.MysqlHostname)
	if tablet.MysqlPort != 0 {
		attrs = append(attrs, slog.Int("mysql_port", int(tablet.MysqlPort)))
	}
	attrs = appendTime(attrs, "primary_term_start_time", protoutil.TimeFromProto(tablet.PrimaryTermStartTime))
	if tablet.DefaultConnCollation != 0 {
		attrs = append(attrs, slog.Uint64("default_conn_collation", uint64(tablet.DefaultConnCollation)))
	}
	attrs = appendTime(attrs, "tablet_start_time", protoutil.TimeFromProto(tablet.TabletStartTime))
	attrs = appendTime(attrs, "tablet_shutdown_time", protoutil.TimeFromProto(tablet.TabletShutdownTime))
	return attrs
}

// The most attributes each helper adds, so it allocates once.
const (
	instanceLogAttrCount = 53
	analysisLogAttrCount = 64
	tabletLogAttrCount   = 15
)

func appendString(attrs []slog.Attr, key, value string) []slog.Attr {
	if value == "" {
		return attrs
	}
	return append(attrs, slog.String(key, value))
}

func appendStrings(attrs []slog.Attr, key string, values []string) []slog.Attr {
	if len(values) == 0 {
		return attrs
	}
	return append(attrs, slog.Any(key, values))
}

func appendMap[V any](attrs []slog.Attr, key string, m map[string]V) []slog.Attr {
	if len(m) == 0 {
		return attrs
	}
	return append(attrs, slog.Any(key, m))
}

func appendAlias(attrs []slog.Attr, key string, alias *topodatapb.TabletAlias) []slog.Attr {
	if alias == nil {
		return attrs
	}
	return append(attrs, slog.String(key, topoproto.TabletAliasString(alias)))
}

func appendTime(attrs []slog.Attr, key string, t time.Time) []slog.Attr {
	if t.IsZero() {
		return attrs
	}
	return append(attrs, slog.Time(key, t))
}

func appendCoordinates(attrs []slog.Attr, key string, c BinlogCoordinates) []slog.Attr {
	if c.LogFile == "" {
		return attrs
	}
	return append(attrs, slog.String(key, c.DisplayString()))
}
