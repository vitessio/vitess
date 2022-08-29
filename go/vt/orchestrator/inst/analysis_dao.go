/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"regexp"
	"time"

	"vitess.io/vitess/go/vt/log"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/process"
	"vitess.io/vitess/go/vt/orchestrator/util"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

var recentInstantAnalysis *cache.Cache

func init() {
	_ = metrics.Register("analysis.change.write.attempt", analysisChangeWriteAttemptCounter)
	_ = metrics.Register("analysis.change.write", analysisChangeWriteCounter)

	go initializeAnalysisDaoPostConfiguration()
}

func initializeAnalysisDaoPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	recentInstantAnalysis = cache.New(time.Duration(config.RecoveryPollSeconds*2)*time.Second, time.Second)
}

type clusterAnalysis struct {
	hasClusterwideAction bool
	primaryKey           *InstanceKey
	durability           reparentutil.Durabler
}

// GetReplicationAnalysis will check for replication problems (dead primary; unreachable primary; etc)
func GetReplicationAnalysis(clusterName string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error) {
	result := []ReplicationAnalysis{}

	// TODO(sougou); deprecate ReduceReplicationAnalysisCount
	args := sqlutils.Args(config.Config.ReasonableReplicationLagSeconds, ValidSecondsFromSeenToLastAttemptedCheck(), config.Config.ReasonableReplicationLagSeconds, clusterName)
	query := `
	SELECT
		vitess_tablet.info AS tablet_info,
		vitess_tablet.hostname,
		vitess_tablet.port,
		vitess_tablet.tablet_type,
		vitess_tablet.primary_timestamp,
		vitess_keyspace.keyspace AS keyspace,
		vitess_keyspace.keyspace_type AS keyspace_type,
		vitess_keyspace.durability_policy AS durability_policy,
		primary_instance.read_only AS read_only,
		MIN(primary_instance.data_center) AS data_center,
		MIN(primary_instance.region) AS region,
		MIN(primary_instance.physical_environment) AS physical_environment,
		MIN(primary_instance.source_host) AS source_host,
		MIN(primary_instance.source_port) AS source_port,
		MIN(primary_instance.cluster_name) AS cluster_name,
		MIN(primary_instance.binary_log_file) AS binary_log_file,
		MIN(primary_instance.binary_log_pos) AS binary_log_pos,
		MIN(primary_instance.suggested_cluster_alias) AS suggested_cluster_alias,
		MIN(primary_tablet.info) AS primary_tablet_info,
		MIN(
			IFNULL(
				primary_instance.binary_log_file = database_instance_stale_binlog_coordinates.binary_log_file
				AND primary_instance.binary_log_pos = database_instance_stale_binlog_coordinates.binary_log_pos
				AND database_instance_stale_binlog_coordinates.first_seen < NOW() - interval ? second,
				0
			)
		) AS is_stale_binlog_coordinates,
		MIN(
			IFNULL(
				cluster_alias.alias,
				primary_instance.cluster_name
			)
		) AS cluster_alias,
		MIN(
			IFNULL(
				cluster_domain_name.domain_name,
				primary_instance.cluster_name
			)
		) AS cluster_domain,
		MIN(
			primary_instance.last_checked <= primary_instance.last_seen
			and primary_instance.last_attempted_check <= primary_instance.last_seen + interval ? second
		) = 1 AS is_last_check_valid,
		/* To be considered a primary, traditional async replication must not be present/valid AND the host should either */
		/* not be a replication group member OR be the primary of the replication group */
		MIN(primary_instance.last_check_partial_success) as last_check_partial_success,
		MIN(
			(
				primary_instance.source_host IN ('', '_')
				OR primary_instance.source_port = 0
				OR substr(primary_instance.source_host, 1, 2) = '//'
			)
			AND (
				primary_instance.replication_group_name = ''
				OR primary_instance.replication_group_member_role = 'PRIMARY'
			)
		) AS is_primary,
		MIN(primary_instance.is_co_primary) AS is_co_primary,
		MIN(
			CONCAT(
				primary_instance.hostname,
				':',
				primary_instance.port
			) = primary_instance.cluster_name
		) AS is_cluster_primary,
		MIN(primary_instance.gtid_mode) AS gtid_mode,
		COUNT(replica_instance.server_id) AS count_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
			),
			0
		) AS count_valid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.replica_io_running != 0
				AND replica_instance.replica_sql_running != 0
			),
			0
		) AS count_valid_replicating_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.replica_io_running = 0
				AND replica_instance.last_io_error like '%%error %%connecting to master%%'
				AND replica_instance.replica_sql_running = 1
			),
			0
		) AS count_replicas_failing_to_connect_to_primary,
		MIN(primary_instance.replication_depth) AS replication_depth,
		GROUP_CONCAT(
			concat(
				replica_instance.Hostname,
				':',
				replica_instance.Port
			)
		) as replica_hosts,
		MIN(
			primary_instance.replica_sql_running = 1
			AND primary_instance.replica_io_running = 0
			AND primary_instance.last_io_error like '%%error %%connecting to master%%'
		) AS is_failing_to_connect_to_primary,
		MIN(
			primary_instance.replica_sql_running = 0
			OR primary_instance.replica_io_running = 0
		) AS replication_stopped,
		MIN(
			primary_downtime.downtime_active is not null
			and ifnull(primary_downtime.end_timestamp, now()) > now()
		) AS is_downtimed,
		MIN(
			IFNULL(primary_downtime.end_timestamp, '')
		) AS downtime_end_timestamp,
		MIN(
			IFNULL(
				unix_timestamp() - unix_timestamp(primary_downtime.end_timestamp),
				0
			)
		) AS downtime_remaining_seconds,
		MIN(
			primary_instance.binlog_server
		) AS is_binlog_server,
		MIN(
			primary_instance.supports_oracle_gtid
		) AS supports_oracle_gtid,
		MIN(
			primary_instance.semi_sync_primary_enabled
		) AS semi_sync_primary_enabled,
		MIN(
			primary_instance.semi_sync_primary_wait_for_replica_count
		) AS semi_sync_primary_wait_for_replica_count,
		MIN(
			primary_instance.semi_sync_primary_clients
		) AS semi_sync_primary_clients,
		MIN(
			primary_instance.semi_sync_primary_status
		) AS semi_sync_primary_status,
		MIN(
			primary_instance.semi_sync_replica_enabled
		) AS semi_sync_replica_enabled,
		SUM(replica_instance.is_co_primary) AS count_co_primary_replicas,
		SUM(replica_instance.oracle_gtid) AS count_oracle_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.oracle_gtid != 0
			),
			0
		) AS count_valid_oracle_gtid_replicas,
		SUM(
			replica_instance.binlog_server
		) AS count_binlog_server_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.binlog_server != 0
			),
			0
		) AS count_valid_binlog_server_replicas,
		SUM(
			replica_instance.semi_sync_replica_enabled
		) AS count_semi_sync_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.semi_sync_replica_enabled != 0
			),
			0
		) AS count_valid_semi_sync_replicas,
		MIN(
			primary_instance.mariadb_gtid
		) AS is_mariadb_gtid,
		SUM(replica_instance.mariadb_gtid) AS count_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.mariadb_gtid != 0
			),
			0
		) AS count_valid_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_replica_updates
			),
			0
		) AS count_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_replica_updates
				AND replica_instance.binlog_format = 'STATEMENT'
			),
			0
		) AS count_statement_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_replica_updates
				AND replica_instance.binlog_format = 'MIXED'
			),
			0
		) AS count_mixed_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_replica_updates
				AND replica_instance.binlog_format = 'ROW'
			),
			0
		) AS count_row_based_logging_replicas,
		IFNULL(
			SUM(replica_instance.sql_delay > 0),
			0
		) AS count_delayed_replicas,
		IFNULL(
			SUM(replica_instance.replica_lag_seconds > ?),
			0
		) AS count_lagging_replicas,
		IFNULL(MIN(replica_instance.gtid_mode), '') AS min_replica_gtid_mode,
		IFNULL(MAX(replica_instance.gtid_mode), '') AS max_replica_gtid_mode,
		IFNULL(
			MAX(
				case when replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now() then '' else replica_instance.gtid_errant end
			),
			''
		) AS max_replica_gtid_errant,
		IFNULL(
			SUM(
				replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now()
			),
			0
		) AS count_downtimed_replicas,
		COUNT(
			DISTINCT case when replica_instance.log_bin
			AND replica_instance.log_replica_updates then replica_instance.major_version else NULL end
		) AS count_distinct_logging_major_versions
	FROM
		vitess_tablet
		JOIN vitess_keyspace ON (
			vitess_tablet.keyspace = vitess_keyspace.keyspace
		)
		JOIN database_instance primary_instance ON (
			vitess_tablet.hostname = primary_instance.hostname
			AND vitess_tablet.port = primary_instance.port
		)
		LEFT JOIN vitess_tablet primary_tablet ON (
			primary_tablet.hostname = primary_instance.source_host
			AND primary_tablet.port = primary_instance.source_port
		)
		LEFT JOIN hostname_resolve ON (
			primary_instance.hostname = hostname_resolve.hostname
		)
		LEFT JOIN database_instance replica_instance ON (
			COALESCE(
				hostname_resolve.resolved_hostname,
				primary_instance.hostname
			) = replica_instance.source_host
			AND primary_instance.port = replica_instance.source_port
		)
		LEFT JOIN database_instance_maintenance ON (
			primary_instance.hostname = database_instance_maintenance.hostname
			AND primary_instance.port = database_instance_maintenance.port
			AND database_instance_maintenance.maintenance_active = 1
		)
		LEFT JOIN database_instance_stale_binlog_coordinates ON (
			primary_instance.hostname = database_instance_stale_binlog_coordinates.hostname
			AND primary_instance.port = database_instance_stale_binlog_coordinates.port
		)
		LEFT JOIN database_instance_downtime as primary_downtime ON (
			primary_instance.hostname = primary_downtime.hostname
			AND primary_instance.port = primary_downtime.port
			AND primary_downtime.downtime_active = 1
		)
		LEFT JOIN database_instance_downtime as replica_downtime ON (
			replica_instance.hostname = replica_downtime.hostname
			AND replica_instance.port = replica_downtime.port
			AND replica_downtime.downtime_active = 1
		)
		LEFT JOIN cluster_alias ON (
			cluster_alias.cluster_name = primary_instance.cluster_name
		)
		LEFT JOIN cluster_domain_name ON (
			cluster_domain_name.cluster_name = primary_instance.cluster_name
		)
	WHERE
		database_instance_maintenance.database_instance_maintenance_id IS NULL
		AND ? IN ('', primary_instance.cluster_name)
	GROUP BY
		vitess_tablet.hostname,
		vitess_tablet.port
	ORDER BY
		vitess_tablet.tablet_type ASC,
		vitess_tablet.primary_timestamp DESC
	`

	clusters := make(map[string]*clusterAnalysis)
	err := db.Db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		a := ReplicationAnalysis{
			Analysis:               NoProblem,
			ProcessingNodeHostname: process.ThisHostname,
			ProcessingNodeToken:    util.ProcessToken.Hash,
		}

		tablet := &topodatapb.Tablet{}
		if err := prototext.Unmarshal([]byte(m.GetString("tablet_info")), tablet); err != nil {
			log.Errorf("could not read tablet %v: %v", m.GetString("tablet_info"), err)
			return nil
		}

		primaryTablet := &topodatapb.Tablet{}
		if str := m.GetString("primary_tablet_info"); str != "" {
			if err := prototext.Unmarshal([]byte(str), primaryTablet); err != nil {
				log.Errorf("could not read tablet %v: %v", str, err)
				return nil
			}
		}

		a.TabletType = tablet.Type
		a.AnalyzedKeyspace = m.GetString("keyspace")
		a.PrimaryTimeStamp = m.GetTime("primary_timestamp")

		if keyspaceType := topodatapb.KeyspaceType(m.GetInt("keyspace_type")); keyspaceType == topodatapb.KeyspaceType_SNAPSHOT {
			log.Errorf("keyspace %v is a snapshot keyspace. Skipping.", a.AnalyzedKeyspace)
			return nil
		}

		a.IsPrimary = m.GetBool("is_primary")
		countCoPrimaryReplicas := m.GetUint("count_co_primary_replicas")
		a.IsCoPrimary = m.GetBool("is_co_primary") || (countCoPrimaryReplicas > 0)
		a.AnalyzedInstanceKey = InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		a.AnalyzedInstancePrimaryKey = InstanceKey{Hostname: m.GetString("source_host"), Port: m.GetInt("source_port")}
		a.AnalyzedInstanceDataCenter = m.GetString("data_center")
		a.AnalyzedInstanceRegion = m.GetString("region")
		a.AnalyzedInstancePhysicalEnvironment = m.GetString("physical_environment")
		a.AnalyzedInstanceBinlogCoordinates = BinlogCoordinates{
			LogFile: m.GetString("binary_log_file"),
			LogPos:  m.GetInt64("binary_log_pos"),
			Type:    BinaryLog,
		}
		isStaleBinlogCoordinates := m.GetBool("is_stale_binlog_coordinates")
		a.SuggestedClusterAlias = m.GetString("suggested_cluster_alias")
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.ClusterDetails.ClusterDomain = m.GetString("cluster_domain")
		a.GTIDMode = m.GetString("gtid_mode")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.LastCheckPartialSuccess = m.GetBool("last_check_partial_success")
		a.CountReplicas = m.GetUint("count_replicas")
		a.CountValidReplicas = m.GetUint("count_valid_replicas")
		a.CountValidReplicatingReplicas = m.GetUint("count_valid_replicating_replicas")
		a.CountReplicasFailingToConnectToPrimary = m.GetUint("count_replicas_failing_to_connect_to_primary")
		a.CountDowntimedReplicas = m.GetUint("count_downtimed_replicas")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToPrimary = m.GetBool("is_failing_to_connect_to_primary")
		a.ReplicationStopped = m.GetBool("replication_stopped")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsBinlogServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.Replicas = *NewInstanceKeyMap()
		_ = a.Replicas.ReadCommaDelimitedList(m.GetString("replica_hosts"))

		countValidOracleGTIDReplicas := m.GetUint("count_valid_oracle_gtid_replicas")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidMariaDBGTIDReplicas := m.GetUint("count_valid_mariadb_gtid_replicas")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidBinlogServerReplicas := m.GetUint("count_valid_binlog_server_replicas")
		a.BinlogServerImmediateTopology = countValidBinlogServerReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		a.SemiSyncPrimaryEnabled = m.GetBool("semi_sync_primary_enabled")
		a.SemiSyncPrimaryStatus = m.GetBool("semi_sync_primary_status")
		a.SemiSyncReplicaEnabled = m.GetBool("semi_sync_replica_enabled")
		a.CountSemiSyncReplicasEnabled = m.GetUint("count_semi_sync_replicas")
		// countValidSemiSyncReplicasEnabled := m.GetUint("count_valid_semi_sync_replicas")
		a.SemiSyncPrimaryWaitForReplicaCount = m.GetUint("semi_sync_primary_wait_for_replica_count")
		a.SemiSyncPrimaryClients = m.GetUint("semi_sync_primary_clients")

		a.MinReplicaGTIDMode = m.GetString("min_replica_gtid_mode")
		a.MaxReplicaGTIDMode = m.GetString("max_replica_gtid_mode")
		a.MaxReplicaGTIDErrant = m.GetString("max_replica_gtid_errant")

		a.CountLoggingReplicas = m.GetUint("count_logging_replicas")
		a.CountStatementBasedLoggingReplicas = m.GetUint("count_statement_based_logging_replicas")
		a.CountMixedBasedLoggingReplicas = m.GetUint("count_mixed_based_logging_replicas")
		a.CountRowBasedLoggingReplicas = m.GetUint("count_row_based_logging_replicas")
		a.CountDistinctMajorVersionsLoggingReplicas = m.GetUint("count_distinct_logging_major_versions")

		a.CountDelayedReplicas = m.GetUint("count_delayed_replicas")
		a.CountLaggingReplicas = m.GetUint("count_lagging_replicas")

		a.IsReadOnly = m.GetUint("read_only") == 1

		if !a.LastCheckValid {
			analysisMessage := fmt.Sprintf("analysis: ClusterName: %+v, IsPrimary: %+v, LastCheckValid: %+v, LastCheckPartialSuccess: %+v, CountReplicas: %+v, CountValidReplicas: %+v, CountValidReplicatingReplicas: %+v, CountLaggingReplicas: %+v, CountDelayedReplicas: %+v, CountReplicasFailingToConnectToPrimary: %+v",
				a.ClusterDetails.ClusterName, a.IsPrimary, a.LastCheckValid, a.LastCheckPartialSuccess, a.CountReplicas, a.CountValidReplicas, a.CountValidReplicatingReplicas, a.CountLaggingReplicas, a.CountDelayedReplicas, a.CountReplicasFailingToConnectToPrimary,
			)
			if util.ClearToLog("analysis_dao", analysisMessage) {
				log.Infof(analysisMessage)
			}
		}
		if clusters[a.SuggestedClusterAlias] == nil {
			clusters[a.SuggestedClusterAlias] = &clusterAnalysis{}
			if a.TabletType == topodatapb.TabletType_PRIMARY {
				a.IsClusterPrimary = true
				clusters[a.SuggestedClusterAlias].primaryKey = &a.AnalyzedInstanceKey
			}
			durabilityPolicy := m.GetString("durability_policy")
			if durabilityPolicy == "" {
				log.Errorf("ignoring keyspace %v because no durability_policy is set. Please set it using SetKeyspaceDurabilityPolicy", a.AnalyzedKeyspace)
				return nil
			}
			durability, err := reparentutil.GetDurabilityPolicy(durabilityPolicy)
			if err != nil {
				log.Errorf("can't get the durability policy %v - %v. Skipping keyspace - %v.", durabilityPolicy, err, a.AnalyzedKeyspace)
				return nil
			}
			clusters[a.SuggestedClusterAlias].durability = durability
		}
		// ca has clusterwide info
		ca := clusters[a.SuggestedClusterAlias]
		if ca.hasClusterwideAction {
			// We can only take one cluster level action at a time.
			return nil
		}
		if ca.durability == nil {
			// We failed to load the durability policy, so we shouldn't run any analysis
			return nil
		}
		if a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas == 0 {
			a.Analysis = DeadPrimaryWithoutReplicas
			a.Description = "Primary cannot be reached by orchestrator and has no replica"
			ca.hasClusterwideAction = true
			//
		} else if a.IsClusterPrimary && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadPrimary
			a.Description = "Primary cannot be reached by orchestrator and none of its replicas is replicating"
			ca.hasClusterwideAction = true
			//
		} else if a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadPrimaryAndReplicas
			a.Description = "Primary cannot be reached by orchestrator and none of its replicas is replicating"
			ca.hasClusterwideAction = true
			//
		} else if a.IsClusterPrimary && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = DeadPrimaryAndSomeReplicas
			a.Description = "Primary cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
			ca.hasClusterwideAction = true
			//
		} else if a.IsClusterPrimary && !a.IsPrimary {
			a.Analysis = PrimaryHasPrimary
			a.Description = "Primary is replicating from somewhere else"
			ca.hasClusterwideAction = true
			//
		} else if a.IsClusterPrimary && a.IsReadOnly {
			a.Analysis = PrimaryIsReadOnly
			a.Description = "Primary is read-only"
			//
		} else if a.IsClusterPrimary && SemiSyncAckers(ca.durability, tablet) != 0 && !a.SemiSyncPrimaryEnabled {
			a.Analysis = PrimarySemiSyncMustBeSet
			a.Description = "Primary semi-sync must be set"
			//
		} else if a.IsClusterPrimary && SemiSyncAckers(ca.durability, tablet) == 0 && a.SemiSyncPrimaryEnabled {
			a.Analysis = PrimarySemiSyncMustNotBeSet
			a.Description = "Primary semi-sync must not be set"
			//
		} else if topo.IsReplicaType(a.TabletType) && ca.primaryKey == nil {
			a.Analysis = ClusterHasNoPrimary
			a.Description = "Cluster has no primary"
			ca.hasClusterwideAction = true
		} else if topo.IsReplicaType(a.TabletType) && !a.IsReadOnly {
			a.Analysis = ReplicaIsWritable
			a.Description = "Replica is writable"
			//
		} else if topo.IsReplicaType(a.TabletType) && a.IsPrimary {
			a.Analysis = NotConnectedToPrimary
			a.Description = "Not connected to the primary"
			//
		} else if topo.IsReplicaType(a.TabletType) && !a.IsPrimary && ca.primaryKey != nil && a.AnalyzedInstancePrimaryKey != *ca.primaryKey {
			a.Analysis = ConnectedToWrongPrimary
			a.Description = "Connected to wrong primary"
			//
		} else if topo.IsReplicaType(a.TabletType) && !a.IsPrimary && a.ReplicationStopped {
			a.Analysis = ReplicationStopped
			a.Description = "Replication is stopped"
			//
		} else if topo.IsReplicaType(a.TabletType) && !a.IsPrimary && IsReplicaSemiSync(ca.durability, primaryTablet, tablet) && !a.SemiSyncReplicaEnabled {
			a.Analysis = ReplicaSemiSyncMustBeSet
			a.Description = "Replica semi-sync must be set"
			//
		} else if topo.IsReplicaType(a.TabletType) && !a.IsPrimary && !IsReplicaSemiSync(ca.durability, primaryTablet, tablet) && a.SemiSyncReplicaEnabled {
			a.Analysis = ReplicaSemiSyncMustNotBeSet
			a.Description = "Replica semi-sync must not be set"
			//
			// TODO(sougou): Events below here are either ignored or not possible.
		} else if a.IsPrimary && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
			a.Analysis = UnreachablePrimaryWithLaggingReplicas
			a.Description = "Primary cannot be reached by orchestrator and all of its replicas are lagging"
			//
		} else if a.IsPrimary && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
			// partial success is here to redice noise
			a.Analysis = UnreachablePrimary
			a.Description = "Primary cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
			//
		} else if a.IsPrimary && !a.LastCheckValid && a.LastCheckPartialSuccess && a.CountReplicasFailingToConnectToPrimary > 0 && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
			// there's partial success, but also at least one replica is failing to connect to primary
			a.Analysis = UnreachablePrimary
			a.Description = "Primary cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
			//
		} else if a.IsPrimary && a.SemiSyncPrimaryEnabled && a.SemiSyncPrimaryStatus && a.SemiSyncPrimaryWaitForReplicaCount > 0 && a.SemiSyncPrimaryClients < a.SemiSyncPrimaryWaitForReplicaCount {
			if isStaleBinlogCoordinates {
				a.Analysis = LockedSemiSyncPrimary
				a.Description = "Semi sync primary is locked since it doesn't get enough replica acknowledgements"
			} else {
				a.Analysis = LockedSemiSyncPrimaryHypothesis
				a.Description = "Semi sync primary seems to be locked, more samplings needed to validate"
			}
			//
		} else if a.IsPrimary && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = PrimarySingleReplicaNotReplicating
			a.Description = "Primary is reachable but its single replica is not replicating"
		} else if a.IsPrimary && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == 0 {
			a.Analysis = PrimarySingleReplicaDead
			a.Description = "Primary is reachable but its single replica is dead"
			//
		} else if a.IsPrimary && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllPrimaryReplicasNotReplicating
			a.Description = "Primary is reachable but none of its replicas is replicating"
			//
		} else if a.IsPrimary && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
			a.Analysis = AllPrimaryReplicasNotReplicatingOrDead
			a.Description = "Primary is reachable but none of its replicas is replicating"
			//
		} else if a.IsBinlogServer && a.IsFailingToConnectToPrimary {
			a.Analysis = BinlogServerFailingToConnectToPrimary
			a.Description = "Binlog server is unable to connect to its primary"
			//
		}
		//		 else if a.IsPrimary && a.CountReplicas == 0 {
		//			a.Analysis = PrimaryWithoutReplicas
		//			a.Description = "Primary has no replicas"
		//		}

		appendAnalysis := func(analysis *ReplicationAnalysis) {
			if a.Analysis == NoProblem && len(a.StructureAnalysis) == 0 && !hints.IncludeNoProblem {
				return
			}
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					return
				}
			}
			if a.IsDowntimed {
				a.SkippableDueToDowntime = true
			}
			if a.CountReplicas == a.CountDowntimedReplicas {
				switch a.Analysis {
				case AllPrimaryReplicasNotReplicating,
					AllPrimaryReplicasNotReplicatingOrDead,
					PrimarySingleReplicaDead:
					a.IsReplicasDowntimed = true
					a.SkippableDueToDowntime = true
				}
			}
			if a.SkippableDueToDowntime && !hints.IncludeDowntimed {
				return
			}
			result = append(result, a)
		}

		{
			// Moving on to structure analysis
			// We also do structural checks. See if there's potential danger in promotions
			if a.IsPrimary && a.CountLoggingReplicas == 0 && a.CountReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, NoLoggingReplicasStructureWarning)
			}
			if a.IsPrimary && a.CountReplicas > 1 &&
				!a.OracleGTIDImmediateTopology &&
				!a.MariaDBGTIDImmediateTopology &&
				!a.BinlogServerImmediateTopology {
				a.StructureAnalysis = append(a.StructureAnalysis, NoFailoverSupportStructureWarning)
			}
			if a.IsPrimary && a.CountStatementBasedLoggingReplicas > 0 && a.CountMixedBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndMixedLoggingReplicasStructureWarning)
			}
			if a.IsPrimary && a.CountStatementBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndRowLoggingReplicasStructureWarning)
			}
			if a.IsPrimary && a.CountMixedBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, MixedAndRowLoggingReplicasStructureWarning)
			}
			if a.IsPrimary && a.CountDistinctMajorVersionsLoggingReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, MultipleMajorVersionsLoggingReplicasStructureWarning)
			}

			if a.CountReplicas > 0 && (a.GTIDMode != a.MinReplicaGTIDMode || a.GTIDMode != a.MaxReplicaGTIDMode) {
				a.StructureAnalysis = append(a.StructureAnalysis, DifferentGTIDModesStructureWarning)
			}
			if a.MaxReplicaGTIDErrant != "" {
				a.StructureAnalysis = append(a.StructureAnalysis, ErrantGTIDStructureWarning)
			}

			if a.IsPrimary && a.IsReadOnly {
				a.StructureAnalysis = append(a.StructureAnalysis, NoWriteablePrimaryStructureWarning)
			}

			if a.IsPrimary && a.SemiSyncPrimaryEnabled && !a.SemiSyncPrimaryStatus && a.SemiSyncPrimaryWaitForReplicaCount > 0 && a.SemiSyncPrimaryClients < a.SemiSyncPrimaryWaitForReplicaCount {
				a.StructureAnalysis = append(a.StructureAnalysis, NotEnoughValidSemiSyncReplicasStructureWarning)
			}
		}
		appendAnalysis(&a)

		if a.CountReplicas > 0 && hints.AuditAnalysis {
			// Interesting enough for analysis
			go auditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	// TODO: result, err = getConcensusReplicationAnalysis(result)
	return result, err
}

// auditInstanceAnalysisInChangelog will write down an instance's analysis in the database_instance_analysis_changelog table.
// To not repeat recurring analysis code, the database_instance_last_analysis table is used, so that only changes to
// analysis codes are written.
func auditInstanceAnalysisInChangelog(instanceKey *InstanceKey, analysisCode AnalysisCode) error {
	if lastWrittenAnalysis, found := recentInstantAnalysis.Get(instanceKey.DisplayString()); found {
		if lastWrittenAnalysis == analysisCode {
			// Surely nothing new.
			// And let's expand the timeout
			recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
			return nil
		}
	}
	// Passed the cache; but does database agree that there's a change? Here's a persistent cache; this comes here
	// to verify no two orchestrator services are doing this without coordinating (namely, one dies, the other taking its place
	// and has no familiarity of the former's cache)
	analysisChangeWriteAttemptCounter.Inc(1)

	lastAnalysisChanged := false
	{
		sqlResult, err := db.ExecOrchestrator(`
			update database_instance_last_analysis set
				analysis = ?,
				analysis_timestamp = now()
			where
				hostname = ?
				and port = ?
				and analysis != ?
			`,
			string(analysisCode), instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			log.Error(err)
			return err
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			log.Error(err)
			return err
		}
		lastAnalysisChanged = (rows > 0)
	}
	if !lastAnalysisChanged {
		_, err := db.ExecOrchestrator(`
			insert ignore into database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
			instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
	if !lastAnalysisChanged {
		return nil
	}

	_, err := db.ExecOrchestrator(`
			insert into database_instance_analysis_changelog (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	log.Error(err)
	return err
}

// ExpireInstanceAnalysisChangelog removes old-enough analysis entries from the changelog
func ExpireInstanceAnalysisChangelog() error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance_analysis_changelog
			where
				analysis_timestamp < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	log.Error(err)
	return err
}

// ReadReplicationAnalysisChangelog
func ReadReplicationAnalysisChangelog() (res [](*ReplicationAnalysisChangelog), err error) {
	query := `
		select
      hostname,
      port,
			analysis_timestamp,
			analysis
		from
			database_instance_analysis_changelog
		order by
			hostname, port, changelog_id
		`
	analysisChangelog := &ReplicationAnalysisChangelog{}
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		key := InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}

		if !analysisChangelog.AnalyzedInstanceKey.Equals(&key) {
			analysisChangelog = &ReplicationAnalysisChangelog{AnalyzedInstanceKey: key, Changelog: []string{}}
			res = append(res, analysisChangelog)
		}
		analysisEntry := fmt.Sprintf("%s;%s,", m.GetString("analysis_timestamp"), m.GetString("analysis"))
		analysisChangelog.Changelog = append(analysisChangelog.Changelog, analysisEntry)

		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return res, err
}
