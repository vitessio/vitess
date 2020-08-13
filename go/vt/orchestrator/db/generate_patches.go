/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package db

// generateSQLPatches contains DDLs for patching schema to the latest version.
// Add new statements at the end of the list so they form a changelog.
var generateSQLPatches = []string{
	`
		ALTER TABLE
			database_instance
			ADD COLUMN read_only TINYINT UNSIGNED NOT NULL AFTER version
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN last_sql_error TEXT NOT NULL AFTER exec_master_log_pos
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN last_io_error TEXT NOT NULL AFTER last_sql_error
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN oracle_gtid TINYINT UNSIGNED NOT NULL AFTER slave_io_running
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN mariadb_gtid TINYINT UNSIGNED NOT NULL AFTER oracle_gtid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN relay_log_file varchar(128) CHARACTER SET ascii NOT NULL AFTER exec_master_log_pos
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN relay_log_pos bigint unsigned NOT NULL AFTER relay_log_file
	`,
	`
		DROP INDEX master_host_port_idx ON database_instance
	`,
	`
		ALTER TABLE
			database_instance
			ADD INDEX master_host_port_idx_database_instance (master_host, master_port)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN pseudo_gtid TINYINT UNSIGNED NOT NULL AFTER mariadb_gtid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN replication_depth TINYINT UNSIGNED NOT NULL AFTER cluster_name
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN has_replication_filters TINYINT UNSIGNED NOT NULL AFTER slave_io_running
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN data_center varchar(32) CHARACTER SET ascii NOT NULL AFTER cluster_name
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN physical_environment varchar(32) CHARACTER SET ascii NOT NULL AFTER data_center
	`,
	`
		ALTER TABLE
			database_instance_maintenance
			ADD KEY active_timestamp_idx (maintenance_active, begin_timestamp)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN uptime INT UNSIGNED NOT NULL AFTER last_seen
	`,
	`
		ALTER TABLE
			cluster_alias
			ADD UNIQUE KEY alias_uidx (alias)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN is_co_master TINYINT UNSIGNED NOT NULL AFTER replication_depth
	`,
	`
		ALTER TABLE
			database_instance_maintenance
			ADD KEY active_end_timestamp_idx (maintenance_active, end_timestamp)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN sql_delay INT UNSIGNED NOT NULL AFTER slave_lag_seconds
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN analysis              varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN cluster_name          varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN cluster_alias         varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN count_affected_slaves int unsigned NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN slave_hosts text CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE hostname_unresolve
			ADD COLUMN last_registered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE hostname_unresolve
			ADD KEY last_registered_idx (last_registered)
	`,
	`
		ALTER TABLE topology_recovery
			ADD KEY cluster_name_in_active_idx (cluster_name, in_active_period)
	`,
	`
		ALTER TABLE topology_recovery
			ADD KEY end_recovery_idx (end_recovery)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN binlog_server TINYINT UNSIGNED NOT NULL AFTER version
	`,
	`
		ALTER TABLE cluster_domain_name
			ADD COLUMN last_registered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE cluster_domain_name
			ADD KEY last_registered_idx (last_registered)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN supports_oracle_gtid TINYINT UNSIGNED NOT NULL AFTER oracle_gtid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN executed_gtid_set text CHARACTER SET ascii NOT NULL AFTER oracle_gtid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN server_uuid varchar(64) CHARACTER SET ascii NOT NULL AFTER server_id
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN suggested_cluster_alias varchar(128) CHARACTER SET ascii NOT NULL AFTER cluster_name
	`,
	`
		ALTER TABLE cluster_alias
			ADD COLUMN last_registered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE cluster_alias
			ADD KEY last_registered_idx (last_registered)
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN is_successful TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER processcing_node_token
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN acknowledged TINYINT UNSIGNED NOT NULL DEFAULT 0
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN acknowledged_by varchar(128) CHARACTER SET utf8 NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN acknowledge_comment text CHARACTER SET utf8 NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN participating_instances text CHARACTER SET ascii NOT NULL after slave_hosts
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN lost_slaves text CHARACTER SET ascii NOT NULL after participating_instances
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN all_errors text CHARACTER SET ascii NOT NULL after lost_slaves
	`,
	`
		ALTER TABLE audit
			ADD COLUMN cluster_name varchar(128) CHARACTER SET ascii NOT NULL DEFAULT '' AFTER port
	`,
	`
		ALTER TABLE candidate_database_instance
			ADD COLUMN priority TINYINT SIGNED NOT NULL DEFAULT 1 comment 'positive promote, nagative unpromotes'
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN acknowledged_at TIMESTAMP NULL after acknowledged
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD KEY acknowledged_idx (acknowledged, acknowledged_at)
	`,
	`
		ALTER TABLE
			blocked_topology_recovery
			ADD KEY last_blocked_idx (last_blocked_timestamp)
	`,
	`
		ALTER TABLE candidate_database_instance
			ADD COLUMN promotion_rule enum('must', 'prefer', 'neutral', 'prefer_not', 'must_not') NOT NULL DEFAULT 'neutral'
	`,
	`
		ALTER TABLE node_health /* sqlite3-skip */
			DROP PRIMARY KEY,
			ADD PRIMARY KEY (hostname, token)
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN extra_info varchar(128) CHARACTER SET utf8 NOT NULL
	`,
	`
		ALTER TABLE agent_seed /* sqlite3-skip */
			MODIFY end_timestamp timestamp NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE active_node /* sqlite3-skip */
			MODIFY last_seen_active timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,

	`
		ALTER TABLE node_health /* sqlite3-skip */
			MODIFY last_seen_active timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE candidate_database_instance /* sqlite3-skip */
			MODIFY last_suggested timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE master_position_equivalence /* sqlite3-skip */
			MODIFY last_suggested timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN last_attempted_check TIMESTAMP NOT NULL DEFAULT '1971-01-01 00:00:00' AFTER last_checked
	`,
	`
		ALTER TABLE
			database_instance /* sqlite3-skip */
			MODIFY last_attempted_check TIMESTAMP NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE
			database_instance_analysis_changelog
			ADD KEY instance_timestamp_idx (hostname, port, analysis_timestamp)
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN last_detection_id bigint unsigned NOT NULL
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD KEY last_detection_idx (last_detection_id)
	`,
	`
		ALTER TABLE node_health_history
			ADD COLUMN command varchar(128) CHARACTER SET utf8 NOT NULL
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN command varchar(128) CHARACTER SET utf8 NOT NULL
	`,
	`
		ALTER TABLE database_instance_topology_history
			ADD COLUMN version varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN gtid_purged text CHARACTER SET ascii NOT NULL AFTER executed_gtid_set
	`,
	`
		ALTER TABLE
			database_instance_coordinates_history
			ADD COLUMN last_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00' AFTER recorded_timestamp
	`,
	`
		ALTER TABLE
			access_token
			ADD COLUMN is_reentrant TINYINT UNSIGNED NOT NULL default 0
	`,
	`
		ALTER TABLE
			access_token
			ADD COLUMN acquired_at timestamp NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE
			database_instance_pool
			ADD COLUMN registered_at timestamp NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN replication_credentials_available TINYINT UNSIGNED NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN has_replication_credentials TINYINT UNSIGNED NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN allow_tls TINYINT UNSIGNED NOT NULL AFTER sql_delay
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_enforced TINYINT UNSIGNED NOT NULL AFTER physical_environment
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN instance_alias varchar(128) CHARACTER SET ascii NOT NULL AFTER physical_environment
	`,
	`
		ALTER TABLE
			topology_recovery
			ADD COLUMN successor_alias varchar(128) DEFAULT NULL
	`,
	`
		ALTER TABLE
			database_instance /* sqlite3-skip */
			MODIFY cluster_name varchar(128) NOT NULL
	`,
	`
		ALTER TABLE
			node_health
			ADD INDEX last_seen_active_idx (last_seen_active)
	`,
	`
		ALTER TABLE
			database_instance_maintenance
			ADD COLUMN processing_node_hostname varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			database_instance_maintenance
			ADD COLUMN processing_node_token varchar(128) NOT NULL
	`,
	`
		ALTER TABLE
			database_instance_maintenance
			ADD COLUMN explicitly_bounded TINYINT UNSIGNED NOT NULL
	`,
	`
		ALTER TABLE node_health_history
			ADD COLUMN app_version varchar(64) CHARACTER SET ascii NOT NULL DEFAULT ""
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN app_version varchar(64) CHARACTER SET ascii NOT NULL DEFAULT ""
	`,
	`
		ALTER TABLE node_health_history /* sqlite3-skip */
			MODIFY app_version varchar(64) CHARACTER SET ascii NOT NULL DEFAULT ""
	`,
	`
		ALTER TABLE node_health /* sqlite3-skip */
			MODIFY app_version varchar(64) CHARACTER SET ascii NOT NULL DEFAULT ""
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN version_comment varchar(128) NOT NULL DEFAULT ''
	`,
	`
		ALTER TABLE active_node
			ADD COLUMN first_seen_active timestamp NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN first_seen_active timestamp NOT NULL DEFAULT '1971-01-01 00:00:00'
	`,
	`
		ALTER TABLE database_instance
			ADD COLUMN major_version varchar(16) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN binlog_row_image varchar(16) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE topology_recovery
			ADD COLUMN uid varchar(128) CHARACTER SET ascii NOT NULL
	`,
	`
		CREATE INDEX uid_idx_topology_recovery ON topology_recovery(uid)
	`,
	`
		CREATE INDEX recovery_uid_idx_topology_recovery_steps ON topology_recovery_steps(recovery_uid)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN last_discovery_latency bigint not null
	`,
	`
		CREATE INDEX end_timestamp_idx_database_instance_downtime ON database_instance_downtime(end_timestamp)
	`,
	`
		CREATE INDEX suggested_cluster_alias_idx_database_instance ON database_instance(suggested_cluster_alias)
	`,
	`
		ALTER TABLE
			topology_failure_detection
			ADD COLUMN is_actionable tinyint not null default 0
	`,
	`
		DROP INDEX hostname_port_active_period_uidx_topology_failure_detection ON topology_failure_detection
	`,
	`
		CREATE UNIQUE INDEX host_port_active_recoverable_uidx_topology_failure_detection ON topology_failure_detection (hostname, port, in_active_period, end_active_period_unixtime, is_actionable)
	`,
	`
		ALTER TABLE raft_snapshot
			ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN db_backend varchar(255) CHARACTER SET ascii NOT NULL DEFAULT ""
	`,
	`
		ALTER TABLE node_health
			ADD COLUMN incrementing_indicator bigint not null default 0
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_master_enabled TINYINT UNSIGNED NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_replica_enabled TINYINT UNSIGNED NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN gtid_mode varchar(32) CHARACTER SET ascii NOT NULL
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN last_check_partial_success tinyint unsigned NOT NULL after last_attempted_check
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN master_uuid varchar(64) CHARACTER SET ascii NOT NULL AFTER oracle_gtid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN gtid_errant text CHARACTER SET ascii NOT NULL AFTER gtid_purged
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN ancestry_uuid text CHARACTER SET ascii NOT NULL AFTER master_uuid
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN replication_sql_thread_state tinyint signed not null default 0 AFTER slave_io_running
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN replication_io_thread_state tinyint signed not null default 0 AFTER replication_sql_thread_state
	`,
	`
		ALTER TABLE
		database_instance_tags /* sqlite3-skip */
		DROP PRIMARY KEY,
		ADD PRIMARY KEY (hostname, port, tag_name)
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN region varchar(32) CHARACTER SET ascii NOT NULL AFTER data_center
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_master_timeout INT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_master_enabled
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_master_wait_for_slave_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_master_timeout
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_master_status TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_master_wait_for_slave_count
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_replica_status TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_master_status
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_master_clients INT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_master_status
	`,
	`
		ALTER TABLE
			database_instance
			ADD COLUMN semi_sync_available TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER semi_sync_enforced
	`,
	`
		ALTER TABLE /* sqlite3-skip */
			database_instance
			MODIFY semi_sync_master_timeout BIGINT UNSIGNED NOT NULL DEFAULT 0
  `,
	// Fields related to Replication Group the instance belongs to
	`
		ALTER TABLE
			database_instance
			ADD COLUMN replication_group_name VARCHAR(64) CHARACTER SET ascii NOT NULL DEFAULT '' AFTER gtid_mode
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_is_single_primary_mode TINYINT UNSIGNED NOT NULL DEFAULT 1 AFTER replication_group_name
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_member_state VARCHAR(16) CHARACTER SET ascii NOT NULL DEFAULT '' AFTER replication_group_is_single_primary_mode
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_member_role VARCHAR(16) CHARACTER SET ascii NOT NULL DEFAULT '' AFTER replication_group_member_state
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_members text CHARACTER SET ascii NOT NULL AFTER replication_group_member_role
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_primary_host varchar(128) CHARACTER SET ascii NOT NULL DEFAULT '' AFTER replication_group_members
	`,
	`
		ALTER TABLE
		database_instance
			ADD COLUMN replication_group_primary_port smallint(5) unsigned NOT NULL DEFAULT 0 AFTER replication_group_primary_host
	`,
}
