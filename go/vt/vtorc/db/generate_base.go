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

// vtorcBackend is a list of SQL statements required to build the vtorc backend
var vtorcBackend = []string{
	`
DROP TABLE IF EXISTS database_instance
`,
	`
CREATE TABLE database_instance (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	last_checked timestamp not null default (''),
	last_seen timestamp NULL DEFAULT NULL,
	server_id int NOT NULL,
	version varchar(128) NOT NULL,
	binlog_format varchar(16) NOT NULL,
	log_bin tinyint NOT NULL,
	log_replica_updates tinyint NOT NULL,
	binary_log_file varchar(128) NOT NULL,
	binary_log_pos bigint NOT NULL,
	source_host varchar(128) NOT NULL,
	source_port smallint NOT NULL,
	replica_sql_running tinyint NOT NULL,
	replica_io_running tinyint NOT NULL,
	source_log_file varchar(128) NOT NULL,
	read_source_log_pos bigint NOT NULL,
	relay_source_log_file varchar(128) NOT NULL,
	exec_source_log_pos bigint NOT NULL,
	replication_lag_seconds bigint DEFAULT NULL,
	replica_lag_seconds bigint DEFAULT NULL,
	read_only TINYint not null default 0,
	last_sql_error TEXT not null default '',
	last_io_error TEXT not null default '',
	oracle_gtid TINYint not null default 0,
	mariadb_gtid TINYint not null default 0,
	relay_log_file varchar(128) not null default '',
	relay_log_pos bigint not null default 0,
	pseudo_gtid TINYint not null default 0,
	replication_depth TINYint not null default 0,
	has_replication_filters TINYint not null default 0,
	data_center varchar(32) not null default '',
	physical_environment varchar(32) not null default '',
	is_co_primary TINYint not null default 0,
	sql_delay int not null default 0,
	binlog_server TINYint not null default 0,
	supports_oracle_gtid TINYint not null default 0,
	executed_gtid_set text not null default '',
	server_uuid varchar(64) not null default '',
	last_attempted_check TIMESTAMP NOT NULL DEFAULT '1971-01-01 00:00:00',
	gtid_purged text not null default '',
	has_replication_credentials TINYint not null default 0,
	allow_tls TINYint not null default 0,
	semi_sync_enforced TINYint not null default 0,
	instance_alias varchar(128) not null default '',
	version_comment varchar(128) NOT NULL DEFAULT '',
	major_version varchar(16) not null default '',
	binlog_row_image varchar(16) not null default '',
	last_discovery_latency bigint not null default 0,
	semi_sync_primary_enabled TINYint not null default 0,
	semi_sync_replica_enabled TINYint not null default 0,
	gtid_mode varchar(32) not null default '',
	last_check_partial_success tinyint not null default 0,
	source_uuid varchar(64) not null default '',
	gtid_errant text not null default '',
	ancestry_uuid text not null default '',
	replication_sql_thread_state tinyint signed not null default 0,
	replication_io_thread_state tinyint signed not null default 0,
	region varchar(32) not null default '',
	semi_sync_primary_timeout int NOT NULL DEFAULT 0,
	semi_sync_primary_wait_for_replica_count int NOT NULL DEFAULT 0,
	semi_sync_primary_status TINYint NOT NULL DEFAULT 0,
	semi_sync_replica_status TINYint NOT NULL DEFAULT 0,
	semi_sync_primary_clients int NOT NULL DEFAULT 0,
	replication_group_name VARCHAR(64) NOT NULL DEFAULT '',
	replication_group_is_single_primary_mode TINYint NOT NULL DEFAULT 1,
	replication_group_member_state VARCHAR(16) NOT NULL DEFAULT '',
	replication_group_member_role VARCHAR(16) NOT NULL DEFAULT '',
	replication_group_members text not null default '',
	replication_group_primary_host varchar(128) NOT NULL DEFAULT '',
	replication_group_primary_port smallint NOT NULL DEFAULT 0,
	PRIMARY KEY (hostname,port)
)`,
	`
CREATE INDEX last_checked_idx_database_instance ON database_instance(last_checked)
	`,
	`
CREATE INDEX last_seen_idx_database_instance ON database_instance(last_seen)
	`,
	`
DROP TABLE IF EXISTS database_instance_maintenance
`,
	`
CREATE TABLE database_instance_maintenance (
	database_instance_maintenance_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	maintenance_active tinyint(4) DEFAULT NULL,
	begin_timestamp timestamp NULL DEFAULT NULL,
	end_timestamp timestamp NULL DEFAULT NULL,
	owner varchar(128) NOT NULL,
	reason text NOT NULL,
	processing_node_hostname varchar(128) not null default '',
	processing_node_token varchar(128) not null default '',
	explicitly_bounded TINYint not null default 0,
	PRIMARY KEY (database_instance_maintenance_id)
)`,
	`
CREATE UNIQUE INDEX maintenance_uidx_database_instance_maintenance ON database_instance_maintenance (maintenance_active, hostname, port)
	`,
	`
DROP TABLE IF EXISTS database_instance_long_running_queries
`,
	`
CREATE TABLE database_instance_long_running_queries (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	process_id bigint(20) NOT NULL,
	process_started_at timestamp not null default (''),
	process_user varchar(16) NOT NULL,
	process_host varchar(128) NOT NULL,
	process_db varchar(128) NOT NULL,
	process_command varchar(16) NOT NULL,
	process_time_seconds int(11) NOT NULL,
	process_state varchar(128) NOT NULL,
	process_info varchar(1024) NOT NULL,
	PRIMARY KEY (hostname,port,process_id)
)`,
	`
CREATE INDEX process_started_at_idx_database_instance_long_running_queries ON database_instance_long_running_queries (process_started_at)
	`,
	`
DROP TABLE IF EXISTS audit
`,
	`
CREATE TABLE audit (
	audit_id integer,
	audit_timestamp timestamp not null default (''),
	audit_type varchar(128) NOT NULL,
	hostname varchar(128) NOT NULL DEFAULT '',
	port smallint NOT NULL,
	message text NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	PRIMARY KEY (audit_id)
)`,
	`
CREATE INDEX audit_timestamp_idx_audit ON audit (audit_timestamp)
	`,
	`
CREATE INDEX host_port_idx_audit ON audit (hostname, port, audit_timestamp)
	`,
	`
DROP TABLE IF EXISTS host_agent
`,
	`
CREATE TABLE host_agent (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	token varchar(128) NOT NULL,
	last_submitted timestamp not null default (''),
	last_checked timestamp NULL DEFAULT NULL,
	last_seen timestamp NULL DEFAULT NULL,
	mysql_port smallint DEFAULT NULL,
	count_mysql_snapshots smallint NOT NULL,
	PRIMARY KEY (hostname)
)`,
	`
CREATE INDEX token_idx_host_agent ON host_agent (token)
	`,
	`
CREATE INDEX last_submitted_idx_host_agent ON host_agent (last_submitted)
	`,
	`
CREATE INDEX last_checked_idx_host_agent ON host_agent (last_checked)
	`,
	`
CREATE INDEX last_seen_idx_host_agent ON host_agent (last_seen)
	`,
	`
DROP TABLE IF EXISTS agent_seed
`,
	`
CREATE TABLE agent_seed (
	agent_seed_id integer,
	target_hostname varchar(128) NOT NULL,
	source_hostname varchar(128) NOT NULL,
	start_timestamp timestamp not null default (''),
	end_timestamp timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	is_complete tinyint NOT NULL DEFAULT '0',
	is_successful tinyint NOT NULL DEFAULT '0',
	PRIMARY KEY (agent_seed_id)
)`,
	`
CREATE INDEX target_hostname_idx_agent_seed ON agent_seed (target_hostname,is_complete)
	`,
	`
CREATE INDEX source_hostname_idx_agent_seed ON agent_seed (source_hostname,is_complete)
	`,
	`
CREATE INDEX start_timestamp_idx_agent_seed ON agent_seed (start_timestamp)
	`,
	`
CREATE INDEX is_complete_idx_agent_seed ON agent_seed (is_complete,start_timestamp)
	`,
	`
CREATE INDEX is_successful_idx_agent_seed ON agent_seed (is_successful, start_timestamp)
	`,
	`
DROP TABLE IF EXISTS agent_seed_state
`,
	`
CREATE TABLE agent_seed_state (
	agent_seed_state_id integer,
	agent_seed_id int NOT NULL,
	state_timestamp timestamp not null default (''),
	state_action varchar(127) NOT NULL,
	error_message varchar(255) NOT NULL,
	PRIMARY KEY (agent_seed_state_id)
)`,
	`
CREATE INDEX agent_seed_idx_agent_seed_state ON agent_seed_state (agent_seed_id, state_timestamp)
	`,
	`
DROP TABLE IF EXISTS hostname_resolve
`,
	`
CREATE TABLE hostname_resolve (
	hostname varchar(128) NOT NULL,
	resolved_hostname varchar(128) NOT NULL,
	resolved_timestamp timestamp not null default (''),
	PRIMARY KEY (hostname)
)`,
	`
CREATE INDEX resolved_timestamp_idx_hostname_resolve ON hostname_resolve (resolved_timestamp)
	`,
	`
DROP TABLE IF EXISTS active_node
`,
	`
CREATE TABLE active_node (
	anchor tinyint NOT NULL,
	hostname varchar(128) NOT NULL,
	token varchar(128) NOT NULL,
	last_seen_active timestamp not null default (''),
	first_seen_active timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	PRIMARY KEY (anchor)
)`,
	`
DROP TABLE IF EXISTS node_health
`,
	`
CREATE TABLE node_health (
	hostname varchar(128) NOT NULL,
	token varchar(128) NOT NULL,
	last_seen_active timestamp not null default (''),
	extra_info varchar(128) not null default '',
	command varchar(128) not null default '',
	app_version varchar(64) NOT NULL DEFAULT "",
	first_seen_active timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	db_backend varchar(255) NOT NULL DEFAULT "",
	incrementing_indicator bigint not null default 0,
	PRIMARY KEY (hostname, token)
)`,
	`
DROP TABLE IF EXISTS topology_recovery
`,
	`
CREATE TABLE topology_recovery (
	recovery_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	in_active_period tinyint NOT NULL DEFAULT 0,
	start_active_period timestamp not null default (''),
	end_active_period_unixtime int,
	end_recovery timestamp NULL DEFAULT NULL,
	processing_node_hostname varchar(128) NOT NULL,
	processcing_node_token varchar(128) NOT NULL,
	successor_hostname varchar(128) DEFAULT NULL,
	successor_port smallint DEFAULT NULL,
	analysis varchar(128) not null default '',
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	count_affected_replicas int not null default 0,
	is_successful TINYint NOT NULL DEFAULT 0,
	acknowledged TINYint NOT NULL DEFAULT 0,
	acknowledged_by varchar(128) not null default '',
	acknowledge_comment text not null default '',
	participating_instances text not null default '',
	lost_replicas text not null default '',
	all_errors text not null default '',
	acknowledged_at TIMESTAMP NULL,
	last_detection_id bigint not null default 0,
	successor_alias varchar(128) DEFAULT NULL,
	uid varchar(128) not null default '',
	PRIMARY KEY (recovery_id)
)`,
	`
CREATE INDEX in_active_start_period_idx_topology_recovery ON topology_recovery (in_active_period, start_active_period)
	`,
	`
CREATE INDEX start_active_period_idx_topology_recovery ON topology_recovery (start_active_period)
	`,
	`
CREATE UNIQUE INDEX hostname_port_active_period_uidx_topology_recovery ON topology_recovery (hostname, port, in_active_period, end_active_period_unixtime)
	`,
	`
DROP TABLE IF EXISTS hostname_unresolve
`,
	`
CREATE TABLE hostname_unresolve (
	hostname varchar(128) NOT NULL,
	unresolved_hostname varchar(128) NOT NULL,
	last_registered timestamp not null default (''),
	PRIMARY KEY (hostname)
)`,
	`
CREATE INDEX unresolved_hostname_idx_hostname_unresolve ON hostname_unresolve (unresolved_hostname)
	`,
	`
DROP TABLE IF EXISTS database_instance_topology_history
`,
	`
CREATE TABLE database_instance_topology_history (
	snapshot_unix_timestamp int NOT NULL,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	source_host varchar(128) NOT NULL,
	source_port smallint NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	version varchar(128) not null default '',
	PRIMARY KEY (snapshot_unix_timestamp, hostname, port)
)`,
	`
CREATE INDEX keyspace_shard_idx_database_instance_topology_history ON database_instance_topology_history (snapshot_unix_timestamp, keyspace, shard)
	`,
	`
DROP TABLE IF EXISTS candidate_database_instance
`,
	`
CREATE TABLE candidate_database_instance (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	last_suggested timestamp not null default (''),
	priority TINYINT SIGNED NOT NULL DEFAULT 1,
	promotion_rule text check(promotion_rule in ('must', 'prefer', 'neutral', 'prefer_not', 'must_not')) NOT NULL DEFAULT 'neutral',
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX last_suggested_idx_candidate_database_instance ON candidate_database_instance (last_suggested)
	`,
	`
DROP TABLE IF EXISTS database_instance_downtime
`,
	`
CREATE TABLE database_instance_downtime (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	downtime_active tinyint(4) DEFAULT NULL,
	begin_timestamp timestamp default (''),
	end_timestamp timestamp NULL DEFAULT NULL,
	owner varchar(128) NOT NULL,
	reason text NOT NULL,
	PRIMARY KEY (hostname, port)
)`,
	`
DROP TABLE IF EXISTS topology_failure_detection
`,
	`
CREATE TABLE topology_failure_detection (
	detection_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	in_active_period tinyint NOT NULL DEFAULT '0',
	start_active_period timestamp not null default (''),
	end_active_period_unixtime int NOT NULL,
	processing_node_hostname varchar(128) NOT NULL,
	processcing_node_token varchar(128) NOT NULL,
	analysis varchar(128) NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	count_affected_replicas int NOT NULL,
	is_actionable tinyint not null default 0,
	PRIMARY KEY (detection_id)
)`,
	`
CREATE INDEX in_active_start_period_idx_topology_failure_detection ON topology_failure_detection (in_active_period, start_active_period)
	`,
	`
DROP TABLE IF EXISTS hostname_resolve_history
`,
	`
CREATE TABLE hostname_resolve_history (
	resolved_hostname varchar(128) NOT NULL,
	hostname varchar(128) NOT NULL,
	resolved_timestamp timestamp not null default (''),
	PRIMARY KEY (resolved_hostname)
)`,
	`
CREATE INDEX hostname_idx_hostname_resolve_history ON hostname_resolve_history (hostname)
	`,
	`
CREATE INDEX resolved_timestamp_idx_hostname_resolve_history ON hostname_resolve_history (resolved_timestamp)
	`,
	`
DROP TABLE IF EXISTS hostname_unresolve_history
`,
	`
CREATE TABLE hostname_unresolve_history (
	unresolved_hostname varchar(128) NOT NULL,
	hostname varchar(128) NOT NULL,
	last_registered timestamp not null default (''),
	PRIMARY KEY (unresolved_hostname)
)`,
	`
CREATE INDEX hostname_idx_hostname_unresolve_history ON hostname_unresolve_history (hostname)
	`,
	`
CREATE INDEX last_registered_idx_hostname_unresolve_history ON hostname_unresolve_history (last_registered)
	`,
	`
DROP TABLE IF EXISTS primary_position_equivalence
`,
	`
CREATE TABLE primary_position_equivalence (
	equivalence_id integer,
	primary1_hostname varchar(128) NOT NULL,
	primary1_port smallint NOT NULL,
	primary1_binary_log_file varchar(128) NOT NULL,
	primary1_binary_log_pos bigint NOT NULL,
	primary2_hostname varchar(128) NOT NULL,
	primary2_port smallint NOT NULL,
	primary2_binary_log_file varchar(128) NOT NULL,
	primary2_binary_log_pos bigint NOT NULL,
	last_suggested timestamp not null default (''),
	PRIMARY KEY (equivalence_id)
)`,
	`
CREATE UNIQUE INDEX equivalence_uidx_primary_position_equivalence ON primary_position_equivalence (primary1_hostname, primary1_port, primary1_binary_log_file, primary1_binary_log_pos, primary2_hostname, primary2_port)
	`,
	`
CREATE INDEX primary2_idx_primary_position_equivalence ON primary_position_equivalence (primary2_hostname, primary2_port, primary2_binary_log_file, primary2_binary_log_pos)
	`,
	`
CREATE INDEX last_suggested_idx_primary_position_equivalence ON primary_position_equivalence (last_suggested)
	`,
	`
DROP TABLE IF EXISTS async_request
`,
	`
CREATE TABLE async_request (
	request_id integer,
	command varchar(128) not null,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	destination_hostname varchar(128) NOT NULL,
	destination_port smallint NOT NULL,
	pattern text NOT NULL,
	gtid_hint varchar(32) not null,
	begin_timestamp timestamp NULL DEFAULT NULL,
	end_timestamp timestamp NULL DEFAULT NULL,
	story text NOT NULL,
	PRIMARY KEY (request_id)
)`,
	`
CREATE INDEX begin_timestamp_idx_async_request ON async_request (begin_timestamp)
	`,
	`
CREATE INDEX end_timestamp_idx_async_request ON async_request (end_timestamp)
	`,
	`
DROP TABLE IF EXISTS blocked_topology_recovery
`,
	`
CREATE TABLE blocked_topology_recovery (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	analysis varchar(128) NOT NULL,
	last_blocked_timestamp timestamp not null default (''),
	blocking_recovery_id bigint,
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX keyspace_shard_blocked_idx_blocked_topology_recovery ON blocked_topology_recovery (keyspace, shard, last_blocked_timestamp)
	`,
	`
DROP TABLE IF EXISTS database_instance_last_analysis
`,
	`
CREATE TABLE database_instance_last_analysis (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	analysis_timestamp timestamp not null default (''),
	analysis varchar(128) NOT NULL,
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX analysis_timestamp_idx_database_instance_last_analysis ON database_instance_last_analysis (analysis_timestamp)
	`,
	`
DROP TABLE IF EXISTS database_instance_analysis_changelog
`,
	`
CREATE TABLE database_instance_analysis_changelog (
	changelog_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	analysis_timestamp timestamp not null default (''),
	analysis varchar(128) NOT NULL,
	PRIMARY KEY (changelog_id)
)`,
	`
CREATE INDEX analysis_timestamp_idx_database_instance_analysis_changelog ON database_instance_analysis_changelog (analysis_timestamp)
	`,
	`
DROP TABLE IF EXISTS node_health_history
`,
	`
CREATE TABLE node_health_history (
	history_id integer,
	hostname varchar(128) NOT NULL,
	token varchar(128) NOT NULL,
	first_seen_active timestamp NOT NULL,
	extra_info varchar(128) NOT NULL,
	command varchar(128) not null default '',
	app_version varchar(64) NOT NULL DEFAULT "",
	PRIMARY KEY (history_id)
)`,
	`
CREATE INDEX first_seen_active_idx_node_health_history ON node_health_history (first_seen_active)
	`,
	`
CREATE UNIQUE INDEX hostname_token_idx_node_health_history ON node_health_history (hostname, token)
	`,
	`
DROP TABLE IF EXISTS database_instance_coordinates_history
`,
	`
CREATE TABLE database_instance_coordinates_history (
	history_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	recorded_timestamp timestamp not null default (''),
	binary_log_file varchar(128) NOT NULL,
	binary_log_pos bigint NOT NULL,
	relay_log_file varchar(128) NOT NULL,
	relay_log_pos bigint NOT NULL,
	last_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	PRIMARY KEY (history_id)
)`,
	`
CREATE INDEX hostname_port_recorded_idx_database_instance_coordinates_history ON database_instance_coordinates_history (hostname, port, recorded_timestamp)
	`,
	`
CREATE INDEX recorded_timestmp_idx_database_instance_coordinates_history ON database_instance_coordinates_history (recorded_timestamp)
	`,
	`
DROP TABLE IF EXISTS database_instance_binlog_files_history
`,
	`
CREATE TABLE database_instance_binlog_files_history (
	history_id integer,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	binary_log_file varchar(128) NOT NULL,
	binary_log_pos bigint NOT NULL,
	first_seen timestamp not null default (''),
	last_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	PRIMARY KEY (history_id)
)`,
	`
CREATE UNIQUE INDEX hostname_port_file_idx_database_instance_binlog_files_history ON database_instance_binlog_files_history (hostname, port, binary_log_file)
	`,
	`
CREATE INDEX last_seen_idx_database_instance_binlog_files_history ON database_instance_binlog_files_history (last_seen)
	`,
	`
DROP TABLE IF EXISTS database_instance_recent_relaylog_history
`,
	`
CREATE TABLE database_instance_recent_relaylog_history (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	current_relay_log_file varchar(128) NOT NULL,
	current_relay_log_pos bigint NOT NULL,
	current_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	prev_relay_log_file varchar(128) NOT NULL,
	prev_relay_log_pos bigint NOT NULL,
	prev_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX current_seen_idx_database_instance_recent_relaylog_history ON database_instance_recent_relaylog_history (current_seen)
	`,
	`
DROP TABLE IF EXISTS vtorc_metadata
`,
	`
CREATE TABLE vtorc_metadata (
	anchor tinyint NOT NULL,
	last_deployed_version varchar(128) NOT NULL,
	last_deployed_timestamp timestamp NOT NULL,
	PRIMARY KEY (anchor)
)`,
	`
DROP TABLE IF EXISTS vtorc_db_deployments
`,
	`
CREATE TABLE vtorc_db_deployments (
	deployed_version varchar(128) NOT NULL,
	deployed_timestamp timestamp NOT NULL,
	PRIMARY KEY (deployed_version)
)`,
	`
DROP TABLE IF EXISTS global_recovery_disable
`,
	`
CREATE TABLE global_recovery_disable (
	disable_recovery tinyint NOT NULL ,
	PRIMARY KEY (disable_recovery)
)`,
	`
DROP TABLE IF EXISTS topology_recovery_steps
`,
	`
CREATE TABLE topology_recovery_steps (
	recovery_step_id integer,
	recovery_uid varchar(128) NOT NULL,
	audit_at timestamp not null default (''),
	message text NOT NULL,
	PRIMARY KEY (recovery_step_id)
)`,
	`
DROP TABLE IF EXISTS raft_store
`,
	`
CREATE TABLE raft_store (
	store_id integer,
	store_key varbinary(512) not null,
	store_value blob not null,
	PRIMARY KEY (store_id)
)`,
	`
CREATE INDEX store_key_idx_raft_store ON raft_store (store_key)
	`,
	`
DROP TABLE IF EXISTS raft_log
`,
	`
CREATE TABLE raft_log (
	log_index integer,
	term bigint not null,
	log_type int not null,
	data blob not null,
	PRIMARY KEY (log_index)
)`,
	`
DROP TABLE IF EXISTS raft_snapshot
`,
	`
CREATE TABLE raft_snapshot (
	snapshot_id integer,
	snapshot_name varchar(128) NOT NULL,
	snapshot_meta varchar(4096) NOT NULL,
	created_at timestamp not null default (''),
	PRIMARY KEY (snapshot_id)
)`,
	`
CREATE UNIQUE INDEX snapshot_name_uidx_raft_snapshot ON raft_snapshot (snapshot_name)
	`,
	`
DROP TABLE IF EXISTS database_instance_peer_analysis
`,
	`
CREATE TABLE database_instance_peer_analysis (
	peer varchar(128) NOT NULL,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	analysis_timestamp timestamp not null default (''),
	analysis varchar(128) NOT NULL,
	PRIMARY KEY (peer, hostname, port)
)`,
	`
DROP TABLE IF EXISTS database_instance_tls
`,
	`
CREATE TABLE database_instance_tls (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	required tinyint NOT NULL DEFAULT 0,
	PRIMARY KEY (hostname,port)
)`,
	`
DROP TABLE IF EXISTS hostname_ips
`,
	`
CREATE TABLE hostname_ips (
	hostname varchar(128) NOT NULL,
	ipv4 varchar(128) NOT NULL,
	ipv6 varchar(128) NOT NULL,
	last_updated timestamp not null default (''),
	PRIMARY KEY (hostname)
)`,
	`
DROP TABLE IF EXISTS database_instance_tags
`,
	`
CREATE TABLE database_instance_tags (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	tag_name varchar(128) NOT NULL,
	tag_value varchar(128) NOT NULL,
	last_updated timestamp not null default (''),
	PRIMARY KEY (hostname, port, tag_name)
)`,
	`
CREATE INDEX tag_name_idx_database_instance_tags ON database_instance_tags (tag_name)
	`,
	`
DROP TABLE IF EXISTS database_instance_stale_binlog_coordinates
`,
	`
CREATE TABLE database_instance_stale_binlog_coordinates (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	binary_log_file varchar(128) NOT NULL,
	binary_log_pos bigint NOT NULL,
	first_seen timestamp not null default (''),
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX first_seen_idx_database_instance_stale_binlog_coordinates ON database_instance_stale_binlog_coordinates (first_seen)
	`,
	`
DROP TABLE IF EXISTS vitess_tablet
`,
	`
CREATE TABLE vitess_tablet (
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	cell varchar(128) NOT NULL,
	tablet_type smallint(5) NOT NULL,
	primary_timestamp timestamp NOT NULL,
	info varchar(512) NOT NULL,
	PRIMARY KEY (hostname, port)
)`,
	`
CREATE INDEX cell_idx_vitess_tablet ON vitess_tablet (cell)
	`,
	`
CREATE INDEX ks_idx_vitess_tablet ON vitess_tablet (keyspace, shard)
	`,
	`
DROP TABLE IF EXISTS vitess_keyspace
`,
	`
CREATE TABLE vitess_keyspace (
	keyspace varchar(128) NOT NULL,
	keyspace_type smallint(5) NOT NULL,
	durability_policy varchar(512) NOT NULL,
	PRIMARY KEY (keyspace)
)`,
	`
CREATE INDEX source_host_port_idx_database_instance_database_instance on database_instance (source_host, source_port)
	`,
	`
CREATE INDEX active_timestamp_idx_database_instance_maintenance on database_instance_maintenance (maintenance_active, begin_timestamp)
	`,
	`
CREATE INDEX active_end_timestamp_idx_database_instance_maintenance on database_instance_maintenance (maintenance_active, end_timestamp)
	`,
	`
CREATE INDEX last_registered_idx_hostname_unresolve on hostname_unresolve (last_registered)
	`,
	`
CREATE INDEX keyspace_shard_in_active_idx_topology_recovery on topology_recovery (keyspace, shard, in_active_period)
	`,
	`
CREATE INDEX end_recovery_idx_topology_recovery on topology_recovery (end_recovery)
	`,
	`
CREATE INDEX acknowledged_idx_topology_recovery on topology_recovery (acknowledged, acknowledged_at)
	`,
	`
CREATE INDEX last_blocked_idx_blocked_topology_recovery on blocked_topology_recovery (last_blocked_timestamp)
	`,
	`
CREATE INDEX instance_timestamp_idx_database_instance_analysis_changelog on database_instance_analysis_changelog (hostname, port, analysis_timestamp)
	`,
	`
CREATE INDEX last_detection_idx_topology_recovery on topology_recovery (last_detection_id)
	`,
	`
CREATE INDEX last_seen_active_idx_node_health on node_health (last_seen_active)
	`,
	`
CREATE INDEX uid_idx_topology_recovery ON topology_recovery(uid)
	`,
	`
CREATE INDEX recovery_uid_idx_topology_recovery_steps ON topology_recovery_steps(recovery_uid)
	`,
	`
CREATE INDEX end_timestamp_idx_database_instance_downtime ON database_instance_downtime(end_timestamp)
	`,
	`
CREATE UNIQUE INDEX host_port_active_recoverable_uidx_topology_failure_detection ON topology_failure_detection (hostname, port, in_active_period, end_active_period_unixtime, is_actionable)
	`,
}
