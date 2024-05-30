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

var TableNames = []string{
	"database_instance",
	"audit",
	"node_health",
	"topology_recovery",
	"database_instance_topology_history",
	"recovery_detection",
	"database_instance_last_analysis",
	"database_instance_analysis_changelog",
	"vtorc_db_deployments",
	"global_recovery_disable",
	"topology_recovery_steps",
	"database_instance_stale_binlog_coordinates",
	"vitess_tablet",
	"vitess_keyspace",
	"vitess_shard",
}

// vtorcBackend is a list of SQL statements required to build the vtorc backend
var vtorcBackend = []string{
	`
DROP TABLE IF EXISTS database_instance
`,
	`
CREATE TABLE database_instance (
	alias varchar(256) NOT NULL,
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
	replica_net_timeout int NOT NULL,
	heartbeat_interval decimal(11,4) NOT NULL,
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
	PRIMARY KEY (alias)
)`,
	`
CREATE INDEX last_checked_idx_database_instance ON database_instance(last_checked)
	`,
	`
CREATE INDEX last_seen_idx_database_instance ON database_instance(last_seen)
	`,
	`
DROP TABLE IF EXISTS audit
`,
	`
CREATE TABLE audit (
	audit_id integer,
	audit_timestamp timestamp not null default (''),
	audit_type varchar(128) NOT NULL,
	alias varchar(256) NOT NULL,
	message text NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	PRIMARY KEY (audit_id)
)`,
	`
CREATE INDEX audit_timestamp_idx_audit ON audit (audit_timestamp)
	`,
	`
CREATE INDEX alias_idx_audit ON audit (alias, audit_timestamp)
	`,
	`
DROP TABLE IF EXISTS node_health
`,
	`
CREATE TABLE node_health (
	last_seen_active timestamp not null default ('')
)`,
	`
DROP TABLE IF EXISTS topology_recovery
`,
	`
CREATE TABLE topology_recovery (
	recovery_id integer,
	alias varchar(256) NOT NULL,
	start_recovery timestamp NOT NULL DEFAULT (''),
	end_recovery timestamp NULL DEFAULT NULL,
	successor_alias varchar(256) DEFAULT NULL,
	analysis varchar(128) not null default '',
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	is_successful TINYint NOT NULL DEFAULT 0,
	all_errors text not null default '',
	detection_id bigint not null default 0,
	PRIMARY KEY (recovery_id)
)`,
	`
CREATE INDEX start_recovery_idx_topology_recovery ON topology_recovery (start_recovery)
	`,
	`
DROP TABLE IF EXISTS database_instance_topology_history
`,
	`
CREATE TABLE database_instance_topology_history (
	snapshot_unix_timestamp int NOT NULL,
	alias varchar(256) NOT NULL,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	source_host varchar(128) NOT NULL,
	source_port smallint NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	version varchar(128) not null default '',
	PRIMARY KEY (snapshot_unix_timestamp, alias)
)`,
	`
CREATE INDEX keyspace_shard_idx_database_instance_topology_history ON database_instance_topology_history (snapshot_unix_timestamp, keyspace, shard)
	`,
	`
DROP TABLE IF EXISTS recovery_detection
`,
	`
CREATE TABLE recovery_detection (
	detection_id integer,
	alias varchar(256) NOT NULL,
	analysis varchar(128) NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	detection_timestamp timestamp NOT NULL default (''),
	PRIMARY KEY (detection_id)
)`,
	`
DROP TABLE IF EXISTS database_instance_last_analysis
`,
	`
CREATE TABLE database_instance_last_analysis (
	alias varchar(256) NOT NULL,
	analysis_timestamp timestamp not null default (''),
	analysis varchar(128) NOT NULL,
	PRIMARY KEY (alias)
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
	alias varchar(256) NOT NULL,
	analysis_timestamp timestamp not null default (''),
	analysis varchar(128) NOT NULL,
	PRIMARY KEY (changelog_id)
)`,
	`
CREATE INDEX analysis_timestamp_idx_database_instance_analysis_changelog ON database_instance_analysis_changelog (analysis_timestamp)
	`,
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
	recovery_id integer NOT NULL,
	audit_at timestamp not null default (''),
	message text NOT NULL,
	PRIMARY KEY (recovery_step_id)
)`,
	`
DROP TABLE IF EXISTS database_instance_stale_binlog_coordinates
`,
	`
CREATE TABLE database_instance_stale_binlog_coordinates (
	alias varchar(256) NOT NULL,
	binary_log_file varchar(128) NOT NULL,
	binary_log_pos bigint NOT NULL,
	first_seen timestamp not null default (''),
	PRIMARY KEY (alias)
)`,
	`
CREATE INDEX first_seen_idx_database_instance_stale_binlog_coordinates ON database_instance_stale_binlog_coordinates (first_seen)
	`,
	`
DROP TABLE IF EXISTS vitess_tablet
`,
	`
CREATE TABLE vitess_tablet (
	alias varchar(256) NOT NULL,
	hostname varchar(128) NOT NULL,
	port smallint NOT NULL,
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	cell varchar(128) NOT NULL,
	tablet_type smallint(5) NOT NULL,
	primary_timestamp timestamp NOT NULL,
	info varchar(512) NOT NULL,
	PRIMARY KEY (alias)
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
DROP TABLE IF EXISTS vitess_shard
`,
	`
CREATE TABLE vitess_shard (
	keyspace varchar(128) NOT NULL,
	shard varchar(128) NOT NULL,
	primary_alias varchar(512) NOT NULL,
	primary_timestamp varchar(512) NOT NULL,
	PRIMARY KEY (keyspace, shard)
)`,
	`
CREATE INDEX source_host_port_idx_database_instance_database_instance on database_instance (source_host, source_port)
	`,
	`
CREATE INDEX keyspace_shard_idx_topology_recovery on topology_recovery (keyspace, shard)
	`,
	`
CREATE INDEX end_recovery_idx_topology_recovery on topology_recovery (end_recovery)
	`,
	`
CREATE INDEX instance_timestamp_idx_database_instance_analysis_changelog on database_instance_analysis_changelog (alias, analysis_timestamp)
	`,
	`
CREATE INDEX detection_idx_topology_recovery on topology_recovery (detection_id)
	`,
	`
CREATE INDEX recovery_id_idx_topology_recovery_steps ON topology_recovery_steps(recovery_id)
	`,
}
