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

// generateSQLBase & generateSQLPatches are lists of SQL statements required to build the orchestrator backend
var generateSQLBase = []string{
	`
        CREATE TABLE IF NOT EXISTS database_instance (
          hostname varchar(128) CHARACTER SET ascii NOT NULL,
          port smallint(5) unsigned NOT NULL,
          last_checked timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          last_seen timestamp NULL DEFAULT NULL,
          server_id int(10) unsigned NOT NULL,
          version varchar(128) CHARACTER SET ascii NOT NULL,
          binlog_format varchar(16) CHARACTER SET ascii NOT NULL,
          log_bin tinyint(3) unsigned NOT NULL,
          log_slave_updates tinyint(3) unsigned NOT NULL,
          binary_log_file varchar(128) CHARACTER SET ascii NOT NULL,
          binary_log_pos bigint(20) unsigned NOT NULL,
          master_host varchar(128) CHARACTER SET ascii NOT NULL,
          master_port smallint(5) unsigned NOT NULL,
          slave_sql_running tinyint(3) unsigned NOT NULL,
          slave_io_running tinyint(3) unsigned NOT NULL,
          master_log_file varchar(128) CHARACTER SET ascii NOT NULL,
          read_master_log_pos bigint(20) unsigned NOT NULL,
          relay_master_log_file varchar(128) CHARACTER SET ascii NOT NULL,
          exec_master_log_pos bigint(20) unsigned NOT NULL,
          seconds_behind_master bigint(20) unsigned DEFAULT NULL,
          slave_lag_seconds bigint(20) unsigned DEFAULT NULL,
          num_slave_hosts int(10) unsigned NOT NULL,
          slave_hosts text CHARACTER SET ascii NOT NULL,
          cluster_name varchar(128) CHARACTER SET ascii NOT NULL,
          PRIMARY KEY (hostname,port)
        ) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX cluster_name_idx ON database_instance
	`,
	`
				CREATE INDEX cluster_name_idx_database_instance ON database_instance(cluster_name)
	`,
	`
				DROP INDEX last_checked_idx ON database_instance
	`,
	`
				CREATE INDEX last_checked_idx_database_instance ON database_instance(last_checked)
	`,
	`
				DROP INDEX last_seen_idx ON database_instance
	`,
	`
				CREATE INDEX last_seen_idx_database_instance ON database_instance(last_seen)
	`,
	`
        CREATE TABLE IF NOT EXISTS database_instance_maintenance (
          database_instance_maintenance_id int(10) unsigned NOT NULL AUTO_INCREMENT,
          hostname varchar(128) NOT NULL,
          port smallint(5) unsigned NOT NULL,
          maintenance_active tinyint(4) DEFAULT NULL,
          begin_timestamp timestamp NULL DEFAULT NULL,
          end_timestamp timestamp NULL DEFAULT NULL,
          owner varchar(128) CHARACTER SET utf8 NOT NULL,
          reason text CHARACTER SET utf8 NOT NULL,
          PRIMARY KEY (database_instance_maintenance_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX maintenance_uidx ON database_instance_maintenance
	`,
	`
				CREATE UNIQUE INDEX maintenance_uidx_database_instance_maintenance ON database_instance_maintenance (maintenance_active, hostname, port)
	`,
	`
        CREATE TABLE IF NOT EXISTS database_instance_long_running_queries (
          hostname varchar(128) NOT NULL,
          port smallint(5) unsigned NOT NULL,
          process_id bigint(20) NOT NULL,
          process_started_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          process_user varchar(16) CHARACTER SET utf8 NOT NULL,
          process_host varchar(128) CHARACTER SET utf8 NOT NULL,
          process_db varchar(128) CHARACTER SET utf8 NOT NULL,
          process_command varchar(16) CHARACTER SET utf8 NOT NULL,
          process_time_seconds int(11) NOT NULL,
          process_state varchar(128) CHARACTER SET utf8 NOT NULL,
          process_info varchar(1024) CHARACTER SET utf8 NOT NULL,
          PRIMARY KEY (hostname,port,process_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX process_started_at_idx ON database_instance_long_running_queries
	`,
	`
				CREATE INDEX process_started_at_idx_database_instance_long_running_queries ON database_instance_long_running_queries (process_started_at)
	`,
	`
        CREATE TABLE IF NOT EXISTS audit (
          audit_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          audit_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          audit_type varchar(128) CHARACTER SET ascii NOT NULL,
          hostname varchar(128) CHARACTER SET ascii NOT NULL DEFAULT '',
          port smallint(5) unsigned NOT NULL,
          message text CHARACTER SET utf8 NOT NULL,
          PRIMARY KEY (audit_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
	`,
	`
				DROP INDEX audit_timestamp_idx ON audit
	`,
	`
				CREATE INDEX audit_timestamp_idx_audit ON audit (audit_timestamp)
	`,
	`
				DROP INDEX host_port_idx ON audit
	`,
	`
				CREATE INDEX host_port_idx_audit ON audit (hostname, port, audit_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS host_agent (
		  hostname varchar(128) NOT NULL,
		  port smallint(5) unsigned NOT NULL,
		  token varchar(128) NOT NULL,
		  last_submitted timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  last_checked timestamp NULL DEFAULT NULL,
		  last_seen timestamp NULL DEFAULT NULL,
		  mysql_port smallint(5) unsigned DEFAULT NULL,
		  count_mysql_snapshots smallint(5) unsigned NOT NULL,
		  PRIMARY KEY (hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX token_idx ON host_agent
	`,
	`
				CREATE INDEX token_idx_host_agent ON host_agent (token)
	`,
	`
				DROP INDEX last_submitted_idx ON host_agent
	`,
	`
				CREATE INDEX last_submitted_idx_host_agent ON host_agent (last_submitted)
	`,
	`
				DROP INDEX last_checked_idx ON host_agent
	`,
	`
				CREATE INDEX last_checked_idx_host_agent ON host_agent (last_checked)
	`,
	`
				DROP INDEX last_seen_idx ON host_agent
	`,
	`
				CREATE INDEX last_seen_idx_host_agent ON host_agent (last_seen)
	`,
	`
		CREATE TABLE IF NOT EXISTS agent_seed (
		  agent_seed_id int(10) unsigned NOT NULL AUTO_INCREMENT,
		  target_hostname varchar(128) NOT NULL,
		  source_hostname varchar(128) NOT NULL,
		  start_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  end_timestamp timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
		  is_complete tinyint(3) unsigned NOT NULL DEFAULT '0',
		  is_successful tinyint(3) unsigned NOT NULL DEFAULT '0',
		  PRIMARY KEY (agent_seed_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX target_hostname_idx ON agent_seed
	`,
	`
				CREATE INDEX target_hostname_idx_agent_seed ON agent_seed (target_hostname,is_complete)
	`,
	`
				DROP INDEX source_hostname_idx ON agent_seed
	`,
	`
				CREATE INDEX source_hostname_idx_agent_seed ON agent_seed (source_hostname,is_complete)
	`,
	`
				DROP INDEX start_timestamp_idx ON agent_seed
	`,
	`
				CREATE INDEX start_timestamp_idx_agent_seed ON agent_seed (start_timestamp)
	`,
	`
				DROP INDEX is_complete_idx ON agent_seed
	`,
	`
				CREATE INDEX is_complete_idx_agent_seed ON agent_seed (is_complete,start_timestamp)
	`,
	`
				DROP INDEX is_successful_idx ON agent_seed
	`,
	`
				CREATE INDEX is_successful_idx_agent_seed ON agent_seed (is_successful, start_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS agent_seed_state (
		  agent_seed_state_id int(10) unsigned NOT NULL AUTO_INCREMENT,
		  agent_seed_id int(10) unsigned NOT NULL,
		  state_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  state_action varchar(127) NOT NULL,
		  error_message varchar(255) NOT NULL,
		  PRIMARY KEY (agent_seed_state_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
				DROP INDEX agent_seed_idx ON agent_seed_state
	`,
	`
				CREATE INDEX agent_seed_idx_agent_seed_state ON agent_seed_state (agent_seed_id, state_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS host_attributes (
		  hostname varchar(128) NOT NULL,
		  attribute_name varchar(128) NOT NULL,
		  attribute_value varchar(128) NOT NULL,
		  submit_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  expire_timestamp timestamp NULL DEFAULT NULL,
		  PRIMARY KEY (hostname,attribute_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX attribute_name_idx ON host_attributes
	`,
	`
		CREATE INDEX attribute_name_idx_host_attributes ON host_attributes (attribute_name)
	`,
	`
		DROP INDEX attribute_value_idx ON host_attributes
	`,
	`
		CREATE INDEX attribute_value_idx_host_attributes ON host_attributes (attribute_value)
	`,
	`
		DROP INDEX submit_timestamp_idx ON host_attributes
	`,
	`
		CREATE INDEX submit_timestamp_idx_host_attributes ON host_attributes (submit_timestamp)
	`,
	`
		DROP INDEX expire_timestamp_idx ON host_attributes
	`,
	`
		CREATE INDEX expire_timestamp_idx_host_attributes ON host_attributes (expire_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS hostname_resolve (
		  hostname varchar(128) NOT NULL,
		  resolved_hostname varchar(128) NOT NULL,
		  resolved_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  PRIMARY KEY (hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX resolved_timestamp_idx ON hostname_resolve
	`,
	`
		CREATE INDEX resolved_timestamp_idx_hostname_resolve ON hostname_resolve (resolved_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS cluster_alias (
		  cluster_name varchar(128) CHARACTER SET ascii NOT NULL,
		  alias varchar(128) NOT NULL,
		  PRIMARY KEY (cluster_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS active_node (
		  anchor tinyint unsigned NOT NULL,
		  hostname varchar(128) CHARACTER SET ascii NOT NULL,
		  token varchar(128) NOT NULL,
		  last_seen_active timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  PRIMARY KEY (anchor)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		INSERT IGNORE INTO active_node (anchor, hostname, token, last_seen_active)
			VALUES (1, '', '', NOW())
	`,
	`
		CREATE TABLE IF NOT EXISTS node_health (
		  hostname varchar(128) CHARACTER SET ascii NOT NULL,
		  token varchar(128) NOT NULL,
		  last_seen_active timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  PRIMARY KEY (hostname, token)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP VIEW IF EXISTS _whats_wrong
	`,
	`
		DROP VIEW IF EXISTS whats_wrong
	`,
	`
		DROP VIEW IF EXISTS whats_wrong_summary
	`,
	`
		CREATE TABLE IF NOT EXISTS topology_recovery (
			recovery_id bigint unsigned not null auto_increment,
			hostname varchar(128) NOT NULL,
			port smallint unsigned NOT NULL,
			in_active_period tinyint unsigned NOT NULL DEFAULT 0,
			start_active_period timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			end_active_period_unixtime int unsigned,
			end_recovery timestamp NULL DEFAULT NULL,
			processing_node_hostname varchar(128) CHARACTER SET ascii NOT NULL,
			processcing_node_token varchar(128) NOT NULL,
			successor_hostname varchar(128) DEFAULT NULL,
			successor_port smallint unsigned DEFAULT NULL,
			PRIMARY KEY (recovery_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX in_active_start_period_idx ON topology_recovery
	`,
	`
		CREATE INDEX in_active_start_period_idx_topology_recovery ON topology_recovery (in_active_period, start_active_period)
	`,
	`
		DROP INDEX start_active_period_idx ON topology_recovery
	`,
	`
		CREATE INDEX start_active_period_idx_topology_recovery ON topology_recovery (start_active_period)
	`,
	`
		DROP INDEX hostname_port_active_period_uidx ON topology_recovery
	`,
	`
		CREATE UNIQUE INDEX hostname_port_active_period_uidx_topology_recovery ON topology_recovery (hostname, port, in_active_period, end_active_period_unixtime)
	`,
	`
		CREATE TABLE IF NOT EXISTS hostname_unresolve (
		  hostname varchar(128) NOT NULL,
		  unresolved_hostname varchar(128) NOT NULL,
		  PRIMARY KEY (hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX unresolved_hostname_idx ON hostname_unresolve
	`,
	`
		CREATE INDEX unresolved_hostname_idx_hostname_unresolve ON hostname_unresolve (unresolved_hostname)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_pool (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			pool varchar(128) NOT NULL,
			PRIMARY KEY (hostname, port, pool)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX pool_idx ON database_instance_pool
	`,
	`
		CREATE INDEX pool_idx_database_instance_pool ON database_instance_pool (pool)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_topology_history (
			snapshot_unix_timestamp INT UNSIGNED NOT NULL,
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			master_host varchar(128) CHARACTER SET ascii NOT NULL,
			master_port smallint(5) unsigned NOT NULL,
			cluster_name tinytext CHARACTER SET ascii NOT NULL,
			PRIMARY KEY (snapshot_unix_timestamp, hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX cluster_name_idx ON database_instance_topology_history
	`,
	`
		CREATE INDEX cluster_name_idx_database_instance_topology_history ON database_instance_topology_history (snapshot_unix_timestamp, cluster_name(128))
	`,
	`
		CREATE TABLE IF NOT EXISTS candidate_database_instance (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			last_suggested TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX last_suggested_idx ON candidate_database_instance
	`,
	`
		CREATE INDEX last_suggested_idx_candidate_database_instance ON candidate_database_instance (last_suggested)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_downtime (
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			downtime_active tinyint(4) DEFAULT NULL,
			begin_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
			end_timestamp timestamp NULL DEFAULT NULL,
			owner varchar(128) CHARACTER SET utf8 NOT NULL,
			reason text CHARACTER SET utf8 NOT NULL,
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS topology_failure_detection (
			detection_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
			hostname varchar(128) NOT NULL,
			port smallint unsigned NOT NULL,
			in_active_period tinyint unsigned NOT NULL DEFAULT '0',
			start_active_period timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			end_active_period_unixtime int unsigned NOT NULL,
			processing_node_hostname varchar(128) NOT NULL,
			processcing_node_token varchar(128) NOT NULL,
			analysis varchar(128) NOT NULL,
			cluster_name varchar(128) NOT NULL,
			cluster_alias varchar(128) NOT NULL,
			count_affected_slaves int unsigned NOT NULL,
			slave_hosts text NOT NULL,
			PRIMARY KEY (detection_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX hostname_port_active_period_uidx ON topology_failure_detection
	`,
	`
		DROP INDEX in_active_start_period_idx ON topology_failure_detection
	`,
	`
		CREATE INDEX in_active_start_period_idx_topology_failure_detection ON topology_failure_detection (in_active_period, start_active_period)
	`,
	`
		CREATE TABLE IF NOT EXISTS hostname_resolve_history (
			resolved_hostname varchar(128) NOT NULL,
			hostname varchar(128) NOT NULL,
			resolved_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (resolved_hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX hostname ON hostname_resolve_history
	`,
	`
		CREATE INDEX hostname_idx_hostname_resolve_history ON hostname_resolve_history (hostname)
	`,
	`
		DROP INDEX resolved_timestamp_idx ON hostname_resolve_history
	`,
	`
		CREATE INDEX resolved_timestamp_idx_hostname_resolve_history ON hostname_resolve_history (resolved_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS hostname_unresolve_history (
			unresolved_hostname varchar(128) NOT NULL,
			hostname varchar(128) NOT NULL,
			last_registered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (unresolved_hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX hostname ON hostname_unresolve_history
	`,
	`
		CREATE INDEX hostname_idx_hostname_unresolve_history ON hostname_unresolve_history (hostname)
	`,
	`
		DROP INDEX last_registered_idx ON hostname_unresolve_history
	`,
	`
		CREATE INDEX last_registered_idx_hostname_unresolve_history ON hostname_unresolve_history (last_registered)
	`,
	`
		CREATE TABLE IF NOT EXISTS cluster_domain_name (
			cluster_name varchar(128) CHARACTER SET ascii NOT NULL,
			domain_name varchar(128) NOT NULL,
			PRIMARY KEY (cluster_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX domain_name_idx ON cluster_domain_name
	`,
	`
		CREATE INDEX domain_name_idx_cluster_domain_name ON cluster_domain_name (domain_name(32))
	`,
	`
		CREATE TABLE IF NOT EXISTS master_position_equivalence (
			equivalence_id bigint unsigned not null auto_increment,
			master1_hostname varchar(128) CHARACTER SET ascii NOT NULL,
			master1_port smallint(5) unsigned NOT NULL,
			master1_binary_log_file varchar(128) CHARACTER SET ascii NOT NULL,
			master1_binary_log_pos bigint(20) unsigned NOT NULL,
			master2_hostname varchar(128) CHARACTER SET ascii NOT NULL,
			master2_port smallint(5) unsigned NOT NULL,
			master2_binary_log_file varchar(128) CHARACTER SET ascii NOT NULL,
			master2_binary_log_pos bigint(20) unsigned NOT NULL,
			last_suggested TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (equivalence_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX equivalence_uidx ON master_position_equivalence
	`,
	`
		CREATE UNIQUE INDEX equivalence_uidx_master_position_equivalence ON master_position_equivalence (master1_hostname, master1_port, master1_binary_log_file, master1_binary_log_pos, master2_hostname, master2_port)
	`,
	`
		DROP INDEX master2_idx ON master_position_equivalence
	`,
	`
		CREATE INDEX master2_idx_master_position_equivalence ON master_position_equivalence (master2_hostname, master2_port, master2_binary_log_file, master2_binary_log_pos)
	`,
	`
		DROP INDEX last_suggested_idx ON master_position_equivalence
	`,
	`
		CREATE INDEX last_suggested_idx_master_position_equivalence ON master_position_equivalence (last_suggested)
	`,
	`
		CREATE TABLE IF NOT EXISTS async_request (
			request_id bigint unsigned NOT NULL AUTO_INCREMENT,
			command varchar(128) charset ascii not null,
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			destination_hostname varchar(128) NOT NULL,
			destination_port smallint(5) unsigned NOT NULL,
			pattern text CHARACTER SET utf8 NOT NULL,
			gtid_hint varchar(32) charset ascii not null,
			begin_timestamp timestamp NULL DEFAULT NULL,
			end_timestamp timestamp NULL DEFAULT NULL,
			story text CHARACTER SET utf8 NOT NULL,
			PRIMARY KEY (request_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX begin_timestamp_idx ON async_request
	`,
	`
		CREATE INDEX begin_timestamp_idx_async_request ON async_request (begin_timestamp)
	`,
	`
		DROP INDEX end_timestamp_idx ON async_request
	`,
	`
		CREATE INDEX end_timestamp_idx_async_request ON async_request (end_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS blocked_topology_recovery (
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			cluster_name varchar(128) NOT NULL,
			analysis varchar(128) NOT NULL,
			last_blocked_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			blocking_recovery_id bigint unsigned,
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX cluster_blocked_idx ON blocked_topology_recovery
	`,
	`
		CREATE INDEX cluster_blocked_idx_blocked_topology_recovery ON blocked_topology_recovery (cluster_name, last_blocked_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_last_analysis (
		  hostname varchar(128) NOT NULL,
		  port smallint(5) unsigned NOT NULL,
		  analysis_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  analysis varchar(128) NOT NULL,
		  PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX analysis_timestamp_idx ON database_instance_last_analysis
	`,
	`
		CREATE INDEX analysis_timestamp_idx_database_instance_last_analysis ON database_instance_last_analysis (analysis_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_analysis_changelog (
			changelog_id bigint unsigned not null auto_increment,
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			analysis_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			analysis varchar(128) NOT NULL,
			PRIMARY KEY (changelog_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX analysis_timestamp_idx ON database_instance_analysis_changelog
	`,
	`
		CREATE INDEX analysis_timestamp_idx_database_instance_analysis_changelog ON database_instance_analysis_changelog (analysis_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS node_health_history (
			history_id bigint unsigned not null auto_increment,
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			token varchar(128) NOT NULL,
			first_seen_active timestamp NOT NULL,
			extra_info varchar(128) CHARACTER SET utf8 NOT NULL,
			PRIMARY KEY (history_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX first_seen_active_idx ON node_health_history
	`,
	`
		CREATE INDEX first_seen_active_idx_node_health_history ON node_health_history (first_seen_active)
	`,
	`
		DROP INDEX hostname_token_idx ON node_health_history
	`,
	`
		CREATE UNIQUE INDEX hostname_token_idx_node_health_history ON node_health_history (hostname, token)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_coordinates_history (
			history_id bigint unsigned not null auto_increment,
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			recorded_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			binary_log_file varchar(128) NOT NULL,
			binary_log_pos bigint(20) unsigned NOT NULL,
			relay_log_file varchar(128) NOT NULL,
			relay_log_pos bigint(20) unsigned NOT NULL,
			PRIMARY KEY (history_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX hostname_port_recorded_timestmp_idx ON database_instance_coordinates_history
	`,
	`
		CREATE INDEX hostname_port_recorded_idx_database_instance_coordinates_history ON database_instance_coordinates_history (hostname, port, recorded_timestamp)
	`,
	`
		DROP INDEX recorded_timestmp_idx ON database_instance_coordinates_history
	`,
	`
		CREATE INDEX recorded_timestmp_idx_database_instance_coordinates_history ON database_instance_coordinates_history (recorded_timestamp)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_binlog_files_history (
			history_id bigint unsigned not null auto_increment,
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			binary_log_file varchar(128) NOT NULL,
			binary_log_pos bigint(20) unsigned NOT NULL,
			first_seen timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
			PRIMARY KEY (history_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX hostname_port_file_idx ON database_instance_binlog_files_history
	`,
	`
		CREATE UNIQUE INDEX hostname_port_file_idx_database_instance_binlog_files_history ON database_instance_binlog_files_history (hostname, port, binary_log_file)
	`,
	`
		DROP INDEX last_seen_idx ON database_instance_binlog_files_history
	`,
	`
		CREATE INDEX last_seen_idx_database_instance_binlog_files_history ON database_instance_binlog_files_history (last_seen)
	`,
	`
		CREATE TABLE IF NOT EXISTS access_token (
			access_token_id bigint unsigned not null auto_increment,
			public_token varchar(128) NOT NULL,
			secret_token varchar(128) NOT NULL,
			generated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			generated_by varchar(128) CHARACTER SET utf8 NOT NULL,
			is_acquired tinyint unsigned NOT NULL DEFAULT '0',
			PRIMARY KEY (access_token_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX public_token_idx ON access_token
	`,
	`
		CREATE UNIQUE INDEX public_token_uidx_access_token ON access_token (public_token)
	`,
	`
		DROP INDEX generated_at_idx ON access_token
	`,
	`
		CREATE INDEX generated_at_idx_access_token ON access_token (generated_at)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_recent_relaylog_history (
			hostname varchar(128) NOT NULL,
			port smallint(5) unsigned NOT NULL,
			current_relay_log_file varchar(128) NOT NULL,
			current_relay_log_pos bigint(20) unsigned NOT NULL,
			current_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
			prev_relay_log_file varchar(128) NOT NULL,
			prev_relay_log_pos bigint(20) unsigned NOT NULL,
			prev_seen timestamp NOT NULL DEFAULT '1971-01-01 00:00:00',
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		DROP INDEX current_seen_idx ON database_instance_recent_relaylog_history
	`,
	`
		CREATE INDEX current_seen_idx_database_instance_recent_relaylog_history ON database_instance_recent_relaylog_history (current_seen)
	`,
	`
		CREATE TABLE IF NOT EXISTS orchestrator_metadata (
			anchor tinyint unsigned NOT NULL,
			last_deployed_version varchar(128) CHARACTER SET ascii NOT NULL,
			last_deployed_timestamp timestamp NOT NULL,
			PRIMARY KEY (anchor)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS orchestrator_db_deployments (
			deployed_version varchar(128) CHARACTER SET ascii NOT NULL,
			deployed_timestamp timestamp NOT NULL,
			PRIMARY KEY (deployed_version)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS global_recovery_disable (
			disable_recovery tinyint unsigned NOT NULL COMMENT 'Insert 1 to disable recovery globally',
			PRIMARY KEY (disable_recovery)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS cluster_alias_override (
			cluster_name varchar(128) CHARACTER SET ascii NOT NULL,
			alias varchar(128) NOT NULL,
			PRIMARY KEY (cluster_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS topology_recovery_steps (
			recovery_step_id bigint unsigned not null auto_increment,
			recovery_uid varchar(128) CHARACTER SET ascii NOT NULL,
			audit_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			message text CHARACTER SET utf8 NOT NULL,
			PRIMARY KEY (recovery_step_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS raft_store (
			store_id bigint unsigned not null auto_increment,
			store_key varbinary(512) not null,
			store_value blob not null,
			PRIMARY KEY (store_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE INDEX store_key_idx_raft_store ON raft_store (store_key)
	`,
	`
		CREATE TABLE IF NOT EXISTS raft_log (
			log_index bigint unsigned not null auto_increment,
			term bigint not null,
			log_type int not null,
			data blob not null,
			PRIMARY KEY (log_index)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS raft_snapshot (
			snapshot_id bigint unsigned not null auto_increment,
			snapshot_name varchar(128) CHARACTER SET utf8 NOT NULL,
			snapshot_meta varchar(4096) CHARACTER SET utf8 NOT NULL,
			PRIMARY KEY (snapshot_id)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE UNIQUE INDEX snapshot_name_uidx_raft_snapshot ON raft_snapshot (snapshot_name)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_peer_analysis (
			peer varchar(128) NOT NULL,
		  hostname varchar(128) NOT NULL,
		  port smallint(5) unsigned NOT NULL,
		  analysis_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		  analysis varchar(128) NOT NULL,
		  PRIMARY KEY (peer, hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_tls (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			required tinyint unsigned NOT NULL DEFAULT 0,
			PRIMARY KEY (hostname,port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS kv_store (
			store_key varchar(255) CHARACTER SET ascii NOT NULL,
			store_value text CHARACTER SET utf8 not null,
			last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (store_key)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS cluster_injected_pseudo_gtid (
			cluster_name varchar(128) NOT NULL,
			time_injected timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (cluster_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS hostname_ips (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			ipv4 varchar(128) CHARACTER SET ascii NOT NULL,
			ipv6 varchar(128) CHARACTER SET ascii NOT NULL,
			last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (hostname)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_tags (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			tag_name varchar(128) CHARACTER SET utf8 NOT NULL,
			tag_value varchar(128) CHARACTER SET utf8 NOT NULL,
			last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (hostname, port, tag_name)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE INDEX tag_name_idx_database_instance_tags ON database_instance_tags (tag_name)
	`,
	`
		CREATE TABLE IF NOT EXISTS database_instance_stale_binlog_coordinates (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			binary_log_file varchar(128) NOT NULL,
			binary_log_pos bigint(20) unsigned NOT NULL,
			first_seen timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE INDEX first_seen_idx_database_instance_stale_binlog_coordinates ON database_instance_stale_binlog_coordinates (first_seen)
	`,
	`
		CREATE TABLE IF NOT EXISTS vitess_tablet (
			hostname varchar(128) CHARACTER SET ascii NOT NULL,
			port smallint(5) unsigned NOT NULL,
			keyspace varchar(128) CHARACTER SET ascii NOT NULL,
			shard varchar(128) CHARACTER SET ascii NOT NULL,
			cell varchar(128) CHARACTER SET ascii NOT NULL,
			tablet_type smallint(5) NOT NULL,
			master_timestamp timestamp NOT NULL,
			info varchar(512) CHARACTER SET ascii NOT NULL,
			PRIMARY KEY (hostname, port)
		) ENGINE=InnoDB DEFAULT CHARSET=ascii
	`,
	`
		CREATE INDEX cell_idx_vitess_tablet ON vitess_tablet (cell)
	`,
	`
		CREATE INDEX ks_idx_vitess_tablet ON vitess_tablet (keyspace, shard)
	`,
}
