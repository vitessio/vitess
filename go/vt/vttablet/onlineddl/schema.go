/*
Copyright 2019 The Vitess Authors.

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

package onlineddl

const (
	// SchemaMigrationsTableName is used by VExec interceptor to call the correct handler
	sqlCreateSidecarDB             = "create database if not exists _vt"
	sqlCreateSchemaMigrationsTable = `CREATE TABLE IF NOT EXISTS _vt.schema_migrations (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		migration_uuid varchar(64) NOT NULL,
		keyspace varchar(256) NOT NULL,
		shard varchar(255) NOT NULL,
		mysql_schema varchar(128) NOT NULL,
		mysql_table varchar(128) NOT NULL,
		migration_statement text NOT NULL,
		strategy varchar(128) NOT NULL,
		options varchar(8192) NOT NULL,
		added_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		requested_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ready_timestamp timestamp NULL DEFAULT NULL,
		started_timestamp timestamp NULL DEFAULT NULL,
		liveness_timestamp timestamp NULL DEFAULT NULL,
		completed_timestamp timestamp NULL DEFAULT NULL,
		cleanup_timestamp timestamp NULL DEFAULT NULL,
		migration_status varchar(128) NOT NULL,
		log_path varchar(1024) NOT NULL,
		artifacts varchar(1024) NOT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uuid_idx (migration_uuid),
		KEY keyspace_shard_idx (keyspace(64),shard(64)),
		KEY status_idx (migration_status, liveness_timestamp),
		KEY cleanup_status_idx (cleanup_timestamp, migration_status)
	) engine=InnoDB DEFAULT CHARSET=utf8mb4`
	alterSchemaMigrationsTableRetries            = "ALTER TABLE _vt.schema_migrations add column retries int unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableTablet             = "ALTER TABLE _vt.schema_migrations add column tablet varchar(128) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableArtifacts          = "ALTER TABLE _vt.schema_migrations modify artifacts TEXT NOT NULL"
	alterSchemaMigrationsTableTabletFailure      = "ALTER TABLE _vt.schema_migrations add column tablet_failure tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableTabletFailureIndex = "ALTER TABLE _vt.schema_migrations add KEY tablet_failure_idx (tablet_failure, migration_status, retries)"
	alterSchemaMigrationsTableProgress           = "ALTER TABLE _vt.schema_migrations add column progress float NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableContext            = "ALTER TABLE _vt.schema_migrations add column migration_context varchar(1024) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableDDLAction          = "ALTER TABLE _vt.schema_migrations add column ddl_action varchar(16) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableMessage            = "ALTER TABLE _vt.schema_migrations add column message TEXT NOT NULL"
	alterSchemaMigrationsTableTableCompleteIndex = "ALTER TABLE _vt.schema_migrations add KEY table_complete_idx (migration_status, keyspace(64), mysql_table(64), completed_timestamp)"
	alterSchemaMigrationsTableETASeconds         = "ALTER TABLE _vt.schema_migrations add column eta_seconds bigint NOT NULL DEFAULT -1"

	sqlScheduleSingleMigration = `UPDATE _vt.schema_migrations
		SET
			migration_status='ready',
			ready_timestamp=NOW()
		WHERE
			migration_status='queued'
		ORDER BY
			requested_timestamp ASC
		LIMIT 1
	`
	sqlUpdateMySQLTable = `UPDATE _vt.schema_migrations
			SET mysql_table=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationStatus = `UPDATE _vt.schema_migrations
			SET migration_status=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationProgress = `UPDATE _vt.schema_migrations
			SET progress=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationETASeconds = `UPDATE _vt.schema_migrations
			SET eta_seconds=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationStartedTimestamp = `UPDATE _vt.schema_migrations
			SET started_timestamp=IFNULL(started_timestamp, NOW())
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationTimestamp = `UPDATE _vt.schema_migrations
			SET %s=NOW()
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationLogPath = `UPDATE _vt.schema_migrations
			SET log_path=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateArtifacts = `UPDATE _vt.schema_migrations
			SET artifacts=concat(%a, ',', artifacts)
		WHERE
			migration_uuid=%a
	`
	sqlClearArtifacts = `UPDATE _vt.schema_migrations
			SET artifacts=''
		WHERE
			migration_uuid=%a
	`
	sqlUpdateTabletFailure = `UPDATE _vt.schema_migrations
			SET tablet_failure=1
		WHERE
			migration_uuid=%a
	`
	sqlUpdateDDLAction = `UPDATE _vt.schema_migrations
			SET ddl_action=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMessage = `UPDATE _vt.schema_migrations
			SET message=%a
		WHERE
			migration_uuid=%a
	`
	sqlRetryMigrationWhere = `UPDATE _vt.schema_migrations
		SET
			migration_status='queued',
			tablet=%a,
			retries=retries + 1,
			tablet_failure=0,
			ready_timestamp=NULL,
			started_timestamp=NULL,
			liveness_timestamp=NULL,
			completed_timestamp=NULL,
			cleanup_timestamp=NULL
		WHERE
			migration_status IN ('failed', 'cancelled')
			AND (%s)
			LIMIT 1
	`
	sqlRetryMigration = `UPDATE _vt.schema_migrations
		SET
			migration_status='queued',
			tablet=%a,
			retries=retries + 1,
			tablet_failure=0,
			ready_timestamp=NULL,
			started_timestamp=NULL,
			liveness_timestamp=NULL,
			completed_timestamp=NULL,
			cleanup_timestamp=NULL
		WHERE
			migration_status IN ('failed', 'cancelled')
			AND migration_uuid=%a
	`
	sqlWhereTabletFailure = `
		tablet_failure=1
		AND migration_status='failed'
		AND retries=0
	`
	sqlSelectRunningMigrations = `SELECT
			migration_uuid,
			strategy
		FROM _vt.schema_migrations
		WHERE
			migration_status='running'
	`
	sqlSelectCompleteMigrationsOnTable = `SELECT
			migration_uuid,
			strategy
		FROM _vt.schema_migrations
		WHERE
			migration_status='complete'
			AND keyspace=%a
			AND mysql_table=%a
		ORDER BY
			completed_timestamp DESC
		LIMIT 1
	`
	sqlSelectCountReadyMigrations = `SELECT
			count(*) as count_ready
		FROM _vt.schema_migrations
		WHERE
			migration_status='ready'
	`
	sqlSelectStaleMigrations = `SELECT
			migration_uuid
		FROM _vt.schema_migrations
		WHERE
			migration_status='running'
			AND liveness_timestamp < NOW() - INTERVAL %a MINUTE
	`
	sqlSelectPendingMigrations = `SELECT
			migration_uuid
		FROM _vt.schema_migrations
		WHERE
			migration_status IN ('queued', 'ready', 'running')
	`
	sqlSelectUncollectedArtifacts = `SELECT
			migration_uuid,
			artifacts
		FROM _vt.schema_migrations
		WHERE
			migration_status IN ('complete', 'failed')
			AND cleanup_timestamp IS NULL
			AND completed_timestamp <= NOW() - INTERVAL %a SECOND
	`
	sqlSelectMigration = `SELECT
			id,
			migration_uuid,
			keyspace,
			shard,
			mysql_schema,
			mysql_table,
			migration_statement,
			strategy,
			options,
			added_timestamp,
			ready_timestamp,
			started_timestamp,
			liveness_timestamp,
			completed_timestamp,
			migration_status,
			log_path,
			retries,
			ddl_action,
			artifacts,
			tablet
		FROM _vt.schema_migrations
		WHERE
			migration_uuid=%a
	`
	sqlSelectReadyMigration = `SELECT
			id,
			migration_uuid,
			keyspace,
			shard,
			mysql_schema,
			mysql_table,
			migration_statement,
			strategy,
			options,
			added_timestamp,
			ready_timestamp,
			started_timestamp,
			liveness_timestamp,
			completed_timestamp,
			migration_status,
			log_path,
			retries,
			ddl_action,
			artifacts,
			tablet
		FROM _vt.schema_migrations
		WHERE
			migration_status='ready'
		LIMIT 1
	`
	sqlSelectPTOSCMigrationTriggers = `SELECT
			TRIGGER_SCHEMA as trigger_schema,
			TRIGGER_NAME as trigger_name
		FROM INFORMATION_SCHEMA.TRIGGERS
		WHERE
			EVENT_OBJECT_SCHEMA=%a
			AND EVENT_OBJECT_TABLE=%a
			AND ACTION_TIMING='AFTER'
			AND LEFT(TRIGGER_NAME, 7)='pt_osc_'
		`
	sqlDropTrigger       = "DROP TRIGGER IF EXISTS `%a`.`%a`"
	sqlShowTablesLike    = "SHOW TABLES LIKE '%a'"
	sqlCreateTableLike   = "CREATE TABLE `%a` LIKE `%a`"
	sqlAlterTableOptions = "ALTER TABLE `%a` %s"
	sqlShowColumnsFrom   = "SHOW COLUMNS FROM `%a`"
	sqlStartVReplStream  = "UPDATE _vt.vreplication set state='Running' where db_name=%a and workflow=%a"
	sqlStopVReplStream   = "UPDATE _vt.vreplication set state='Stopped' where db_name=%a and workflow=%a"
	sqlDeleteVReplStream = "DELETE FROM _vt.vreplication where db_name=%a and workflow=%a"
	sqlReadVReplStream   = `SELECT
			id,
			workflow,
			source,
			pos,
			time_updated,
			transaction_timestamp,
			state,
			message
		FROM _vt.vreplication
		WHERE
			workflow=%a

		`
	sqlReadCountCopyState = `SELECT
			count(*) as cnt
		FROM
			_vt.copy_state
		WHERE vrepl_id=%a
		`
	sqlSwapTables  = "RENAME TABLE `%a` TO `%a`, `%a` TO `%a`, `%a` TO `%a`"
	sqlRenameTable = "RENAME TABLE `%a` TO `%a`"
)

const (
	retryMigrationHint     = "retry"
	cancelMigrationHint    = "cancel"
	cancelAllMigrationHint = "cancel-all"
)

var (
	sqlCreateOnlineDDLUser = []string{
		`CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'`,
		`ALTER USER %s IDENTIFIED BY '%s'`,
	}
	sqlGrantOnlineDDLUser = []string{
		`GRANT SUPER, REPLICATION SLAVE ON *.* TO %s`,
		`GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON *.* TO %s`,
	}
	sqlDropOnlineDDLUser = `DROP USER IF EXISTS %s`
)

var applyDDL = []string{
	sqlCreateSidecarDB,
	sqlCreateSchemaMigrationsTable,
	alterSchemaMigrationsTableRetries,
	alterSchemaMigrationsTableTablet,
	alterSchemaMigrationsTableArtifacts,
	alterSchemaMigrationsTableTabletFailure,
	alterSchemaMigrationsTableTabletFailureIndex,
	alterSchemaMigrationsTableProgress,
	alterSchemaMigrationsTableContext,
	alterSchemaMigrationsTableDDLAction,
	alterSchemaMigrationsTableMessage,
	alterSchemaMigrationsTableTableCompleteIndex,
	alterSchemaMigrationsTableETASeconds,
}
