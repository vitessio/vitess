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
	sqlInsertMigration = `INSERT IGNORE INTO _vt.schema_migrations (
		migration_uuid,
		keyspace,
		shard,
		mysql_schema,
		mysql_table,
		migration_statement,
		strategy,
		options,
		ddl_action,
		requested_timestamp,
		migration_context,
		migration_status,
		tablet,
		retain_artifacts_seconds,
		postpone_launch,
		postpone_completion,
		allow_concurrent,
		reverted_uuid,
		is_view
	) VALUES (
		%a, %a, %a, %a, %a, %a, %a, %a, %a, NOW(6), %a, %a, %a, %a, %a, %a, %a, %a, %a
	)`

	sqlSelectQueuedMigrations = `SELECT
			migration_uuid,
			ddl_action,
			is_view,
			is_immediate_operation,
			postpone_launch,
			postpone_completion,
			ready_to_complete
		FROM _vt.schema_migrations
		WHERE
			migration_status='queued'
			AND reviewed_timestamp IS NOT NULL
		ORDER BY id
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
	sqlUpdateMigrationStatusFailedOrCancelled = `UPDATE _vt.schema_migrations
			SET migration_status=IF(cancelled_timestamp IS NULL, 'failed', 'cancelled')
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
	sqlUpdateMigrationRowsCopied = `UPDATE _vt.schema_migrations
			SET rows_copied=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationIsView = `UPDATE _vt.schema_migrations
			SET is_view=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationSetImmediateOperation = `UPDATE _vt.schema_migrations
			SET is_immediate_operation=1
		WHERE
			migration_uuid=%a
	`
	sqlSetMigrationReadyToComplete = `UPDATE _vt.schema_migrations SET
			ready_to_complete=1,
			ready_to_complete_timestamp=NOW(6)
		WHERE
			migration_uuid=%a
	`
	sqlClearMigrationReadyToComplete = `UPDATE _vt.schema_migrations SET
			ready_to_complete=0
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationUserThrottleRatio = `UPDATE _vt.schema_migrations
			SET user_throttle_ratio=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationStartedTimestamp = `UPDATE _vt.schema_migrations SET
			started_timestamp =IFNULL(started_timestamp,  NOW(6)),
			liveness_timestamp=IFNULL(liveness_timestamp, NOW(6))
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationTimestamp = `UPDATE _vt.schema_migrations
			SET %s=NOW(6)
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationVitessLivenessIndicator = `UPDATE _vt.schema_migrations
			SET vitess_liveness_indicator=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationLogPath = `UPDATE _vt.schema_migrations
			SET log_path=%a, log_file=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateArtifacts = `UPDATE _vt.schema_migrations
			SET artifacts=concat(%a, ',', artifacts), cleanup_timestamp=NULL
		WHERE
			migration_uuid=%a
	`
	sqlClearSingleArtifact = `UPDATE _vt.schema_migrations
			SET artifacts=replace(artifacts, concat(%a, ','), '')
		WHERE
			migration_uuid=%a
	`
	sqlClearArtifacts = `UPDATE _vt.schema_migrations
			SET artifacts=''
		WHERE
			migration_uuid=%a
	`
	sqlUpdateSpecialPlan = `UPDATE _vt.schema_migrations
			SET special_plan=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateStage = `UPDATE _vt.schema_migrations
			SET stage=%a
		WHERE
			migration_uuid=%a
	`
	sqlIncrementCutoverAttempts = `UPDATE _vt.schema_migrations
			SET cutover_attempts=cutover_attempts+1
		WHERE
			migration_uuid=%a
	`
	sqlUpdateReadyForCleanup = `UPDATE _vt.schema_migrations
			SET retain_artifacts_seconds=-1
		WHERE
			migration_uuid=%a
	`
	sqlUpdateLaunchMigration = `UPDATE _vt.schema_migrations
			SET postpone_launch=0
		WHERE
			migration_uuid=%a
			AND postpone_launch != 0
	`
	sqlUpdateCompleteMigration = `UPDATE _vt.schema_migrations
			SET postpone_completion=0
		WHERE
			migration_uuid=%a
			AND postpone_completion != 0
	`
	sqlUpdateTablet = `UPDATE _vt.schema_migrations
			SET tablet=%a
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
	sqlUpdateSchemaAnalysis = `UPDATE _vt.schema_migrations
			SET added_unique_keys=%a, removed_unique_keys=%a, removed_unique_key_names=%a,
			dropped_no_default_column_names=%a, expanded_column_names=%a,
			revertible_notes=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationTableRows = `UPDATE _vt.schema_migrations
			SET table_rows=%a
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationProgressByRowsCopied = `UPDATE _vt.schema_migrations
			SET
				progress=CASE
					WHEN table_rows=0 THEN 100
					ELSE LEAST(100, 100*%a/table_rows)
				END
		WHERE
			migration_uuid=%a
	`
	sqlUpdateMigrationETASecondsByProgress = `UPDATE _vt.schema_migrations
			SET
				eta_seconds=CASE
					WHEN progress=0 THEN -1
					WHEN table_rows=0 THEN 0
					ELSE GREATEST(0,
						TIMESTAMPDIFF(SECOND, started_timestamp, NOW())*((100/progress)-1)
					)
				END
		WHERE
			migration_uuid=%a
	`
	sqlUpdateLastThrottled = `UPDATE _vt.schema_migrations
			SET last_throttled_timestamp=FROM_UNIXTIME(%a), component_throttled=%a
		WHERE
			migration_uuid=%a
	`
	sqlRetryMigrationWhere = `UPDATE _vt.schema_migrations
		SET
			migration_status='queued',
			tablet=%a,
			retries=retries + 1,
			tablet_failure=0,
			message='',
			stage='',
			cutover_attempts=0,
			ready_timestamp=NULL,
			started_timestamp=NULL,
			liveness_timestamp=NULL,
			cancelled_timestamp=NULL,
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
			message='',
			stage='',
			cutover_attempts=0,
			ready_timestamp=NULL,
			started_timestamp=NULL,
			liveness_timestamp=NULL,
			cancelled_timestamp=NULL,
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
			postpone_completion,
			timestampdiff(second, started_timestamp, now()) as elapsed_seconds
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
	sqlSelectCompleteMigrationsByContextAndSQL = `SELECT
			migration_uuid,
			strategy
		FROM _vt.schema_migrations
		WHERE
			migration_status='complete'
			AND keyspace=%a
			AND migration_context=%a
			AND migration_statement=%a
		LIMIT 1
	`
	sqlSelectStaleMigrations = `SELECT
			migration_uuid
		FROM _vt.schema_migrations
		WHERE
			migration_status='running'
			AND liveness_timestamp < NOW() - INTERVAL %a MINUTE
	`
	sqlSelectPendingMigrations = `SELECT
			migration_uuid,
			migration_context,
			keyspace,
			mysql_table,
			migration_status
		FROM _vt.schema_migrations
		WHERE
			migration_status IN ('queued', 'ready', 'running')
		ORDER BY id
	`
	sqlSelectQueuedUnreviewedMigrations = `SELECT
			migration_uuid
		FROM _vt.schema_migrations
		WHERE
			migration_status='queued'
			AND reviewed_timestamp IS NULL
		ORDER BY id
	`
	sqlSelectUncollectedArtifacts = `SELECT
			migration_uuid,
			artifacts,
			log_path
		FROM _vt.schema_migrations
		WHERE
			migration_status IN ('complete', 'failed')
			AND cleanup_timestamp IS NULL
			AND completed_timestamp <= IF(retain_artifacts_seconds=0,
				NOW() - INTERVAL %a SECOND,
				NOW() - INTERVAL retain_artifacts_seconds SECOND
			)
	`
	sqlFixCompletedTimestamp = `UPDATE _vt.schema_migrations
		SET
			completed_timestamp=NOW(6)
		WHERE
			migration_status='failed'
			AND cleanup_timestamp IS NULL
			AND completed_timestamp IS NULL
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
			log_file,
			retries,
			ddl_action,
			artifacts,
			tablet,
			added_unique_keys,
			removed_unique_keys,
			migration_context,
			retain_artifacts_seconds,
			is_view,
			ready_to_complete,
			ready_to_complete_timestamp is not null as was_ready_to_complete,
			reverted_uuid,
			rows_copied,
			vitess_liveness_indicator,
			user_throttle_ratio,
			last_throttled_timestamp,
			cancelled_timestamp,
			component_throttled,
			postpone_launch,
			postpone_completion,
			is_immediate_operation,
			reviewed_timestamp
		FROM _vt.schema_migrations
		WHERE
			migration_uuid=%a
	`
	sqlSelectReadyMigrations = `SELECT
			migration_uuid
		FROM _vt.schema_migrations
		WHERE
			migration_status='ready'
		ORDER BY id
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
	sqlSelectColumnTypes = `
		select
				*,
				COLUMN_DEFAULT IS NULL AS is_default_null
			from
				information_schema.columns
			where
				table_schema=%a
				and table_name=%a
		`
	selSelectCountFKParentConstraints = `
		SELECT
			COUNT(*) as num_fk_constraints
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
			REFERENCED_TABLE_SCHEMA=%a AND REFERENCED_TABLE_NAME=%a
			AND REFERENCED_TABLE_NAME IS NOT NULL
		`
	selSelectCountFKChildConstraints = `
		SELECT
			COUNT(*) as num_fk_constraints
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
			TABLE_SCHEMA=%a AND TABLE_NAME=%a
			AND REFERENCED_TABLE_NAME IS NOT NULL
		`
	sqlSelectUniqueKeys = `
	SELECT
		COLUMNS.TABLE_SCHEMA as table_schema,
		COLUMNS.TABLE_NAME as table_name,
		COLUMNS.COLUMN_NAME as column_name,
		UNIQUES.INDEX_NAME as index_name,
		UNIQUES.COLUMN_NAMES as column_names,
		UNIQUES.COUNT_COLUMN_IN_INDEX as count_column_in_index,
		COLUMNS.DATA_TYPE as data_type,
		COLUMNS.CHARACTER_SET_NAME as character_set_name,
		LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
		(DATA_TYPE='float' OR DATA_TYPE='double') AS is_float,
		has_nullable
	FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
		SELECT
			TABLE_SCHEMA,
			TABLE_NAME,
			INDEX_NAME,
			COUNT(*) AS COUNT_COLUMN_IN_INDEX,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
			SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
			SUM(NULLABLE='YES') > 0 AS has_nullable
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE
			NON_UNIQUE=0
			AND TABLE_SCHEMA=%a
			AND TABLE_NAME=%a
		GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
	) AS UNIQUES
	ON (
		COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
	)
	WHERE
		COLUMNS.TABLE_SCHEMA=%a
		AND COLUMNS.TABLE_NAME=%a
	ORDER BY
		COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
		CASE UNIQUES.INDEX_NAME
			WHEN 'PRIMARY' THEN 0
			ELSE 1
		END,
		CASE has_nullable
			WHEN 0 THEN 0
			ELSE 1
		END,
		CASE IFNULL(CHARACTER_SET_NAME, '')
				WHEN '' THEN 0
				ELSE 1
		END,
		CASE DATA_TYPE
			WHEN 'tinyint' THEN 0
			WHEN 'smallint' THEN 1
			WHEN 'int' THEN 2
			WHEN 'bigint' THEN 3
			ELSE 100
		END,
		COUNT_COLUMN_IN_INDEX
	`
	sqlDropTrigger       = "DROP TRIGGER IF EXISTS `%a`.`%a`"
	sqlShowTablesLike    = "SHOW TABLES LIKE '%a'"
	sqlDropTable         = "DROP TABLE `%a`"
	sqlDropTableIfExists = "DROP TABLE IF EXISTS `%a`"
	sqlShowColumnsFrom   = "SHOW COLUMNS FROM `%a`"
	sqlShowTableStatus   = "SHOW TABLE STATUS LIKE '%a'"
	sqlShowCreateTable   = "SHOW CREATE TABLE `%a`"
	sqlGetAutoIncrement  = `
		SELECT
			AUTO_INCREMENT
		FROM INFORMATION_SCHEMA.TABLES
		WHERE
			TABLES.TABLE_SCHEMA=%a
			AND TABLES.TABLE_NAME=%a
			AND AUTO_INCREMENT IS NOT NULL
		`
	sqlAlterTableAutoIncrement      = "ALTER TABLE `%s` AUTO_INCREMENT=%a"
	sqlAlterTableExchangePartition  = "ALTER TABLE `%a` EXCHANGE PARTITION `%a` WITH TABLE `%a`"
	sqlAlterTableRemovePartitioning = "ALTER TABLE `%a` REMOVE PARTITIONING"
	sqlAlterTableDropPartition      = "ALTER TABLE `%a` DROP PARTITION `%a`"
	sqlStartVReplStream             = "UPDATE _vt.vreplication set state='Running' where db_name=%a and workflow=%a"
	sqlStopVReplStream              = "UPDATE _vt.vreplication set state='Stopped' where db_name=%a and workflow=%a"
	sqlDeleteVReplStream            = "DELETE FROM _vt.vreplication where db_name=%a and workflow=%a"
	sqlReadVReplStream              = `SELECT
			id,
			workflow,
			source,
			pos,
			time_updated,
			transaction_timestamp,
			time_heartbeat,
			time_throttled,
			component_throttled,
			state,
			message,
			rows_copied
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
	sqlSwapTables         = "RENAME TABLE `%a` TO `%a`, `%a` TO `%a`, `%a` TO `%a`"
	sqlRenameTable        = "RENAME TABLE `%a` TO `%a`"
	sqlLockTwoTablesWrite = "LOCK TABLES `%a` WRITE, `%a` WRITE"
	sqlUnlockTables       = "UNLOCK TABLES"
	sqlCreateSentryTable  = "CREATE TABLE IF NOT EXISTS `%a` (id INT PRIMARY KEY)"
	sqlFindProcess        = "SELECT id, Info as info FROM information_schema.processlist WHERE id=%a AND Info LIKE %a"
)

const (
	retryMigrationHint     = "retry"
	cancelMigrationHint    = "cancel"
	cancelAllMigrationHint = "cancel-all"
	completeMigrationHint  = "complete"
)

var (
	sqlCreateOnlineDDLUser = []string{
		`CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'`,
		`ALTER USER %s IDENTIFIED BY '%s'`,
	}
	sqlGrantOnlineDDLSuper = []string{
		`GRANT SUPER ON *.* TO %s`,
	}
	sqlGrantOnlineDDLUser = []string{
		`GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s`,
		`GRANT ALTER, CREATE, CREATE VIEW, SHOW VIEW, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON *.* TO %s`,
	}
	sqlDropOnlineDDLUser = `DROP USER IF EXISTS %s`
)
