/*
Copyright 2022 The Vitess Authors.

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

package mysql

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	SetSuperUser                = `SET GLOBAL super_read_only='ON'`
	UnSetSuperUser              = `SET GLOBAL super_read_only='OFF'`
	sqlCreateLocalMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.local_metadata (
	  name VARCHAR(255) NOT NULL,
	  value MEDIUMBLOB NOT NULL,
	  PRIMARY KEY (name)
	  ) ENGINE=InnoDB`
	sqlCreateShardMetadataTable = `CREATE TABLE IF NOT EXISTS _vt.shard_metadata (
	  name VARCHAR(255) NOT NULL,
	  value MEDIUMBLOB NOT NULL,
	  PRIMARY KEY (name)
	  ) ENGINE=InnoDB`
	sqlUpdateLocalMetadataTable = "UPDATE _vt.local_metadata SET db_name='%s' WHERE db_name=''"
	sqlUpdateShardMetadataTable = "UPDATE _vt.shard_metadata SET db_name='%s' WHERE db_name=''"
	sqlRetryMigrationWhere      = `UPDATE _vt.schema_migrations
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
	sqlFixCompletedTimestamp = `UPDATE _vt.schema_migrations
		SET
			completed_timestamp=NOW()
		WHERE
			migration_status='failed'
			AND cleanup_timestamp IS NULL
			AND completed_timestamp IS NULL
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
	alterSchemaMigrationsTableRetries                  = "ALTER TABLE _vt.schema_migrations add column retries int unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableTablet                   = "ALTER TABLE _vt.schema_migrations add column tablet varchar(128) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableArtifacts                = "ALTER TABLE _vt.schema_migrations modify artifacts TEXT NOT NULL"
	alterSchemaMigrationsTableTabletFailure            = "ALTER TABLE _vt.schema_migrations add column tablet_failure tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableTabletFailureIndex       = "ALTER TABLE _vt.schema_migrations add KEY tablet_failure_idx (tablet_failure, migration_status, retries)"
	alterSchemaMigrationsTableProgress                 = "ALTER TABLE _vt.schema_migrations add column progress float NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableContext                  = "ALTER TABLE _vt.schema_migrations add column migration_context varchar(1024) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableDDLAction                = "ALTER TABLE _vt.schema_migrations add column ddl_action varchar(16) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableMessage                  = "ALTER TABLE _vt.schema_migrations add column message TEXT NOT NULL"
	alterSchemaMigrationsTableTableCompleteIndex       = "ALTER TABLE _vt.schema_migrations add KEY table_complete_idx (migration_status, keyspace(64), mysql_table(64), completed_timestamp)"
	alterSchemaMigrationsTableETASeconds               = "ALTER TABLE _vt.schema_migrations add column eta_seconds bigint NOT NULL DEFAULT -1"
	alterSchemaMigrationsTableRowsCopied               = "ALTER TABLE _vt.schema_migrations add column rows_copied bigint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableTableRows                = "ALTER TABLE _vt.schema_migrations add column table_rows bigint NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableAddedUniqueKeys          = "ALTER TABLE _vt.schema_migrations add column added_unique_keys int unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableRemovedUniqueKeys        = "ALTER TABLE _vt.schema_migrations add column removed_unique_keys int unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableLogFile                  = "ALTER TABLE _vt.schema_migrations add column log_file varchar(1024) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableRetainArtifacts          = "ALTER TABLE _vt.schema_migrations add column retain_artifacts_seconds bigint NOT NULL DEFAULT 0"
	alterSchemaMigrationsTablePostponeCompletion       = "ALTER TABLE _vt.schema_migrations add column postpone_completion tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableContextIndex             = "ALTER TABLE _vt.schema_migrations add KEY migration_context_idx (migration_context(64))"
	alterSchemaMigrationsTableRemovedUniqueNames       = "ALTER TABLE _vt.schema_migrations add column removed_unique_key_names text NOT NULL"
	alterSchemaMigrationsTableRemovedNoDefaultColNames = "ALTER TABLE _vt.schema_migrations add column dropped_no_default_column_names text NOT NULL"
	alterSchemaMigrationsTableExpandedColNames         = "ALTER TABLE _vt.schema_migrations add column expanded_column_names text NOT NULL"
	alterSchemaMigrationsTableRevertibleNotes          = "ALTER TABLE _vt.schema_migrations add column revertible_notes text NOT NULL"
	alterSchemaMigrationsTableAllowConcurrent          = "ALTER TABLE _vt.schema_migrations add column allow_concurrent tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableRevertedUUID             = "ALTER TABLE _vt.schema_migrations add column reverted_uuid varchar(64) NOT NULL DEFAULT ''"
	alterSchemaMigrationsTableRevertedUUIDIndex        = "ALTER TABLE _vt.schema_migrations add KEY reverted_uuid_idx (reverted_uuid(64))"
	alterSchemaMigrationsTableIsView                   = "ALTER TABLE _vt.schema_migrations add column is_view tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableReadyToComplete          = "ALTER TABLE _vt.schema_migrations add column ready_to_complete tinyint unsigned NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableStowawayTable            = "ALTER TABLE _vt.schema_migrations add column stowaway_table tinytext NOT NULL"
	alterSchemaMigrationsTableVreplLivenessIndicator   = "ALTER TABLE _vt.schema_migrations add column vitess_liveness_indicator bigint NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableUserThrottleRatio        = "ALTER TABLE _vt.schema_migrations add column user_throttle_ratio float NOT NULL DEFAULT 0"
	alterSchemaMigrationsTableSpecialPlan              = "ALTER TABLE _vt.schema_migrations add column special_plan text NOT NULL"
	queryToTriggerWithDDL                              = "SELECT _vt_no_such_column__init_schema FROM _vt.vreplication LIMIT 1"
)

type schemaInitializerFunc struct {
	name          string
	initFuncOld   func() error
	initFunc      func(conn *Conn) error
	resultHandler func(result *sqltypes.Result) error
	initialized   bool
}

type schemaInitializer struct {
	funcs       []*schemaInitializerFunc
	initialized bool
	mu          sync.Mutex
}

var (
	SchemaInitializer          *schemaInitializer
	reparentJournalQueries     []string
	tmInitialization           []string
	upsertLocalMetadata        []string
	vreplicationSchema         []string
	applyDDL                   []string
	retryMigrationSchema       []string
	artifactSchema             []string
	selectUncollectedArtifacts string
	VTDatabaseInit2            = []string{
		CreateVTDatabase,
		CreateSchemaCopyTable,
	}

	replicationDisable         string
	maximumPositionSize        = 64000
	sqlAlterLocalMetadataTable = []string{
		`ALTER TABLE _vt.local_metadata ADD COLUMN db_name VARBINARY(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE _vt.local_metadata DROP PRIMARY KEY, ADD PRIMARY KEY(name, db_name)`,
		// VARCHAR(255) is not long enough to hold replication positions, hence changing to
		// MEDIUMBLOB.
		`ALTER TABLE _vt.local_metadata CHANGE value value MEDIUMBLOB NOT NULL`,
	}
	sqlAlterShardMetadataTable = []string{
		`ALTER TABLE _vt.shard_metadata ADD COLUMN db_name VARBINARY(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE _vt.shard_metadata DROP PRIMARY KEY, ADD PRIMARY KEY(name, db_name)`,
	}

	applyDDLs = []string{
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
		alterSchemaMigrationsTableRowsCopied,
		alterSchemaMigrationsTableTableRows,
		alterSchemaMigrationsTableAddedUniqueKeys,
		alterSchemaMigrationsTableRemovedUniqueKeys,
		alterSchemaMigrationsTableLogFile,
		alterSchemaMigrationsTableRetainArtifacts,
		alterSchemaMigrationsTablePostponeCompletion,
		alterSchemaMigrationsTableContextIndex,
		alterSchemaMigrationsTableRemovedUniqueNames,
		alterSchemaMigrationsTableRemovedNoDefaultColNames,
		alterSchemaMigrationsTableExpandedColNames,
		alterSchemaMigrationsTableRevertibleNotes,
		alterSchemaMigrationsTableAllowConcurrent,
		alterSchemaMigrationsTableRevertedUUID,
		alterSchemaMigrationsTableRevertedUUIDIndex,
		alterSchemaMigrationsTableIsView,
		alterSchemaMigrationsTableReadyToComplete,
		alterSchemaMigrationsTableStowawayTable,
		alterSchemaMigrationsTableVreplLivenessIndicator,
		alterSchemaMigrationsTableUserThrottleRatio,
		alterSchemaMigrationsTableSpecialPlan,
	}
)

func init() {
	// this should be the first query. This will disable replication
	// since we close the connection in the end so we don't need to reset it back.
	replicationDisable = "SET @@session.sql_log_bin = 0"
	SchemaInitializer = newSchemaInitializer()
}
func newSchemaInitializer() *schemaInitializer {
	return &schemaInitializer{}
}

func createReparentJournal() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS _vt.reparent_journal (
  time_created_ns BIGINT UNSIGNED NOT NULL,
  action_name VARBINARY(250) NOT NULL,
  primary_alias VARBINARY(32) NOT NULL,
  replication_position VARBINARY(%v) DEFAULT NULL,
  PRIMARY KEY (time_created_ns))
ENGINE=InnoDB`, maximumPositionSize)}
}

func alterReparentJournal() []string {
	return []string{
		"ALTER TABLE _vt.reparent_journal CHANGE COLUMN master_alias primary_alias VARBINARY(32) NOT NULL",
	}
}

func (si *schemaInitializer) RegisterMutations(tabletName string, localMetadata map[string]string, sqlWhereTabletFailure string) error {
	log.Infof("registering mutations")
	// this should be the first query. This will disable replication
	// since we close the connection in the end so we don't need to reset it back.
	replicationDisable = "SET @@session.sql_log_bin = 0"

	// Reparent journal mutation
	reparentJournalQueries = append([]string{}, createReparentJournal()...)
	reparentJournalQueries = append(reparentJournalQueries, alterReparentJournal()...)

	// Tablet Manager initialization
	tmInitialization = append([]string{}, "CREATE DATABASE IF NOT EXISTS _vt")
	// Tablet Manager local metadata
	tmInitialization = append(tmInitialization, sqlCreateLocalMetadataTable)
	tmInitialization = append(tmInitialization, sqlAlterLocalMetadataTable...)
	sql := fmt.Sprintf(sqlUpdateLocalMetadataTable, tabletName)
	tmInitialization = append(tmInitialization, sql)
	// Tablet Manager shared metadata
	tmInitialization = append(tmInitialization, sqlCreateShardMetadataTable)
	tmInitialization = append(tmInitialization, sqlAlterShardMetadataTable...)
	sql = fmt.Sprintf(sqlUpdateShardMetadataTable, tabletName)
	tmInitialization = append(tmInitialization, sql)

	// upsert local metadata
	upsertLocalMetadata = append([]string{}, "BEGIN")
	for name, val := range localMetadata {
		nameValue := sqltypes.NewVarChar(name)
		valValue := sqltypes.NewVarChar(val)
		dbNameValue := sqltypes.NewVarBinary(tabletName)

		queryBuf := bytes.Buffer{}
		queryBuf.WriteString("INSERT INTO _vt.local_metadata (name,value, db_name) VALUES (")
		nameValue.EncodeSQL(&queryBuf)
		queryBuf.WriteByte(',')
		valValue.EncodeSQL(&queryBuf)
		queryBuf.WriteByte(',')
		dbNameValue.EncodeSQL(&queryBuf)
		queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
		valValue.EncodeSQL(&queryBuf)
		upsertLocalMetadata = append(upsertLocalMetadata, queryBuf.String())
	}
	upsertLocalMetadata = append(upsertLocalMetadata, "COMMIT")

	// Vreplication Schema
	vreplicationSchema = append([]string{}, queryToTriggerWithDDL)

	// Online DDL
	applyDDL = append([]string{}, applyDDLs...)

	parsed := sqlparser.BuildParsedQuery(sqlRetryMigrationWhere, ":tablet", sqlWhereTabletFailure)
	bindVars := map[string]*querypb.BindVariable{
		"tablet": sqltypes.StringBindVariable(tabletName),
	}
	schema, err := parsed.GenerateQuery(bindVars, nil)
	if err == nil {
		retryMigrationSchema = append([]string{}, schema)
	}

	artifactSchema = append([]string{}, sqlFixCompletedTimestamp)

	query, err := sqlparser.ParseAndBind(sqlSelectUncollectedArtifacts,
		sqltypes.Int64BindVariable(int64((24 * time.Hour).Seconds())),
	)
	selectUncollectedArtifacts = query

	return err
}

// Set super-read-only flag to 'ON'
func (si *schemaInitializer) setSuperReadOnlyUser(conn *Conn) error {
	var err error
	log.Infof("%s", SetSuperUser)
	// setting super_read_only to true, given it was set during tm_init.start()
	if _, err = conn.ExecuteFetch(SetSuperUser, 0, false); err != nil {
		log.Warningf("SetSuperReadOnly(true) failed during revert: %v", err)
	}

	return err
}

// Set super-read-only flag to 'OFF'
func (si *schemaInitializer) unsetSuperReadOnlyUser(conn *Conn) error {
	var err error
	// setting super_read_only to true, given it was set during tm_init.start()
	log.Infof("%s", UnSetSuperUser)
	if _, err = conn.ExecuteFetch(UnSetSuperUser, 0, false); err != nil {
		log.Warningf("SetSuperReadOnly(true) failed during revert: %v", err)
	}
	return err
}

func (si *schemaInitializer) RunAllMutations(ctx context.Context, conn *Conn) error {
	log.Infof("run all mutation")

	if err := si.unsetSuperReadOnlyUser(conn); err != nil {
		log.Infof("error in setting super read-only user %s", err)
		return err
	}
	defer func() {
		if err := si.setSuperReadOnlyUser(conn); err != nil {
			log.Infof("error in un-setting super read-only user %s", err)
		}
	}()

	if _, err := conn.ExecuteFetch(replicationDisable, 0, false); err != nil {
		log.Infof("error in replicationDisable %s", err)
		//return err
	}

	for _, sql := range tmInitialization {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in tablet manager initialization %s", err)
			//return err
		}
	}

	for _, sql := range upsertLocalMetadata {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in upsert local metadata %s", err)
			//return err
		}
	}

	for _, sql := range reparentJournalQueries {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in reparent journal %s", err)
			//return err
		}
	}

	for _, sql := range vreplicationSchema {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in vreplication schema %s", err)
			//return err
		}
	}

	for _, sql := range applyDDL {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in apply DDL %s", err)
			//return err
		}
	}

	for _, sql := range VTDatabaseInit2 {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			log.Infof("error in VT database init %s", err)
			//return err
		}
	}

	/*for _, sql := range retryMigrationSchema {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			return err
		}
	}*/

	/*for _, sql := range artifactSchema {
		if _, err := conn.ExecuteFetch(sql, 0, false); err != nil {
			return err
		}
	}*/

	/*r, err := conn.ExecuteFetch(selectUncollectedArtifacts, math.MaxInt32, true)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		artifacts := row["artifacts"].ToString()
		logPath := row["log_path"].ToString()

		// Remove tables:
		artifactTables := textutil.SplitDelimitedList(artifacts)

		timeNow := time.Now()
		for i, artifactTable := range artifactTables {
			// We wish to generate distinct timestamp values for each table in this UUID,
			// because all tables will be renamed as _something_UUID_timestamp. Since UUID
			// is shared for all artifacts in this loop, we differentiate via timestamp
			t := timeNow.Add(time.Duration(i) * time.Second).UTC()
			if err := e.gcArtifactTable(ctx, artifactTable, uuid, t); err != nil {
				return err
			}
			log.Infof("Executor.gcArtifacts: renamed away artifact %s", artifactTable)
		}

		// Remove logs:
		{
			// logPath is in 'hostname:/path/to/logs' format
			tokens := strings.SplitN(logPath, ":", 2)
			logPath = tokens[len(tokens)-1]
			if err := os.RemoveAll(logPath); err != nil {
				return err
			}
		}

		// while the next function only applies to 'online' strategy ALTER and REVERT, there is no
		// harm in invoking it for other migrations.
		if err := e.deleteVReplicationEntry(ctx, uuid); err != nil {
			return err
		}

		if err := e.updateMigrationTimestamp(ctx, "cleanup_timestamp", uuid); err != nil {
			return err
		}
	}*/

	return nil
}

func (si *schemaInitializer) isRegistered(name string) bool {
	for _, f := range si.funcs {
		if f.name == name {
			return true
		}
	}
	return false
}

func (si *schemaInitializer) RegisterSchemaInitializer(name string, initFunc func(conn *Conn) error, atHead bool) error {
	si.mu.Lock()
	log.Infof("SchemaInitializer: registering function: %s %d", name, len(si.funcs))
	defer si.mu.Unlock()
	if si.isRegistered(name) {
		return nil
	}
	if si.initialized {
		/*if err := initFunc(); err != nil {
			return err
		}*/
	} else {
		initFunc := &schemaInitializerFunc{name: name, initFuncOld: nil, initFunc: initFunc}
		if atHead {
			si.funcs = append([]*schemaInitializerFunc{initFunc}, si.funcs...)
		} else {
			si.funcs = append(si.funcs, initFunc)
		}
	}
	return nil
}

func (si *schemaInitializer) RegisterSchemaInitializerOld(name string, initFunc func() error, atHead bool) error {
	si.mu.Lock()
	log.Infof("SchemaInitializerold: registering function: %s %d", name, len(si.funcs))
	defer si.mu.Unlock()
	if si.isRegistered(name) {
		return nil
	}
	if si.initialized {
		/*if err := initFuncOld(); err != nil {
			return err
		}*/
	} else {
		initFunc := &schemaInitializerFunc{name: name, initFuncOld: initFunc}
		if atHead {
			si.funcs = append([]*schemaInitializerFunc{initFunc}, si.funcs...)
		} else {
			si.funcs = append(si.funcs, initFunc)
		}
	}
	return nil
}

func (si *schemaInitializer) InitializeSchemaOld() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.initialized {
		return nil
	}

	for _, f := range si.funcs {
		if f.initialized {
			continue
		}
		log.Infof("SchemaInitializerold: running init function: %s", f.name)
		/*if err := f.initFuncOld(); err != nil && !f.initialized {
			log.Infof("error: %s", err)
			return err
		}
		f.initialized = true*/
	}
	return nil
}

func (si *schemaInitializer) InitializeSchema(ctx context.Context, conn *Conn) error {
	log.Infof("run all mutation")
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.initialized {
		return nil
	}

	if err := si.unsetSuperReadOnlyUser(conn); err != nil {
		log.Infof("error in setting super read-only user %s", err)
		return err
	}
	defer func() {
		if err := si.setSuperReadOnlyUser(conn); err != nil {
			log.Infof("error in un-setting super read-only user %s", err)
		}
	}()

	if _, err := conn.ExecuteFetch(replicationDisable, 0, false); err != nil {
		log.Infof("error in disabling replication %s", err)
		return err
	}

	for _, f := range si.funcs {
		if f.initialized {
			continue
		}
		log.Infof("SchemaInitializer: running init function: %s", f.name)
		if err := f.initFunc(conn); err != nil {
			log.Infof("error: %s", err)
			return err
		}
		f.initialized = true
	}
	si.initialized = true
	return nil
}

func (si *schemaInitializer) PrintSchema() {
	for _, f := range si.funcs {
		log.Infof("function --> %s", f.name)
	}
}
