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

/*
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/withddl"

	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/vexec"
)

var (
	// ErrExecutorNotWritableTablet  is generated when executor is asked to run gh-ost on a read-only server
	ErrExecutorNotWritableTablet = errors.New("cannot run migration on non-writable tablet")
	// ErrExecutorMigrationAlreadyRunning is generated when an attempt is made to run an operation that conflicts with a running migration
	ErrExecutorMigrationAlreadyRunning = errors.New("cannot run migration since a migration is already running")
	// ErrMigrationNotFound is returned by readMigration when given UUI cannot be found
	ErrMigrationNotFound = errors.New("migration not found")
)

var vexecUpdateTemplates = []string{
	`update _vt.schema_migrations set migration_status='val' where mysql_schema='val'`,
	`update _vt.schema_migrations set migration_status='val' where migration_uuid='val' and mysql_schema='val'`,
	`update _vt.schema_migrations set migration_status='val' where migration_uuid='val' and mysql_schema='val' and shard='val'`,
}

var vexecInsertTemplates = []string{
	`INSERT IGNORE INTO _vt.schema_migrations (
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
		migration_status
	) VALUES (
		'val', 'val', 'val', 'val', 'val', 'val', 'val', 'val', 'val', FROM_UNIXTIME(0), 'val', 'val'
	)`,
}

var emptyResult = &sqltypes.Result{}
var acceptableDropTableIfExistsErrorCodes = []int{mysql.ERCantFindFile, mysql.ERNoSuchTable}

var ghostOverridePath = flag.String("gh-ost-path", "", "override default gh-ost binary full path")
var ptOSCOverridePath = flag.String("pt-osc-path", "", "override default pt-online-schema-change binary full path")
var migrationCheckInterval = flag.Duration("migration_check_interval", 1*time.Minute, "Interval between migration checks")
var retainOnlineDDLTables = flag.Duration("retain_online_ddl_tables", 24*time.Hour, "How long should vttablet keep an old migrated table before purging it")
var migrationNextCheckIntervals = []time.Duration{1 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}

const (
	maxPasswordLength                        = 32 // MySQL's *replication* password may not exceed 32 characters
	staleMigrationMinutes                    = 10
	progressPctStarted               float64 = 0
	progressPctFull                  float64 = 100.0
	etaSecondsUnknown                        = -1
	etaSecondsNow                            = 0
	rowsCopiedUnknown                        = 0
	emptyHint                                = ""
	readyToCompleteHint                      = "ready_to_complete"
	databasePoolSize                         = 3
	vreplicationCutOverThreshold             = 5 * time.Second
	vreplicationTestSuiteWaitSeconds         = 5
)

var (
	migrationLogFileName     = "migration.log"
	migrationFailureFileName = "migration-failure.log"
	onlineDDLUser            = "vt-online-ddl-internal"
	onlineDDLGrant           = fmt.Sprintf("'%s'@'%s'", onlineDDLUser, "%")
)

type mysqlVariables struct {
	host           string
	port           int
	readOnly       bool
	version        string
	versionComment string
}

// Executor wraps and manages the execution of a gh-ost migration.
type Executor struct {
	env                   tabletenv.Env
	pool                  *connpool.Pool
	tabletTypeFunc        func() topodatapb.TabletType
	ts                    *topo.Server
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, bufferQueries bool)
	tabletAlias           *topodatapb.TabletAlias

	keyspace string
	shard    string
	dbName   string

	initMutex      sync.Mutex
	migrationMutex sync.Mutex
	// ownedRunningMigrations lists UUIDs owned by this executor (consider this a map[string]bool)
	// A UUID listed in this map stands for a migration that is executing, and that this executor can control.
	// Migrations found to be running which are not listed in this map will either:
	// - be adopted by this executor (possible for vreplication migrations), or
	// - be terminated (example: pt-osc migration gone rogue, process still running even as the migration failed)
	// The Executor auto-reviews the map and cleans up migrations thought to be running which are not running.
	ownedRunningMigrations        sync.Map
	tickReentranceFlag            int64
	reviewedRunningMigrationsFlag bool

	ticks             *timer.Timer
	isOpen            bool
	schemaInitialized bool

	initVreplicationDDLOnce sync.Once
}

type cancellableMigration struct {
	uuid    string
	message string
}

func newCancellableMigration(uuid string, message string) *cancellableMigration {
	return &cancellableMigration{uuid: uuid, message: message}
}

// GhostBinaryFileName returns the full path+name of the gh-ost binary
func GhostBinaryFileName() (fileName string, isOverride bool) {
	if *ghostOverridePath != "" {
		return *ghostOverridePath, true
	}
	return path.Join(os.TempDir(), "vt-gh-ost"), false
}

// PTOSCFileName returns the full path+name of the pt-online-schema-change binary
// Note that vttablet does not include pt-online-schema-change
func PTOSCFileName() (fileName string, isOverride bool) {
	if *ptOSCOverridePath != "" {
		return *ptOSCOverridePath, true
	}
	return "/usr/bin/pt-online-schema-change", false
}

// newGCTableRetainTime returns the time until which a new GC table is to be retained
func newGCTableRetainTime() time.Time {
	return time.Now().UTC().Add(*retainOnlineDDLTables)
}

// NewExecutor creates a new gh-ost executor.
func NewExecutor(env tabletenv.Env, tabletAlias *topodatapb.TabletAlias, ts *topo.Server,
	tabletTypeFunc func() topodatapb.TabletType,
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, bufferQueries bool),
) *Executor {
	return &Executor{
		env:         env,
		tabletAlias: proto.Clone(tabletAlias).(*topodatapb.TabletAlias),

		pool: connpool.NewPool(env, "OnlineDDLExecutorPool", tabletenv.ConnPoolConfig{
			Size:               databasePoolSize,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		tabletTypeFunc:        tabletTypeFunc,
		ts:                    ts,
		toggleBufferTableFunc: toggleBufferTableFunc,
		ticks:                 timer.NewTimer(*migrationCheckInterval),
	}
}

func (e *Executor) execQuery(ctx context.Context, query string) (result *sqltypes.Result, err error) {
	defer e.env.LogError()

	conn, err := e.pool.Get(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Recycle()
	return conn.Exec(ctx, query, math.MaxInt32, true)
}

// TabletAliasString returns tablet alias as string (duh)
func (e *Executor) TabletAliasString() string {
	return topoproto.TabletAliasString(e.tabletAlias)
}

func (e *Executor) initSchema(ctx context.Context) error {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()

	if e.schemaInitialized {
		return nil
	}

	defer e.env.LogError()

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, ddl := range ApplyDDL {
		_, err := conn.ExecuteFetch(ddl, math.MaxInt32, false)
		if mysql.IsSchemaApplyError(err) {
			continue
		}
		if err != nil {
			return err
		}
	}
	e.schemaInitialized = true
	return nil
}

// InitDBConfig initializes keysapce
func (e *Executor) InitDBConfig(keyspace, shard, dbName string) {
	e.keyspace = keyspace
	e.shard = shard
	e.dbName = dbName
}

// Open opens database pool and initializes the schema
func (e *Executor) Open() error {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if e.isOpen || !e.env.Config().EnableOnlineDDL {
		return nil
	}
	e.reviewedRunningMigrationsFlag = false // will be set as "true" by reviewRunningMigrations()
	e.pool.Open(e.env.Config().DB.AppWithDB(), e.env.Config().DB.DbaWithDB(), e.env.Config().DB.AppDebugWithDB())
	e.ticks.Start(e.onMigrationCheckTick)
	e.triggerNextCheckInterval()

	if _, err := sqlparser.QueryMatchesTemplates("select 1 from dual", vexecUpdateTemplates); err != nil {
		// this validates vexecUpdateTemplates
		return err
	}

	e.isOpen = true

	return nil
}

// Close frees resources
func (e *Executor) Close() {
	log.Infof("onlineDDL Executor - Acquiring lock - initMutex")
	e.initMutex.Lock()
	log.Infof("onlineDDL Executor - Acquired lock - initMutex")
	defer e.initMutex.Unlock()
	if !e.isOpen {
		return
	}

	log.Infof("onlineDDL Executor - Stopping timer ticks")
	e.ticks.Stop()
	log.Infof("onlineDDL Executor - Closing the conpool")
	e.pool.Close()
	e.isOpen = false
	log.Infof("onlineDDL Executor - finished Close execution")
}

// triggerNextCheckInterval the next tick sooner than normal
func (e *Executor) triggerNextCheckInterval() {
	for _, interval := range migrationNextCheckIntervals {
		e.ticks.TriggerAfter(interval)
	}
}

// allowConcurrentMigration checks if the given migration is allowed to run concurrently.
// First, the migration itself must declare --allow-concurrent. But then, there's also some
// restrictions on which migrations exactly are allowed such concurrency.
func (e *Executor) allowConcurrentMigration(onlineDDL *schema.OnlineDDL) bool {
	if !onlineDDL.StrategySetting().IsAllowConcurrent() {
		return false
	}

	action, err := onlineDDL.GetAction()
	if err != nil {
		return false
	}
	switch action {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction:
		// CREATE TABLE, DROP TABLE are allowed to run concurrently.
		return true
	case sqlparser.RevertDDLAction:
		// REVERT is allowed to run concurrently.
		// Reminder that REVERT is supported for CREATE, DROP and for 'online' ALTER, but never for
		// 'gh-ost' or 'pt-osc' ALTERs
		return true
	}
	return false
}

// isAnyNonConcurrentMigrationRunning sees if there's any migration running right now
// that does not have -allow-concurrent.
// such a running migration will for example prevent a new non-concurrent migration from running.
func (e *Executor) isAnyNonConcurrentMigrationRunning() bool {
	nonConcurrentMigrationFound := false

	e.ownedRunningMigrations.Range(func(_, val any) bool {
		onlineDDL, ok := val.(*schema.OnlineDDL)
		if !ok {
			return true
		}
		if !e.allowConcurrentMigration(onlineDDL) {
			// The migratoin may have declared itself to be --allow-concurrent, but our scheduler
			// reserves the right to say "no, you're NOT in fact allowed to run concurrently"
			// (as example, think a `gh-ost` ALTER migration that says --allow-concurrent)
			nonConcurrentMigrationFound = true
			return false // stop iteration, no need to review other migrations
		}
		return true
	})

	return nonConcurrentMigrationFound
}

// isAnyMigrationRunningOnTable sees if there's any migration running right now
// operating on given table.
func (e *Executor) isAnyMigrationRunningOnTable(tableName string) bool {
	sameTableMigrationFound := false
	e.ownedRunningMigrations.Range(func(_, val any) bool {
		onlineDDL, ok := val.(*schema.OnlineDDL)
		if !ok {
			return true
		}
		if onlineDDL.Table == tableName {
			sameTableMigrationFound = true
			return false // stop iteration, no need to review other migrations
		}
		return true
	})
	return sameTableMigrationFound
}

// isAnyConflictingMigrationRunning checks if there's any running migration that conflicts with the
// given migration, such that they can't both run concurrently.
func (e *Executor) isAnyConflictingMigrationRunning(onlineDDL *schema.OnlineDDL) bool {

	if e.isAnyNonConcurrentMigrationRunning() && !e.allowConcurrentMigration(onlineDDL) {
		return true
	}
	if e.isAnyMigrationRunningOnTable(onlineDDL.Table) {
		return true
	}

	return false
}

func (e *Executor) ghostPanicFlagFileName(uuid string) string {
	return path.Join(os.TempDir(), fmt.Sprintf("ghost.%s.panic.flag", uuid))
}

func (e *Executor) createGhostPanicFlagFile(uuid string) error {
	_, err := os.Create(e.ghostPanicFlagFileName(uuid))
	return err
}

func (e *Executor) deleteGhostPanicFlagFile(uuid string) error {
	// We use RemoveAll because if the file does not exist that's fine. Remove will return an error
	// if file does not exist; RemoveAll does not.
	return os.RemoveAll(e.ghostPanicFlagFileName(uuid))
}

func (e *Executor) ghostPostponeFlagFileName(uuid string) string {
	return path.Join(os.TempDir(), fmt.Sprintf("ghost.%s.postpone.flag", uuid))
}

func (e *Executor) deleteGhostPostponeFlagFile(uuid string) error {
	// We use RemoveAll because if the file does not exist that's fine. Remove will return an error
	// if file does not exist; RemoveAll does not.
	return os.RemoveAll(e.ghostPostponeFlagFileName(uuid))
}

func (e *Executor) ptPidFileName(uuid string) string {
	return path.Join(os.TempDir(), fmt.Sprintf("pt-online-schema-change.%s.pid", uuid))
}

// readMySQLVariables contacts the backend MySQL server to read some of its configuration
func (e *Executor) readMySQLVariables(ctx context.Context) (variables *mysqlVariables, err error) {
	conn, err := e.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, `select
			@@global.hostname as hostname,
			@@global.port as port,
			@@global.read_only as read_only,
			@@global.version AS version,
			@@global.version_comment AS version_comment
		from dual`, 1, true)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read MySQL variables: %v", err)
	}
	row := tm.Named().Row()
	if row == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL variables: %+v", tm.Rows)
	}
	variables = &mysqlVariables{}

	if e.env.Config().DB.Host != "" {
		variables.host = e.env.Config().DB.Host
	} else {
		variables.host = row["hostname"].ToString()
	}

	if e.env.Config().DB.Port != 0 {
		variables.port = e.env.Config().DB.Port
	} else if port, err := row.ToInt64("port"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.port %v: %v", tm, err)
	} else {
		variables.port = int(port)
	}
	if variables.readOnly, err = row.ToBool("read_only"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.read_only %v: %v", tm, err)
	}

	variables.version = row["version"].ToString()
	variables.versionComment = row["version_comment"].ToString()

	return variables, nil
}

// createOnlineDDLUser creates a gh-ost or pt-osc user account with all
// neccessary privileges and with a random password
func (e *Executor) createOnlineDDLUser(ctx context.Context) (password string, err error) {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return password, err
	}
	defer conn.Close()

	password = RandomHash()[0:maxPasswordLength]

	for _, query := range sqlCreateOnlineDDLUser {
		parsed := sqlparser.BuildParsedQuery(query, onlineDDLGrant, password)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return password, err
		}
	}
	for _, query := range sqlGrantOnlineDDLSuper {
		parsed := sqlparser.BuildParsedQuery(query, onlineDDLGrant)
		conn.ExecuteFetch(parsed.Query, 0, false)
		// We ignore failure, since we might not be able to grant
		// SUPER privs (e.g. Aurora)
	}
	for _, query := range sqlGrantOnlineDDLUser {
		parsed := sqlparser.BuildParsedQuery(query, onlineDDLGrant)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return password, err
		}
	}
	return password, err
}

// dropOnlineDDLUser drops the given ddl user account at the end of migration
func (e *Executor) dropOnlineDDLUser(ctx context.Context) error {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	defer conn.Close()

	parsed := sqlparser.BuildParsedQuery(sqlDropOnlineDDLUser, onlineDDLGrant)
	_, err = conn.ExecuteFetch(parsed.Query, 0, false)
	return err
}

// tableExists checks if a given table exists.
func (e *Executor) tableExists(ctx context.Context, tableName string) (bool, error) {
	tableName = strings.ReplaceAll(tableName, `_`, `\_`)
	parsed := sqlparser.BuildParsedQuery(sqlShowTablesLike, tableName)
	rs, err := e.execQuery(ctx, parsed.Query)
	if err != nil {
		return false, err
	}
	row := rs.Named().Row()
	return (row != nil), nil
}

// showCreateTable returns the SHOW CREATE statement for a table or a view
func (e *Executor) showCreateTable(ctx context.Context, tableName string) (string, error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowCreateTable, tableName)
	rs, err := e.execQuery(ctx, parsed.Query)
	if err != nil {
		return "", err
	}
	if len(rs.Rows) == 0 {
		return "", nil
	}
	row := rs.Rows[0]
	return row[1].ToString(), nil
}

func (e *Executor) parseAlterOptions(ctx context.Context, onlineDDL *schema.OnlineDDL) string {
	// Temporary hack (2020-08-11)
	// Because sqlparser does not do full blown ALTER TABLE parsing,
	// and because we don't want gh-ost to know about WITH_GHOST and WITH_PT syntax,
	// we resort to regexp-based parsing of the query.
	// TODO(shlomi): generate _alter options_ via sqlparser when it full supports ALTER TABLE syntax.
	_, _, alterOptions := schema.ParseAlterTableOptions(onlineDDL.SQL)
	return alterOptions
}

// executeDirectly runs a DDL query directly on the backend MySQL server
func (e *Executor) executeDirectly(ctx context.Context, onlineDDL *schema.OnlineDDL, acceptableMySQLErrorCodes ...int) (acceptableErrorCodeFound bool, err error) {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return false, err
	}
	defer conn.Close()

	restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
	defer restoreSQLModeFunc()
	if err != nil {
		return false, err
	}

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint)
	_, err = conn.ExecuteFetch(onlineDDL.SQL, 0, false)

	if err != nil {
		// let's see if this error is actually acceptable
		if merr, ok := err.(*mysql.SQLError); ok {
			for _, acceptableCode := range acceptableMySQLErrorCodes {
				if merr.Num == acceptableCode {
					// we don't consider this to be an error.
					acceptableErrorCodeFound = true
					err = nil
					break
				}
			}
		}
	}
	if err != nil {
		return false, err
	}
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)

	return acceptableErrorCodeFound, nil
}

// validateTableForAlterAction checks whether a table is good to undergo a ALTER operation. It returns detailed error if not.
func (e *Executor) validateTableForAlterAction(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	// Validate table does not participate in foreign key relationship:
	for _, fkQuery := range []string{selSelectCountFKParentConstraints, selSelectCountFKChildConstraints} {
		query, err := sqlparser.ParseAndBind(fkQuery,
			sqltypes.StringBindVariable(onlineDDL.Schema),
			sqltypes.StringBindVariable(onlineDDL.Table),
		)
		if err != nil {
			return err
		}
		r, err := e.execQuery(ctx, query)
		if err != nil {
			return err
		}
		row := r.Named().Row()
		if row == nil {
			return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result from INFORMATION_SCHEMA.KEY_COLUMN_USAGE query: %s", query)
		}
		countFKConstraints := row.AsInt64("num_fk_constraints", 0)
		if countFKConstraints > 0 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s participates in FOREIGN KEY constraint. foreign key constraints are not supported in online DDL", onlineDDL.Table)
		}
	}
	return nil
}

// primaryPosition returns the MySQL/MariaDB position (typically GTID pos) on the tablet
func (e *Executor) primaryPosition(ctx context.Context) (pos mysql.Position, err error) {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return pos, err
	}
	defer conn.Close()

	pos, err = conn.PrimaryPosition()
	return pos, err
}

// terminateVReplMigration stops vreplication, then removes the _vt.vreplication entry for the given migration
func (e *Executor) terminateVReplMigration(ctx context.Context, uuid string) error {
	tmClient := tmclient.NewTabletManagerClient()
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlStopVReplStream,
		sqltypes.StringBindVariable(e.dbName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	// silently skip error; stopping the stream is just a graceful act; later deleting it is more important
	_, _ = e.vreplicationExec(ctx, tmClient, tablet.Tablet, query)

	if err := e.deleteVReplicationEntry(ctx, uuid); err != nil {
		return err
	}
	return nil
}

// cutOverVReplMigration stops vreplication, then removes the _vt.vreplication entry for the given migration
func (e *Executor) cutOverVReplMigration(ctx context.Context, s *VReplStream) error {
	// sanity checks:
	vreplTable, err := getVreplTable(ctx, s)
	if err != nil {
		return err
	}

	// get topology client & entities:
	tmClient := tmclient.NewTabletManagerClient()
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	// information about source tablet
	onlineDDL, _, err := e.readMigration(ctx, s.workflow)
	if err != nil {
		return err
	}
	isVreplicationTestSuite := onlineDDL.StrategySetting().IsVreplicationTestSuite()

	// A bit early on, we generate names for stowaway and temporary tables
	// We do this here because right now we're in a safe place where nothing happened yet. If there's an error now, bail out
	// and no harm done.
	// Later on, when traffic is blocked and tables renamed, that's a more dangerous place to be in; we want as little logic
	// in that place as possible.
	var stowawayTableName string
	if !isVreplicationTestSuite {
		stowawayTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		if err != nil {
			return err
		}
		// Audit stowawayTableName. If operation is complete, we remove the audit. But if this tablet fails while
		// the original table is renamed (into stowaway table), then this will be both the evidence and the information we need
		// to restore the table back into existence. This can (and will) be done by a different vttablet process
		if err := e.updateMigrationStowawayTable(ctx, onlineDDL.UUID, stowawayTableName); err != nil {
			return err
		}
		defer e.updateMigrationStowawayTable(ctx, onlineDDL.UUID, "")
	}

	bufferingCtx, bufferingContextCancel := context.WithCancel(ctx)
	defer bufferingContextCancel()
	// Preparation is complete. We proceed to cut-over.
	toggleBuffering := func(bufferQueries bool) error {
		e.toggleBufferTableFunc(bufferingCtx, onlineDDL.Table, bufferQueries)
		if !bufferQueries {
			// called after new table is in place.
			// unbuffer existing queries:
			bufferingContextCancel()
			// force re-read of tables
			if err := tmClient.RefreshState(ctx, tablet.Tablet); err != nil {
				return err
			}
		}
		return nil
	}
	var reenableOnce sync.Once
	reenableWritesOnce := func() {
		reenableOnce.Do(func() {
			toggleBuffering(false)
		})
	}
	// stop writes on source:
	err = toggleBuffering(true)
	defer reenableWritesOnce()
	if err != nil {
		return err
	}

	// swap out the table
	// Give a fraction of a second for a scenario where a query is in
	// query executor, it passed the ACLs and is _about to_ execute. This will be nicer to those queries:
	// they will be able to complete before the rename, rather than block briefly on the rename only to find
	// the table no longer exists.
	time.Sleep(100 * time.Millisecond)
	if isVreplicationTestSuite {
		// The testing suite may inject queries internally from the server via a recurring EVENT.
		// Those queries are unaffected by query rules (ACLs) because they don't go through Vitess.
		// We therefore hard-rename the table into an agreed upon name, and we won't swap it with
		// the original table. We will actually make the table disappear, creating a void.
		testSuiteBeforeTableName := fmt.Sprintf("%s_before", onlineDDL.Table)
		parsed := sqlparser.BuildParsedQuery(sqlRenameTable, onlineDDL.Table, testSuiteBeforeTableName)
		if _, err := e.execQuery(ctx, parsed.Query); err != nil {
			return err
		}
	} else {
		// real production
		parsed := sqlparser.BuildParsedQuery(sqlRenameTable, onlineDDL.Table, stowawayTableName)
		if _, err := e.execQuery(ctx, parsed.Query); err != nil {
			return err
		}
	}

	// We have just created a gaping hole, the original table does not exist.
	// we expect to fill that hole by swapping in the vrepl table. But if anything goes wrong we prepare
	// to rename the table back:
	defer func() {
		if _, err := e.renameTableIfApplicable(ctx, stowawayTableName, onlineDDL.Table); err != nil {
			vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "cannot rename back swapped table: %v into %v: %v", stowawayTableName, onlineDDL.Table, err)
		}
	}()
	// Right now: new queries are buffered, any existing query will have executed, and worst case scenario is
	// that some leftover query finds the table is not actually there anymore...
	// At any case, there's definitely no more writes to the table since it does not exist. We can
	// safely take the (GTID) pos now.
	postWritesPos, err := e.primaryPosition(ctx)
	if err != nil {
		return err
	}
	_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", s.workflow)

	// Writes are now disabled on table. Read up-to-date vreplication info, specifically to get latest (and fixed) pos:
	s, err = e.readVReplStream(ctx, s.workflow, false)
	if err != nil {
		return err
	}

	waitForPos := func() error {
		ctx, cancel := context.WithTimeout(ctx, 2*vreplicationCutOverThreshold)
		defer cancel()
		// Wait for target to reach the up-to-date pos
		if err := tmClient.VReplicationWaitForPos(ctx, tablet.Tablet, int(s.id), mysql.EncodePosition(postWritesPos)); err != nil {
			return err
		}
		// Target is now in sync with source!
		return nil
	}
	log.Infof("VReplication migration %v waiting for position %v", s.workflow, mysql.EncodePosition(postWritesPos))
	if err := waitForPos(); err != nil {
		return err
	}
	// Stop vreplication
	if _, err := e.vreplicationExec(ctx, tmClient, tablet.Tablet, binlogplayer.StopVReplication(uint32(s.id), "stopped for online DDL cutover")); err != nil {
		return err
	}

	// rename tables atomically (remember, writes on source tables are stopped)
	{
		if isVreplicationTestSuite {
			// this is used in Vitess endtoend testing suite
			testSuiteAfterTableName := fmt.Sprintf("%s_after", onlineDDL.Table)
			parsed := sqlparser.BuildParsedQuery(sqlRenameTable, vreplTable, testSuiteAfterTableName)
			if _, err := e.execQuery(ctx, parsed.Query); err != nil {
				return err
			}
		} else {
			// Normal (non-testing) alter table
			conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
			if err != nil {
				return err
			}
			defer conn.Close()

			parsed := sqlparser.BuildParsedQuery(sqlRenameTwoTables,
				vreplTable, onlineDDL.Table,
				stowawayTableName, vreplTable,
			)
			if _, err := e.execQuery(ctx, parsed.Query); err != nil {
				return err
			}
		}
	}
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)

	go func() {
		// Tables are swapped! Let's take the opportunity to ReloadSchema now
		// We do this in a goroutine because it might take time on a schema with thousands of tables, and we don't want to delay
		// the cut-over.
		// this means ReloadSchema is not in sync with the actual schema change. Users will still need to run tracker if they want to sync.
		// In the future, we will want to reload the single table, instead of reloading the schema.
		if err := tmClient.ReloadSchema(ctx, tablet.Tablet, ""); err != nil {
			vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Error on ReloadSchema while cutting over vreplication migration UUID: %+v", onlineDDL.UUID)
		}
	}()

	// Tables are now swapped! Migration is successful
	reenableWritesOnce() // this function is also deferred, in case of early return; but now would be a good time to resume writes, before we publish the migration as "complete"
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, s.rowsCopied, emptyHint)
	return nil

	// deferred function will re-enable writes now
	// deferred function will unlock keyspace
}

// initMigrationSQLMode sets sql_mode according to DDL strategy, and returns a function that
// restores sql_mode to original state
func (e *Executor) initMigrationSQLMode(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (deferFunc func(), err error) {
	deferFunc = func() {}
	if !onlineDDL.StrategySetting().IsAllowZeroInDateFlag() {
		// No need to change sql_mode.
		return deferFunc, nil
	}

	// Grab current sql_mode value
	rs, err := conn.ExecuteFetch(`select @@session.sql_mode as sql_mode`, 1, true)
	if err != nil {
		return deferFunc, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}
	sqlMode, err := rs.Named().Row().ToString("sql_mode")
	if err != nil {
		return deferFunc, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}
	// Pre-calculate restore function
	deferFunc = func() {
		restoreSQLModeQuery := fmt.Sprintf("set @@session.sql_mode='%s'", sqlMode)
		conn.ExecuteFetch(restoreSQLModeQuery, 0, false)
	}
	// Change sql_mode
	changeSSQLModeQuery := fmt.Sprintf("set @@session.sql_mode=REPLACE(REPLACE('%s', 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", sqlMode)
	if _, err := conn.ExecuteFetch(changeSSQLModeQuery, 0, false); err != nil {
		return deferFunc, err
	}
	return deferFunc, nil
}

func (e *Executor) initVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (v *VRepl, err error) {
	restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
	defer restoreSQLModeFunc()
	if err != nil {
		return v, err
	}

	vreplTableName := fmt.Sprintf("_%s_%s_vrepl", onlineDDL.UUID, ReadableTimestamp())
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	{
		// Apply CREATE TABLE for materialized table
		parsed := sqlparser.BuildParsedQuery(sqlCreateTableLike, vreplTableName, onlineDDL.Table)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return v, err
		}
	}
	alterOptions := e.parseAlterOptions(ctx, onlineDDL)
	{
		// Apply ALTER TABLE to materialized table
		parsed := sqlparser.BuildParsedQuery(sqlAlterTableOptions, vreplTableName, alterOptions)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return v, err
		}
	}
	v = NewVRepl(onlineDDL.UUID, e.keyspace, e.shard, e.dbName, onlineDDL.Table, vreplTableName, alterOptions)
	return v, nil
}

// postInitVreplicationOriginalMigration runs extra changes after a vreplication online DDL has been initialized.
// This function is called after both source and target tables have been analyzed, so there's more information
// about the two, and about the transition between the two.
func (e *Executor) postInitVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, v *VRepl, conn *dbconnpool.DBConnection) (err error) {
	if v.sourceAutoIncrement > 0 && !v.parser.IsAutoIncrementDefined() {
		restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
		defer restoreSQLModeFunc()
		if err != nil {
			return err
		}

		// Apply ALTER TABLE AUTO_INCREMENT=?
		parsed := sqlparser.BuildParsedQuery(sqlAlterTableAutoIncrement, v.targetTable, ":auto_increment")
		bindVars := map[string]*querypb.BindVariable{
			"auto_increment": sqltypes.Uint64BindVariable(v.sourceAutoIncrement),
		}
		bound, err := parsed.GenerateQuery(bindVars, nil)
		if err != nil {
			return err
		}
		if _, err := conn.ExecuteFetch(bound, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) initVreplicationRevertMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) (v *VRepl, err error) {
	// Getting here we've already validated that migration is revertible

	// Validation: vreplication still exists for reverted migration
	revertStream, err := e.readVReplStream(ctx, revertMigration.UUID, false)
	if err != nil {
		// cannot read the vreplication stream which we want to revert
		return nil, fmt.Errorf("can not revert vreplication migration %s because vreplication stream %s was not found", revertMigration.UUID, revertMigration.UUID)
	}

	onlineDDL.Table = revertMigration.Table
	if err := e.updateMySQLTable(ctx, onlineDDL.UUID, onlineDDL.Table); err != nil {
		return nil, err
	}

	vreplTableName, err := getVreplTable(ctx, revertStream)
	if err != nil {
		return nil, err
	}

	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	v = NewVRepl(onlineDDL.UUID, e.keyspace, e.shard, e.dbName, onlineDDL.Table, vreplTableName, "")
	v.pos = revertStream.pos
	return v, nil
}

// ExecuteWithVReplication sets up the grounds for a vreplication schema migration
func (e *Executor) ExecuteWithVReplication(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) error {
	// make sure there's no vreplication workflow running under same name
	_ = e.terminateVReplMigration(ctx, onlineDDL.UUID)

	if e.isAnyConflictingMigrationRunning(onlineDDL) {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return ErrExecutorNotWritableTablet
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()

	e.ownedRunningMigrations.Store(onlineDDL.UUID, onlineDDL)
	if err := e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint); err != nil {
		return err
	}

	var v *VRepl
	if revertMigration == nil {
		// Original ALTER TABLE request for vreplication
		v, err = e.initVreplicationOriginalMigration(ctx, onlineDDL, conn)
	} else {
		// this is a revert request
		v, err = e.initVreplicationRevertMigration(ctx, onlineDDL, revertMigration)
	}
	if err != nil {
		return err
	}
	if err := v.analyze(ctx, conn); err != nil {
		return err
	}
	if err := e.updateMigrationTableRows(ctx, onlineDDL.UUID, v.tableRows); err != nil {
		return err
	}
	removedUniqueKeyNames := []string{}
	for _, uniqueKey := range v.removedUniqueKeys {
		removedUniqueKeyNames = append(removedUniqueKeyNames, uniqueKey.Name)
	}

	if err := e.updateSchemaAnalysis(ctx, onlineDDL.UUID,
		len(v.addedUniqueKeys),
		len(v.removedUniqueKeys),
		strings.Join(sqlescape.EscapeIDs(removedUniqueKeyNames), ","),
		strings.Join(sqlescape.EscapeIDs(v.droppedNoDefaultColumnNames), ","),
		strings.Join(sqlescape.EscapeIDs(v.expandedColumnNames), ","),
		v.revertibleNotes,
	); err != nil {
		return err
	}
	if revertMigration == nil {
		// Original ALTER TABLE request for vreplication
		if err := e.validateTableForAlterAction(ctx, onlineDDL); err != nil {
			return err
		}
		if err := e.postInitVreplicationOriginalMigration(ctx, onlineDDL, v, conn); err != nil {
			return err
		}
	}

	{
		// We need to talk to tabletmanager's VREngine. But we're on TabletServer. While we live in the same
		// process as VREngine, it is actually simpler to get hold of it via gRPC, just like wrangler does.
		tmClient := tmclient.NewTabletManagerClient()
		tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
		if err != nil {
			return err
		}

		// reload schema before migration
		if err := tmClient.ReloadSchema(ctx, tablet.Tablet, ""); err != nil {
			return err
		}

		// create vreplication entry
		insertVReplicationQuery, err := v.generateInsertStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tmClient, tablet.Tablet, insertVReplicationQuery); err != nil {
			return err
		}

		{
			// temporary hack. todo: this should be done when inserting any _vt.vreplication record across all workflow types
			query := fmt.Sprintf("update _vt.vreplication set workflow_type = %d where workflow = '%s'",
				binlogdatapb.VReplicationWorkflowType_ONLINEDDL, v.workflow)
			if _, err := e.vreplicationExec(ctx, tmClient, tablet.Tablet, query); err != nil {
				return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", tablet.Tablet, query)
			}
		}
		// start stream!
		startVReplicationQuery, err := v.generateStartStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tmClient, tablet.Tablet, startVReplicationQuery); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteWithGhost validates and runs a gh-ost process.
// Validation included testing the backend MySQL server and the gh-ost binary itself
// Execution runs first a dry run, then an actual migration
func (e *Executor) ExecuteWithGhost(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	if e.isAnyConflictingMigrationRunning(onlineDDL) {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return ErrExecutorNotWritableTablet
	}
	variables, err := e.readMySQLVariables(ctx)
	if err != nil {
		log.Errorf("Error before running gh-ost: %+v", err)
		return err
	}
	if variables.readOnly {
		err := fmt.Errorf("Error before running gh-ost: MySQL server is read_only")
		log.Errorf(err.Error())
		return err
	}
	onlineDDLPassword, err := e.createOnlineDDLUser(ctx)
	if err != nil {
		err := fmt.Errorf("Error creating gh-ost user: %+v", err)
		log.Errorf(err.Error())
		return err
	}
	tempDir, err := createTempDir(onlineDDL.UUID)
	if err != nil {
		log.Errorf("Error creating temporary directory: %+v", err)
		return err
	}
	binaryFileName, _ := GhostBinaryFileName()
	credentialsConfigFileContent := fmt.Sprintf(`[client]
user=%s
password=${ONLINE_DDL_PASSWORD}
`, onlineDDLUser)
	credentialsConfigFileName, err := createTempScript(tempDir, "gh-ost-conf.cfg", credentialsConfigFileContent)
	if err != nil {
		log.Errorf("Error creating config file: %+v", err)
		return err
	}
	wrapperScriptContent := fmt.Sprintf(`#!/bin/bash
ghost_log_path="%s"
ghost_log_file="%s"
ghost_log_failure_file="%s"

mkdir -p "$ghost_log_path"

export ONLINE_DDL_PASSWORD
%s "$@" > "$ghost_log_path/$ghost_log_file" 2>&1
exit_code=$?
grep -o '\bFATAL\b.*' "$ghost_log_path/$ghost_log_file" | tail -1 > "$ghost_log_path/$ghost_log_failure_file"
exit $exit_code
	`, tempDir, migrationLogFileName, migrationFailureFileName, binaryFileName,
	)
	wrapperScriptFileName, err := createTempScript(tempDir, "gh-ost-wrapper.sh", wrapperScriptContent)
	if err != nil {
		log.Errorf("Error creating wrapper script: %+v", err)
		return err
	}
	onHookContent := func(status schema.OnlineDDLStatus, hint string) string {
		return fmt.Sprintf(`#!/bin/bash
	curl --max-time 10 -s 'http://localhost:%d/schema-migration/report-status?uuid=%s&status=%s&hint=%s&dryrun='"$GH_OST_DRY_RUN"'&progress='"$GH_OST_PROGRESS"'&eta='"$GH_OST_ETA_SECONDS"'&rowscopied='"$GH_OST_COPIED_ROWS"
			`, *servenv.Port, onlineDDL.UUID, string(status), hint)
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-startup", onHookContent(schema.OnlineDDLStatusRunning, emptyHint)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-status", onHookContent(schema.OnlineDDLStatusRunning, emptyHint)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-success", onHookContent(schema.OnlineDDLStatusComplete, emptyHint)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-failure", onHookContent(schema.OnlineDDLStatusFailed, emptyHint)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-begin-postponed", onHookContent(schema.OnlineDDLStatusRunning, readyToCompleteHint)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	serveSocketFile := path.Join(tempDir, "serve.sock")

	if err := e.deleteGhostPanicFlagFile(onlineDDL.UUID); err != nil {
		log.Errorf("Error removing gh-ost panic flag file %s: %+v", e.ghostPanicFlagFileName(onlineDDL.UUID), err)
		return err
	}
	if err := e.deleteGhostPostponeFlagFile(onlineDDL.UUID); err != nil {
		log.Errorf("Error removing gh-ost postpone flag file %s before migration: %+v", e.ghostPostponeFlagFileName(onlineDDL.UUID), err)
		return err
	}
	// Validate gh-ost binary:
	_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "validating gh-ost --version")
	log.Infof("Will now validate gh-ost binary")
	_, err = execCmd(
		"bash",
		[]string{
			wrapperScriptFileName,
			"--version",
		},
		os.Environ(),
		"/tmp",
		nil,
		nil,
	)
	if err != nil {
		log.Errorf("Error testing gh-ost binary: %+v", err)
		return err
	}
	_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "validated gh-ost --version")
	log.Infof("+ OK")

	if err := e.updateMigrationLogPath(ctx, onlineDDL.UUID, variables.host, tempDir); err != nil {
		return err
	}

	runGhost := func(execute bool) error {
		alterOptions := e.parseAlterOptions(ctx, onlineDDL)
		forceTableNames := fmt.Sprintf("%s_%s", onlineDDL.UUID, ReadableTimestamp())

		if err := e.updateArtifacts(ctx, onlineDDL.UUID,
			fmt.Sprintf("_%s_gho", forceTableNames),
			fmt.Sprintf("_%s_ghc", forceTableNames),
			fmt.Sprintf("_%s_del", forceTableNames),
		); err != nil {
			return err
		}

		os.Setenv("ONLINE_DDL_PASSWORD", onlineDDLPassword)
		args := []string{
			wrapperScriptFileName,
			fmt.Sprintf(`--host=%s`, variables.host),
			fmt.Sprintf(`--port=%d`, variables.port),
			fmt.Sprintf(`--conf=%s`, credentialsConfigFileName), // user & password found here
			`--allow-on-master`,
			`--max-load=Threads_running=900`,
			`--critical-load=Threads_running=1000`,
			`--critical-load-hibernate-seconds=60`,
			`--approve-renamed-columns`,
			`--debug`,
			`--exact-rowcount`,
			`--default-retries=120`,
			fmt.Sprintf("--force-table-names=%s", forceTableNames),
			fmt.Sprintf("--serve-socket-file=%s", serveSocketFile),
			fmt.Sprintf("--hooks-path=%s", tempDir),
			fmt.Sprintf(`--hooks-hint-token=%s`, onlineDDL.UUID),
			fmt.Sprintf(`--throttle-http=http://localhost:%d/throttler/check?app=online-ddl:gh-ost:%s&p=low`, *servenv.Port, onlineDDL.UUID),
			fmt.Sprintf(`--database=%s`, e.dbName),
			fmt.Sprintf(`--table=%s`, onlineDDL.Table),
			fmt.Sprintf(`--alter=%s`, alterOptions),
			fmt.Sprintf(`--panic-flag-file=%s`, e.ghostPanicFlagFileName(onlineDDL.UUID)),
			fmt.Sprintf(`--execute=%t`, execute),
		}
		if onlineDDL.StrategySetting().IsAllowZeroInDateFlag() {
			args = append(args, "--allow-zero-in-date")
		}
		if execute && onlineDDL.StrategySetting().IsPostponeCompletion() {
			args = append(args, "--postpone-cut-over-flag-file", e.ghostPostponeFlagFileName(onlineDDL.UUID))
		}

		args = append(args, onlineDDL.StrategySetting().RuntimeOptions()...)
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("executing gh-ost --execute=%v", execute))
		_, err := execCmd("bash", args, os.Environ(), "/tmp", nil, nil)
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("executed gh-ost --execute=%v, err=%v", execute, err))
		if err != nil {
			// See if we can get more info from the failure file
			if content, ferr := os.ReadFile(path.Join(tempDir, migrationFailureFileName)); ferr == nil {
				failureMessage := strings.TrimSpace(string(content))
				if failureMessage != "" {
					// This message was produced by gh-ost itself. It is more informative than the default "migration failed..." message. Overwrite.
					return errors.New(failureMessage)
				}
			}
		}
		return err
	}

	e.ownedRunningMigrations.Store(onlineDDL.UUID, onlineDDL)

	go func() error {
		defer e.ownedRunningMigrations.Delete(onlineDDL.UUID)
		defer e.deleteGhostPostponeFlagFile(onlineDDL.UUID) // irrespective whether the file was in fact in use or not
		defer e.dropOnlineDDLUser(ctx)
		defer e.gcArtifacts(ctx)

		log.Infof("Will now dry-run gh-ost on: %s:%d", variables.host, variables.port)
		if err := runGhost(false); err != nil {
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())

			log.Errorf("Error executing gh-ost dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run gh-ost on: %s:%d", variables.host, variables.port)
		startedMigrations.Add(1)
		if err := runGhost(true); err != nil {
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())
			failedMigrations.Add(1)
			log.Errorf("Error running gh-ost: %+v", err)
			return err
		}
		// Migration successful!
		successfulMigrations.Add(1)
		log.Infof("+ OK")
		return nil
	}()
	return nil
}

// ExecuteWithPTOSC validates and runs a pt-online-schema-change process.
// Validation included testing the backend MySQL server and the pt-online-schema-change binary itself
// Execution runs first a dry run, then an actual migration
func (e *Executor) ExecuteWithPTOSC(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	if e.isAnyConflictingMigrationRunning(onlineDDL) {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return ErrExecutorNotWritableTablet
	}
	variables, err := e.readMySQLVariables(ctx)
	if err != nil {
		log.Errorf("Error before running pt-online-schema-change: %+v", err)
		return err
	}
	if variables.readOnly {
		err := fmt.Errorf("Error before running pt-online-schema-change: MySQL server is read_only")
		log.Errorf(err.Error())
		return err
	}
	onlineDDLPassword, err := e.createOnlineDDLUser(ctx)
	if err != nil {
		err := fmt.Errorf("Error creating pt-online-schema-change user: %+v", err)
		log.Errorf(err.Error())
		return err
	}
	tempDir, err := createTempDir(onlineDDL.UUID)
	if err != nil {
		log.Errorf("Error creating temporary directory: %+v", err)
		return err
	}

	binaryFileName, _ := PTOSCFileName()
	wrapperScriptContent := fmt.Sprintf(`#!/bin/bash
pt_log_path="%s"
pt_log_file="%s"

mkdir -p "$pt_log_path"

export MYSQL_PWD
%s "$@" > "$pt_log_path/$pt_log_file" 2>&1
	`, tempDir, migrationLogFileName, binaryFileName,
	)
	wrapperScriptFileName, err := createTempScript(tempDir, "pt-online-schema-change-wrapper.sh", wrapperScriptContent)
	if err != nil {
		log.Errorf("Error creating wrapper script: %+v", err)
		return err
	}
	pluginCode := `
	package pt_online_schema_change_plugin;

	use strict;
	use LWP::Simple;

	sub new {
	  my($class, % args) = @_;
	  my $self = { %args };
	  return bless $self, $class;
	}

	sub init {
	  my($self, % args) = @_;
	}

	sub before_create_new_table {
	  my($self, % args) = @_;
	  get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusRunning}}&hint=&dryrun={{DRYRUN}}");
	}

	sub before_exit {
		my($self, % args) = @_;
		my $exit_status = $args{exit_status};
	  if ($exit_status == 0) {
	    get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusComplete}}&hint=&dryrun={{DRYRUN}}");
	  } else {
	    get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusFailed}}&hint=&dryrun={{DRYRUN}}");
	  }
	}

	sub get_slave_lag {
		my ($self, %args) = @_;

		return sub {
			if (head("http://localhost:{{VTTABLET_PORT}}/throttler/check?app=online-ddl:pt-osc:{{MIGRATION_UUID}}&p=low")) {
				# Got HTTP 200 OK, means throttler is happy
				return 0;
			}	else {
				# Throttler requests to hold back
				return 2147483647; # maxint, report *very* high lag
			}
		};
	}

	1;
	`
	pluginCode = strings.ReplaceAll(pluginCode, "{{VTTABLET_PORT}}", fmt.Sprintf("%d", *servenv.Port))
	pluginCode = strings.ReplaceAll(pluginCode, "{{MIGRATION_UUID}}", onlineDDL.UUID)
	pluginCode = strings.ReplaceAll(pluginCode, "{{OnlineDDLStatusRunning}}", string(schema.OnlineDDLStatusRunning))
	pluginCode = strings.ReplaceAll(pluginCode, "{{OnlineDDLStatusComplete}}", string(schema.OnlineDDLStatusComplete))
	pluginCode = strings.ReplaceAll(pluginCode, "{{OnlineDDLStatusFailed}}", string(schema.OnlineDDLStatusFailed))

	// Validate pt-online-schema-change binary:
	log.Infof("Will now validate pt-online-schema-change binary")
	_, err = execCmd(
		"bash",
		[]string{
			wrapperScriptFileName,
			"--version",
		},
		os.Environ(),
		"/tmp",
		nil,
		nil,
	)
	if err != nil {
		log.Errorf("Error testing pt-online-schema-change binary: %+v", err)
		return err
	}
	log.Infof("+ OK")

	if err := e.updateMigrationLogPath(ctx, onlineDDL.UUID, variables.host, tempDir); err != nil {
		return err
	}

	alterOptions := e.parseAlterOptions(ctx, onlineDDL)

	// The following sleep() is temporary and artificial. Because we create a new user for this
	// migration, and because we throttle by replicas, we need to wait for the replicas to be
	// caught up with the new user creation. Otherwise, the OSC tools will fail connecting to the replicas...
	// Once we have a built in throttling service , we will no longe rneed to have the OSC tools probe the
	// replicas. Instead, they will consult with our throttling service.
	// TODO(shlomi): replace/remove this when we have a proper throttling solution
	time.Sleep(time.Second)

	runPTOSC := func(execute bool) error {
		os.Setenv("MYSQL_PWD", onlineDDLPassword)
		newTableName := fmt.Sprintf("_%s_%s_new", onlineDDL.UUID, ReadableTimestamp())

		if err := e.updateArtifacts(ctx, onlineDDL.UUID,
			fmt.Sprintf("_%s_old", onlineDDL.Table),
			fmt.Sprintf("__%s_old", onlineDDL.Table),
			newTableName,
		); err != nil {
			return err
		}

		executeFlag := "--dry-run"
		if execute {
			executeFlag = "--execute"
		}
		finalPluginCode := strings.ReplaceAll(pluginCode, "{{DRYRUN}}", fmt.Sprintf("%t", !execute))
		pluginFile, err := createTempScript(tempDir, "pt-online-schema-change-plugin", finalPluginCode)
		if err != nil {
			log.Errorf("Error creating script: %+v", err)
			return err
		}
		args := []string{
			wrapperScriptFileName,
			`--pid`,
			e.ptPidFileName(onlineDDL.UUID),
			`--plugin`,
			pluginFile,
			`--new-table-name`,
			newTableName,
			`--alter`,
			alterOptions,
			`--check-slave-lag`, // We use primary's identity so that pt-online-schema-change calls our lag plugin for exactly 1 server
			fmt.Sprintf(`h=%s,P=%d,D=%s,t=%s,u=%s`, variables.host, variables.port, e.dbName, onlineDDL.Table, onlineDDLUser),
			executeFlag,
			fmt.Sprintf(`h=%s,P=%d,D=%s,t=%s,u=%s`, variables.host, variables.port, e.dbName, onlineDDL.Table, onlineDDLUser),
		}

		if execute {
			args = append(args,
				`--no-drop-new-table`,
				`--no-drop-old-table`,
			)
		}
		args = append(args, onlineDDL.StrategySetting().RuntimeOptions()...)
		_, err = execCmd("bash", args, os.Environ(), "/tmp", nil, nil)
		return err
	}

	e.ownedRunningMigrations.Store(onlineDDL.UUID, onlineDDL)

	go func() error {
		defer e.ownedRunningMigrations.Delete(onlineDDL.UUID)
		defer e.dropOnlineDDLUser(ctx)
		defer e.gcArtifacts(ctx)

		log.Infof("Will now dry-run pt-online-schema-change on: %s:%d", variables.host, variables.port)
		if err := runPTOSC(false); err != nil {
			// perhaps pt-osc was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			log.Errorf("Error executing pt-online-schema-change dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run pt-online-schema-change on: %s:%d", variables.host, variables.port)
		startedMigrations.Add(1)
		if err := runPTOSC(true); err != nil {
			// perhaps pt-osc was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			_ = e.dropPTOSCMigrationTriggers(ctx, onlineDDL)
			failedMigrations.Add(1)
			log.Errorf("Error running pt-online-schema-change: %+v", err)
			return err
		}
		// Migration successful!
		successfulMigrations.Add(1)
		log.Infof("+ OK")
		return nil
	}()
	return nil
}

func (e *Executor) readMigration(ctx context.Context, uuid string) (onlineDDL *schema.OnlineDDL, row sqltypes.RowNamedValues, err error) {

	parsed := sqlparser.BuildParsedQuery(sqlSelectMigration, ":migration_uuid")
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return onlineDDL, nil, err
	}
	r, err := e.execQuery(ctx, bound)
	if err != nil {
		return onlineDDL, nil, err
	}
	row = r.Named().Row()
	if row == nil {
		// No results
		return nil, nil, ErrMigrationNotFound
	}
	onlineDDL = &schema.OnlineDDL{
		Keyspace:         row["keyspace"].ToString(),
		Table:            row["mysql_table"].ToString(),
		Schema:           row["mysql_schema"].ToString(),
		SQL:              row["migration_statement"].ToString(),
		UUID:             row["migration_uuid"].ToString(),
		Strategy:         schema.DDLStrategy(row["strategy"].ToString()),
		Options:          row["options"].ToString(),
		Status:           schema.OnlineDDLStatus(row["migration_status"].ToString()),
		Retries:          row.AsInt64("retries", 0),
		TabletAlias:      row["tablet"].ToString(),
		MigrationContext: row["migration_context"].ToString(),
	}
	return onlineDDL, row, nil
}

// readPendingMigrationsUUIDs returns UUIDs for migrations in pending state (queued/ready/running)
func (e *Executor) readPendingMigrationsUUIDs(ctx context.Context) (uuids []string, err error) {
	r, err := e.execQuery(ctx, sqlSelectPendingMigrations)
	if err != nil {
		return uuids, err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		uuids = append(uuids, uuid)
	}
	return uuids, err
}

// terminateMigration attempts to interrupt and hard-stop a running migration
func (e *Executor) terminateMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) (foundRunning bool, err error) {
	// It's possible the killing the migration fails for whatever reason, in which case
	// the logic will retry killing it later on.
	// Whatever happens in this function, this executor stops owning the given migration.
	defer e.ownedRunningMigrations.Delete(onlineDDL.UUID)

	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
		// migration could have started by a different tablet. We need to actively verify if it is running
		s, _ := e.readVReplStream(ctx, onlineDDL.UUID, true)
		foundRunning = (s != nil && s.isRunning())
		if err := e.terminateVReplMigration(ctx, onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error terminating migration, vreplication exec error: %+v", err)
		}
		_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
	case schema.DDLStrategyPTOSC:
		// see if pt-osc is running (could have been executed by this vttablet or one that crashed in the past)
		if running, pid, _ := e.isPTOSCMigrationRunning(ctx, onlineDDL.UUID); running {
			foundRunning = true
			// Because pt-osc doesn't offer much control, we take a brute force approach to killing it,
			// revoking its privileges, and cleaning up its triggers.
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
				return foundRunning, nil
			}
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
				return foundRunning, nil
			}
			if err := e.dropOnlineDDLUser(ctx); err != nil {
				return foundRunning, nil
			}
			if err := e.dropPTOSCMigrationTriggers(ctx, onlineDDL); err != nil {
				return foundRunning, nil
			}
		}
	case schema.DDLStrategyGhost:
		// double check: is the running migration the very same one we wish to cancel?
		if _, ok := e.ownedRunningMigrations.Load(onlineDDL.UUID); ok {
			// assuming all goes well in next steps, we can already report that there has indeed been a migration
			foundRunning = true
		}
		// gh-ost migrations are easy to kill: just touch their specific panic flag files. We trust
		// gh-ost to terminate. No need to KILL it. And there's no trigger cleanup.
		if err := e.createGhostPanicFlagFile(onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error terminating gh-ost migration, flag file error: %+v", err)
		}
	}
	return foundRunning, nil
}

// CancelMigration attempts to abort a scheduled or a running migration
func (e *Executor) CancelMigration(ctx context.Context, uuid string, message string) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var rowsAffected uint64

	onlineDDL, _, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed:
		return emptyResult, nil
	case schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady:
		log.Infof("CancelMigration: cancelling %s with status: %v", uuid, onlineDDL.Status)
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusCancelled); err != nil {
			return nil, err
		}
		rowsAffected = 1
	}
	defer e.triggerNextCheckInterval()

	migrationFound, err := e.terminateMigration(ctx, onlineDDL)
	defer e.updateMigrationMessage(ctx, onlineDDL.UUID, message)

	if migrationFound {
		log.Infof("CancelMigration: terminated %s with status: %v", uuid, onlineDDL.Status)
		rowsAffected = 1
	}
	if err != nil {
		return result, err
	}

	result = &sqltypes.Result{
		RowsAffected: rowsAffected,
	}
	return result, nil
}

// cancelMigrations attempts to abort a list of migrations
func (e *Executor) cancelMigrations(ctx context.Context, cancellable []*cancellableMigration) (err error) {
	for _, migration := range cancellable {
		log.Infof("cancelMigrations: cancelling %s; reason: %s", migration.uuid, migration.message)
		if _, err := e.CancelMigration(ctx, migration.uuid, migration.message); err != nil {
			return err
		}
	}
	return nil
}

// CancelPendingMigrations cancels all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) CancelPendingMigrations(ctx context.Context, message string) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("CancelPendingMigrations: cancelling %s", uuid)
		res, err := e.CancelMigration(ctx, uuid, message)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	return result, nil
}

// scheduleNextMigration attemps to schedule a single migration to run next.
// possibly there are migrations to run.
// The effect of this function is to move a migration from 'queued' state to 'ready' state, is all.
func (e *Executor) scheduleNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var onlyScheduleOneMigration sync.Once

	r, err := e.execQuery(ctx, sqlSelectQueuedMigrations)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		postponeCompletion := row.AsBool("postpone_completion", false)
		readyToComplete := row.AsBool("ready_to_complete", false)
		ddlAction := row["ddl_action"].ToString()

		if !readyToComplete {
			// Whether postponsed or not, CREATE and DROP operations are inherently "ready to complete"
			// because their operation is instantaneous.
			switch ddlAction {
			case sqlparser.CreateStr, sqlparser.DropStr:
				if err := e.updateMigrationReadyToComplete(ctx, uuid, true); err != nil {
					return err
				}
			}
		}
		if ddlAction == sqlparser.AlterStr || !postponeCompletion {
			// Any non-postponed migration can be scheduled
			// postponed ALTER can be scheduled
			// We only schedule a single migration in the execution of this function
			onlyScheduleOneMigration.Do(func() {
				err = e.updateMigrationStatus(ctx, uuid, schema.OnlineDDLStatusReady)
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

// reviewQueuedMigrations iterates queued migrations and sees if any information needs to be updated
func (e *Executor) reviewQueuedMigrations(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	// Review REVERT migrations
	// These migrations are submitted with some details missing. This is because the statement
	//   REVERT VITESS_MIGRATION '<uuid>'
	// doesn't have much detail, we need to extract the info from the reverted migration. Missing details:
	// - What table is affected?
	// - What ddl action (CREATE, DROP, ALTER) is being reverted, or what is the counter-operation to be executed?

	r, err := e.execQuery(ctx, sqlSelectQueuedRevertMigrations)
	if err != nil {
		return err
	}

	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		onlineDDL, _, err := e.readMigration(ctx, uuid)
		if err != nil {
			return err
		}
		reviewEmptyTableRevertMigrations := func() error {
			if onlineDDL.Table != "" {
				return nil
			}
			// Table name is empty. Let's populate it.

			// Try to update table name and ddl_action
			// Failure to do so fails the migration
			revertUUID, err := onlineDDL.GetRevertUUID()
			if err != nil {
				return e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot analyze revert UUID for revert migration %s: %v", onlineDDL.UUID, err))
			}
			revertedMigration, row, err := e.readMigration(ctx, revertUUID)
			if err != nil {
				return e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot read migration %s reverted by migration %s: %s", revertUUID, onlineDDL.UUID, err))
			}
			revertedActionStr := row["ddl_action"].ToString()
			mimickedActionStr := ""

			switch revertedActionStr {
			case sqlparser.CreateStr:
				mimickedActionStr = sqlparser.DropStr
			case sqlparser.DropStr:
				mimickedActionStr = sqlparser.CreateStr
			case sqlparser.AlterStr:
				mimickedActionStr = sqlparser.AlterStr
			default:
				return e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertedMigration.UUID, revertedActionStr))
			}
			if err := e.updateDDLAction(ctx, onlineDDL.UUID, mimickedActionStr); err != nil {
				return err
			}
			if err := e.updateMigrationIsView(ctx, onlineDDL.UUID, row.AsBool("is_view", false)); err != nil {
				return err
			}
			if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertedMigration.Table); err != nil {
				return err
			}
			return nil
		}
		if err := reviewEmptyTableRevertMigrations(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) validateMigrationRevertible(ctx context.Context, revertMigration *schema.OnlineDDL, revertingMigrationUUID string) (err error) {
	// Validation: migration to revert exists and is in complete state
	action, actionStr, err := revertMigration.GetActionStr()
	if err != nil {
		return err
	}
	switch action {
	case sqlparser.AlterDDLAction:
		if revertMigration.Strategy != schema.DDLStrategyOnline && revertMigration.Strategy != schema.DDLStrategyVitess {
			return fmt.Errorf("can only revert a %s strategy migration. Migration %s has %s strategy", schema.DDLStrategyOnline, revertMigration.UUID, revertMigration.Strategy)
		}
	case sqlparser.RevertDDLAction:
	case sqlparser.CreateDDLAction:
	case sqlparser.DropDDLAction:
	default:
		return fmt.Errorf("cannot revert migration %s: unexpected action %s", revertMigration.UUID, actionStr)
	}
	if revertMigration.Status != schema.OnlineDDLStatusComplete {
		return fmt.Errorf("can only revert a migration in a '%s' state. Migration %s is in '%s' state", schema.OnlineDDLStatusComplete, revertMigration.UUID, revertMigration.Status)
	}
	{
		// Validation: see if there's a pending migration on this table:
		r, err := e.execQuery(ctx, sqlSelectPendingMigrations)
		if err != nil {
			return err
		}
		// we identify running migrations on requested table
		for _, row := range r.Named().Rows {
			pendingUUID := row["migration_uuid"].ToString()
			if pendingUUID == revertingMigrationUUID {
				// that's fine; the migration we're looking at is the very one that's trying to issue this revert
				continue
			}
			keyspace := row["keyspace"].ToString()
			table := row["mysql_table"].ToString()
			status := schema.OnlineDDLStatus(row["migration_status"].ToString())

			if keyspace == e.keyspace && table == revertMigration.Table {
				return fmt.Errorf("can not revert migration %s on table %s because migration %s is in %s status. May only revert if all migrations on this table are completed or failed", revertMigration.UUID, revertMigration.Table, pendingUUID, status)
			}
		}
		{
			// Validation: see that we're reverting the last successful migration on this table:
			query, err := sqlparser.ParseAndBind(sqlSelectCompleteMigrationsOnTable,
				sqltypes.StringBindVariable(e.keyspace),
				sqltypes.StringBindVariable(revertMigration.Table),
			)
			if err != nil {
				return err
			}
			r, err := e.execQuery(ctx, query)
			if err != nil {
				return err
			}
			for _, row := range r.Named().Rows {
				completeUUID := row["migration_uuid"].ToString()
				if completeUUID != revertMigration.UUID {
					return fmt.Errorf("can not revert migration %s on table %s because it is not the last migration to complete on that table. The last migration to complete was %s", revertMigration.UUID, revertMigration.Table, completeUUID)
				}
			}
		}
	}
	return nil
}

// executeRevert is called for 'revert' migrations (SQL is of the form "revert 99caeca2_74e2_11eb_a693_f875a4d24e90", not a real SQL of course).
// In this function we:
// - figure out whether the revert is valid: can we really revert requested migration?
// - what type of migration we're reverting? (CREATE/DROP/ALTER)
// - revert appropriately to the type of migration
func (e *Executor) executeRevert(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	revertUUID, err := onlineDDL.GetRevertUUID()
	if err != nil {
		return fmt.Errorf("cannot run a revert migration %v: %+v", onlineDDL.UUID, err)
	}

	revertMigration, row, err := e.readMigration(ctx, revertUUID)
	if err != nil {
		return err
	}
	if err := e.validateMigrationRevertible(ctx, revertMigration, onlineDDL.UUID); err != nil {
		return err
	}
	revertedActionStr := row["ddl_action"].ToString()
	if onlineDDL.Table == "" {
		// table name should be populated by reviewQueuedMigrations
		// but this was a newly added functionality. To be backwards compatible,
		// we double check here, and populate table name and ddl_action.

		// TODO: remove in v14
		mimickedActionStr := ""

		switch revertedActionStr {
		case sqlparser.CreateStr:
			mimickedActionStr = sqlparser.DropStr
		case sqlparser.DropStr:
			mimickedActionStr = sqlparser.CreateStr
		case sqlparser.AlterStr:
			mimickedActionStr = sqlparser.AlterStr
		default:
			return fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertMigration.UUID, revertedActionStr)
		}
		if err := e.updateDDLAction(ctx, onlineDDL.UUID, mimickedActionStr); err != nil {
			return err
		}
		if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertMigration.Table); err != nil {
			return err
		}
	}

	switch revertedActionStr {
	case sqlparser.CreateStr:
		{
			// We are reverting a CREATE migration. The revert is to DROP, only we don't actually
			// drop the table, we rename it into lifecycle
			// Possibly this was a CREATE TABLE IF NOT EXISTS, and possibly the table already existed
			// before the DDL, in which case the CREATE was a noop. In that scenario we _do not_ drop
			// the table.
			// We can tell the difference by looking at the artifacts. A successful CREATE TABLE, where
			// a table actually gets created, has a sentry, dummy artifact. A noop has not.

			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// This indicates no table was actually created. this must have been a CREATE TABLE IF NOT EXISTS where the table already existed.
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			}

			for _, artifactTable := range artifactTables {
				if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
					return err
				}
				onlineDDL.SQL = sqlparser.BuildParsedQuery(sqlRenameTable, revertMigration.Table, artifactTable).Query
				if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
					return err
				}
			}
		}
	case sqlparser.DropStr:
		{
			// We are reverting a DROP migration. But the table wasn't really dropped, because that's not how
			// we run DROP migrations. It was renamed. So we need to rename it back.
			// But we impose as if we are now CREATE-ing the table.

			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// Could happen on `DROP TABLE IF EXISTS` where the table did not exist...
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			}
			for _, artifactTable := range artifactTables {
				if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
					return err
				}
				onlineDDL.SQL = sqlparser.BuildParsedQuery(sqlRenameTable, artifactTable, revertMigration.Table).Query
				if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
					return err
				}
			}
		}
	case sqlparser.AlterStr:
		{
			if row.AsBool("is_view", false) {
				artifacts := row["artifacts"].ToString()
				artifactTables := textutil.SplitDelimitedList(artifacts)
				if len(artifactTables) > 1 {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
				}
				if len(artifactTables) == 0 {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot run migration %s reverting %s: found %d artifact tables, expected 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
				}
				for _, artifactTable := range artifactTables {
					if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
						return err
					}
					onlineDDL.SQL, _, err = e.generateSwapTablesStatement(ctx, onlineDDL.Table, artifactTable)
					if err != nil {
						return err
					}
					if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
						return err
					}
				}
				return nil
			}
			// Real table
			if err := e.ExecuteWithVReplication(ctx, onlineDDL, revertMigration); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertMigration.UUID, revertedActionStr)
	}

	return nil
}

// evaluateDeclarativeDiff is called for -declarative CREATE statements, where the table already exists. The function generates a SQL diff, which can be:
// - empty, in which case the migration is noop and implicitly successful, or
// - non-empty, in which case the migration turns to be an ALTER
func (e *Executor) evaluateDeclarativeDiff(ctx context.Context, onlineDDL *schema.OnlineDDL) (diff schemadiff.EntityDiff, err error) {

	// Modify the CREATE TABLE statement to indicate a different, made up table name, known as the "comparison table"
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return nil, err
	}
	// Is this CREATE TABLE or CREATE VIEW?
	comparisonTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return nil, err
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	{
		// Create the comparison table
		ddlStmt.SetTable("", comparisonTableName)
		modifiedCreateSQL := sqlparser.String(ddlStmt)

		restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
		defer restoreSQLModeFunc()
		if err != nil {
			return nil, err
		}

		if _, err := conn.ExecuteFetch(modifiedCreateSQL, 0, false); err != nil {
			return nil, err
		}

		defer func() {
			// Drop the comparison table
			parsed := sqlparser.BuildParsedQuery(sqlDropTable, comparisonTableName)
			_, _ = conn.ExecuteFetch(parsed.Query, 0, false)
			// Nothing bad happens for not checking the error code. The table is GC/HOLD. If we
			// can't drop it now, it still gets collected later by tablegc mechanism
		}()
	}

	existingShowCreateTable, err := e.showCreateTable(ctx, onlineDDL.Table)
	if err != nil {
		return nil, err
	}
	if existingShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "unexpected: cannot find table or view %v", onlineDDL.Table)
	}
	newShowCreateTable, err := e.showCreateTable(ctx, comparisonTableName)
	if err != nil {
		return nil, err
	}
	if newShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected: cannot find table or view even as it was just created: %v", onlineDDL.Table)
	}
	hints := &schemadiff.DiffHints{AutoIncrementStrategy: schemadiff.AutoIncrementApplyHigher}
	switch ddlStmt.(type) {
	case *sqlparser.CreateTable:
		diff, err = schemadiff.DiffCreateTablesQueries(existingShowCreateTable, newShowCreateTable, hints)
	case *sqlparser.CreateView:
		diff, err = schemadiff.DiffCreateViewsQueries(existingShowCreateTable, newShowCreateTable, hints)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected CREATE TABLE or CREATE VIEW in online DDL statement: %v", onlineDDL.SQL)
	}
	if err != nil {
		return nil, err
	}
	return diff, nil
}

// getCompletedMigrationByContextAndSQL chceks if there exists a completed migration with exact same
// context and SQL as given migration. If so, it returns its UUID.
func (e *Executor) getCompletedMigrationByContextAndSQL(ctx context.Context, onlineDDL *schema.OnlineDDL) (completedUUID string, err error) {
	if onlineDDL.MigrationContext == "" {
		// only applies to migrations with an explicit context
		return "", nil
	}
	query, err := sqlparser.ParseAndBind(sqlSelectCompleteMigrationsByContextAndSQL,
		sqltypes.StringBindVariable(e.keyspace),
		sqltypes.StringBindVariable(onlineDDL.MigrationContext),
		sqltypes.StringBindVariable(onlineDDL.SQL),
	)
	if err != nil {
		return "", err
	}
	r, err := e.execQuery(ctx, query)
	if err != nil {
		return "", err
	}
	for _, row := range r.Named().Rows {
		completedUUID = row["migration_uuid"].ToString()
	}
	return completedUUID, nil
}

// failMigration marks a migration as failed
func (e *Executor) failMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, err error) error {
	_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
	if err != nil {
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())
	}
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)
	return err
}

func (e *Executor) executeDropDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	// Drop statement.
	// Normally, we're going to modify DROP to RENAME (see later on). But if table name is
	// already a GC-lifecycle table, then we don't put it through yet another GC lifecycle,
	// we just drop it.
	if schema.IsGCTableName(onlineDDL.Table) {
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
		return nil
	}

	// We transform a DROP TABLE into a RENAME TABLE statement, so as to remove the table safely and asynchronously.

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}

	var toTableName string
	onlineDDL.SQL, toTableName, err = schema.GenerateRenameStatementWithUUID(onlineDDL.Table, schema.HoldTableGCState, onlineDDL.GetGCUUID(), newGCTableRetainTime())
	if err != nil {
		return failMigration(err)
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, toTableName); err != nil {
		return err
	}

	acceptableErrorCodes := []int{}
	if ddlStmt.GetIfExists() {
		acceptableErrorCodes = acceptableDropTableIfExistsErrorCodes
	}
	acceptableErrCodeFound, err := e.executeDirectly(ctx, onlineDDL, acceptableErrorCodes...)
	if err != nil {
		return failMigration(err)
	}
	if acceptableErrCodeFound {
		// Table did not exist after all. There is no artifact
		if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) executeCreateDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}
	if _, isCreateView := ddlStmt.(*sqlparser.CreateView); isCreateView {
		if ddlStmt.GetIsReplace() {
			// This is a CREATE OR REPLACE VIEW
			exists, err := e.tableExists(ctx, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				// the view already exists. This CREATE OR REPLACE VIEW statement should
				// actually turn into an ALTER
				if err := e.executeAlterViewOnline(ctx, onlineDDL); err != nil {
					return failMigration(err)
				}
				return nil
			}
		}
	}
	// from now on, whether a VIEW or a TABLE, they get the same treatment

	sentryArtifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return failMigration(err)
	}
	// we create a dummy artifact. Its existence means the table was created by this migration.
	// It will be read by the revert operation.
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, sentryArtifactTableName); err != nil {
		return err
	}

	if ddlStmt.GetIfNotExists() {
		// This is a CREATE TABLE IF NOT EXISTS
		// We want to know if the table actually exists before running this migration.
		// If so, then the operation is noop, and when we revert the migration, we also do a noop.
		exists, err := e.tableExists(ctx, onlineDDL.Table)
		if err != nil {
			return failMigration(err)
		}
		if exists {
			// the table already exists. This CREATE TABLE IF NOT EXISTS statement is a noop.
			// We therefore clear the artifact field. A revert operation will use this as a hint.
			if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
				return failMigration(err)
			}
		}
	}
	if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
		return failMigration(err)
	}
	return nil
}

// generateSwapTablesStatement creates a RENAME statement that swaps two tables, with assistance
// of temporary third table. It returns the name of generated third table, though normally
// that table should not exist before & after operation, only _during_ operation time.
func (e *Executor) generateSwapTablesStatement(ctx context.Context, tableName1, tableName2 string) (query string, swapTableName string, err error) {
	swapTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return "", swapTableName, err
	}
	parsed := sqlparser.BuildParsedQuery(sqlSwapTables,
		tableName1, swapTableName,
		tableName2, tableName1,
		swapTableName, tableName2,
	)
	return parsed.Query, swapTableName, nil
}

// renameTableIfApplicable renames a table, assuming it exists and that the target does not exist.
func (e *Executor) renameTableIfApplicable(ctx context.Context, fromTableName, toTableName string) (attemptMade bool, err error) {
	if fromTableName == "" {
		return false, nil
	}
	exists, err := e.tableExists(ctx, fromTableName)
	if err != nil {
		return false, err
	}
	if !exists {
		// can't rename from table when it does not exist
		return false, nil
	}
	exists, err = e.tableExists(ctx, toTableName)
	if err != nil {
		return false, err
	}
	if exists {
		// target table exists, abort.
		return false, nil
	}
	parsed := sqlparser.BuildParsedQuery(sqlRenameTable, fromTableName, toTableName)
	_, err = e.execQuery(ctx, parsed.Query)
	return true, err
}

func (e *Executor) executeAlterViewOnline(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	artifactViewName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return err
	}
	stmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return err
	}
	switch viewStmt := stmt.(type) {
	case *sqlparser.CreateView:
		stmt.SetTable("", artifactViewName)
	case *sqlparser.AlterView:
		// consolidate the logic. We treat ALTER like we treat CREATE OR REPLACE
		// it actually easier for us to issue a CREATE OR REPLACE, because it
		// actually creates a view...
		stmt = &sqlparser.CreateView{
			Algorithm:   viewStmt.Algorithm,
			Definer:     viewStmt.Definer,
			Security:    viewStmt.Security,
			Columns:     viewStmt.Columns,
			Select:      viewStmt.Select,
			CheckOption: viewStmt.CheckOption,
			IsReplace:   true,
			Comments:    viewStmt.Comments,
		}
		stmt.SetTable("", artifactViewName)
	default:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "executeAlterViewOnline only supports CreateView and AlterView statements. Got: %v", sqlparser.String(viewStmt))
	}
	artifactViewCreateSQL := sqlparser.String(stmt)

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint)

	if _, err := conn.ExecuteFetch(artifactViewCreateSQL, 0, false); err != nil {
		return err
	}
	if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
		return err
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactViewName); err != nil {
		return err
	}

	// view created in requested format, but under different name. We now swap the views
	swapQuery, _, err := e.generateSwapTablesStatement(ctx, onlineDDL.Table, artifactViewName)
	if err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(swapQuery, 0, false); err != nil {
		return err
	}
	// Make sure this is considered as an ALTER.
	// Either the user issued a ALTER VIEW, and the action is trivially ALTER,
	// or the user issues a CREATE OR REPLACE, and the view existed, in which case this is implicitly an ALTER
	if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.AlterStr); err != nil {
		return err
	}

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)

	return nil
}

func (e *Executor) executeAlterDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}
	if _, isAlterView := ddlStmt.(*sqlparser.AlterView); isAlterView {
		// Same treatment for all online strategies
		exists, err := e.tableExists(ctx, onlineDDL.Table)
		if err != nil {
			return failMigration(err)
		}
		if !exists {
			// We cannot ALTER VIEW if the view does not exist. We could bail out directly here,
			// but we prefer to actually get an authentic MySQL error. We know MySQL will fail running
			// this statement.
			_, err := e.executeDirectly(ctx, onlineDDL)
			return failMigration(err)
		}
		// OK, view exists
		if err := e.executeAlterViewOnline(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
		return nil
	}

	// This is a real TABLE
	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.ExecuteWithVReplication(ctx, onlineDDL, nil); err != nil {
				failMigration(err)
			}
		}()
	case schema.DDLStrategyGhost:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.ExecuteWithGhost(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	case schema.DDLStrategyPTOSC:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.ExecuteWithPTOSC(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	default:
		{
			return failMigration(fmt.Errorf("Unsupported strategy: %+v", onlineDDL.Strategy))
		}
	}
	return nil
}

// executeMigration executes a single migration. It analyzes the migration type:
// - is it declarative?
// - is it CREATE / DROP / ALTER?
// - it is a Revert request?
// - what's the migration strategy?
// The function invokes the appropriate handlers for each of those cases.
func (e *Executor) executeMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	defer e.triggerNextCheckInterval()
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}

	ddlAction, err := onlineDDL.GetAction()
	if err != nil {
		return failMigration(err)
	}

	// See if this is a duplicate submission. A submission is considered duplicate if it has the exact same
	// migration context and DDL as a previous one. We are only interested in our scenario in a duplicate
	// whose predecessor is "complete". If this is the case, then we can mark our own migration as
	// implicitly "complete", too.
	{
		completedUUID, err := e.getCompletedMigrationByContextAndSQL(ctx, onlineDDL)
		if err != nil {
			return err
		}
		if completedUUID != "" {
			// Yep. We mark this migration as implicitly complete, and we're done with it!
			_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("duplicate DDL as %s for migration context %s", completedUUID, onlineDDL.MigrationContext))
			return nil
		}
	}

	if onlineDDL.StrategySetting().IsDeclarative() {
		switch ddlAction {
		case sqlparser.RevertDDLAction:
			// No special action. Declarative Revert migrations are handled like any normal Revert migration.
		case sqlparser.AlterDDLAction:
			return failMigration(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "strategy is declarative. ALTER cannot run in declarative mode for migration %v", onlineDDL.UUID))
		case sqlparser.DropDDLAction:
			// This DROP is declarative, meaning it may:
			// - actually DROP a table, if that table exists, or
			// - Implicitly do nothing, if the table does not exist
			{
				// Sanity: reject IF NOT EXISTS statements, because they don't make sense (or are ambiguous) in declarative mode
				ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
				if err != nil {
					return failMigration(err)
				}
				if ddlStmt.GetIfExists() {
					return failMigration(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "strategy is declarative. IF EXISTS does not work in declarative mode for migration %v", onlineDDL.UUID))
				}
			}
			exists, err := e.tableExists(ctx, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				// table does exist, so this declarative DROP turns out to really be an actual DROP. No further action is needed here
			} else {
				// table does not exist. We mark this DROP as implicitly sucessful
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
				_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "no change")
				return nil
			}
		case sqlparser.CreateDDLAction:
			// This CREATE is declarative, meaning it may:
			// - actually CREATE a table, if that table does not exist, or
			// - ALTER the table, if it exists and is different, or
			// - Implicitly do nothing, if the table exists and is identical to CREATE statement

			// Sanity: reject IF NOT EXISTS statements, because they don't make sense (or are ambiguous) in declarative mode
			ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
			if err != nil {
				return failMigration(err)
			}
			if ddlStmt.GetIfNotExists() {
				return failMigration(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "strategy is declarative. IF NOT EXISTS does not work in declarative mode for migration %v", onlineDDL.UUID))
			}
			if ddlStmt.GetIsReplace() {
				return failMigration(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "strategy is declarative. OR REPLACE does not work in declarative mode for migration %v", onlineDDL.UUID))
			}

			exists, err := e.tableExists(ctx, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				diff, err := e.evaluateDeclarativeDiff(ctx, onlineDDL)
				if err != nil {
					return failMigration(err)
				}
				if diff == nil || diff.IsEmpty() {
					// No diff! We mark this CREATE as implicitly sucessful
					_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
					_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "no change")
					return nil
				}
				// alterClause is non empty. We convert this migration into an ALTER
				if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.AlterStr); err != nil {
					return failMigration(err)
				}
				if createViewStmt, isCreateView := ddlStmt.(*sqlparser.CreateView); isCreateView {
					// Rewrite as CREATE OR REPLACE
					// this will be handled later on.
					createViewStmt.IsReplace = true
					onlineDDL.SQL = sqlparser.String(createViewStmt)
				} else {
					// a TABLE
					ddlAction = sqlparser.AlterDDLAction
					onlineDDL.SQL = diff.CanonicalStatementString()
				}
				_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, diff.CanonicalStatementString())
			} else {
				{
					// table does not exist, so this declarative CREATE turns out to really be an actual CREATE. No further action is needed here.
					// the statement is empty, but I want to keep the 'else' clause here just for sake of this comment.
				}
			}
		}
	} // endif onlineDDL.IsDeclarative()
	// Noting that if the migration is declarative, then it may have been modified in the above block, to meet the next operations.

	switch ddlAction {
	case sqlparser.DropDDLAction:
		go func() error {
			return e.executeDropDDLActionMigration(ctx, onlineDDL)
		}()
	case sqlparser.CreateDDLAction:
		go func() error {
			return e.executeCreateDDLActionMigration(ctx, onlineDDL)
		}()
	case sqlparser.AlterDDLAction:
		return e.executeAlterDDLActionMigration(ctx, onlineDDL)
	case sqlparser.RevertDDLAction:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.executeRevert(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	}
	return nil
}

// runNextMigration picks up to one 'ready' migration that is able to run, and executes it.
// Possible scenarios:
// - no migration is in 'ready' state -- nothing to be done
// - a migration is 'ready', but conflicts with other running migrations -- try another 'ready' migration
// - multiple migrations are 'ready' -- we just handle one here
// Note that per the above breakdown, and due to potential conflicts, it is possible to have one or
// more 'ready' migration, and still none is executed.
func (e *Executor) runNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if !e.reviewedRunningMigrationsFlag {
		// Since Open(), we havent's once executed reviewRunningMigrations() successfully.
		// This means we may not have a good picture of what is actually running. Perhaps there's
		// a vreplication migration from a pre-PRS/ERS that we still need to learn about?
		// We're going to be careful here, and avoid running new migrations until we have
		// a better picture. It will likely take a couple seconds till next iteration.
		// This delay only takes place shortly after Open().
		return nil
	}

	// getNonConflictingMigration finds a single 'ready' migration which does not conflict with running migrations.
	// Conflicts are:
	// - a migration is 'ready' but is not set to run _concurrently_, and there's a running migration that is also non-concurrent
	// - a migration is 'ready' but there's another migration 'running' on the exact same table
	getNonConflictingMigration := func() (*schema.OnlineDDL, error) {
		r, err := e.execQuery(ctx, sqlSelectReadyMigrations)
		if err != nil {
			return nil, err
		}
		for _, row := range r.Named().Rows {
			uuid := row["migration_uuid"].ToString()
			onlineDDL, _, err := e.readMigration(ctx, uuid)
			if err != nil {
				return nil, err
			}
			if !e.isAnyConflictingMigrationRunning(onlineDDL) {
				// This migration seems good to go
				return onlineDDL, err
			}
		}
		// no non-conflicting migration found...
		// Either all ready migrations are conflicting, or there are no ready migrations...
		return nil, nil
	}
	onlineDDL, err := getNonConflictingMigration()
	if err != nil {
		return err
	}
	if onlineDDL == nil {
		// nothing to do
		return nil
	}
	{
		// We strip out any VT query comments because our simplified parser doesn't work well with comments
		ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
		if err == nil {
			ddlStmt.SetComments(sqlparser.Comments{})
			onlineDDL.SQL = sqlparser.String(ddlStmt)
		}
	}
	e.executeMigration(ctx, onlineDDL)
	return nil
}

// isPTOSCMigrationRunning sees if pt-online-schema-change is running a specific migration,
// by examining its PID file
func (e *Executor) isPTOSCMigrationRunning(ctx context.Context, uuid string) (isRunning bool, pid int, err error) {
	// Try and read its PID file:
	content, err := os.ReadFile(e.ptPidFileName(uuid))
	if err != nil {
		// file probably does not exist (migration not running)
		// or any other issue --> we can't confirm that the migration is actually running
		return false, pid, err
	}
	contentString := strings.TrimSpace(string(content))
	//
	pid, err = strconv.Atoi(contentString)
	if err != nil {
		// can't get the PID right. Can't confirm migration is running.
		return false, pid, err
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		// can't find the process. Can't confirm migration is running.
		return false, pid, err
	}
	err = p.Signal(syscall.Signal(0))
	if err != nil {
		// can't verify process is running. Can't confirm migration is running.
		return false, pid, err
	}
	// AHA! We are able to confirm this pt-osc migration is actually running!
	return true, pid, nil
}

// dropOnlineDDLUser drops the given ddl user account at the end of migration
func (e *Executor) dropPTOSCMigrationTriggers(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	defer conn.Close()

	parsed := sqlparser.BuildParsedQuery(sqlSelectPTOSCMigrationTriggers, ":mysql_schema", ":mysql_table")
	bindVars := map[string]*querypb.BindVariable{
		"mysql_schema": sqltypes.StringBindVariable(onlineDDL.Schema),
		"mysql_table":  sqltypes.StringBindVariable(onlineDDL.Table),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	r, err := e.execQuery(ctx, bound)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		// iterate pt-osc triggers and drop them
		triggerSchema := row.AsString("trigger_schema", "")
		triggerName := row.AsString("trigger_name", "")

		dropParsed := sqlparser.BuildParsedQuery(sqlDropTrigger, triggerSchema, triggerName)
		if _, err := conn.ExecuteFetch(dropParsed.Query, 0, false); err != nil {
			return err
		}
	}

	return err
}

// readVReplStream reads _vt.vreplication entries for given workflow
func (e *Executor) readVReplStream(ctx context.Context, uuid string, okIfMissing bool) (*VReplStream, error) {
	query, err := sqlparser.ParseAndBind(sqlReadVReplStream,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	r, err := e.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 && okIfMissing {
		return nil, nil
	}
	row := r.Named().Row()
	if row == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Cannot find unique workflow for UUID: %+v", uuid)
	}
	s := &VReplStream{
		id:                   row.AsInt64("id", 0),
		workflow:             row.AsString("workflow", ""),
		source:               row.AsString("source", ""),
		pos:                  row.AsString("pos", ""),
		timeUpdated:          row.AsInt64("time_updated", 0),
		timeHeartbeat:        row.AsInt64("time_heartbeat", 0),
		transactionTimestamp: row.AsInt64("transaction_timestamp", 0),
		state:                row.AsString("state", ""),
		message:              row.AsString("message", ""),
		rowsCopied:           row.AsInt64("rows_copied", 0),
		bls:                  &binlogdatapb.BinlogSource{},
	}
	if err := prototext.Unmarshal([]byte(s.source), s.bls); err != nil {
		return nil, err
	}
	return s, nil
}

// isVReplMigrationReadyToCutOver sees if the vreplication migration has completed the row copy
// and is up to date with the binlogs.
func (e *Executor) isVReplMigrationReadyToCutOver(ctx context.Context, s *VReplStream) (isReady bool, err error) {
	// Check all the cases where migration is still running:
	{
		// when ready to cut-over, pos must have some value
		if s.pos == "" {
			return false, nil
		}
	}
	{
		// Both time_updated and transaction_timestamp must be in close priximity to each
		// other and to the time now, otherwise that means we're lagging and it's not a good time
		// to cut-over
		durationDiff := func(t1, t2 time.Time) time.Duration {
			diff := t1.Sub(t2)
			if diff < 0 {
				diff = -diff
			}
			return diff
		}
		timeNow := time.Now()
		timeUpdated := time.Unix(s.timeUpdated, 0)
		if durationDiff(timeNow, timeUpdated) > vreplicationCutOverThreshold {
			return false, nil
		}
		// Let's look at transaction timestamp. This gets written by any ongoing
		// writes on the server (whether on this table or any other table)
		transactionTimestamp := time.Unix(s.transactionTimestamp, 0)
		if durationDiff(timeNow, transactionTimestamp) > vreplicationCutOverThreshold {
			return false, nil
		}
	}
	{
		// copy_state must have no entries for this vreplication id: if entries are
		// present that means copy is still in progress
		query, err := sqlparser.ParseAndBind(sqlReadCountCopyState,
			sqltypes.Int64BindVariable(s.id),
		)
		if err != nil {
			return false, err
		}
		r, err := e.execQuery(ctx, query)
		if err != nil {
			return false, err
		}
		csRow := r.Named().Row()
		if csRow == nil {
			return false, err
		}
		count := csRow.AsInt64("cnt", 0)
		if count > 0 {
			// Still copying
			return false, nil
		}
	}

	return true, nil
}

// isVReplMigrationRunning sees if there is a VReplication migration actively running
func (e *Executor) isVReplMigrationRunning(ctx context.Context, uuid string) (isRunning bool, s *VReplStream, err error) {
	s, err = e.readVReplStream(ctx, uuid, true)
	if err != nil {
		return false, s, err
	}
	if s == nil {
		return false, s, nil
	}
	switch s.state {
	case binlogplayer.BlpError:
		return false, s, nil
	case binlogplayer.VReplicationInit, binlogplayer.VReplicationCopying, binlogplayer.BlpRunning:
		return true, s, nil
	}
	if strings.Contains(strings.ToLower(s.message), "error") {
		return false, s, nil
	}
	return false, s, nil
}

// reviewRunningMigrations iterates migrations in 'running' state. Normally there's only one running, which was
// spawned by this tablet; but vreplication migrations could also resume from failure.
func (e *Executor) reviewRunningMigrations(ctx context.Context) (countRunnning int, cancellable []*cancellableMigration, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	r, err := e.execQuery(ctx, sqlSelectRunningMigrations)
	if err != nil {
		return countRunnning, cancellable, err
	}
	uuidsFoundRunning := map[string]bool{}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		onlineDDL, migrationRow, err := e.readMigration(ctx, uuid)
		if err != nil {
			return countRunnning, cancellable, err
		}
		postponeCompletion := row.AsBool("postpone_completion", false)
		elapsedSeconds := row.AsInt64("elapsed_seconds", 0)

		if stowawayTable := row.AsString("stowaway_table", ""); stowawayTable != "" {
			// whoa
			// stowawayTable is an original table stowed away while cutting over a vrepl migration, see call to cutOverVReplMigration() down below in this function.
			// In a normal operation, the table should not exist outside the scope of cutOverVReplMigration
			// If it exists, that means a tablet crashed while running a cut-over, and left the database in a bad state, where the migrated table does not exist.
			// thankfully, we have tracked this situation and just realized what happened. Now, first thing to do is to restore the original table.
			log.Infof("found stowaway table %s journal in migration %s for table %s", stowawayTable, uuid, onlineDDL.Table)
			attemptMade, err := e.renameTableIfApplicable(ctx, stowawayTable, onlineDDL.Table)
			if err != nil {
				// unable to restore table; we bail out, and we will try again next round.
				return countRunnning, cancellable, err
			}
			// success
			if attemptMade {
				log.Infof("stowaway table %s restored back into %s", stowawayTable, onlineDDL.Table)
			} else {
				log.Infof("stowaway table %s did not exist and there was no need to restore it", stowawayTable)
			}
			// OK good, table restored. We can remove the record.
			if err := e.updateMigrationStowawayTable(ctx, uuid, ""); err != nil {
				return countRunnning, cancellable, err
			}
		}

		uuidsFoundRunning[uuid] = true

		switch onlineDDL.StrategySetting().Strategy {
		case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
			{
				// We check the _vt.vreplication table
				s, err := e.readVReplStream(ctx, uuid, true)
				if err != nil {
					return countRunnning, cancellable, err
				}
				isVreplicationTestSuite := onlineDDL.StrategySetting().IsVreplicationTestSuite()
				if isVreplicationTestSuite {
					e.triggerNextCheckInterval()
				}
				if s != nil && s.isFailed() {
					cancellable = append(cancellable, newCancellableMigration(uuid, s.message))
				}
				if s != nil && s.isRunning() {
					// This VRepl migration may have started from outside this tablet, so
					// this executor may not own the migration _yet_. We make sure to own it.
					// VReplication migrations are unique in this respect: we are able to complete
					// a vreplicaiton migration started by another tablet.
					e.ownedRunningMigrations.Store(uuid, onlineDDL)
					if lastVitessLivenessIndicator := migrationRow.AsInt64("vitess_liveness_indicator", 0); lastVitessLivenessIndicator < s.livenessTimeIndicator() {
						_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
						_ = e.updateVitessLivenessIndicator(ctx, uuid, s.livenessTimeIndicator())
					}
					_ = e.updateMigrationTablet(ctx, uuid)
					_ = e.updateRowsCopied(ctx, uuid, s.rowsCopied)
					_ = e.updateMigrationProgressByRowsCopied(ctx, uuid, s.rowsCopied)
					_ = e.updateMigrationETASecondsByProgress(ctx, uuid)

					isReady, err := e.isVReplMigrationReadyToCutOver(ctx, s)
					if err != nil {
						return countRunnning, cancellable, err
					}
					if isReady && isVreplicationTestSuite {
						// This is a endtoend test suite execution. We intentionally delay it by at least
						// vreplicationTestSuiteWaitSeconds
						if elapsedSeconds < vreplicationTestSuiteWaitSeconds {
							isReady = false
						}
					}
					// Indicate to outside observers whether the migration is generally ready to complete.
					// In the case of a postponed migration, we will not complete it, but the user will
					// understand whether "now is a good time" or "not there yet"
					_ = e.updateMigrationReadyToComplete(ctx, uuid, isReady)
					if postponeCompletion {
						// override. Even if migration is ready, we do not complet it.
						isReady = false
					}
					if isReady {
						if err := e.cutOverVReplMigration(ctx, s); err != nil {
							return countRunnning, cancellable, err
						}
					}
				}
			}
		case schema.DDLStrategyPTOSC:
			{
				// Since pt-osc doesn't have a "liveness" plugin entry point, we do it externally:
				// if the process is alive, we update the `liveness_timestamp` for this migration.
				running, _, err := e.isPTOSCMigrationRunning(ctx, uuid)
				if err != nil {
					return countRunnning, cancellable, err
				}
				if running {
					_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
				}
				if _, ok := e.ownedRunningMigrations.Load(uuid); !ok {
					// Ummm, the migration is running but we don't own it. This means the migration
					// is rogue. Maybe executed by another tablet. Anyway, if we don't own it, we can't
					// complete the migration. Even if it runs, the logic around announcing it as complete
					// is missing. So we may as well cancel it.
					message := fmt.Sprintf("cancelling a pt-osc running migration %s which is not owned (not started, or is assumed to be terminated) by this executor", uuid)
					cancellable = append(cancellable, newCancellableMigration(uuid, message))
				}
			}
		case schema.DDLStrategyGhost:
			{
				if _, ok := e.ownedRunningMigrations.Load(uuid); !ok {
					// Ummm, the migration is running but we don't own it. This means the migration
					// is rogue. Maybe executed by another tablet. Anyway, if we don't own it, we can't
					// complete the migration. Even if it runs, the logic around announcing it as complete
					// is missing. So we may as well cancel it.
					message := fmt.Sprintf("cancelling a gh-ost running migration %s which is not owned (not started, or is assumed to be terminated) by this executor", uuid)
					cancellable = append(cancellable, newCancellableMigration(uuid, message))
				}
			}
		}
		countRunnning++
	}
	{
		// now, let's look at UUIDs we own and _think_ should be running, and see which of tham _isn't_ actually running or pending...
		pendingUUIDS, err := e.readPendingMigrationsUUIDs(ctx)
		if err != nil {
			return countRunnning, cancellable, err
		}
		uuidsFoundPending := map[string]bool{}
		for _, uuid := range pendingUUIDS {
			uuidsFoundPending[uuid] = true
		}

		e.ownedRunningMigrations.Range(func(k, _ any) bool {
			uuid, ok := k.(string)
			if !ok {
				return true
			}
			// due to race condition, it's possible that ownedRunningMigrations will list a migration
			// that is _just about to run_ but is still, in fact, in `ready` state. This is fine.
			// If we find such a migration, we do nothing. We're only looking for migrations we really
			// don't have any information of.
			if !uuidsFoundRunning[uuid] && !uuidsFoundPending[uuid] {
				log.Infof("removing migration %s from ownedRunningMigrations because it's not running and not pending", uuid)
				e.ownedRunningMigrations.Delete(uuid)
			}
			return true
		})
	}

	e.reviewedRunningMigrationsFlag = true
	return countRunnning, cancellable, nil
}

// reviewStaleMigrations marks as 'failed' migrations whose status is 'running' but which have
// shown no liveness in past X minutes. It also attempts to terminate them
func (e *Executor) reviewStaleMigrations(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlSelectStaleMigrations,
		sqltypes.Int64BindVariable(staleMigrationMinutes),
	)
	if err != nil {
		return err
	}
	r, err := e.execQuery(ctx, query)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()

		onlineDDL, _, err := e.readMigration(ctx, uuid)
		if err != nil {
			return err
		}
		message := fmt.Sprintf("stale migration %s: found running but indicates no liveness", onlineDDL.UUID)
		if onlineDDL.TabletAlias != e.TabletAliasString() {
			// This means another tablet started the migration, and the migration has failed due to the tablet failure (e.g. primary failover)
			if err := e.updateTabletFailure(ctx, onlineDDL.UUID); err != nil {
				return err
			}
			message = fmt.Sprintf("%s; executed by different tablet %s", message, onlineDDL.TabletAlias)
		}
		if _, err := e.terminateMigration(ctx, onlineDDL); err != nil {
			message = fmt.Sprintf("error terminating migration (%v): %v", message, err)
			e.updateMigrationMessage(ctx, onlineDDL.UUID, message)
			continue // we still want to handle rest of migrations
		}
		if err := e.updateMigrationMessage(ctx, onlineDDL.UUID, message); err != nil {
			return err
		}
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed); err != nil {
			return err
		}
		_ = e.updateMigrationStartedTimestamp(ctx, uuid)
		// Because the migration is stale, it may not update completed_timestamp. It is essential to set completed_timestamp
		// as this is then used when cleaning artifacts
		if err := e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID); err != nil {
			return err
		}
	}

	return nil
}

// retryTabletFailureMigrations looks for migrations failed by tablet failure (e.g. by failover)
// and retry them (put them back in the queue)
func (e *Executor) retryTabletFailureMigrations(ctx context.Context) error {
	_, err := e.retryMigrationWhere(ctx, sqlWhereTabletFailure)
	return err
}

// vreplicationExec runs a vreplication query, and makes sure to initialize vreplication
func (e *Executor) vreplicationExec(ctx context.Context, tmClient tmclient.TabletManagerClient, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	e.initVreplicationDDLOnce.Do(func() {
		// Ensure vreplication schema is up-to-date by invoking a query with non-existing columns.
		// This will make vreplication run through its WithDDL schema changes.
		_, _ = tmClient.VReplicationExec(ctx, tablet, withddl.QueryToTriggerWithDDL)
	})
	return tmClient.VReplicationExec(ctx, tablet, query)
}

// deleteVReplicationEntry cleans up a _vt.vreplication entry; this function is called as part of
// migration termination and as part of artifact cleanup
func (e *Executor) deleteVReplicationEntry(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlDeleteVReplStream,
		sqltypes.StringBindVariable(e.dbName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	tmClient := tmclient.NewTabletManagerClient()
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	if _, err := e.vreplicationExec(ctx, tmClient, tablet.Tablet, query); err != nil {
		return err
	}
	return nil
}

// gcArtifactTable garbage-collects a single table
func (e *Executor) gcArtifactTable(ctx context.Context, artifactTable, uuid string, t time.Time) error {
	tableExists, err := e.tableExists(ctx, artifactTable)
	if err != nil {
		return err
	}
	if !tableExists {
		return nil
	}
	// We've already concluded in gcArtifacts() that this table was held for long enough.
	// We therefore move it into PURGE state.
	renameStatement, _, err := schema.GenerateRenameStatementWithUUID(artifactTable, schema.PurgeTableGCState, schema.OnlineDDLToGCUUID(uuid), t)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, renameStatement)
	return err
}

// gcArtifacts garbage-collects migration artifacts from completed/failed migrations
func (e *Executor) gcArtifacts(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if _, err := e.execQuery(ctx, sqlFixCompletedTimestamp); err != nil {
		// This query fixes a bug where stale migrations were marked as 'failed' without updating 'completed_timestamp'
		// see https://github.com/vitessio/vitess/issues/8499
		// Running this query retroactively sets completed_timestamp
		// This 'if' clause can be removed in version v13
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlSelectUncollectedArtifacts,
		sqltypes.Int64BindVariable(int64((*retainOnlineDDLTables).Seconds())),
	)
	if err != nil {
		return err
	}
	r, err := e.execQuery(ctx, query)
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
	}

	return nil
}

// onMigrationCheckTick runs all migrations life cycle
func (e *Executor) onMigrationCheckTick() {
	// This function can be called by multiple triggers. First, there's the normal ticker.
	// Then, any time a migration completes, we set a timer to trigger this function.
	// also, any time a new INSERT arrives, we set a timer to trigger this function.
	// Some of these may be correlated. To avoid spamming of this function we:
	// - ensure the function is non-reentrant, using tickReentranceFlag
	// - clean up tickReentranceFlag 1 second after function completes; this throttles calls to
	//   this function at no more than 1/sec rate.
	if atomic.CompareAndSwapInt64(&e.tickReentranceFlag, 0, 1) {
		defer time.AfterFunc(time.Second, func() { atomic.StoreInt64(&e.tickReentranceFlag, 0) })
	} else {
		// An instance of this function is already running
		return
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return
	}
	if e.keyspace == "" {
		log.Errorf("Executor.onMigrationCheckTick(): empty keyspace")
		return
	}

	ctx := context.Background()
	if err := e.initSchema(ctx); err != nil {
		log.Error(err)
		return
	}
	if err := e.retryTabletFailureMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.reviewQueuedMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.scheduleNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if err := e.runNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if _, cancellable, err := e.reviewRunningMigrations(ctx); err != nil {
		log.Error(err)
	} else if err := e.cancelMigrations(ctx, cancellable); err != nil {
		log.Error(err)
	}
	if err := e.reviewStaleMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.gcArtifacts(ctx); err != nil {
		log.Error(err)
	}
}

func (e *Executor) updateMigrationStartedTimestamp(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationStartedTimestamp,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) updateMigrationTimestamp(ctx context.Context, timestampColumn string, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationTimestamp, timestampColumn,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) updateMigrationLogPath(ctx context.Context, uuid string, hostname, logPath string) error {
	logFile := path.Join(logPath, migrationLogFileName)
	hostLogPath := fmt.Sprintf("%s:%s", hostname, logPath)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationLogPath,
		sqltypes.StringBindVariable(hostLogPath),
		sqltypes.StringBindVariable(logFile),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateArtifacts(ctx context.Context, uuid string, artifacts ...string) error {
	bindArtifacts := strings.Join(artifacts, ",")
	query, err := sqlparser.ParseAndBind(sqlUpdateArtifacts,
		sqltypes.StringBindVariable(bindArtifacts),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) clearArtifacts(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlClearArtifacts,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

// updateMigrationTablet sets 'tablet' column to be this executor's tablet alias for given migration
func (e *Executor) updateMigrationTablet(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateTablet,
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

// updateTabletFailure marks a given migration as "tablet_failed"
func (e *Executor) updateTabletFailure(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateTabletFailure,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) updateMigrationStatus(ctx context.Context, uuid string, status schema.OnlineDDLStatus) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatus,
		sqltypes.StringBindVariable(string(status)),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateDDLAction(ctx context.Context, uuid string, actionStr string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateDDLAction,
		sqltypes.StringBindVariable(actionStr),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationMessage(ctx context.Context, uuid string, message string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMessage,
		sqltypes.StringBindVariable(message),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateSchemaAnalysis(ctx context.Context, uuid string,
	addedUniqueKeys, removedUnqiueKeys int, removedUniqueKeyNames string,
	droppedNoDefaultColumnNames string, expandedColumnNames string,
	revertibleNotes string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateSchemaAnalysis,
		sqltypes.Int64BindVariable(int64(addedUniqueKeys)),
		sqltypes.Int64BindVariable(int64(removedUnqiueKeys)),
		sqltypes.StringBindVariable(removedUniqueKeyNames),
		sqltypes.StringBindVariable(droppedNoDefaultColumnNames),
		sqltypes.StringBindVariable(expandedColumnNames),
		sqltypes.StringBindVariable(revertibleNotes),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMySQLTable(ctx context.Context, uuid string, tableName string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMySQLTable,
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationETASeconds(ctx context.Context, uuid string, etaSeconds int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationETASeconds,
		sqltypes.Int64BindVariable(etaSeconds),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationProgress(ctx context.Context, uuid string, progress float64) error {
	if progress <= 0 {
		// progress starts at 0, and can only increase.
		// A value of "0" either means "This is the actual current progress" or "No information"
		// In both cases there's nothing to update
		return nil
	}
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationProgress,
		sqltypes.Float64BindVariable(progress),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationProgressByRowsCopied(ctx context.Context, uuid string, rowsCopied int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationProgressByRowsCopied,
		sqltypes.Int64BindVariable(rowsCopied),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationETASecondsByProgress(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationETASecondsByProgress,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationTableRows(ctx context.Context, uuid string, tableRows int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationTableRows,
		sqltypes.Int64BindVariable(tableRows),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateRowsCopied(ctx context.Context, uuid string, rowsCopied int64) error {
	if rowsCopied <= 0 {
		// Number of rows can only be positive. Zero or negative must mean "no information" and
		// we don't update the table value.
		return nil
	}
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationRowsCopied,
		sqltypes.Int64BindVariable(rowsCopied),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateVitessLivenessIndicator(ctx context.Context, uuid string, livenessIndicator int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationVitessLivenessIndicator,
		sqltypes.Int64BindVariable(livenessIndicator),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationIsView(ctx context.Context, uuid string, isView bool) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationIsView,
		sqltypes.BoolBindVariable(isView),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationReadyToComplete(ctx context.Context, uuid string, isReady bool) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationReadyToComplete,
		sqltypes.BoolBindVariable(isReady),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationStowawayTable(ctx context.Context, uuid string, tableName string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStowawayTable,
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

// retryMigrationWhere retries a migration based on a given WHERE clause
func (e *Executor) retryMigrationWhere(ctx context.Context, whereExpr string) (result *sqltypes.Result, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()
	parsed := sqlparser.BuildParsedQuery(sqlRetryMigrationWhere, ":tablet", whereExpr)
	bindVars := map[string]*querypb.BindVariable{
		"tablet": sqltypes.StringBindVariable(e.TabletAliasString()),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	result, err = e.execQuery(ctx, bound)
	return result, err
}

// RetryMigration marks given migration for retry
func (e *Executor) RetryMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in RETRY: %s", uuid)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlRetryMigration,
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	return e.execQuery(ctx, query)
}

// CleanupMigration sets migration is ready for artifact cleanup. Artifacts are not immediately deleted:
// all we do is set retain_artifacts_seconds to a very small number (it's actually a negative) so that the
// next iteration of gcArtifacts() picks up the migration's artifacts and schedules them for deletion
func (e *Executor) CleanupMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in CLEANUP: %s", uuid)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateReadyForCleanup,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	return e.execQuery(ctx, query)
}

// CompleteMigration clears the postpone_completion flag for a given migration, assuming it was set in the first place
func (e *Executor) CompleteMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in COMPLETE: %s", uuid)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateCompleteMigration,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	if err := e.deleteGhostPostponeFlagFile(uuid); err != nil {
		// This should work without error even if the migration is not a gh-ost migration, and even
		// if the file does not exist. An error here indicates a general system error of sorts.
		return nil, err
	}
	return e.execQuery(ctx, query)
}

// SubmitMigration inserts a new migration request
func (e *Executor) SubmitMigration(
	ctx context.Context,
	stmt sqlparser.Statement,
) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if ddlStmt, ok := stmt.(sqlparser.DDLStatement); ok {
		// This validation should have taken place on submission. However, the query may have mutated
		// during transfer, and this validation is here to catch any malformed mutation.
		if !ddlStmt.IsFullyParsed() {
			return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error parsing statement")
		}
	}

	onlineDDL, err := schema.OnlineDDLFromCommentedStatement(stmt)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Error submitting migration %s: %v", sqlparser.String(stmt), err)
	}
	_, actionStr, err := onlineDDL.GetActionStr()
	if err != nil {
		return nil, err
	}
	revertedUUID, _ := onlineDDL.GetRevertUUID() // Empty value if the migration is not actually a REVERT. Safe to ignore error.

	retainArtifactsSeconds := int64((*retainOnlineDDLTables).Seconds())
	query, err := sqlparser.ParseAndBind(sqlInsertMigration,
		sqltypes.StringBindVariable(onlineDDL.UUID),
		sqltypes.StringBindVariable(e.keyspace),
		sqltypes.StringBindVariable(e.shard),
		sqltypes.StringBindVariable(e.dbName),
		sqltypes.StringBindVariable(onlineDDL.Table),
		sqltypes.StringBindVariable(onlineDDL.SQL),
		sqltypes.StringBindVariable(string(onlineDDL.Strategy)),
		sqltypes.StringBindVariable(onlineDDL.Options),
		sqltypes.StringBindVariable(actionStr),
		sqltypes.StringBindVariable(onlineDDL.MigrationContext),
		sqltypes.StringBindVariable(string(schema.OnlineDDLStatusQueued)),
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.Int64BindVariable(retainArtifactsSeconds),
		sqltypes.BoolBindVariable(onlineDDL.StrategySetting().IsPostponeCompletion()),
		sqltypes.BoolBindVariable(e.allowConcurrentMigration(onlineDDL)),
		sqltypes.StringBindVariable(revertedUUID),
		sqltypes.BoolBindVariable(onlineDDL.IsView()),
	)
	if err != nil {
		return nil, err
	}

	if err := e.initSchema(ctx); err != nil {
		log.Error(err)
		return nil, err
	}

	if onlineDDL.StrategySetting().IsSingleton() || onlineDDL.StrategySetting().IsSingletonContext() {
		// we need two wrap some logic within a mutex, so as to make sure we can't submit the migration whilst
		// another query submits a conflicting migration
		validateSingleton := func() error {
			// We wrap everything in a function because we want the mutex released. We will need to reaquire it later on
			// for other bookkeeping work.
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			pendingUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
			if err != nil {
				return err
			}
			switch {
			case onlineDDL.StrategySetting().IsSingleton():
				// We will reject this migration if there's any pending migration
				if len(pendingUUIDs) > 0 {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migrations [%s]", strings.Join(pendingUUIDs, ", "))
				}
			case onlineDDL.StrategySetting().IsSingletonContext():
				// We will reject this migration if there's any pending migration within a different context
				for _, pendingUUID := range pendingUUIDs {
					pendingOnlineDDL, _, err := e.readMigration(ctx, pendingUUID)
					if err != nil {
						return err
					}
					if pendingOnlineDDL.MigrationContext != onlineDDL.MigrationContext {
						return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migration: %s in different context: %s", pendingUUID, pendingOnlineDDL.MigrationContext)
					}
				}
			}
			// OK to go! We can submit the migration:
			result, err = e.execQuery(ctx, query)
			return err
		}
		if err := validateSingleton(); err != nil {
			return nil, err
		}
		// mutex aquired and released within validateSingleton(). We are now mutex free
	} else {
		// not a singleton. We submit the migration:
		result, err = e.execQuery(ctx, query)
		if err != nil {
			return nil, err
		}
	}

	defer e.triggerNextCheckInterval()

	// The query was a INSERT IGNORE because we allow a recurring submission of same migration.
	// However, let's validate that the duplication (identified via UUID) was intentional.
	storedMigration, _, err := e.readMigration(ctx, onlineDDL.UUID)
	if storedMigration.MigrationContext != onlineDDL.MigrationContext {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "migration rejected: found migration %s with different context: %s than submmitted migration's context: %s", onlineDDL.UUID, storedMigration.MigrationContext, onlineDDL.MigrationContext)
	}

	// Finally, possibly this migration already existed, and this is a resubmission of same UUID.
	// possibly, the existing migration is in 'failed' or 'cancelled' state, in which case this
	// resubmission should retry the migration.
	if _, err := e.RetryMigration(ctx, onlineDDL.UUID); err != nil {
		return result, err
	}

	return result, nil
}

// ShowMigrationLogs reads the migration log for a given migration
func (e *Executor) ShowMigrationLogs(ctx context.Context, stmt *sqlparser.ShowMigrationLogs) (result *sqltypes.Result, err error) {
	if !e.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	_, row, err := e.readMigration(ctx, stmt.UUID)
	if err != nil {
		return nil, err
	}
	logFile := row["log_file"].ToString()
	if logFile == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "No log file for migration %v", stmt.UUID)
	}
	content, err := os.ReadFile(logFile)
	if err != nil {
		return nil, err
	}

	result = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "migration_log",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	result.Rows = append(result.Rows, []sqltypes.Value{
		sqltypes.NewVarChar(string(content)),
	})
	return result, nil
}

// onSchemaMigrationStatus is called when a status is set/changed for a running migration
func (e *Executor) onSchemaMigrationStatus(ctx context.Context,
	uuid string, status schema.OnlineDDLStatus, dryRun bool, progressPct float64, etaSeconds int64, rowsCopied int64, hint string) (err error) {
	if dryRun && status != schema.OnlineDDLStatusFailed {
		// We don't consider dry-run reports unless there's a failure
		return nil
	}
	switch status {
	case schema.OnlineDDLStatusReady:
		{
			err = e.updateMigrationTimestamp(ctx, "ready_timestamp", uuid)
		}
	case schema.OnlineDDLStatusRunning:
		{
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
		}
	case schema.OnlineDDLStatusComplete:
		{
			progressPct = progressPctFull
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "completed_timestamp", uuid)
		}
	case schema.OnlineDDLStatusFailed:
		{
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "completed_timestamp", uuid)
		}
	}
	if err != nil {
		return err
	}
	if err = e.updateMigrationStatus(ctx, uuid, status); err != nil {
		return err
	}
	if err = e.updateMigrationProgress(ctx, uuid, progressPct); err != nil {
		return err
	}
	if err = e.updateMigrationETASeconds(ctx, uuid, etaSeconds); err != nil {
		return err
	}
	if err := e.updateRowsCopied(ctx, uuid, rowsCopied); err != nil {
		return err
	}
	if hint == readyToCompleteHint {
		if err := e.updateMigrationReadyToComplete(ctx, uuid, true); err != nil {
			return err
		}
	}
	if !dryRun {
		switch status {
		case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed:
			e.triggerNextCheckInterval()
		}
	}

	return nil
}

// OnSchemaMigrationStatus is called by TabletServer's API, which is invoked by a running gh-ost migration's hooks.
func (e *Executor) OnSchemaMigrationStatus(ctx context.Context,
	uuidParam, statusParam, dryrunParam, progressParam, etaParam, rowsCopiedParam, hint string) (err error) {
	status := schema.OnlineDDLStatus(statusParam)
	dryRun := (dryrunParam == "true")
	var progressPct float64
	if pct, err := strconv.ParseFloat(progressParam, 64); err == nil {
		progressPct = pct
	}
	var etaSeconds int64 = etaSecondsUnknown
	if eta, err := strconv.ParseInt(etaParam, 10, 64); err == nil {
		etaSeconds = eta
	}
	var rowsCopied int64
	if rows, err := strconv.ParseInt(rowsCopiedParam, 10, 64); err == nil {
		rowsCopied = rows
	}

	return e.onSchemaMigrationStatus(ctx, uuidParam, status, dryRun, progressPct, etaSeconds, rowsCopied, hint)
}

// VExec is called by a VExec invocation
// Implements vitess.io/vitess/go/vt/vttablet/vexec.Executor interface
func (e *Executor) VExec(ctx context.Context, vx *vexec.TabletVExec) (qr *querypb.QueryResult, err error) {
	response := func(result *sqltypes.Result, err error) (*querypb.QueryResult, error) {
		if err != nil {
			return nil, err
		}
		return sqltypes.ResultToProto3(result), nil
	}

	if err := e.initSchema(ctx); err != nil {
		log.Error(err)
		return nil, err
	}

	switch stmt := vx.Stmt.(type) {
	case *sqlparser.Delete:
		return nil, fmt.Errorf("DELETE statements not supported for this table. query=%s", vx.Query)
	case *sqlparser.Select:
		return response(e.execQuery(ctx, vx.Query))
	case *sqlparser.Insert:
		match, err := sqlparser.QueryMatchesTemplates(vx.Query, vexecInsertTemplates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s", strings.Join(vexecInsertTemplates, "; "))
		}
		// Vexec naturally runs outside shard/schema context. It does not supply values for those columns.
		// We can fill them in.
		vx.ReplaceInsertColumnVal("shard", vx.ToStringVal(e.shard))
		vx.ReplaceInsertColumnVal("mysql_schema", vx.ToStringVal(e.dbName))
		vx.AddOrReplaceInsertColumnVal("tablet", vx.ToStringVal(e.TabletAliasString()))
		e.triggerNextCheckInterval()
		return response(e.execQuery(ctx, vx.Query))
	case *sqlparser.Update:
		match, err := sqlparser.QueryMatchesTemplates(vx.Query, vexecUpdateTemplates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s; query=%s", strings.Join(vexecUpdateTemplates, "; "), vx.Query)
		}
		if shard, _ := vx.ColumnStringVal(vx.WhereCols, "shard"); shard != "" {
			// shard is specified.
			if shard != e.shard {
				// specified shard is not _this_ shard. So we're skipping this UPDATE
				return sqltypes.ResultToProto3(emptyResult), nil
			}
		}
		statusVal, err := vx.ColumnStringVal(vx.UpdateCols, "migration_status")
		if err != nil {
			return nil, err
		}
		switch statusVal {
		case retryMigrationHint:
			return response(e.retryMigrationWhere(ctx, sqlparser.String(stmt.Where.Expr)))
		case completeMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.CompleteMigration(ctx, uuid))
		case cancelMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.CancelMigration(ctx, uuid, "cancel by user"))
		case cancelAllMigrationHint:
			uuid, _ := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if uuid != "" {
				return nil, fmt.Errorf("Unexpetced UUID: %s", uuid)
			}
			return response(e.CancelPendingMigrations(ctx, "cancel-all by user"))
		default:
			return nil, fmt.Errorf("Unexpected value for migration_status: %v. Supported values are: %s, %s",
				statusVal, retryMigrationHint, cancelMigrationHint)
		}
	default:
		return nil, fmt.Errorf("No handler for this query: %s", vx.Query)
	}
}
