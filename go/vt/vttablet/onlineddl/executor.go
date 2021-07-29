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
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
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
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/vexec"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/planetscale/tengo"
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
	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server
	tabletAlias    *topodatapb.TabletAlias

	keyspace string
	shard    string
	dbName   string

	initMutex             sync.Mutex
	migrationMutex        sync.Mutex
	vreplMigrationRunning int64
	ghostMigrationRunning int64
	ptoscMigrationRunning int64
	lastMigrationUUID     string
	tickReentranceFlag    int64

	ticks             *timer.Timer
	isOpen            bool
	schemaInitialized bool
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
func NewExecutor(env tabletenv.Env, tabletAlias *topodatapb.TabletAlias, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType) *Executor {
	return &Executor{
		env:         env,
		tabletAlias: proto.Clone(tabletAlias).(*topodatapb.TabletAlias),

		pool: connpool.NewPool(env, "OnlineDDLExecutorPool", tabletenv.ConnPoolConfig{
			Size:               databasePoolSize,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		ticks:          timer.NewTimer(*migrationCheckInterval),
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

	conn, err := e.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	for _, ddl := range ApplyDDL {
		_, err := conn.Exec(ctx, ddl, math.MaxInt32, false)
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
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if !e.isOpen {
		return
	}

	e.ticks.Stop()
	e.pool.Close()
	e.isOpen = false
}

// triggerNextCheckInterval the next tick sooner than normal
func (e *Executor) triggerNextCheckInterval() {
	for _, interval := range migrationNextCheckIntervals {
		e.ticks.TriggerAfter(interval)
	}
}

// isAnyMigrationRunning sees if there's any migration running right now
func (e *Executor) isAnyMigrationRunning() bool {
	if atomic.LoadInt64(&e.vreplMigrationRunning) > 0 {
		return true
	}
	if atomic.LoadInt64(&e.ghostMigrationRunning) > 0 {
		return true
	}
	if atomic.LoadInt64(&e.ptoscMigrationRunning) > 0 {
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

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown)
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
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown)

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
	{
		query, err := sqlparser.ParseAndBind(sqlStopVReplStream,
			sqltypes.StringBindVariable(e.dbName),
			sqltypes.StringBindVariable(uuid),
		)
		if err != nil {
			return err
		}
		// silently skip error; stopping the stream is just a graceful act; later deleting it is more important
		_, _ = tmClient.VReplicationExec(ctx, tablet.Tablet, query)
	}
	{
		query, err := sqlparser.ParseAndBind(sqlDeleteVReplStream,
			sqltypes.StringBindVariable(e.dbName),
			sqltypes.StringBindVariable(uuid),
		)
		if err != nil {
			return err
		}
		// silently skip error; stopping the stream is just a graceful act; later deleting it is more important
		if _, err := tmClient.VReplicationExec(ctx, tablet.Tablet, query); err != nil {
			return err
		}
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
	shardInfo, err := e.ts.GetShard(ctx, e.keyspace, e.shard)
	if err != nil {
		return err
	}

	// information about source tablet
	onlineDDL, _, err := e.readMigration(ctx, s.workflow)
	if err != nil {
		return err
	}
	isVreplicationTestSuite := onlineDDL.StrategySetting().IsVreplicationTestSuite()

	// come up with temporary name for swap table
	swapTable, err := schema.CreateUUID()
	if err != nil {
		return err
	}
	swapTable = strings.Replace(swapTable, "-", "", -1)
	swapTable = fmt.Sprintf("_swap_%s", swapTable)

	// Preparation is complete. We proceed to cut-over.

	// lock keyspace:
	{
		lctx, unlockKeyspace, err := e.ts.LockKeyspace(ctx, e.keyspace, "OnlineDDLCutOver")
		if err != nil {
			return err
		}
		// lctx has the lock info, needed for UpdateShardFields
		ctx = lctx
		defer unlockKeyspace(&err)
	}
	toggleWrites := func(allowWrites bool) error {
		if _, err := e.ts.UpdateShardFields(ctx, e.keyspace, shardInfo.ShardName(), func(si *topo.ShardInfo) error {
			err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, allowWrites, []string{onlineDDL.Table})
			return err
		}); err != nil {
			return err
		}
		if err := tmClient.RefreshState(ctx, tablet.Tablet); err != nil {
			return err
		}
		return nil
	}
	// stop writes on source:
	if err := toggleWrites(false); err != nil {
		return err
	}
	defer toggleWrites(true)

	if isVreplicationTestSuite {
		// The testing suite may inject queries internally from the server via a recurring EVENT.
		// Those queries are unaffected by UpdateSourceBlacklistedTables() because they don't go through Vitess.
		// We therefore hard-rename the table here, such that the queries will hard-fail.
		beforeTableName := fmt.Sprintf("%s_before", onlineDDL.Table)
		parsed := sqlparser.BuildParsedQuery(sqlRenameTable,
			onlineDDL.Table, beforeTableName,
		)
		if _, err = e.execQuery(ctx, parsed.Query); err != nil {
			return err
		}
	}
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
	if _, err := tmClient.VReplicationExec(ctx, tablet.Tablet, binlogplayer.StopVReplication(uint32(s.id), "stopped for online DDL cutover")); err != nil {
		return err
	}

	// rename tables atomically (remember, writes on source tables are stopped)
	{
		if isVreplicationTestSuite {
			// this is used in Vitess endtoend testing suite
			afterTableName := fmt.Sprintf("%s_after", onlineDDL.Table)
			parsed := sqlparser.BuildParsedQuery(sqlRenameTable,
				vreplTable, afterTableName,
			)
			if _, err = e.execQuery(ctx, parsed.Query); err != nil {
				return err
			}
		} else {
			// Normal (non-testing) alter table
			parsed := sqlparser.BuildParsedQuery(sqlSwapTables,
				onlineDDL.Table, swapTable,
				vreplTable, onlineDDL.Table,
				swapTable, vreplTable,
			)
			if _, err = e.execQuery(ctx, parsed.Query); err != nil {
				return err
			}
		}
	}

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
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, s.rowsCopied)
	return nil

	// deferred function will re-enable writes now
	// deferred function will unlock keyspace
}

func (e *Executor) initVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (v *VRepl, err error) {
	vreplTableName := fmt.Sprintf("_%s_%s_vrepl", onlineDDL.UUID, ReadableTimestamp())
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
func (e *Executor) postInitVreplicationOriginalMigration(ctx context.Context, v *VRepl, conn *dbconnpool.DBConnection) (err error) {
	if v.sourceAutoIncrement > 0 && !v.parser.IsAutoIncrementDefined() {
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

	v = NewVRepl(onlineDDL.UUID, e.keyspace, e.shard, e.dbName, onlineDDL.Table, vreplTableName, "")
	v.pos = revertStream.pos
	return v, nil
}

// ExecuteWithVReplication sets up the grounds for a vreplication schema migration
func (e *Executor) ExecuteWithVReplication(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) error {
	// make sure there's no vreplication workflow running under same name
	_ = e.terminateVReplMigration(ctx, onlineDDL.UUID)

	if e.isAnyMigrationRunning() {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
		return ErrExecutorNotWritableTablet
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()

	atomic.StoreInt64(&e.vreplMigrationRunning, 1)
	e.lastMigrationUUID = onlineDDL.UUID
	if err := e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown); err != nil {
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
	if err := e.updateMigrationAddedRemovedUniqueKeys(ctx, onlineDDL.UUID, len(v.addedUniqueKeys), len(v.removedUniqueKeys)); err != nil {
		return err
	}
	if revertMigration == nil {
		// Original ALTER TABLE request for vreplication
		if err := e.validateTableForAlterAction(ctx, onlineDDL); err != nil {
			return err
		}
		if err := e.postInitVreplicationOriginalMigration(ctx, v, conn); err != nil {
			return err
		}
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, v.targetTable); err != nil {
		return err
	}

	{
		// We need to talk to tabletmanager's VREngine. But we're on TabletServer. While we live in the same
		// process as VREngine, it is actually simpler to get hold of it via gRPC, just like wrangler does.
		tmClient := tmclient.NewTabletManagerClient()
		tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
		if err != nil {
			return err
		}
		// reload schema
		if err := tmClient.ReloadSchema(ctx, tablet.Tablet, ""); err != nil {
			return err
		}

		// create vreplication entry
		insertVReplicationQuery, err := v.generateInsertStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := tmClient.VReplicationExec(ctx, tablet.Tablet, insertVReplicationQuery); err != nil {
			return err
		}
		// start stream!
		startVReplicationQuery, err := v.generateStartStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := tmClient.VReplicationExec(ctx, tablet.Tablet, startVReplicationQuery); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteWithGhost validates and runs a gh-ost process.
// Validation included testing the backend MySQL server and the gh-ost binary itself
// Execution runs first a dry run, then an actual migration
func (e *Executor) ExecuteWithGhost(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	if e.isAnyMigrationRunning() {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
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
	onHookContent := func(status schema.OnlineDDLStatus) string {
		return fmt.Sprintf(`#!/bin/bash
	curl --max-time 10 -s 'http://localhost:%d/schema-migration/report-status?uuid=%s&status=%s&dryrun='"$GH_OST_DRY_RUN"'&progress='"$GH_OST_PROGRESS"'&eta='"$GH_OST_ETA_SECONDS"'&rowscopied='"$GH_OST_COPIED_ROWS"
			`, *servenv.Port, onlineDDL.UUID, string(status))
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-startup", onHookContent(schema.OnlineDDLStatusRunning)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-status", onHookContent(schema.OnlineDDLStatusRunning)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-success", onHookContent(schema.OnlineDDLStatusComplete)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	if _, err := createTempScript(tempDir, "gh-ost-on-failure", onHookContent(schema.OnlineDDLStatusFailed)); err != nil {
		log.Errorf("Error creating script: %+v", err)
		return err
	}
	serveSocketFile := path.Join(tempDir, "serve.sock")

	if err := e.deleteGhostPanicFlagFile(onlineDDL.UUID); err != nil {
		log.Errorf("Error removing gh-ost panic flag file %s: %+v", e.ghostPanicFlagFileName(onlineDDL.UUID), err)
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
		args = append(args, onlineDDL.StrategySetting().RuntimeOptions()...)
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("executing gh-ost --execute=%v", execute))
		_, err := execCmd("bash", args, os.Environ(), "/tmp", nil, nil)
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("executed gh-ost --execute=%v, err=%v", execute, err))
		if err != nil {
			// See if we can get more info from the failure file
			if content, ferr := ioutil.ReadFile(path.Join(tempDir, migrationFailureFileName)); ferr == nil {
				failureMessage := strings.TrimSpace(string(content))
				if failureMessage != "" {
					// This message was produced by gh-ost itself. It is more informative than the default "migration failed..." message. Overwrite.
					return errors.New(failureMessage)
				}
			}
		}
		return err
	}

	atomic.StoreInt64(&e.ghostMigrationRunning, 1)
	e.lastMigrationUUID = onlineDDL.UUID

	go func() error {
		defer atomic.StoreInt64(&e.ghostMigrationRunning, 0)
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
	if e.isAnyMigrationRunning() {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
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
	  get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusRunning}}&dryrun={{DRYRUN}}");
	}

	sub before_exit {
		my($self, % args) = @_;
		my $exit_status = $args{exit_status};
	  if ($exit_status == 0) {
	    get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusComplete}}&dryrun={{DRYRUN}}");
	  } else {
	    get("http://localhost:{{VTTABLET_PORT}}/schema-migration/report-status?uuid={{MIGRATION_UUID}}&status={{OnlineDDLStatusFailed}}&dryrun={{DRYRUN}}");
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

	atomic.StoreInt64(&e.ptoscMigrationRunning, 1)
	e.lastMigrationUUID = onlineDDL.UUID

	go func() error {
		defer atomic.StoreInt64(&e.ptoscMigrationRunning, 0)
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
		Keyspace:       row["keyspace"].ToString(),
		Table:          row["mysql_table"].ToString(),
		Schema:         row["mysql_schema"].ToString(),
		SQL:            row["migration_statement"].ToString(),
		UUID:           row["migration_uuid"].ToString(),
		Strategy:       schema.DDLStrategy(row["strategy"].ToString()),
		Options:        row["options"].ToString(),
		Status:         schema.OnlineDDLStatus(row["migration_status"].ToString()),
		Retries:        row.AsInt64("retries", 0),
		TabletAlias:    row["tablet"].ToString(),
		RequestContext: row["migration_context"].ToString(),
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
func (e *Executor) terminateMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, lastMigrationUUID string) (foundRunning bool, err error) {
	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline:
		// migration could have started by a different tablet. We need to actively verify if it is running
		foundRunning, _, _ = e.isVReplMigrationRunning(ctx, onlineDDL.UUID)
		if err := e.terminateVReplMigration(ctx, onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error cancelling migration, vreplication exec error: %+v", err)
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
		if atomic.LoadInt64(&e.ghostMigrationRunning) > 0 {
			// double check: is the running migration the very same one we wish to cancel?
			if onlineDDL.UUID == lastMigrationUUID {
				// assuming all goes well in next steps, we can already report that there has indeed been a migration
				foundRunning = true
			}
		}
		// gh-ost migrations are easy to kill: just touch their specific panic flag files. We trust
		// gh-ost to terminate. No need to KILL it. And there's no trigger cleanup.
		if err := e.createGhostPanicFlagFile(onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error cancelling migration, flag file error: %+v", err)
		}
	}
	return foundRunning, nil
}

// CancelMigration attempts to abort a scheduled or a running migration
func (e *Executor) CancelMigration(ctx context.Context, uuid string, terminateRunningMigration bool, message string) (result *sqltypes.Result, err error) {
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
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusCancelled); err != nil {
			return nil, err
		}
		rowsAffected = 1
	}

	if terminateRunningMigration {
		migrationFound, err := e.terminateMigration(ctx, onlineDDL, e.lastMigrationUUID)
		defer e.updateMigrationMessage(ctx, onlineDDL.UUID, message)

		if migrationFound {
			rowsAffected = 1
		}
		if err != nil {
			return result, err
		}
	}

	result = &sqltypes.Result{
		RowsAffected: rowsAffected,
	}

	return result, nil
}

// cancelMigrations attempts to abort a list of migrations
func (e *Executor) cancelMigrations(ctx context.Context, uuids []string, message string) (err error) {
	for _, uuid := range uuids {
		log.Infof("cancelMigrations: cancelling %s", uuid)
		if _, err := e.CancelMigration(ctx, uuid, true, message); err != nil {
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
		res, err := e.CancelMigration(ctx, uuid, true, message)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	return result, nil
}

// scheduleNextMigration attemps to schedule a single migration to run next.
// possibly there's no migrations to run. Possibly there's a migration running right now,
// in which cases nothing happens.
func (e *Executor) scheduleNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if e.isAnyMigrationRunning() {
		return ErrExecutorMigrationAlreadyRunning
	}

	{
		r, err := e.execQuery(ctx, sqlSelectCountReadyMigrations)
		if err != nil {
			return err
		}

		row := r.Named().Row()
		countReady, err := row.ToInt64("count_ready")
		if err != nil {
			return err
		}

		if countReady > 0 {
			// seems like there's already one migration that's good to go
			return nil
		}
	} // Cool, seems like no migration is ready. Let's try and make a single 'queued' migration 'ready'

	_, err := e.execQuery(ctx, sqlScheduleSingleMigration)

	return err
}

func (e *Executor) validateMigrationRevertible(ctx context.Context, revertMigration *schema.OnlineDDL) (err error) {
	// Validation: migration to revert exists and is in complete state
	action, actionStr, err := revertMigration.GetActionStr()
	if err != nil {
		return err
	}
	switch action {
	case sqlparser.AlterDDLAction:
		if revertMigration.Strategy != schema.DDLStrategyOnline {
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
	revertUUID, _ := onlineDDL.GetRevertUUID()
	if err != nil {
		return fmt.Errorf("cannot run a revert migration %v: %+v", onlineDDL.UUID, err)
	}

	revertMigration, row, err := e.readMigration(ctx, revertUUID)
	if err != nil {
		return err
	}
	if err := e.validateMigrationRevertible(ctx, revertMigration); err != nil {
		return err
	}
	revertActionStr := row["ddl_action"].ToString()
	switch revertActionStr {
	case sqlparser.CreateStr:
		{
			// We are reverting a CREATE migration. The revert is to DROP, only we don't actually
			// drop the table, we rename it into lifecycle
			// Possibly this was a CREATE TABLE IF NOT EXISTS, and possibly the table already existed
			// before the DDL, in which case the CREATE was a noop. In that scenario we _do not_ drop
			// the table.
			// We can tell the difference by looking at the artifacts. A successful CREATE TABLE, where
			// a table actually gets created, has a sentry, dummy artifact. A noop has not.

			if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.DropStr); err != nil {
				return err
			}
			if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertMigration.Table); err != nil {
				return err
			}

			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// This indicates no table was actually created. this must have been a CREATE TABLE IF NOT EXISTS where the table already existed.
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown)
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
			if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.CreateStr); err != nil {
				return err
			}
			if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertMigration.Table); err != nil {
				return err
			}
			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// Could happen on `DROP TABLE IF EXISTS` where the table did not exist...
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown)
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
			if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.AlterStr); err != nil {
				return err
			}
			if err := e.ExecuteWithVReplication(ctx, onlineDDL, revertMigration); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertMigration.UUID, revertActionStr)
	}

	return nil
}

// evaluateDeclarativeDiff is called for -declarative CREATE statements, where the table already exists. The function generates a SQL diff, which can be:
// - empty, in which case the migration is noop and implicitly successful, or
// - non-empty, in which case the migration turns to be an ALTER
func (e *Executor) evaluateDeclarativeDiff(ctx context.Context, onlineDDL *schema.OnlineDDL) (alterClause string, err error) {

	// Modify the CREATE TABLE statement to indicate a different, made up table name, known as the "comparison table"
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return "", err
	}
	comparisonTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return "", err
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	{
		// Create the comparison table
		ddlStmt.SetTable("", comparisonTableName)
		modifiedCreateSQL := sqlparser.String(ddlStmt)

		if _, err := conn.ExecuteFetch(modifiedCreateSQL, 0, false); err != nil {
			return "", err
		}

		defer func() {
			// Drop the comparison table
			parsed := sqlparser.BuildParsedQuery(sqlDropTable, comparisonTableName)
			_, _ = conn.ExecuteFetch(parsed.Query, 0, false)
			// Nothing bad happens for not checking the error code. The table is GC/HOLD. If we
			// can't drop it now, it still gets collected later by tablegc mechanism
		}()

	}

	// Compare the existing (to be potentially migrated) table with the declared (newly created) table:
	// all things are tengo related
	if err := func() error {
		variables, err := e.readMySQLVariables(ctx)
		if err != nil {
			return err
		}
		flavor := tengo.ParseFlavor(variables.version, variables.versionComment)

		// Create a temporary account for tengo to use
		onlineDDLPassword, err := e.createOnlineDDLUser(ctx)
		if err != nil {
			return err
		}
		defer e.dropOnlineDDLUser(ctx)

		// tengo requires sqlx.DB
		cfg := mysqldriver.NewConfig()
		cfg.User = onlineDDLUser
		cfg.Passwd = onlineDDLPassword
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", variables.host, variables.port)
		cfg.DBName = e.dbName
		cfg.ParseTime = true
		cfg.InterpolateParams = true
		cfg.Timeout = 1 * time.Second
		mysqlDSN := cfg.FormatDSN()

		db, err := sqlx.Open("mysql", mysqlDSN)
		if err != nil {
			return err
		}
		defer db.Close()

		// Read existing table
		existingTable, err := tengo.QuerySchemaTable(ctx, db, e.dbName, onlineDDL.Table, flavor)
		if err != nil {
			return err
		}
		// Read comparison table
		comparisonTable, err := tengo.QuerySchemaTable(ctx, db, e.dbName, comparisonTableName, flavor)
		if err != nil {
			return err
		}
		// We created the comparison tablein same schema as original table, but under different name (because obviously we can't have
		// two tables with identical name in same schema). It is our preference to create the table in the same schema.
		// unfortunately, tengo does not allow comparing tables with different names. After asking tengo to read table info, we cheat
		// and override the name of the table:
		comparisonTable.Name = existingTable.Name
		// We also override `.CreateStatement` (output of SHOW CREATE TABLE), because tengo has a validation, where if it doesn't
		// find any ALTER changes, then the CreateStatement-s must be identical (or else it errors with UnsupportedDiffError)
		comparisonTable.CreateStatement, err = schema.ReplaceTableNameInCreateTableStatement(comparisonTable.CreateStatement, existingTable.Name)
		if err != nil {
			return err
		}
		// Diff the two tables
		diff := tengo.NewAlterTable(existingTable, comparisonTable)
		if diff == nil {
			// No change. alterClause remains empty
			return nil
		}
		mods := tengo.StatementModifiers{
			AllowUnsafe: true,
			NextAutoInc: tengo.NextAutoIncIfIncreased,
		}
		alterClause, err = diff.Clauses(mods)
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return "", err
	}

	return alterClause, nil
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
		_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
		if err != nil {
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, err.Error())
		}
		return err
	}

	ddlAction, err := onlineDDL.GetAction()
	if err != nil {
		return failMigration(err)
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

			exists, err := e.tableExists(ctx, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				// table does exist, so this declarative DROP turns out to really be an actual DROP. No further action is needed here
			} else {
				// table does not exist. We mark this DROP as implicitly sucessful
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown)
				_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "no change")
				return nil
			}
		case sqlparser.CreateDDLAction:
			// This CREATE is declarative, meaning it may:
			// - actually CREATE a table, if that table does not exist, or
			// - ALTER the table, if it exists and is different, or
			// - Implicitly do nothing, if the table exists and is identical to CREATE statement

			{
				// Sanity: reject IF NOT EXISTS statements, because they don't make sense (or are ambiguous) in declarative mode
				ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
				if err != nil {
					return failMigration(err)
				}
				if ddlStmt.GetIfNotExists() {
					return failMigration(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "strategy is declarative. IF NOT EXISTS does not work in declarative mode for migration %v", onlineDDL.UUID))
				}
			}
			exists, err := e.tableExists(ctx, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				alterClause, err := e.evaluateDeclarativeDiff(ctx, onlineDDL)
				if err != nil {
					return failMigration(err)
				}
				if alterClause == "" {
					// No diff! We mark this CREATE as implicitly sucessful
					_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown)
					_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "no change")
					return nil
				}
				// alterClause is non empty. We convert this migration into an ALTER
				if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.AlterStr); err != nil {
					return failMigration(err)
				}
				ddlAction = sqlparser.AlterDDLAction
				onlineDDL.SQL = fmt.Sprintf("ALTER TABLE `%s` %s", onlineDDL.Table, alterClause)
				_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, alterClause)
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
		}()
	case sqlparser.CreateDDLAction:
		go func() error {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			sentryArtifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
			if err != nil {
				return failMigration(err)
			}
			// we create a dummy artifact. Its existence means the table was created by this migration.
			// It will be read by the revert operation.
			if err := e.updateArtifacts(ctx, onlineDDL.UUID, sentryArtifactTableName); err != nil {
				return err
			}
			ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
			if err != nil {
				return failMigration(err)
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
				failMigration(err)
			}
			return nil
		}()
	case sqlparser.AlterDDLAction:
		switch onlineDDL.Strategy {
		case schema.DDLStrategyOnline:
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

func (e *Executor) runNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if e.isAnyMigrationRunning() {
		return ErrExecutorMigrationAlreadyRunning
	}

	r, err := e.execQuery(ctx, sqlSelectReadyMigration)
	if err != nil {
		return err
	}
	named := r.Named()
	for i, row := range named.Rows {
		onlineDDL := &schema.OnlineDDL{
			Keyspace: row["keyspace"].ToString(),
			Table:    row["mysql_table"].ToString(),
			Schema:   row["mysql_schema"].ToString(),
			SQL:      row["migration_statement"].ToString(),
			UUID:     row["migration_uuid"].ToString(),
			Strategy: schema.DDLStrategy(row["strategy"].ToString()),
			Options:  row["options"].ToString(),
			Status:   schema.OnlineDDLStatus(row["migration_status"].ToString()),
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
		// the query should only ever return a single row at the most
		// but let's make it also explicit here that we only run a single migration
		if i == 0 {
			break
		}
	}
	return nil
}

// isPTOSCMigrationRunning sees if pt-online-schema-change is running a specific migration,
// by examining its PID file
func (e *Executor) isPTOSCMigrationRunning(ctx context.Context, uuid string) (isRunning bool, pid int, err error) {
	// Try and read its PID file:
	content, err := ioutil.ReadFile(e.ptPidFileName(uuid))
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
	if strings.Contains(strings.ToLower(s.message), "error") {
		return false, s, nil
	}
	switch s.state {
	case binlogplayer.VReplicationInit, binlogplayer.VReplicationCopying, binlogplayer.BlpRunning:
		return true, s, nil
	}
	return false, s, nil
}

// reviewRunningMigrations iterates migrations in 'running' state. Normally there's only one running, which was
// spawned by this tablet; but vreplication migrations could also resume from failure.
func (e *Executor) reviewRunningMigrations(ctx context.Context) (countRunnning int, cancellable []string, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	r, err := e.execQuery(ctx, sqlSelectRunningMigrations)
	if err != nil {
		return countRunnning, cancellable, err
	}
	// we identify running vreplication migrations in this function
	atomic.StoreInt64(&e.vreplMigrationRunning, 0)
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		strategy := schema.DDLStrategy(row["strategy"].ToString())
		strategySettings := schema.NewDDLStrategySetting(strategy, row["options"].ToString())
		elapsedSeconds := row.AsInt64("elapsed_seconds", 0)

		switch strategy {
		case schema.DDLStrategyOnline:
			{
				// We check the _vt.vreplication table
				running, s, err := e.isVReplMigrationRunning(ctx, uuid)
				if err != nil {
					return countRunnning, cancellable, err
				}
				isVreplicationTestSuite := strategySettings.IsVreplicationTestSuite()
				if isVreplicationTestSuite {
					e.triggerNextCheckInterval()
				}
				if running {
					// This VRepl migration may have started from outside this tablet, so
					// vreplMigrationRunning could be zero. Whatever the case is, we're under
					// migrationMutex lock and it's now safe to ensure vreplMigrationRunning is 1
					atomic.StoreInt64(&e.vreplMigrationRunning, 1)
					_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)

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
				if uuid != e.lastMigrationUUID {
					// This executor can only spawn one migration at a time. And that
					// migration is identified by e.lastMigrationUUID.
					// If we find a _running_ migration that does not have this UUID, it _must_
					// mean the migration was started by a former vttablet (ie vttablet crashed and restarted)
					cancellable = append(cancellable, uuid)
				}
			}
		}
		countRunnning++

		if uuid != e.lastMigrationUUID {
			// This executor can only run one migration at a time. And that
			// migration is identified by e.lastMigrationUUID.
			// If we find a _running_ migration that does not have this UUID, it _must_
			// mean the migration was started by a former vttablet (ie vttablet crashed and restarted)
			cancellable = append(cancellable, uuid)
		}
	}
	return countRunnning, cancellable, err
}

// reviewStaleMigrations marks as 'failed' migrations whose status is 'running' but which have
// shown no liveness in past X minutes
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
		// If this is pt-osc migration, then it may have crashed without having its triggers cleaned up.
		// make sure to drop them.
		if onlineDDL.Strategy == schema.DDLStrategyPTOSC {
			if err := e.dropPTOSCMigrationTriggers(ctx, onlineDDL); err != nil {
				return err
			}
		}
		if onlineDDL.TabletAlias != e.TabletAliasString() {
			// This means another tablet started the migration, and the migration has failed due to the tablet failure (e.g. master failover)
			if err := e.updateTabletFailure(ctx, onlineDDL.UUID); err != nil {
				return err
			}
		}
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed); err != nil {
			return err
		}
		_ = e.updateMigrationStartedTimestamp(ctx, uuid)
		if err := e.updateMigrationTimestamp(ctx, "completed_timestamp", uuid); err != nil {
			return err
		}
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, "stale migration")
	}

	return nil
}

// retryTabletFailureMigrations looks for migrations failed by tablet failure (e.g. by failover)
// and retry them (put them back in the queue)
func (e *Executor) retryTabletFailureMigrations(ctx context.Context) error {
	_, err := e.retryMigrationWhere(ctx, sqlWhereTabletFailure)
	return err
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

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
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
	if err := e.scheduleNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if err := e.runNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if _, cancellable, err := e.reviewRunningMigrations(ctx); err != nil {
		log.Error(err)
	} else if err := e.cancelMigrations(ctx, cancellable, "auto cancel"); err != nil {
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

func (e *Executor) updateMigrationAddedRemovedUniqueKeys(ctx context.Context, uuid string, addedUniqueKeys, removedUnqiueKeys int) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateAddedRemovedUniqueKeys,
		sqltypes.Int64BindVariable(int64(addedUniqueKeys)),
		sqltypes.Int64BindVariable(int64(removedUnqiueKeys)),
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
	onlineDDL, err := schema.OnlineDDLFromCommentedStatement(stmt)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Error submitting migration %s: %v", sqlparser.String(stmt), err)
	}
	_, actionStr, err := onlineDDL.GetActionStr()
	if err != nil {
		return nil, err
	}

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
		sqltypes.StringBindVariable(onlineDDL.RequestContext),
		sqltypes.StringBindVariable(string(schema.OnlineDDLStatusQueued)),
		sqltypes.StringBindVariable(e.TabletAliasString()),
	)
	if err != nil {
		return nil, err
	}

	if err := e.initSchema(ctx); err != nil {
		log.Error(err)
		return nil, err
	}

	if onlineDDL.StrategySetting().IsSingleton() || onlineDDL.StrategySetting().IsSingletonContext() {
		e.migrationMutex.Lock()
		defer e.migrationMutex.Unlock()

		pendingUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
		if err != nil {
			return nil, err
		}
		switch {
		case onlineDDL.StrategySetting().IsSingleton():
			// We will reject this migration if there's any pending migration
			if len(pendingUUIDs) > 0 {
				return result, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migrations [%s]", strings.Join(pendingUUIDs, ", "))
			}
		case onlineDDL.StrategySetting().IsSingletonContext():
			// We will reject this migration if there's any pending migration within a different context
			for _, pendingUUID := range pendingUUIDs {
				pendingOnlineDDL, _, err := e.readMigration(ctx, pendingUUID)
				if err != nil {
					return nil, err
				}
				if pendingOnlineDDL.RequestContext != onlineDDL.RequestContext {
					return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migration: %s in different context: %s", pendingUUID, pendingOnlineDDL.RequestContext)
				}
			}
		}
	}

	defer e.triggerNextCheckInterval()

	return e.execQuery(ctx, query)
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
	content, err := ioutil.ReadFile(logFile)
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
	uuid string, status schema.OnlineDDLStatus, dryRun bool, progressPct float64, etaSeconds int64, rowsCopied int64) (err error) {
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
	uuidParam, statusParam, dryrunParam, progressParam, etaParam, rowsCopiedParam string) (err error) {
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

	return e.onSchemaMigrationStatus(ctx, uuidParam, status, dryRun, progressPct, etaSeconds, rowsCopied)
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
		case cancelMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.CancelMigration(ctx, uuid, true, "cancel by user"))
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
