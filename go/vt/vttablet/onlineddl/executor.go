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
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/syscallutil"
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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	// ErrExecutorNotWritableTablet  is generated when executor is asked to run gh-ost on a read-only server
	ErrExecutorNotWritableTablet = errors.New("cannot run migration on non-writable tablet")
	// ErrExecutorMigrationAlreadyRunning is generated when an attempt is made to run an operation that conflicts with a running migration
	ErrExecutorMigrationAlreadyRunning = errors.New("cannot run migration since a migration is already running")
	// ErrMigrationNotFound is returned by readMigration when given UUI cannot be found
	ErrMigrationNotFound = errors.New("migration not found")
)

var (
	// fixCompletedTimestampDone fixes a nil `completed_timestamp` columns, see
	// https://github.com/vitessio/vitess/issues/13927
	// The fix is in release-18.0
	// TODO: remove in release-19.0
	fixCompletedTimestampDone bool
)

var emptyResult = &sqltypes.Result{}
var acceptableDropTableIfExistsErrorCodes = []sqlerror.ErrorCode{sqlerror.ERCantFindFile, sqlerror.ERNoSuchTable}
var copyAlgorithm = sqlparser.AlgorithmValue(sqlparser.CopyStr)

var (
	ghostOverridePath       string
	ptOSCOverridePath       string
	migrationCheckInterval  = 1 * time.Minute
	retainOnlineDDLTables   = 24 * time.Hour
	defaultCutOverThreshold = 10 * time.Second
	maxConcurrentOnlineDDLs = 256

	migrationNextCheckIntervals = []time.Duration{1 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}
	maxConstraintNameLength     = 64
	cutoverIntervals            = []time.Duration{0, 1 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute}
)

func init() {
	servenv.OnParseFor("vtcombo", registerOnlineDDLFlags)
	servenv.OnParseFor("vttablet", registerOnlineDDLFlags)
}

func registerOnlineDDLFlags(fs *pflag.FlagSet) {
	fs.StringVar(&ghostOverridePath, "gh-ost-path", ghostOverridePath, "override default gh-ost binary full path")
	fs.StringVar(&ptOSCOverridePath, "pt-osc-path", ptOSCOverridePath, "override default pt-online-schema-change binary full path")
	fs.DurationVar(&migrationCheckInterval, "migration_check_interval", migrationCheckInterval, "Interval between migration checks")
	fs.DurationVar(&retainOnlineDDLTables, "retain_online_ddl_tables", retainOnlineDDLTables, "How long should vttablet keep an old migrated table before purging it")
	fs.IntVar(&maxConcurrentOnlineDDLs, "max_concurrent_online_ddl", maxConcurrentOnlineDDLs, "Maximum number of online DDL changes that may run concurrently")
}

const (
	maxPasswordLength                        = 32 // MySQL's *replication* password may not exceed 32 characters
	staleMigrationMinutes                    = 180
	progressPctStarted               float64 = 0
	progressPctFull                  float64 = 100.0
	etaSecondsUnknown                        = -1
	etaSecondsNow                            = 0
	rowsCopiedUnknown                        = 0
	emptyHint                                = ""
	readyToCompleteHint                      = "ready_to_complete"
	databasePoolSize                         = 3
	qrBufferExtraTimeout                     = 5 * time.Second
	grpcTimeout                              = 30 * time.Second
	vreplicationTestSuiteWaitSeconds         = 5
)

var (
	migrationLogFileName     = "migration.log"
	migrationFailureFileName = "migration-failure.log"
	onlineDDLUser            = "vt-online-ddl-internal"
	onlineDDLGrant           = fmt.Sprintf("'%s'@'%s'", onlineDDLUser, "%")
)

type ConstraintType int

const (
	UnknownConstraintType ConstraintType = iota
	CheckConstraintType
	ForeignKeyConstraintType
)

var (
	constraintIndicatorMap = map[int]string{
		int(CheckConstraintType):      "chk",
		int(ForeignKeyConstraintType): "fk",
	}
)

func GetConstraintType(constraintInfo sqlparser.ConstraintInfo) ConstraintType {
	if _, ok := constraintInfo.(*sqlparser.CheckConstraintDefinition); ok {
		return CheckConstraintType
	}
	if _, ok := constraintInfo.(*sqlparser.ForeignKeyDefinition); ok {
		return ForeignKeyConstraintType
	}
	return UnknownConstraintType
}

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
	lagThrottler          *throttle.Throttler
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool)
	requestGCChecksFunc   func()
	tabletAlias           *topodatapb.TabletAlias

	keyspace string
	shard    string
	dbName   string

	initMutex      sync.Mutex
	migrationMutex sync.Mutex
	submitMutex    sync.Mutex // used when submitting migrations
	// ownedRunningMigrations lists UUIDs owned by this executor (consider this a map[string]bool)
	// A UUID listed in this map stands for a migration that is executing, and that this executor can control.
	// Migrations found to be running which are not listed in this map will either:
	// - be adopted by this executor (possible for vreplication migrations), or
	// - be terminated (example: pt-osc migration gone rogue, process still running even as the migration failed)
	// The Executor auto-reviews the map and cleans up migrations thought to be running which are not running.
	ownedRunningMigrations        sync.Map
	vreplicationLastError         map[string]*vterrors.LastError
	tickReentranceFlag            int64
	reviewedRunningMigrationsFlag bool

	ticks  *timer.Timer
	isOpen int64

	// This will be a pointer to the executeQuery function unless
	// a custom sidecar database is used, then it will point to
	// the executeQueryWithSidecarDBReplacement function. This
	// variable assignment must be managed in the Open function.
	execQuery func(ctx context.Context, query string) (result *sqltypes.Result, err error)
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
	if ghostOverridePath != "" {
		return ghostOverridePath, true
	}
	return path.Join(os.TempDir(), "vt-gh-ost"), false
}

// PTOSCFileName returns the full path+name of the pt-online-schema-change binary
// Note that vttablet does not include pt-online-schema-change
func PTOSCFileName() (fileName string, isOverride bool) {
	if ptOSCOverridePath != "" {
		return ptOSCOverridePath, true
	}
	return "/usr/bin/pt-online-schema-change", false
}

// newGCTableRetainTime returns the time until which a new GC table is to be retained
func newGCTableRetainTime() time.Time {
	return time.Now().UTC().Add(retainOnlineDDLTables)
}

// getMigrationCutOverThreshold returns the cut-over threshold for the given migration. The migration's
// DDL Strategy may explicitly set the threshold; otherwise, we return the default cut-over threshold.
func getMigrationCutOverThreshold(onlineDDL *schema.OnlineDDL) time.Duration {
	if threshold, _ := onlineDDL.StrategySetting().CutOverThreshold(); threshold != 0 {
		return threshold
	}
	return defaultCutOverThreshold
}

// NewExecutor creates a new gh-ost executor.
func NewExecutor(env tabletenv.Env, tabletAlias *topodatapb.TabletAlias, ts *topo.Server,
	lagThrottler *throttle.Throttler,
	tabletTypeFunc func() topodatapb.TabletType,
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool),
	requestGCChecksFunc func(),
) *Executor {
	// sanitize flags
	if maxConcurrentOnlineDDLs < 1 {
		maxConcurrentOnlineDDLs = 1 // or else nothing will ever run
	}
	return &Executor{
		env:         env,
		tabletAlias: tabletAlias.CloneVT(),

		pool: connpool.NewPool(env, "OnlineDDLExecutorPool", tabletenv.ConnPoolConfig{
			Size:        databasePoolSize,
			IdleTimeout: env.Config().OltpReadPool.IdleTimeout,
		}),
		tabletTypeFunc:        tabletTypeFunc,
		ts:                    ts,
		lagThrottler:          lagThrottler,
		toggleBufferTableFunc: toggleBufferTableFunc,
		requestGCChecksFunc:   requestGCChecksFunc,
		ticks:                 timer.NewTimer(migrationCheckInterval),
		// Gracefully return an error if any caller tries to execute
		// a query before the executor has been fully opened.
		execQuery: func(ctx context.Context, query string) (result *sqltypes.Result, err error) {
			return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "onlineddl executor is closed")
		},
	}
}

func (e *Executor) executeQuery(ctx context.Context, query string) (result *sqltypes.Result, err error) {
	defer e.env.LogError()

	conn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return result, err
	}
	defer conn.Recycle()

	return conn.Conn.Exec(ctx, query, -1, true)
}

func (e *Executor) executeQueryWithSidecarDBReplacement(ctx context.Context, query string) (result *sqltypes.Result, err error) {
	defer e.env.LogError()

	conn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return result, err
	}
	defer conn.Recycle()

	// Replace any provided sidecar DB qualifiers with the correct one.
	uq, err := e.env.Environment().Parser().ReplaceTableQualifiers(query, sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	return conn.Conn.Exec(ctx, uq, -1, true)
}

// TabletAliasString returns tablet alias as string (duh)
func (e *Executor) TabletAliasString() string {
	return topoproto.TabletAliasString(e.tabletAlias)
}

// InitDBConfig initializes keyspace
func (e *Executor) InitDBConfig(keyspace, shard, dbName string) {
	e.keyspace = keyspace
	e.shard = shard
	e.dbName = dbName
}

// Open opens database pool and initializes the schema
func (e *Executor) Open() error {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if atomic.LoadInt64(&e.isOpen) > 0 || !e.env.Config().EnableOnlineDDL {
		return nil
	}
	log.Infof("onlineDDL Executor Open()")

	e.reviewedRunningMigrationsFlag = false // will be set as "true" by reviewRunningMigrations()
	e.ownedRunningMigrations.Range(func(k, _ any) bool {
		e.ownedRunningMigrations.Delete(k)
		return true
	})
	e.vreplicationLastError = make(map[string]*vterrors.LastError)

	if sidecar.GetName() != sidecar.DefaultName {
		e.execQuery = e.executeQueryWithSidecarDBReplacement
	} else {
		e.execQuery = e.executeQuery
	}

	e.pool.Open(e.env.Config().DB.AppWithDB(), e.env.Config().DB.DbaWithDB(), e.env.Config().DB.AppDebugWithDB())
	e.ticks.Start(e.onMigrationCheckTick)
	e.triggerNextCheckInterval()

	atomic.StoreInt64(&e.isOpen, 1)

	return nil
}

// Close frees resources
func (e *Executor) Close() {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return
	}
	log.Infof("onlineDDL Executor Close()")

	e.ticks.Stop()
	e.pool.Close()
	atomic.StoreInt64(&e.isOpen, 0)
}

// triggerNextCheckInterval the next tick sooner than normal
func (e *Executor) triggerNextCheckInterval() {
	for _, interval := range migrationNextCheckIntervals {
		e.ticks.TriggerAfter(interval)
	}
}

// matchesShards checks whether given comma delimited shard names include this tablet's shard. If the input param is empty then
// that implicitly means "true"
func (e *Executor) matchesShards(commaDelimitedShards string) bool {
	shards := textutil.SplitDelimitedList(commaDelimitedShards)
	if len(shards) == 0 {
		// Nothing explicitly defined, so implicitly all shards are allowed
		return true
	}
	for _, shard := range shards {
		if shard == e.shard {
			return true
		}
	}
	return false
}

// countOwnedRunningMigrations returns an estimate of current count of running migrations; this is
// normally an accurate number, but can be inexact because the executor periodically reviews
// e.ownedRunningMigrations and adds/removes migrations based on actual migration state.
func (e *Executor) countOwnedRunningMigrations() (count int) {
	e.ownedRunningMigrations.Range(func(_, val any) bool {
		if _, ok := val.(*schema.OnlineDDL); ok {
			count++
		}
		return true // continue iteration
	})
	return count
}

// allowConcurrentMigration checks if the given migration is allowed to run concurrently.
// First, the migration itself must declare --allow-concurrent. But then, there's also some
// restrictions on which migrations exactly are allowed such concurrency.
func (e *Executor) allowConcurrentMigration(onlineDDL *schema.OnlineDDL) (action sqlparser.DDLAction, allowConcurrent bool) {
	if !onlineDDL.StrategySetting().IsAllowConcurrent() {
		return action, false
	}

	var err error
	action, err = onlineDDL.GetAction(e.env.Environment().Parser())
	if err != nil {
		return action, false
	}
	switch action {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction:
		// CREATE TABLE, DROP TABLE are allowed to run concurrently.
		return action, true
	case sqlparser.AlterDDLAction:
		// ALTER is only allowed concurrent execution if this is a Vitess migration
		strategy := onlineDDL.StrategySetting().Strategy
		return action, (strategy == schema.DDLStrategyOnline || strategy == schema.DDLStrategyVitess)
	case sqlparser.RevertDDLAction:
		// REVERT is allowed to run concurrently.
		// Reminder that REVERT is supported for CREATE, DROP and for 'vitess' ALTER, but never for
		// 'gh-ost' or 'pt-osc' ALTERs
		return action, true
	}
	return action, false
}

func (e *Executor) proposedMigrationConflictsWithRunningMigration(runningMigration, proposedMigration *schema.OnlineDDL) bool {
	if runningMigration.Table == proposedMigration.Table {
		// migrations operate on same table
		return true
	}
	_, isRunningMigrationAllowConcurrent := e.allowConcurrentMigration(runningMigration)
	proposedMigrationAction, isProposedMigrationAllowConcurrent := e.allowConcurrentMigration(proposedMigration)
	if !isRunningMigrationAllowConcurrent && !isProposedMigrationAllowConcurrent {
		// neither allowed concurrently
		return true
	}
	if proposedMigrationAction == sqlparser.AlterDDLAction {
		// A new ALTER migration conflicts with an existing migration if the existing migration is still not ready to complete.
		// Specifically, if the running migration is an ALTER, and is still busy with copying rows (copy_state), then
		// we consider the two to be conflicting. But, if the running migration is done copying rows, and is now only
		// applying binary logs, and is up-to-date, then we consider a new ALTER migration to be non-conflicting.
		if atomic.LoadInt64(&runningMigration.WasReadyToComplete) == 0 {
			return true
		}
	}
	return false
}

// isAnyConflictingMigrationRunning checks if there's any running migration that conflicts with the
// given migration, such that they can't both run concurrently.
func (e *Executor) isAnyConflictingMigrationRunning(onlineDDL *schema.OnlineDDL) (conflictFound bool, conflictingMigration *schema.OnlineDDL) {
	e.ownedRunningMigrations.Range(func(_, val any) bool {
		runningMigration, ok := val.(*schema.OnlineDDL)
		if !ok {
			return true // continue iteration
		}
		if e.proposedMigrationConflictsWithRunningMigration(runningMigration, onlineDDL) {
			conflictingMigration = runningMigration
			return false // stop iteration, no need to review other migrations
		}
		return true // continue iteration
	})
	return (conflictingMigration != nil), conflictingMigration
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
	conn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tm, err := conn.Conn.Exec(ctx, `select
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
	} else if port, err := row.ToInt("port"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.port %v: %v", tm, err)
	} else {
		variables.port = port
	}
	if variables.readOnly, err = row.ToBool("read_only"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.read_only %v: %v", tm, err)
	}

	variables.version = row["version"].ToString()
	variables.versionComment = row["version_comment"].ToString()

	return variables, nil
}

// createOnlineDDLUser creates a gh-ost or pt-osc user account with all
// necessary privileges and with a random password
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
func (e *Executor) executeDirectly(ctx context.Context, onlineDDL *schema.OnlineDDL, acceptableMySQLErrorCodes ...sqlerror.ErrorCode) (acceptableErrorCodeFound bool, err error) {
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
		if merr, ok := err.(*sqlerror.SQLError); ok {
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
	defer e.reloadSchema(ctx)
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)

	return acceptableErrorCodeFound, nil
}

// doesConnectionInfoMatch checks if theres a MySQL connection in PROCESSLIST whose Info matches given text
func (e *Executor) doesConnectionInfoMatch(ctx context.Context, connID int64, submatch string) (bool, error) {
	findProcessQuery, err := sqlparser.ParseAndBind(sqlFindProcess,
		sqltypes.Int64BindVariable(connID),
		sqltypes.StringBindVariable("%"+submatch+"%"),
	)
	if err != nil {
		return false, err
	}
	rs, err := e.execQuery(ctx, findProcessQuery)
	if err != nil {
		return false, err
	}
	return len(rs.Rows) == 1, nil
}

// tableParticipatesInForeignKeyRelationship checks if a given table is either a parent or a child in at least one foreign key constraint
func (e *Executor) tableParticipatesInForeignKeyRelationship(ctx context.Context, schema string, table string) (bool, error) {
	for _, fkQuery := range []string{selSelectCountFKParentConstraints, selSelectCountFKChildConstraints} {
		query, err := sqlparser.ParseAndBind(fkQuery,
			sqltypes.StringBindVariable(schema),
			sqltypes.StringBindVariable(table),
		)
		if err != nil {
			return false, err
		}
		r, err := e.execQuery(ctx, query)
		if err != nil {
			return false, err
		}
		row := r.Named().Row()
		if row == nil {
			return false, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result from INFORMATION_SCHEMA.KEY_COLUMN_USAGE query: %s", query)
		}
		countFKConstraints := row.AsInt64("num_fk_constraints", 0)
		if countFKConstraints > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (e *Executor) validateTableForAlterAction(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	participatesInFK, err := e.tableParticipatesInForeignKeyRelationship(ctx, onlineDDL.Schema, onlineDDL.Table)
	if err != nil {
		return vterrors.Wrapf(err, "error while attempting to validate whether table %s participates in FOREIGN KEY constraint", onlineDDL.Table)
	}
	if participatesInFK {
		if !onlineDDL.StrategySetting().IsAllowForeignKeysFlag() {
			// FK migrations not allowed
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s participates in a FOREIGN KEY constraint and FOREIGN KEY constraints are not supported in Online DDL unless the *experimental and unsafe* --unsafe-allow-foreign-keys strategy flag is specified", onlineDDL.Table)
		}
		// FK migrations allowed. Validate that underlying MySQL server supports it.
		preserveFKSupported, err := e.isPreserveForeignKeySupported(ctx)
		if err != nil {
			return vterrors.Wrapf(err, "error while attempting to validate whether 'rename_table_preserve_foreign_key' is supported")
		}
		if !preserveFKSupported {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s participates in a FOREIGN KEY constraint and underlying database server does not support `rename_table_preserve_foreign_key`", onlineDDL.Table)
		}
	}
	return nil
}

// primaryPosition returns the MySQL/MariaDB position (typically GTID pos) on the tablet
func (e *Executor) primaryPosition(ctx context.Context) (pos replication.Position, err error) {
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
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		log.Errorf("FAIL vreplicationExec: uuid=%s, query=%v, error=%v", uuid, query, err)
	}

	if err := e.deleteVReplicationEntry(ctx, uuid); err != nil {
		return err
	}
	return nil
}

// killTableLockHoldersAndAccessors kills any active queries using the given table, and also kills
// connections with open transactions, holding locks on the table.
// This is done on a best-effort basis, by issuing `KILL` and `KILL QUERY` commands. As MySQL goes,
// it is not guaranteed that the queries/transactions will terminate in a timely manner.
func (e *Executor) killTableLockHoldersAndAccessors(ctx context.Context, tableName string) error {
	log.Infof("killTableLockHoldersAndAccessors: %v", tableName)
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()

	{
		// First, let's look at PROCESSLIST for queries that _might_ be operating on our table. This may have
		// plenty false positives as we're simply looking for the table name as a query substring.
		likeVariable := "%" + tableName + "%"
		query, err := sqlparser.ParseAndBind(sqlFindProcessByInfo, sqltypes.StringBindVariable(likeVariable))
		if err != nil {
			return err
		}
		rs, err := conn.Conn.ExecuteFetch(query, -1, true)
		if err != nil {
			return err
		}

		log.Infof("killTableLockHoldersAndAccessors: found %v potential queries", len(rs.Rows))
		// Now that we have some list of queries, we actually parse them to find whether the query actually references our table:
		for _, row := range rs.Named().Rows {
			threadId := row.AsInt64("id", 0)
			infoQuery := row.AsString("info", "")
			stmt, err := e.env.Environment().Parser().Parse(infoQuery)
			if err != nil {
				log.Error(vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unable to parse processlist Info query: %v", infoQuery))
				continue
			}
			queryUsesTable := false
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.TableName:
					if node.Name.String() == tableName {
						queryUsesTable = true
						return false, nil
					}
				case *sqlparser.AliasedTableExpr:
					if alasedTableName, ok := node.Expr.(sqlparser.TableName); ok {
						if alasedTableName.Name.String() == tableName {
							queryUsesTable = true
							return false, nil
						}
					}
				}
				return true, nil
			}, stmt)

			if queryUsesTable {
				log.Infof("killTableLockHoldersAndAccessors: killing query %v: %.100s", threadId, infoQuery)
				killQuery := fmt.Sprintf("KILL QUERY %d", threadId)
				if _, err := conn.Conn.ExecuteFetch(killQuery, 1, false); err != nil {
					log.Error(vterrors.Errorf(vtrpcpb.Code_ABORTED, "could not kill query %v. Ignoring", threadId))
				}
			}
		}
	}
	capableOf := mysql.ServerVersionCapableOf(conn.ServerVersion)
	capable, err := capableOf(capabilities.PerformanceSchemaDataLocksTableCapability)
	if err != nil {
		return err
	}
	if capable {
		{
			// Kill connections that have open transactions locking the table. These potentially (probably?) are not
			// actively running a query on our table. They're doing other things while holding locks on our table.
			query, err := sqlparser.ParseAndBind(sqlProcessWithLocksOnTable, sqltypes.StringBindVariable(tableName))
			if err != nil {
				return err
			}
			rs, err := conn.Conn.ExecuteFetch(query, -1, true)
			if err != nil {
				return err
			}
			log.Infof("killTableLockHoldersAndAccessors: found %v locking transactions", len(rs.Rows))
			for _, row := range rs.Named().Rows {
				threadId := row.AsInt64("trx_mysql_thread_id", 0)
				log.Infof("killTableLockHoldersAndAccessors: killing connection %v with transaction on table", threadId)
				killConnection := fmt.Sprintf("KILL %d", threadId)
				_, _ = conn.Conn.ExecuteFetch(killConnection, 1, false)
			}
		}
	}
	return nil
}

// cutOverVReplMigration stops vreplication, then removes the _vt.vreplication entry for the given migration
func (e *Executor) cutOverVReplMigration(ctx context.Context, s *VReplStream, shouldForceCutOver bool) error {
	if err := e.incrementCutoverAttempts(ctx, s.workflow); err != nil {
		return err
	}

	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	// sanity checks:
	vreplTable, err := getVreplTable(s)
	if err != nil {
		return err
	}

	// get topology client & entities:
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
	e.updateMigrationStage(ctx, onlineDDL.UUID, "starting cut-over")

	var sentryTableName string

	migrationCutOverThreshold := getMigrationCutOverThreshold(onlineDDL)

	waitForPos := func(s *VReplStream, pos replication.Position) error {
		ctx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
		defer cancel()
		// Wait for target to reach the up-to-date pos
		if err := tmClient.VReplicationWaitForPos(ctx, tablet.Tablet, s.id, replication.EncodePosition(pos)); err != nil {
			return err
		}
		// Target is now in sync with source!
		return nil
	}

	if !isVreplicationTestSuite {
		// A bit early on, we generate a name for the sentry table
		// We do this here because right now we're in a safe place where nothing happened yet. If there's an error now, bail out
		// and no harm done.
		// Later on, when traffic is blocked and tables renamed, that's a more dangerous place to be in; we want as little logic
		// in that place as possible.
		sentryTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		if err != nil {
			return nil
		}

		// We create the sentry table before toggling writes, because this involves a WaitForPos, which takes some time. We
		// don't want to overload the buffering time with this excessive wait.

		if err := e.updateArtifacts(ctx, onlineDDL.UUID, sentryTableName); err != nil {
			return err
		}

		dropSentryTableQuery := sqlparser.BuildParsedQuery(sqlDropTableIfExists, sentryTableName)
		defer func() {
			// cut-over attempts may fail. We create a new, unique sentry table for every
			// cut-over attempt. We could just leave them hanging around, and let gcArtifacts()
			// and the table GC mechanism to take care of them. But then again, if we happen
			// to have many cut-over attempts, that just proliferates and overloads the schema,
			// and also bloats the `artifacts` column.
			// The thing is, the sentry table is empty, and we really don't need it once the cut-over
			// step is done (whether successful or failed). So, it's a cheap operation to drop the
			// table right away, which we do, and then also reduce the `artifact` column length by
			// removing the entry
			_, err := e.execQuery(ctx, dropSentryTableQuery.Query)
			if err == nil {
				e.clearSingleArtifact(ctx, onlineDDL.UUID, sentryTableName)
			}
			// This was a best effort optimization. Possibly the error is not nil. Which means we
			// still have a record of the sentry table, and gcArtifacts() will still be able to take
			// care of it in the future.
		}()
		parsed := sqlparser.BuildParsedQuery(sqlCreateSentryTable, sentryTableName)
		if _, err := e.execQuery(ctx, parsed.Query); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "sentry table created: %s", sentryTableName)

		postSentryPos, err := e.primaryPosition(ctx)
		if err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for post-sentry pos: %v", replication.EncodePosition(postSentryPos))
		if err := waitForPos(s, postSentryPos); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "post-sentry pos reached")
	}

	lockConn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer lockConn.Recycle()
	defer lockConn.Conn.Exec(ctx, sqlUnlockTables, 1, false)

	renameCompleteChan := make(chan error)
	renameWasSuccessful := false
	renameConn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer renameConn.Recycle()
	defer func() {
		if !renameWasSuccessful {
			renameConn.Conn.Kill("premature exit while renaming tables", 0)
		}
	}()
	// See if backend MySQL server supports 'rename_table_preserve_foreign_key' variable
	preserveFKSupported, err := e.isPreserveForeignKeySupported(ctx)
	if err != nil {
		return err
	}
	if preserveFKSupported {
		// This code is only applicable when MySQL supports the 'rename_table_preserve_foreign_key' variable. This variable
		// does not exist in vanilla MySQL.
		// See
		// - https://github.com/planetscale/mysql-server/commit/bb777e3e86387571c044fb4a2beb4f8c60462ced
		// - https://github.com/planetscale/mysql-server/commit/c2f1344a6863518d749f2eb01a4c74ca08a5b889
		// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps3.
		log.Infof("@@rename_table_preserve_foreign_key supported")
	}

	renameQuery := sqlparser.BuildParsedQuery(sqlSwapTables, onlineDDL.Table, sentryTableName, vreplTable, onlineDDL.Table, sentryTableName, vreplTable)

	waitForRenameProcess := func() error {
		// This function waits until it finds the RENAME TABLE... query running in MySQL's PROCESSLIST, or until timeout
		// The function assumes that one of the renamed tables is locked, thus causing the RENAME to block. If nothing
		// is locked, then the RENAME will be near-instantaneous and it's unlikely that the function will find it.
		renameWaitCtx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
		defer cancel()

		for {
			renameProcessFound, err := e.doesConnectionInfoMatch(renameWaitCtx, renameConn.Conn.ID(), "rename")
			if err != nil {
				return err
			}
			if renameProcessFound {
				return nil
			}
			select {
			case <-renameWaitCtx.Done():
				return vterrors.Errorf(vtrpcpb.Code_ABORTED, "timeout for rename query: %s", renameQuery.Query)
			case err := <-renameCompleteChan:
				// We expect the RENAME to run and block, not yet complete. The caller of this function
				// will only unblock the RENAME after the function is complete
				return vterrors.Errorf(vtrpcpb.Code_ABORTED, "rename returned unexpectedly: err=%v", err)
			case <-time.After(time.Second):
				// sleep
			}
		}
	}

	bufferingCtx, bufferingContextCancel := context.WithCancel(ctx)
	defer bufferingContextCancel()
	// Preparation is complete. We proceed to cut-over.
	toggleBuffering := func(bufferQueries bool) error {
		log.Infof("toggling buffering: %t in migration %v", bufferQueries, onlineDDL.UUID)
		timeout := migrationCutOverThreshold + qrBufferExtraTimeout

		e.toggleBufferTableFunc(bufferingCtx, onlineDDL.Table, timeout, bufferQueries)
		if !bufferQueries {
			grpcCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
			defer cancel()
			// called after new table is in place.
			// unbuffer existing queries:
			bufferingContextCancel()
			// force re-read of tables
			if err := tmClient.RefreshState(grpcCtx, tablet.Tablet); err != nil {
				return err
			}
		}
		log.Infof("toggled buffering: %t in migration %v", bufferQueries, onlineDDL.UUID)
		return nil
	}

	var reenableOnce sync.Once
	reenableWritesOnce := func() {
		reenableOnce.Do(func() {
			log.Infof("re-enabling writes in migration %v", onlineDDL.UUID)
			toggleBuffering(false)
			go log.Infof("cutOverVReplMigration %v: unbuffered queries", s.workflow)
		})
	}
	e.updateMigrationStage(ctx, onlineDDL.UUID, "buffering queries")
	// stop writes on source:
	err = toggleBuffering(true)
	defer reenableWritesOnce()
	if err != nil {
		return err
	}
	// Give a fraction of a second for a scenario where a query is in
	// query executor, it passed the ACLs and is _about to_ execute. This will be nicer to those queries:
	// they will be able to complete before the rename, rather than block briefly on the rename only to find
	// the table no longer exists.
	e.updateMigrationStage(ctx, onlineDDL.UUID, "graceful wait for buffering")
	time.Sleep(100 * time.Millisecond)

	if shouldForceCutOver {
		if err := e.killTableLockHoldersAndAccessors(ctx, onlineDDL.Table); err != nil {
			return err
		}
	}

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
		e.updateMigrationStage(ctx, onlineDDL.UUID, "test suite 'before' table renamed")
	} else {
		// real production

		e.updateMigrationStage(ctx, onlineDDL.UUID, "locking tables")
		lockCtx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
		defer cancel()
		lockTableQuery := sqlparser.BuildParsedQuery(sqlLockTwoTablesWrite, sentryTableName, onlineDDL.Table)
		if _, err := lockConn.Conn.Exec(lockCtx, lockTableQuery.Query, 1, false); err != nil {
			return err
		}

		e.updateMigrationStage(ctx, onlineDDL.UUID, "renaming tables")
		go func() {
			defer close(renameCompleteChan)
			_, err := renameConn.Conn.Exec(ctx, renameQuery.Query, 1, false)
			renameCompleteChan <- err
		}()
		// the rename should block, because of the LOCK. Wait for it to show up.
		e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for RENAME to block")
		if err := waitForRenameProcess(); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "RENAME found")
	}

	e.updateMigrationStage(ctx, onlineDDL.UUID, "reading post-lock pos")
	postWritesPos, err := e.primaryPosition(ctx)
	if err != nil {
		return err
	}

	// Right now: new queries are buffered, any existing query will have executed, and worst case scenario is
	// that some leftover query finds the table is not actually there anymore...
	// At any case, there's definitely no more writes to the table since it does not exist. We can
	// safely take the (GTID) pos now.
	_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", s.workflow)

	// Writes are now disabled on table. Read up-to-date vreplication info, specifically to get latest (and fixed) pos:
	s, err = e.readVReplStream(ctx, s.workflow, false)
	if err != nil {
		return err
	}

	e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for post-lock pos: %v", replication.EncodePosition(postWritesPos))
	if err := waitForPos(s, postWritesPos); err != nil {
		e.updateMigrationStage(ctx, onlineDDL.UUID, "timeout while waiting for post-lock pos: %v", err)
		return err
	}
	go log.Infof("cutOverVReplMigration %v: done waiting for position %v", s.workflow, replication.EncodePosition(postWritesPos))
	// Stop vreplication
	e.updateMigrationStage(ctx, onlineDDL.UUID, "stopping vreplication")
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, binlogplayer.StopVReplication(s.id, "stopped for online DDL cutover")); err != nil {
		return err
	}
	go log.Infof("cutOverVReplMigration %v: stopped vreplication", s.workflow)

	// rename tables atomically (remember, writes on source tables are stopped)
	{
		if isVreplicationTestSuite {
			// this is used in Vitess endtoend testing suite
			testSuiteAfterTableName := fmt.Sprintf("%s_after", onlineDDL.Table)
			parsed := sqlparser.BuildParsedQuery(sqlRenameTable, vreplTable, testSuiteAfterTableName)
			if _, err := e.execQuery(ctx, parsed.Query); err != nil {
				return err
			}
			e.updateMigrationStage(ctx, onlineDDL.UUID, "test suite 'after' table renamed")
		} else {
			e.updateMigrationStage(ctx, onlineDDL.UUID, "validating rename is still in place")
			if err := waitForRenameProcess(); err != nil {
				return err
			}

			// Normal (non-testing) alter table
			e.updateMigrationStage(ctx, onlineDDL.UUID, "dropping sentry table")

			{
				dropTableQuery := sqlparser.BuildParsedQuery(sqlDropTable, sentryTableName)
				lockCtx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
				defer cancel()
				if _, err := lockConn.Conn.Exec(lockCtx, dropTableQuery.Query, 1, false); err != nil {
					return err
				}
			}
			{
				lockCtx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
				defer cancel()
				e.updateMigrationStage(ctx, onlineDDL.UUID, "unlocking tables")
				if _, err := lockConn.Conn.Exec(lockCtx, sqlUnlockTables, 1, false); err != nil {
					return err
				}
			}
			{
				lockCtx, cancel := context.WithTimeout(ctx, migrationCutOverThreshold)
				defer cancel()
				e.updateMigrationStage(lockCtx, onlineDDL.UUID, "waiting for RENAME to complete")
				if err := <-renameCompleteChan; err != nil {
					return err
				}
				renameWasSuccessful = true
			}
		}
	}
	e.updateMigrationStage(ctx, onlineDDL.UUID, "cut-over complete")
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)

	go func() {
		// Tables are swapped! Let's take the opportunity to ReloadSchema now
		// We do this in a goroutine because it might take time on a schema with thousands of tables, and we don't want to delay
		// the cut-over.
		// this means ReloadSchema is not in sync with the actual schema change. Users will still need to run tracker if they want to sync.
		// In the future, we will want to reload the single table, instead of reloading the schema.
		if err := e.reloadSchema(ctx); err != nil {
			vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Error on ReloadSchema while cutting over vreplication migration UUID: %+v", onlineDDL.UUID)
		}
	}()

	// Tables are now swapped! Migration is successful
	e.updateMigrationStage(ctx, onlineDDL.UUID, "re-enabling writes")
	reenableWritesOnce() // this function is also deferred, in case of early return; but now would be a good time to resume writes, before we publish the migration as "complete"
	go log.Infof("cutOverVReplMigration %v: marking as complete", s.workflow)
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, s.rowsCopied, emptyHint)
	return nil

	// deferred function will re-enable writes now
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
	changeSQLModeQuery := fmt.Sprintf("set @@session.sql_mode=REPLACE(REPLACE('%s', 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", sqlMode)
	if _, err := conn.ExecuteFetch(changeSQLModeQuery, 0, false); err != nil {
		return deferFunc, err
	}
	return deferFunc, nil
}

// newConstraintName generates a new, unique name for a constraint. Our problem is that a MySQL
// constraint's name is unique in the schema (!). And so as we duplicate the original table, we must
// create completely new names for all constraints.
// Moreover, we really want this name to be consistent across all shards. We therefore use a deterministic
// UUIDv5 (SHA) function over the migration UUID, table name, and constraint's _contents_.
// We _also_ include the original constraint name as prefix, as room allows
// for example, if the original constraint name is "check_1",
// we might generate "check_1_cps1okb4uafunfqusi2lp22u3".
// If we then again migrate a table whose constraint name is "check_1_cps1okb4uafunfqusi2lp22u3	" we
// get for example "check_1_19l09s37kbhj4axnzmi10e18k" (hash changes, and we still try to preserve original name)
//
// Furthermore, per bug report https://bugs.mysql.com/bug.php?id=107772, if the user doesn't provide a name for
// their CHECK constraint, then MySQL picks a name in this format <tablename>_chk_<number>.
// Example: sometable_chk_1
// Next, when MySQL is asked to RENAME TABLE and sees a constraint with this format, it attempts to rename
// the constraint with the new table's name. This is problematic for Vitess, because we often rename tables to
// very long names, such as _vt_HOLD_394f9e6dfc3d11eca0390a43f95f28a3_20220706091048.
// As we rename the constraint to e.g. `sometable_chk_1_cps1okb4uafunfqusi2lp22u3`, this makes MySQL want to
// call the new constraint something like _vt_HOLD_394f9e6dfc3d11eca0390a43f95f28a3_20220706091048_chk_1_cps1okb4uafunfqusi2lp22u3,
// which exceeds the 64 character limit for table names. Long story short, we also trim down <tablename> if the constraint seems
// to be auto-generated.
func (e *Executor) newConstraintName(onlineDDL *schema.OnlineDDL, constraintType ConstraintType, hashExists map[string]bool, seed string, oldName string) string {
	constraintIndicator := constraintIndicatorMap[int(constraintType)]
	oldName = schemadiff.ExtractConstraintOriginalName(onlineDDL.Table, oldName)
	hash := textutil.UUIDv5Base36(onlineDDL.UUID, onlineDDL.Table, seed)
	for i := 1; hashExists[hash]; i++ {
		hash = textutil.UUIDv5Base36(onlineDDL.UUID, onlineDDL.Table, seed, fmt.Sprintf("%d", i))
	}
	hashExists[hash] = true
	suffix := "_" + hash
	maxAllowedNameLength := maxConstraintNameLength - len(suffix)
	newName := oldName
	if newName == "" {
		newName = constraintIndicator // start with something that looks consistent with MySQL's naming
	}
	if len(newName) > maxAllowedNameLength {
		newName = newName[0:maxAllowedNameLength]
	}
	newName = newName + suffix
	return newName
}

// validateAndEditCreateTableStatement inspects the CreateTable AST and does the following:
// - extra validation (no FKs for now...)
// - generate new and unique names for all constraints (CHECK and FK; yes, why not handle FK names; even as we don't support FKs today, we may in the future)
func (e *Executor) validateAndEditCreateTableStatement(ctx context.Context, onlineDDL *schema.OnlineDDL, createTable *sqlparser.CreateTable) (constraintMap map[string]string, err error) {
	constraintMap = map[string]string{}
	hashExists := map[string]bool{}

	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ForeignKeyDefinition:
			if !onlineDDL.StrategySetting().IsAllowForeignKeysFlag() {
				return false, schema.ErrForeignKeyFound
			}
		case *sqlparser.ConstraintDefinition:
			oldName := node.Name.String()
			newName := e.newConstraintName(onlineDDL, GetConstraintType(node.Details), hashExists, sqlparser.CanonicalString(node.Details), oldName)
			node.Name = sqlparser.NewIdentifierCI(newName)
			constraintMap[oldName] = newName
		}
		return true, nil
	}
	if err := sqlparser.Walk(validateWalk, createTable); err != nil {
		return constraintMap, err
	}
	return constraintMap, nil
}

// validateAndEditAlterTableStatement inspects the AlterTable statement and:
// - modifies any CONSTRAINT name according to given name mapping
// - explode ADD FULLTEXT KEY into multiple statements
func (e *Executor) validateAndEditAlterTableStatement(ctx context.Context, onlineDDL *schema.OnlineDDL, alterTable *sqlparser.AlterTable, constraintMap map[string]string) (alters []*sqlparser.AlterTable, err error) {
	hashExists := map[string]bool{}
	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.DropKey:
			if node.Type == sqlparser.CheckKeyType || node.Type == sqlparser.ForeignKeyType {
				// drop a check or a foreign key constraint
				mappedName, ok := constraintMap[node.Name.String()]
				if !ok {
					return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Found DROP CONSTRAINT: %v, but could not find constraint name in map", sqlparser.CanonicalString(node))
				}
				node.Name = sqlparser.NewIdentifierCI(mappedName)
			}
		case *sqlparser.AddConstraintDefinition:
			oldName := node.ConstraintDefinition.Name.String()
			newName := e.newConstraintName(onlineDDL, GetConstraintType(node.ConstraintDefinition.Details), hashExists, sqlparser.CanonicalString(node.ConstraintDefinition.Details), oldName)
			node.ConstraintDefinition.Name = sqlparser.NewIdentifierCI(newName)
			constraintMap[oldName] = newName
		}
		return true, nil
	}
	if err := sqlparser.Walk(validateWalk, alterTable); err != nil {
		return alters, err
	}
	alters = append(alters, alterTable)
	// Handle ADD FULLTEXT KEY statements
	countAddFullTextStatements := 0
	redactedOptions := make([]sqlparser.AlterOption, 0, len(alterTable.AlterOptions))
	for i := range alterTable.AlterOptions {
		opt := alterTable.AlterOptions[i]
		switch opt := opt.(type) {
		case sqlparser.AlgorithmValue:
			// we do not pass ALGORITHM. We choose our own ALGORITHM.
			continue
		case *sqlparser.AddIndexDefinition:
			if opt.IndexDefinition.Info.Type == sqlparser.IndexTypeFullText {
				countAddFullTextStatements++
				if countAddFullTextStatements > 1 {
					// We've already got one ADD FULLTEXT KEY. We can't have another
					// in the same statement
					extraAlterTable := &sqlparser.AlterTable{
						Table:        alterTable.Table,
						AlterOptions: []sqlparser.AlterOption{opt, copyAlgorithm},
					}
					alters = append(alters, extraAlterTable)
					continue
				}
			}
		}
		redactedOptions = append(redactedOptions, opt)
	}
	alterTable.AlterOptions = redactedOptions
	alterTable.AlterOptions = append(alterTable.AlterOptions, copyAlgorithm)
	return alters, nil
}

// duplicateCreateTable parses the given `CREATE TABLE` statement, and returns:
// - The format CreateTable AST
// - A new CreateTable AST, with the table renamed as `newTableName`, and with constraints renamed deterministically
// - Map of renamed constraints
func (e *Executor) duplicateCreateTable(ctx context.Context, onlineDDL *schema.OnlineDDL, originalShowCreateTable string, newTableName string) (
	originalCreateTable *sqlparser.CreateTable,
	newCreateTable *sqlparser.CreateTable,
	constraintMap map[string]string,
	err error,
) {
	stmt, err := e.env.Environment().Parser().ParseStrictDDL(originalShowCreateTable)
	if err != nil {
		return nil, nil, nil, err
	}
	originalCreateTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CreateTable statement, got: %v", sqlparser.CanonicalString(stmt))
	}
	newCreateTable = sqlparser.CloneRefOfCreateTable(originalCreateTable)
	newCreateTable.SetTable(newCreateTable.GetTable().Qualifier.CompliantName(), newTableName)
	// manipulate CreateTable statement: take care of constraints names which have to be
	// unique across the schema
	constraintMap, err = e.validateAndEditCreateTableStatement(ctx, onlineDDL, newCreateTable)
	if err != nil {
		return nil, nil, nil, err
	}
	return originalCreateTable, newCreateTable, constraintMap, nil
}

// createDuplicateTableLike creates the table named by `newTableName` in the likeness of onlineDDL.Table
// This function emulates MySQL's `CREATE TABLE LIKE ...` statement. The difference is that this function takes control over the generated CONSTRAINT names,
// if any, such that they are deterministic across shards, as well as preserve original names where possible.
func (e *Executor) createDuplicateTableLike(ctx context.Context, newTableName string, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (
	originalShowCreateTable string,
	constraintMap map[string]string,
	err error,
) {
	originalShowCreateTable, err = e.showCreateTable(ctx, onlineDDL.Table)
	if err != nil {
		return "", nil, err
	}
	_, vreplCreateTable, constraintMap, err := e.duplicateCreateTable(ctx, onlineDDL, originalShowCreateTable, newTableName)
	if err != nil {
		return "", nil, err
	}
	// Create the vrepl (shadow) table:
	if _, err := conn.ExecuteFetch(sqlparser.CanonicalString(vreplCreateTable), 0, false); err != nil {
		return "", nil, err
	}
	return originalShowCreateTable, constraintMap, nil
}

// initVreplicationOriginalMigration performs the first steps towards running a VRepl ALTER migration:
// - analyze the original table
// - formalize a new CreateTable statement
// - inspect the ALTER TABLE query
// - formalize an AlterTable statement
// - create the vrepl table
// - modify the vrepl table
// - Create and return a VRepl instance
func (e *Executor) initVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (v *VRepl, err error) {
	restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
	defer restoreSQLModeFunc()
	if err != nil {
		return v, err
	}

	vreplTableName, err := schema.GenerateInternalTableName(schema.InternalTableVreplicationHint.String(), onlineDDL.UUID, time.Now())
	if err != nil {
		return v, err
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	originalShowCreateTable, constraintMap, err := e.createDuplicateTableLike(ctx, vreplTableName, onlineDDL, conn)
	if err != nil {
		return nil, err
	}

	stmt, err := e.env.Environment().Parser().ParseStrictDDL(onlineDDL.SQL)
	if err != nil {
		return nil, err
	}
	alterTable, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected AlterTable statement, got: %v", sqlparser.CanonicalString(stmt))
	}
	// ALTER TABLE should apply to the vrepl table
	alterTable.SetTable(alterTable.GetTable().Qualifier.CompliantName(), vreplTableName)
	// Also, change any constraint names:
	alters, err := e.validateAndEditAlterTableStatement(ctx, onlineDDL, alterTable, constraintMap)
	if err != nil {
		return v, err
	}
	// Apply ALTER TABLE to materialized table
	for _, alter := range alters {
		if _, err := conn.ExecuteFetch(sqlparser.CanonicalString(alter), 0, false); err != nil {
			return v, err
		}
	}

	vreplShowCreateTable, err := e.showCreateTable(ctx, vreplTableName)
	if err != nil {
		return v, err
	}

	v = NewVRepl(e.env.Environment(), onlineDDL.UUID, e.keyspace, e.shard, e.dbName, onlineDDL.Table, vreplTableName, originalShowCreateTable, vreplShowCreateTable, onlineDDL.SQL, onlineDDL.StrategySetting().IsAnalyzeTableFlag())
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

	vreplTableName, err := getVreplTable(revertStream)
	if err != nil {
		return nil, err
	}

	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	v = NewVRepl(e.env.Environment(), onlineDDL.UUID, e.keyspace, e.shard, e.dbName, onlineDDL.Table, vreplTableName, "", "", "", false)
	v.pos = revertStream.pos
	return v, nil
}

// ExecuteWithVReplication sets up the grounds for a vreplication schema migration
func (e *Executor) ExecuteWithVReplication(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) error {
	// make sure there's no vreplication workflow running under same name
	_ = e.terminateVReplMigration(ctx, onlineDDL.UUID)

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
		strings.Join(sqlescape.EscapeIDs(v.removedForeignKeyNames), ","),
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
		tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
		if err != nil {
			return err
		}

		// reload schema before migration
		if err := e.reloadSchema(ctx); err != nil {
			return err
		}

		// create vreplication entry
		insertVReplicationQuery, err := v.generateInsertStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tablet.Tablet, insertVReplicationQuery); err != nil {
			return err
		}

		{
			// temporary hack. todo: this should be done when inserting any _vt.vreplication record across all workflow types
			query := fmt.Sprintf("update _vt.vreplication set workflow_type = %d where workflow = '%s'",
				binlogdatapb.VReplicationWorkflowType_OnlineDDL, v.workflow)
			if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
				return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", tablet.Tablet, query)
			}
		}
		// start stream!
		startVReplicationQuery, err := v.generateStartStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tablet.Tablet, startVReplicationQuery); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteWithGhost validates and runs a gh-ost process.
// Validation included testing the backend MySQL server and the gh-ost binary itself
// Execution runs first a dry run, then an actual migration
func (e *Executor) ExecuteWithGhost(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
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
			`, servenv.Port(), onlineDDL.UUID, string(status), hint)
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
		forceTableNames := fmt.Sprintf("%s_%s", onlineDDL.UUID, schema.ReadableTimestamp())

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
			fmt.Sprintf(`--throttle-http=http://localhost:%d/throttler/check?app=%s:%s:%s&p=low`, servenv.Port(), throttlerapp.OnlineDDLName, throttlerapp.GhostName, onlineDDL.UUID),
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
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failed" status
			_ = e.failMigration(ctx, onlineDDL, err)

			log.Errorf("Error executing gh-ost dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run gh-ost on: %s:%d", variables.host, variables.port)
		startedMigrations.Add(1)
		if err := runGhost(true); err != nil {
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.failMigration(ctx, onlineDDL, err)
			failedMigrations.Add(1)
			log.Errorf("Error running gh-ost: %+v", err)
			return err
		}
		// Migration successful!
		defer e.reloadSchema(ctx)
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
			if (head("http://localhost:{{VTTABLET_PORT}}/throttler/check?app={{THROTTLER_ONLINE_DDL_APP}}:{{THROTTLER_PT_OSC_APP}}:{{MIGRATION_UUID}}&p=low")) {
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
	pluginCode = strings.ReplaceAll(pluginCode, "{{VTTABLET_PORT}}", fmt.Sprintf("%d", servenv.Port()))
	pluginCode = strings.ReplaceAll(pluginCode, "{{MIGRATION_UUID}}", onlineDDL.UUID)
	pluginCode = strings.ReplaceAll(pluginCode, "{{THROTTLER_ONLINE_DDL_APP}}", throttlerapp.OnlineDDLName.String())
	pluginCode = strings.ReplaceAll(pluginCode, "{{THROTTLER_PT_OSC_APP}}", throttlerapp.PTOSCName.String())

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
	// Once we have a built in throttling service , we will no longer need to have the OSC tools probe the
	// replicas. Instead, they will consult with our throttling service.
	// TODO(shlomi): replace/remove this when we have a proper throttling solution
	time.Sleep(time.Second)

	runPTOSC := func(execute bool) error {
		os.Setenv("MYSQL_PWD", onlineDDLPassword)
		newTableName := fmt.Sprintf("_%s_%s_new", onlineDDL.UUID, schema.ReadableTimestamp())

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
			_ = e.failMigration(ctx, onlineDDL, err)
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			log.Errorf("Error executing pt-online-schema-change dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run pt-online-schema-change on: %s:%d", variables.host, variables.port)
		startedMigrations.Add(1)
		if err := runPTOSC(true); err != nil {
			// perhaps pt-osc was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.failMigration(ctx, onlineDDL, err)
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			_ = e.dropPTOSCMigrationTriggers(ctx, onlineDDL)
			failedMigrations.Add(1)
			log.Errorf("Error running pt-online-schema-change: %+v", err)
			return err
		}
		// Migration successful!
		defer e.reloadSchema(ctx)
		successfulMigrations.Add(1)
		log.Infof("+ OK")
		return nil
	}()
	return nil
}

func (e *Executor) readMigration(ctx context.Context, uuid string) (onlineDDL *schema.OnlineDDL, row sqltypes.RowNamedValues, err error) {

	query, err := sqlparser.ParseAndBind(sqlSelectMigration,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return onlineDDL, nil, err
	}
	r, err := e.execQuery(ctx, query)
	if err != nil {
		return onlineDDL, nil, err
	}
	row = r.Named().Row()
	if row == nil {
		// No results
		return nil, nil, ErrMigrationNotFound
	}
	onlineDDL = &schema.OnlineDDL{
		Keyspace:           row["keyspace"].ToString(),
		Table:              row["mysql_table"].ToString(),
		Schema:             row["mysql_schema"].ToString(),
		SQL:                row["migration_statement"].ToString(),
		UUID:               row["migration_uuid"].ToString(),
		Strategy:           schema.DDLStrategy(row["strategy"].ToString()),
		Options:            row["options"].ToString(),
		Status:             schema.OnlineDDLStatus(row["migration_status"].ToString()),
		Retries:            row.AsInt64("retries", 0),
		ReadyToComplete:    row.AsInt64("ready_to_complete", 0),
		WasReadyToComplete: row.AsInt64("was_ready_to_complete", 0),
		TabletAlias:        row["tablet"].ToString(),
		MigrationContext:   row["migration_context"].ToString(),
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
	log.Infof("terminateMigration: request to terminate %s", onlineDDL.UUID)
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
	case schema.DDLStrategyPTOSC:
		// see if pt-osc is running (could have been executed by this vttablet or one that crashed in the past)
		if running, pid, _ := e.isPTOSCMigrationRunning(ctx, onlineDDL.UUID); running {
			foundRunning = true
			// Because pt-osc doesn't offer much control, we take a brute force approach to killing it,
			// revoking its privileges, and cleaning up its triggers.
			if err := syscallutil.Kill(pid, syscall.SIGTERM); err != nil {
				return foundRunning, nil
			}
			if err := syscallutil.Kill(pid, syscall.SIGKILL); err != nil {
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
func (e *Executor) CancelMigration(ctx context.Context, uuid string, message string, issuedByUser bool) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	log.Infof("CancelMigration: request to cancel %s with message: %v", uuid, message)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var rowsAffected uint64

	onlineDDL, _, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled:
		log.Infof("CancelMigration: migration %s is in non-cancellable status: %v", uuid, onlineDDL.Status)
		return emptyResult, nil
	}
	// From this point on, we're actually cancelling a migration
	if issuedByUser {
		// if this was issued by the user, then we mark the `cancelled_timestamp`, and based on that,
		// the migration state will be 'cancelled'.
		// If this was not issued by the user, then this is an internal state machine cancellation of the
		// migration, e.g. because it is stale or has an unrecoverable error. In this case we do not mark
		// the timestamp, and as result, the state will transition to 'failed'
		if err := e.updateMigrationTimestamp(ctx, "cancelled_timestamp", uuid); err != nil {
			return nil, err
		}
	}
	defer e.failMigration(ctx, onlineDDL, errors.New(message))
	defer e.triggerNextCheckInterval()

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady:
		log.Infof("CancelMigration: cancelling %s with status: %v", uuid, onlineDDL.Status)
		return &sqltypes.Result{RowsAffected: 1}, nil
	}

	migrationFound, err := e.terminateMigration(ctx, onlineDDL)
	if migrationFound {
		log.Infof("CancelMigration: terminated %s with status: %v", uuid, onlineDDL.Status)
		rowsAffected = 1
	} else {
		log.Infof("CancelMigration: migration %s wasn't found to be running", uuid)
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
func (e *Executor) cancelMigrations(ctx context.Context, cancellable []*cancellableMigration, issuedByUser bool) (err error) {
	for _, migration := range cancellable {
		log.Infof("cancelMigrations: cancelling %s; reason: %s", migration.uuid, migration.message)
		if _, err := e.CancelMigration(ctx, migration.uuid, migration.message, issuedByUser); err != nil {
			return err
		}
	}
	return nil
}

// CancelPendingMigrations cancels all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) CancelPendingMigrations(ctx context.Context, message string, issuedByUser bool) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("CancelPendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("CancelPendingMigrations: cancelling %s", uuid)
		res, err := e.CancelMigration(ctx, uuid, message, issuedByUser)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("CancelPendingMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

func (e *Executor) validateThrottleParams(ctx context.Context, expireString string, ratioLiteral *sqlparser.Literal) (duration time.Duration, ratio float64, err error) {
	duration = time.Hour * 24 * 365 * 100
	if expireString != "" {
		duration, err = time.ParseDuration(expireString)
		if err != nil || duration < 0 {
			return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid EXPIRE value: %s. Try '120s', '30m', '1h', etc. Allowed units are (s)ec, (m)in, (h)hour", expireString)
		}
	}
	ratio = throttle.DefaultThrottleRatio
	if ratioLiteral != nil {
		ratio, err = strconv.ParseFloat(ratioLiteral.Val, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid RATIO value: %s. Try any decimal number between '0.0' (no throttle) and `1.0` (fully throttled)", ratioLiteral.Val)
		}
	}
	return duration, ratio, nil
}

// ThrottleMigration
func (e *Executor) ThrottleMigration(ctx context.Context, uuid string, expireString string, ratioLiteral *sqlparser.Literal) (result *sqltypes.Result, err error) {
	duration, ratio, err := e.validateThrottleParams(ctx, expireString, ratioLiteral)
	if err != nil {
		return nil, err
	}
	if err := e.lagThrottler.CheckIsOpen(); err != nil {
		return nil, err
	}
	_ = e.lagThrottler.ThrottleApp(uuid, time.Now().Add(duration), ratio, false)
	return emptyResult, nil
}

// ThrottleAllMigrations
func (e *Executor) ThrottleAllMigrations(ctx context.Context, expireString string, ratioLiteral *sqlparser.Literal) (result *sqltypes.Result, err error) {
	duration, ratio, err := e.validateThrottleParams(ctx, expireString, ratioLiteral)
	if err != nil {
		return nil, err
	}
	if err := e.lagThrottler.CheckIsOpen(); err != nil {
		return nil, err
	}
	_ = e.lagThrottler.ThrottleApp(throttlerapp.OnlineDDLName.String(), time.Now().Add(duration), ratio, false)
	return emptyResult, nil
}

// UnthrottleMigration
func (e *Executor) UnthrottleMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if err := e.lagThrottler.CheckIsOpen(); err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	_ = e.lagThrottler.UnthrottleApp(uuid)
	return emptyResult, nil
}

// UnthrottleAllMigrations
func (e *Executor) UnthrottleAllMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if err := e.lagThrottler.CheckIsOpen(); err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	_ = e.lagThrottler.UnthrottleApp(throttlerapp.OnlineDDLName.String())
	return emptyResult, nil
}

// scheduleNextMigration attempts to schedule a single migration to run next.
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
		postponeLaunch := row.AsBool("postpone_launch", false)
		postponeCompletion := row.AsBool("postpone_completion", false)
		readyToComplete := row.AsBool("ready_to_complete", false)
		isImmediateOperation := row.AsBool("is_immediate_operation", false)

		if postponeLaunch {
			// We don't even look into this migration until its postpone_launch flag is cleared
			continue
		}

		if !readyToComplete {
			// see if we need to update ready_to_complete
			if isImmediateOperation {
				// Whether postponed or not, CREATE and DROP operations, as well as VIEW operations,
				// are inherently "ready to complete" because their operation is immediate.
				if err := e.updateMigrationReadyToComplete(ctx, uuid, true); err != nil {
					return err
				}
			}
		}

		if !(isImmediateOperation && postponeCompletion) {
			// Any non-postponed migration can be scheduled
			// postponed ALTER can be scheduled (because gh-ost or vreplication will postpone the cut-over)
			// We only schedule a single migration in the execution of this function
			onlyScheduleOneMigration.Do(func() {
				err = e.updateMigrationStatus(ctx, uuid, schema.OnlineDDLStatusReady)
				log.Infof("Executor.scheduleNextMigration: scheduling migration %s; err: %v", uuid, err)
				e.triggerNextCheckInterval()
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

// reviewEmptyTableRevertMigrations reviews a queued REVERT migration. Such a migration has the following SQL:
// "REVERT VITESS_MIGRATION '...'"
// There's nothing in this SQL to indicate:
// - which table is involved?
// - is this a table or a view?
// - Are we reverting a CREATE? A DROP? An ALTER?
// This function fills in the blanks and updates the database row.
func (e *Executor) reviewEmptyTableRevertMigrations(ctx context.Context, onlineDDL *schema.OnlineDDL) (changesMade bool, err error) {
	if onlineDDL.Table != "" {
		return false, nil
	}
	// Table name is empty. Let's populate it.

	// Try to update table name and ddl_action
	// Failure to do so fails the migration
	revertUUID, err := onlineDDL.GetRevertUUID(e.env.Environment().Parser())
	if err != nil {
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot analyze revert UUID for revert migration %s: %v", onlineDDL.UUID, err))
	}
	revertedMigration, revertedRow, err := e.readMigration(ctx, revertUUID)
	if err != nil {
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot read migration %s reverted by migration %s: %s", revertUUID, onlineDDL.UUID, err))
	}
	revertedActionStr := revertedRow["ddl_action"].ToString()

	mimickedActionStr := ""
	switch revertedActionStr {
	case sqlparser.CreateStr:
		mimickedActionStr = sqlparser.DropStr
	case sqlparser.DropStr:
		mimickedActionStr = sqlparser.CreateStr
	case sqlparser.AlterStr:
		mimickedActionStr = sqlparser.AlterStr
	default:
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertedMigration.UUID, revertedActionStr))
	}
	if err := e.updateDDLAction(ctx, onlineDDL.UUID, mimickedActionStr); err != nil {
		return false, err
	}
	if err := e.updateMigrationIsView(ctx, onlineDDL.UUID, revertedRow.AsBool("is_view", false)); err != nil {
		return false, err
	}
	if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertedMigration.Table); err != nil {
		return false, err
	}
	return true, nil
}

// reviewImmediateOperations reviews a queued migration and determines whether it is an "immediate operation".
// Immediate operations are ones that can be performed within a split second, or rather, do not require long
// running processes. Immediate operations are:
// - CREATE TABLE
// - DROP TABLE (which we convert into RENAME)
// - All VIEW operations
// - An INSTANT DDL accompanied by relevant ddl strategy flags
// Non immediate operations are:
// - A gh-ost migration
// - A vitess (vreplication) migration
func (e *Executor) reviewImmediateOperations(
	ctx context.Context,
	capableOf capabilities.CapableOf,
	onlineDDL *schema.OnlineDDL,
	ddlAction string,
	isRevert bool,
	isView bool,
) (bool, error) {
	switch ddlAction {
	case sqlparser.CreateStr, sqlparser.DropStr:
		return true, nil
	case sqlparser.AlterStr:
		switch {
		case isView:
			return true, nil
		case isRevert:
			// REVERT for a true ALTER TABLE. not an immediate operation
			return false, nil
		default:
			specialPlan, err := e.analyzeSpecialAlterPlan(ctx, onlineDDL, capableOf)
			if err != nil {
				return false, err
			}
			return (specialPlan != nil), nil
		}
	}
	return false, nil
}

// reviewQueuedMigration investigates a single migration found in `queued` state.
// It analyzes whether the migration can & should be fulfilled immediately (e.g. via INSTANT DDL or just because it's a CREATE or DROP),
// or backfills necessary information if it's a REVERT.
// If all goes well, it sets `reviewed_timestamp` which then allows the state machine to schedule the migration.
func (e *Executor) reviewQueuedMigration(ctx context.Context, uuid string, capableOf capabilities.CapableOf) error {
	onlineDDL, row, err := e.readMigration(ctx, uuid)
	if err != nil {
		return err
	}
	// handle REVERT migrations: populate table name and update ddl action and is_view:
	ddlAction := row["ddl_action"].ToString()
	isRevert := false
	if ddlAction == schema.RevertActionStr {
		isRevert = true
		rowModified, err := e.reviewEmptyTableRevertMigrations(ctx, onlineDDL)
		if err != nil {
			return err
		}
		if rowModified {
			// re-read migration and entire row
			onlineDDL, row, err = e.readMigration(ctx, uuid)
			if err != nil {
				return err
			}
			ddlAction = row["ddl_action"].ToString()
		}
	}
	isView := row.AsBool("is_view", false)
	isImmediate, err := e.reviewImmediateOperations(ctx, capableOf, onlineDDL, ddlAction, isRevert, isView)
	if err != nil {
		return err
	}
	if isImmediate {
		if err := e.updateMigrationSetImmediateOperation(ctx, onlineDDL.UUID); err != nil {
			return err
		}
	}
	// Find conditions where the migration cannot take place:
	switch onlineDDL.Strategy {
	case schema.DDLStrategyMySQL:
		strategySetting := onlineDDL.StrategySetting()
		if strategySetting.IsPostponeCompletion() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--postpone-completion not supported in 'mysql' strategy")
		}
		if strategySetting.IsAllowZeroInDateFlag() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--allow-zero-in-date not supported in 'mysql' strategy")
		}
	}

	// The review is complete. We've backfilled details on the migration row. We mark
	// the migration as having been reviewed. The function scheduleNextMigration() will then
	// have access to this row.
	if err := e.updateMigrationTimestamp(ctx, "reviewed_timestamp", uuid); err != nil {
		return err
	}
	return nil
}

// reviewQueuedMigrations iterates through queued migrations and sees if any information needs to be updated.
// The function analyzes the queued migration and fills in some blanks:
// - If this is a REVERT migration, what table is affected? What's the operation?
// - Is this migration an "immediate operation"?
func (e *Executor) reviewQueuedMigrations(ctx context.Context) error {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()
	capableOf := mysql.ServerVersionCapableOf(conn.ServerVersion)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	r, err := e.execQuery(ctx, sqlSelectQueuedUnreviewedMigrations)
	if err != nil {
		return err
	}

	for _, uuidRow := range r.Named().Rows {
		uuid := uuidRow["migration_uuid"].ToString()
		if err := e.reviewQueuedMigration(ctx, uuid, capableOf); err != nil {
			e.failMigration(ctx, &schema.OnlineDDL{UUID: uuid}, err)
		}
	}
	return nil
}

func (e *Executor) validateMigrationRevertible(ctx context.Context, revertMigration *schema.OnlineDDL, revertingMigrationUUID string) (err error) {
	// Validation: migration to revert exists and is in complete state
	action, actionStr, err := revertMigration.GetActionStr(e.env.Environment().Parser())
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
	revertUUID, err := onlineDDL.GetRevertUUID(e.env.Environment().Parser())
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
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
		return nil, vterrors.Wrapf(err, "in evaluateDeclarativeDiff(), for onlineDDL.Table")
	}
	if existingShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "unexpected: cannot find table or view %v", onlineDDL.Table)
	}
	newShowCreateTable, err := e.showCreateTable(ctx, comparisonTableName)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in evaluateDeclarativeDiff(), for comparisonTableName")
	}
	if newShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected: cannot find table or view even as it was just created: %v", onlineDDL.Table)
	}
	senv := schemadiff.NewEnv(e.env.Environment(), e.env.Environment().CollationEnv().DefaultConnectionCharset())
	hints := &schemadiff.DiffHints{
		AutoIncrementStrategy: schemadiff.AutoIncrementApplyHigher,
	}
	switch ddlStmt.(type) {
	case *sqlparser.CreateTable:
		diff, err = schemadiff.DiffCreateTablesQueries(senv, existingShowCreateTable, newShowCreateTable, hints)
	case *sqlparser.CreateView:
		diff, err = schemadiff.DiffCreateViewsQueries(senv, existingShowCreateTable, newShowCreateTable, hints)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected CREATE TABLE or CREATE VIEW in online DDL statement: %v", onlineDDL.SQL)
	}
	if err != nil {
		return nil, err
	}
	return diff, nil
}

// getCompletedMigrationByContextAndSQL checks if there exists a completed migration with exact same
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
func (e *Executor) failMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, withError error) error {
	defer e.triggerNextCheckInterval()
	_ = e.updateMigrationStatusFailedOrCancelled(ctx, onlineDDL.UUID)
	if withError != nil {
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, withError.Error())
	}
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)
	return withError
}

// analyzeDropDDLActionMigration analyzes a DROP <TABLE|VIEW> migration.
func (e *Executor) analyzeDropDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	// Schema analysis:
	originalShowCreateTable, err := e.showCreateTable(ctx, onlineDDL.Table)
	if err != nil {
		if sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError); isSQLErr {
			switch sqlErr.Num {
			case sqlerror.ERNoSuchTable:
				// The table does not exist. For analysis purposed, that's fine.
				return nil
			default:
				return vterrors.Wrapf(err, "attempting to read definition of %v", onlineDDL.Table)
			}
		}
	}
	stmt, err := e.env.Environment().Parser().ParseStrictDDL(originalShowCreateTable)
	if err != nil {
		return err
	}

	var removedForeignKeyNames []string
	if createTable, ok := stmt.(*sqlparser.CreateTable); ok {
		// This is a table rather than a view.

		// Analyze foreign keys:

		for _, constraint := range createTable.TableSpec.Constraints {
			if GetConstraintType(constraint.Details) == ForeignKeyConstraintType {
				removedForeignKeyNames = append(removedForeignKeyNames, constraint.Name.String())
			}
		}
		// Write analysis:
	}
	if err := e.updateSchemaAnalysis(ctx, onlineDDL.UUID,
		0, 0, "", strings.Join(sqlescape.EscapeIDs(removedForeignKeyNames), ","), "", "", "",
	); err != nil {
		return err
	}
	return nil
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

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
	if err != nil {
		return failMigration(err)
	}

	if err := e.analyzeDropDDLActionMigration(ctx, onlineDDL); err != nil {
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

	acceptableErrorCodes := []sqlerror.ErrorCode{}
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

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
	if originalCreateTable, ok := ddlStmt.(*sqlparser.CreateTable); ok {
		newCreateTable := sqlparser.CloneRefOfCreateTable(originalCreateTable)
		// Rewrite this CREATE TABLE statement such that CONSTRAINT names are edited,
		// specifically removing any <tablename> prefix.
		if _, err := e.validateAndEditCreateTableStatement(ctx, onlineDDL, newCreateTable); err != nil {
			return failMigration(err)
		}
		ddlStmt = newCreateTable
		onlineDDL.SQL = sqlparser.String(newCreateTable)
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

func (e *Executor) executeAlterViewOnline(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	artifactViewName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return err
	}
	stmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
			Comments:    sqlparser.CloneRefOfParsedComments(viewStmt.Comments),
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

// addInstantAlgorithm adds or modifies the AlterTable's ALGORITHM to INSTANT
func (e *Executor) addInstantAlgorithm(alterTable *sqlparser.AlterTable) {
	instantOpt := sqlparser.AlgorithmValue("INSTANT")
	for i, opt := range alterTable.AlterOptions {
		if _, ok := opt.(sqlparser.AlgorithmValue); ok {
			// replace an existing algorithm
			alterTable.AlterOptions[i] = instantOpt
			return
		}
	}
	// append an algorithm
	alterTable.AlterOptions = append(alterTable.AlterOptions, instantOpt)
}

// executeSpecialAlterDDLActionMigrationIfApplicable sees if the given migration can be executed via special execution path, that isn't a full blown online schema change process.
func (e *Executor) executeSpecialAlterDDLActionMigrationIfApplicable(ctx context.Context, onlineDDL *schema.OnlineDDL) (specialMigrationExecuted bool, err error) {
	// Before we jump on to strategies... Some ALTERs can be optimized without having to run through
	// a full online schema change process. Let's find out if this is the case!
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return false, err
	}
	defer conn.Close()
	capableOf := mysql.ServerVersionCapableOf(conn.ServerVersion)

	specialPlan, err := e.analyzeSpecialAlterPlan(ctx, onlineDDL, capableOf)
	if err != nil {
		return false, err
	}
	if specialPlan == nil {
		return false, nil
	}

	switch specialPlan.operation {
	case instantDDLSpecialOperation:
		e.addInstantAlgorithm(specialPlan.alterTable)
		onlineDDL.SQL = sqlparser.CanonicalString(specialPlan.alterTable)
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return false, err
		}
	case dropRangePartitionSpecialOperation:
		dropPartition := func() error {
			artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
			if err != nil {
				return err
			}
			if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTableName); err != nil {
				return err
			}

			// Apply CREATE TABLE for artifact table
			if _, _, err := e.createDuplicateTableLike(ctx, artifactTableName, onlineDDL, conn); err != nil {
				return err
			}
			// Remove partitioning
			parsed := sqlparser.BuildParsedQuery(sqlAlterTableRemovePartitioning, artifactTableName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			// Exchange with partition
			partitionName := specialPlan.Detail("partition_name")
			parsed = sqlparser.BuildParsedQuery(sqlAlterTableExchangePartition, onlineDDL.Table, partitionName, artifactTableName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			// Drop table's partition
			parsed = sqlparser.BuildParsedQuery(sqlAlterTableDropPartition, onlineDDL.Table, partitionName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			return nil
		}
		if err := dropPartition(); err != nil {
			return false, err
		}
	case addRangePartitionSpecialOperation:
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return false, err
		}
	default:
		return false, nil
	}
	if err := e.updateMigrationSpecialPlan(ctx, onlineDDL.UUID, specialPlan.String()); err != nil {
		return true, err
	}
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
	return true, nil
}

// executeAlterDDLActionMigration
func (e *Executor) executeAlterDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
	// This is a real TABLE and not a VIEW

	// Before we jump on to strategies... Some ALTERs can be optimized without having to run through
	// a full online schema change process. Let's find out if this is the case!
	specialMigrationExecuted, err := e.executeSpecialAlterDDLActionMigrationIfApplicable(ctx, onlineDDL)
	if err != nil {
		return failMigration(err)
	}
	if specialMigrationExecuted {
		return nil
	}

	// OK, nothing special about this ALTER. Let's go ahead and execute it.
	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
		if err := e.ExecuteWithVReplication(ctx, onlineDDL, nil); err != nil {
			return failMigration(err)
		}
	case schema.DDLStrategyGhost:
		if err := e.ExecuteWithGhost(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
	case schema.DDLStrategyPTOSC:
		if err := e.ExecuteWithPTOSC(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
	case schema.DDLStrategyMySQL:
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
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

	ddlAction, err := onlineDDL.GetAction(e.env.Environment().Parser())
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
				ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
				// table does not exist. We mark this DROP as implicitly successful
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
			ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
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
					// No diff! We mark this CREATE as implicitly successful
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
		if err := e.executeRevert(ctx, onlineDDL); err != nil {
			failMigration(err)
		}
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
		pendingMigrationsUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
		if err != nil {
			return nil, err
		}
		r, err := e.execQuery(ctx, sqlSelectReadyMigrations)
		if err != nil {
			return nil, err
		}
		for _, row := range r.Named().Rows {
			uuid := row["migration_uuid"].ToString()
			onlineDDL, migrationRow, err := e.readMigration(ctx, uuid)
			if err != nil {
				return nil, err
			}
			isImmediateOperation := migrationRow.AsBool("is_immediate_operation", false)

			if conflictFound, _ := e.isAnyConflictingMigrationRunning(onlineDDL); conflictFound {
				continue // this migration conflicts with a running one
			}
			if e.countOwnedRunningMigrations() >= maxConcurrentOnlineDDLs {
				continue // too many running migrations
			}
			if isImmediateOperation && onlineDDL.StrategySetting().IsInOrderCompletion() {
				// This migration is immediate: if we run it now, it will complete within a second or two at most.
				if len(pendingMigrationsUUIDs) > 0 && pendingMigrationsUUIDs[0] != onlineDDL.UUID {
					continue
				}
			}
			// This migration seems good to go
			return onlineDDL, err
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
		ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL, e.env.Environment().Parser())
		if err == nil {
			ddlStmt.SetComments(sqlparser.Comments{})
			onlineDDL.SQL = sqlparser.String(ddlStmt)
		}
	}
	log.Infof("Executor.runNextMigration: migration %s is non conflicting and will be executed next", onlineDDL.UUID)
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
		id:                   row.AsInt32("id", 0),
		workflow:             row.AsString("workflow", ""),
		source:               row.AsString("source", ""),
		pos:                  row.AsString("pos", ""),
		timeUpdated:          row.AsInt64("time_updated", 0),
		timeHeartbeat:        row.AsInt64("time_heartbeat", 0),
		timeThrottled:        row.AsInt64("time_throttled", 0),
		componentThrottled:   row.AsString("component_throttled", ""),
		transactionTimestamp: row.AsInt64("transaction_timestamp", 0),
		state:                binlogdatapb.VReplicationWorkflowState(binlogdatapb.VReplicationWorkflowState_value[row.AsString("state", "")]),
		message:              row.AsString("message", ""),
		rowsCopied:           row.AsInt64("rows_copied", 0),
		bls:                  &binlogdatapb.BinlogSource{},
	}
	if err := prototext.Unmarshal([]byte(s.source), s.bls); err != nil {
		return nil, err
	}
	return s, nil
}

// isPreserveForeignKeySupported checks if the underlying MySQL server supports 'rename_table_preserve_foreign_key'
// Online DDL is not possible on vanilla MySQL 8.0 for reasons described in https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/.
// However, Online DDL is made possible in via these changes:
// - https://github.com/planetscale/mysql-server/commit/bb777e3e86387571c044fb4a2beb4f8c60462ced
// - https://github.com/planetscale/mysql-server/commit/c2f1344a6863518d749f2eb01a4c74ca08a5b889
// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps3.
// Said changes introduce a new global/session boolean variable named 'rename_table_preserve_foreign_key'. It defaults 'false'/0 for backwards compatibility.
// When enabled, a `RENAME TABLE` to a FK parent "pins" the children's foreign keys to the table name rather than the table pointer. Which means after the RENAME,
// the children will point to the newly instated table rather than the original, renamed table.
// (Note: this applies to a particular type of RENAME where we swap tables, see the above blog post).
func (e *Executor) isPreserveForeignKeySupported(ctx context.Context) (isSupported bool, err error) {
	rs, err := e.execQuery(ctx, sqlShowVariablesLikePreserveForeignKey)
	if err != nil {
		return false, err
	}
	return len(rs.Rows) > 0, nil
}

// isVReplMigrationReadyToCutOver sees if the vreplication migration has completed the row copy
// and is up to date with the binlogs.
func (e *Executor) isVReplMigrationReadyToCutOver(ctx context.Context, onlineDDL *schema.OnlineDDL, s *VReplStream) (isReady bool, err error) {
	// Check all the cases where migration is still running:
	{
		// when ready to cut-over, pos must have some value
		if s.pos == "" {
			return false, nil
		}
	}
	{
		// Both time_updated and transaction_timestamp must be in close proximity to each
		// other and to the time now, otherwise that means we're lagging and it's not a good time
		// to cut-over
		durationDiff := func(t1, t2 time.Time) time.Duration {
			return t1.Sub(t2).Abs()
		}
		migrationCutOverThreshold := getMigrationCutOverThreshold(onlineDDL)

		timeNow := time.Now()
		timeUpdated := time.Unix(s.timeUpdated, 0)
		if durationDiff(timeNow, timeUpdated) > migrationCutOverThreshold {
			return false, nil
		}
		// Let's look at transaction timestamp. This gets written by any ongoing
		// writes on the server (whether on this table or any other table)
		transactionTimestamp := time.Unix(s.transactionTimestamp, 0)
		if durationDiff(timeNow, transactionTimestamp) > migrationCutOverThreshold {
			return false, nil
		}
	}
	{
		// copy_state must have no entries for this vreplication id: if entries are
		// present that means copy is still in progress
		query, err := sqlparser.ParseAndBind(sqlReadCountCopyState,
			sqltypes.Int32BindVariable(s.id),
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

// shouldCutOverAccordingToBackoff is called when a vitess migration (ALTER TABLE) is generally ready to cut-over.
// This function further determines whether the migration should cut-over or not, by considering:
//   - backoff: we cut-over by increasing intervals, see `cutoverIntervals`
//   - forced cut-over: either via `--force-cut-over-after` DDL strategy, or via user command, we override
//     any backoff (and will also potentially KILL queries and connections holding locks on the migrated tabl)
func shouldCutOverAccordingToBackoff(
	shouldForceCutOverIndicator bool,
	forceCutOverAfter time.Duration,
	sinceReadyToComplete time.Duration,
	sinceLastCutoverAttempt time.Duration,
	cutoverAttempts int64,
) (
	shouldCutOver bool, shouldForceCutOver bool,
) {
	if shouldForceCutOverIndicator {
		// That's very simple: the user indicated they want to force cut over.
		return true, true
	}
	// shouldForceCutOver means the time since migration was ready to complete
	// is beyond the --force-cut-over-after setting, or the column `force_cutover` is "1", and this means:
	// - we do not want to backoff, we want to cutover asap
	// - we agree to brute-force KILL any pending queries on the migrated table so as to ensure it's unlocked.
	if forceCutOverAfter > 0 && sinceReadyToComplete > forceCutOverAfter {
		// time since migration was ready to complete is beyond the --force-cut-over-after setting
		return true, true
	}

	// Backoff mechanism. Do not attempt to cut-over every single minute. Check how much time passed since last cut-over attempt
	desiredTimeSinceLastCutover := cutoverIntervals[len(cutoverIntervals)-1]
	if int(cutoverAttempts) < len(cutoverIntervals) {
		desiredTimeSinceLastCutover = cutoverIntervals[cutoverAttempts]
	}
	if sinceLastCutoverAttempt >= desiredTimeSinceLastCutover {
		// Yes! Time since last cut-over complies with our expected cut-over interval
		return true, false
	}
	// Don't cut-over yet
	return false, false
}

// reviewRunningMigrations iterates migrations in 'running' state. Normally there's only one running, which was
// spawned by this tablet; but vreplication migrations could also resume from failure.
func (e *Executor) reviewRunningMigrations(ctx context.Context) (countRunnning int, cancellable []*cancellableMigration, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if atomic.LoadInt64(&e.isOpen) == 0 {
		return countRunnning, cancellable, nil
	}

	var currentUserThrottleRatio float64

	// No point in reviewing throttler info if it's not enabled&open
	for _, app := range e.lagThrottler.ThrottledApps() {
		if throttlerapp.OnlineDDLName.Equals(app.AppName) {
			currentUserThrottleRatio = app.Ratio
			break
		}
	}

	r, err := e.execQuery(ctx, sqlSelectRunningMigrations)
	if err != nil {
		return countRunnning, cancellable, err
	}
	pendingMigrationsUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return countRunnning, cancellable, err
	}
	uuidsFoundRunning := map[string]bool{}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		cutoverAttempts := row.AsInt64("cutover_attempts", 0)
		sinceLastCutoverAttempt := time.Second * time.Duration(row.AsInt64("seconds_since_last_cutover_attempt", 0))
		sinceReadyToComplete := time.Second * time.Duration(row.AsInt64("seconds_since_ready_to_complete", 0))
		onlineDDL, migrationRow, err := e.readMigration(ctx, uuid)
		if err != nil {
			return countRunnning, cancellable, err
		}
		postponeCompletion := row.AsBool("postpone_completion", false)
		shouldForceCutOver := row.AsBool("force_cutover", false)
		elapsedSeconds := row.AsInt64("elapsed_seconds", 0)
		strategySetting := onlineDDL.StrategySetting()
		// --force-cut-over-after flag is validated when DDL strategy is first parsed.
		// There should never be an error here. But if there is, we choose to skip it,
		// otherwise migrations will never complete.
		forceCutOverAfter, errForceCutOverAfter := strategySetting.ForceCutOverAfter()
		if errForceCutOverAfter != nil {
			forceCutOverAfter = 0
		}

		uuidsFoundRunning[uuid] = true

		_ = e.updateMigrationUserThrottleRatio(ctx, uuid, currentUserThrottleRatio)
		switch strategySetting.Strategy {
		case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
			reviewVReplRunningMigration := func() error {
				// We check the _vt.vreplication table
				s, err := e.readVReplStream(ctx, uuid, true)
				if err != nil {
					return err
				}
				isVreplicationTestSuite := strategySetting.IsVreplicationTestSuite()
				if isVreplicationTestSuite {
					e.triggerNextCheckInterval()
				}
				if s == nil {
					return nil
				}
				// Let's see if vreplication indicates an error. Many errors are recoverable, and
				// we do not wish to fail on first sight. We will use LastError to repeatedly
				// check if this error persists, until finally, after some timeout, we give up.
				if _, ok := e.vreplicationLastError[uuid]; !ok {
					e.vreplicationLastError[uuid] = vterrors.NewLastError(
						fmt.Sprintf("Online DDL migration %v", uuid),
						staleMigrationMinutes*time.Minute,
					)
				}
				lastError := e.vreplicationLastError[uuid]
				isTerminal, vreplError := s.hasError()
				lastError.Record(vreplError)
				if isTerminal || !lastError.ShouldRetry() {
					cancellable = append(cancellable, newCancellableMigration(uuid, s.message))
				}
				if !s.isRunning() {
					return nil
				}
				// This VRepl migration may have started from outside this tablet, so
				// this executor may not own the migration _yet_. We make sure to own it.
				// VReplication migrations are unique in this respect: we are able to complete
				// a vreplication migration started by another tablet.
				e.ownedRunningMigrations.Store(uuid, onlineDDL)
				if lastVitessLivenessIndicator := migrationRow.AsInt64("vitess_liveness_indicator", 0); lastVitessLivenessIndicator < s.livenessTimeIndicator() {
					_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
					_ = e.updateVitessLivenessIndicator(ctx, uuid, s.livenessTimeIndicator())
				}
				if onlineDDL.TabletAlias != e.TabletAliasString() {
					_ = e.updateMigrationTablet(ctx, uuid)
					log.Infof("migration %s adopted by tablet %s", uuid, e.TabletAliasString())
				}
				_ = e.updateRowsCopied(ctx, uuid, s.rowsCopied)
				_ = e.updateMigrationProgressByRowsCopied(ctx, uuid, s.rowsCopied)
				_ = e.updateMigrationETASecondsByProgress(ctx, uuid)
				_ = e.updateMigrationLastThrottled(ctx, uuid, time.Unix(s.timeThrottled, 0), s.componentThrottled)

				isReady, err := e.isVReplMigrationReadyToCutOver(ctx, onlineDDL, s)
				if err != nil {
					_ = e.updateMigrationMessage(ctx, uuid, err.Error())
					return err
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
				if !isReady {
					return nil
				}
				if postponeCompletion {
					// override. Even if migration is ready, we do not complete it.
					return nil
				}
				if strategySetting.IsInOrderCompletion() {
					if len(pendingMigrationsUUIDs) > 0 && pendingMigrationsUUIDs[0] != onlineDDL.UUID {
						// wait for earlier pending migrations to complete
						return nil
					}
				}
				shouldCutOver, shouldForceCutOver := shouldCutOverAccordingToBackoff(
					shouldForceCutOver, forceCutOverAfter, sinceReadyToComplete, sinceLastCutoverAttempt, cutoverAttempts,
				)
				if !shouldCutOver {
					return nil
				}
				if err := e.cutOverVReplMigration(ctx, s, shouldForceCutOver); err != nil {
					_ = e.updateMigrationMessage(ctx, uuid, err.Error())
					log.Errorf("cutOverVReplMigration failed: err=%v", err)
					if merr, ok := err.(*sqlerror.SQLError); ok {
						switch merr.Num {
						case sqlerror.ERTooLongIdent:
							go e.CancelMigration(ctx, uuid, err.Error(), false)
						}
					}
					return err
				}
				return nil
			}
			if err := reviewVReplRunningMigration(); err != nil {
				return countRunnning, cancellable, err
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
					message := fmt.Sprintf("cancelling a gh-ost running migration %s which is not owned by this executor. This can happen when the migration was started by a different tablet. Then, either a MySQL failure, a PRS, or ERS took place. gh-ost does not survive a MySQL restart or a shard failing over to a new PRIMARY", uuid)
					cancellable = append(cancellable, newCancellableMigration(uuid, message))
				}
			}
		}
		countRunnning++
	}
	{
		// now, let's look at UUIDs we own and _think_ should be running, and see which of them _isn't_ actually running or pending...
		uuidsFoundPending := map[string]bool{}
		for _, uuid := range pendingMigrationsUUIDs {
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
		log.Infof("reviewStaleMigrations: stale migration found: %s", onlineDDL.UUID)
		message := fmt.Sprintf("stale migration %s: found running but indicates no liveness in the past %v minutes", onlineDDL.UUID, staleMigrationMinutes)
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
		defer e.triggerNextCheckInterval()
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

func (e *Executor) tabletManagerClient() tmclient.TabletManagerClient {
	return tmclient.NewTabletManagerClient()
}

// vreplicationExec runs a vreplication query, and makes sure to initialize vreplication
func (e *Executor) vreplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	grpcCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	return tmClient.VReplicationExec(grpcCtx, tablet, query)
}

// reloadSchema issues a ReloadSchema on this tablet
func (e *Executor) reloadSchema(ctx context.Context) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	grpcCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	return tmClient.ReloadSchema(grpcCtx, tablet.Tablet, "")
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
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		return err
	}
	return nil
}

// gcArtifactTable garbage-collects a single table
func (e *Executor) gcArtifactTable(ctx context.Context, artifactTable, uuid string, t time.Time) (string, error) {
	tableExists, err := e.tableExists(ctx, artifactTable)
	if err != nil {
		return "", err
	}
	if !tableExists {
		return "", nil
	}
	// The fact we're here means the table is not needed anymore. We can throw it away.
	// We do so by renaming it into a GC table. We use the HOLD state and with a timestamp that is
	// in the past. So as we rename the table:
	// - The Online DDL executor completely loses it and has no more access to its data
	// - TableGC will find it on next iteration, see that it's been on HOLD "long enough", and will
	//   take it from there to transition it into PURGE or EVAC, or DROP, and eventually drop it.
	renameStatement, toTableName, err := schema.GenerateRenameStatementWithUUID(artifactTable, schema.HoldTableGCState, schema.OnlineDDLToGCUUID(uuid), t)
	if err != nil {
		return toTableName, err
	}
	_, err = e.execQuery(ctx, renameStatement)
	return toTableName, err
}

// gcArtifacts garbage-collects migration artifacts from completed/failed migrations
func (e *Executor) gcArtifacts(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	// v18 fix. Remove in v19
	if !fixCompletedTimestampDone {
		if _, err := e.execQuery(ctx, sqlFixCompletedTimestamp); err != nil {
			// This query fixes a bug where stale migrations were marked as 'cancelled' or 'failed' without updating 'completed_timestamp'
			// Running this query retroactively sets completed_timestamp
			// This fix is created in v18 and can be removed in v19
			return err
		}
		fixCompletedTimestampDone = true
	}

	query, err := sqlparser.ParseAndBind(sqlSelectUncollectedArtifacts,
		sqltypes.Int64BindVariable(int64((retainOnlineDDLTables).Seconds())),
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

		log.Infof("Executor.gcArtifacts: will GC artifacts for migration %s", uuid)
		// Remove tables:
		artifactTables := textutil.SplitDelimitedList(artifacts)

		timeNow := time.Now()
		for i, artifactTable := range artifactTables {
			// We wish to generate distinct timestamp values for each table in this UUID,
			// because all tables will be renamed as _something_UUID_timestamp. Since UUID
			// is shared for multiple artifacts in this loop, we differentiate via timestamp.
			// Also, the timestamp we create is in the past, so that the table GC mechanism can
			// take it away from there on next iteration.
			log.Infof("Executor.gcArtifacts: will GC artifact %s for migration %s", artifactTable, uuid)
			timestampInThePast := timeNow.Add(time.Duration(-i) * time.Second).UTC()
			toTableName, err := e.gcArtifactTable(ctx, artifactTable, uuid, timestampInThePast)
			if err == nil {
				// artifact was renamed away and is gone. There' no need to list it in `artifacts` column.
				e.clearSingleArtifact(ctx, uuid, artifactTable)
				e.requestGCChecksFunc()
			} else {
				return vterrors.Wrapf(err, "in gcArtifacts() for %s", artifactTable)
			}
			log.Infof("Executor.gcArtifacts: renamed away artifact %s to %s", artifactTable, toTableName)
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
		log.Infof("Executor.gcArtifacts: done migration %s", uuid)
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
	} else if err := e.cancelMigrations(ctx, cancellable, false); err != nil {
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
	if err != nil {
		log.Errorf("FAIL updateMigrationStartedTimestamp: uuid=%s, error=%v", uuid, err)
	}
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
	if err != nil {
		log.Errorf("FAIL updateMigrationStartedTimestamp: uuid=%s, timestampColumn=%v, error=%v", uuid, timestampColumn, err)
	}
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

func (e *Executor) clearSingleArtifact(ctx context.Context, uuid string, artifact string) error {
	query, err := sqlparser.ParseAndBind(sqlClearSingleArtifact,
		sqltypes.StringBindVariable(artifact),
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

func (e *Executor) updateMigrationSpecialPlan(ctx context.Context, uuid string, specialPlan string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateSpecialPlan,
		sqltypes.StringBindVariable(specialPlan),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationStage(ctx context.Context, uuid string, stage string, args ...interface{}) error {
	msg := fmt.Sprintf(stage, args...)
	log.Infof("updateMigrationStage: uuid=%s, stage=%s", uuid, msg)
	query, err := sqlparser.ParseAndBind(sqlUpdateStage,
		sqltypes.StringBindVariable(msg),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) incrementCutoverAttempts(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlIncrementCutoverAttempts,
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

func (e *Executor) updateMigrationStatusFailedOrCancelled(ctx context.Context, uuid string) error {
	log.Infof("updateMigrationStatus: transitioning migration: %s into status failed or cancelled", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusFailedOrCancelled,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationStatus(ctx context.Context, uuid string, status schema.OnlineDDLStatus) error {
	log.Infof("updateMigrationStatus: transitioning migration: %s into status: %s", uuid, string(status))
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatus,
		sqltypes.StringBindVariable(string(status)),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	if err != nil {
		log.Errorf("FAIL updateMigrationStatus: uuid=%s, query=%v, error=%v", uuid, query, err)
	}
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
	log.Infof("updateMigrationMessage: uuid=%s, message=%s", uuid, message)

	maxlen := 16383
	update := func(message string) error {
		if len(message) > maxlen {
			message = message[0:maxlen]
		}
		message = strings.ToValidUTF8(message, "�")
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
	err := update(message)
	if err != nil {
		// If, for some reason, we're unable to update the error message, let's write a generic message
		err = update("unable to update with original migration error message")
	}
	return err
}

func (e *Executor) updateSchemaAnalysis(ctx context.Context, uuid string,
	addedUniqueKeys, removedUniqueKeys int, removedUniqueKeyNames string,
	removedForeignKeyNames string,
	droppedNoDefaultColumnNames string, expandedColumnNames string,
	revertibleNotes string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateSchemaAnalysis,
		sqltypes.Int64BindVariable(int64(addedUniqueKeys)),
		sqltypes.Int64BindVariable(int64(removedUniqueKeys)),
		sqltypes.StringBindVariable(removedUniqueKeyNames),
		sqltypes.StringBindVariable(removedForeignKeyNames),
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

func (e *Executor) updateMigrationLastThrottled(ctx context.Context, uuid string, lastThrottledTime time.Time, throttledCompnent string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateLastThrottled,
		sqltypes.StringBindVariable(lastThrottledTime.Format(sqltypes.TimestampFormat)),
		sqltypes.StringBindVariable(throttledCompnent),
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

func (e *Executor) updateMigrationSetImmediateOperation(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationSetImmediateOperation,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, query)
	return err
}

func (e *Executor) updateMigrationReadyToComplete(ctx context.Context, uuid string, isReady bool) error {
	var queryTemplate string
	if isReady {
		queryTemplate = sqlSetMigrationReadyToComplete
	} else {
		queryTemplate = sqlClearMigrationReadyToComplete
	}
	query, err := sqlparser.ParseAndBind(queryTemplate,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	if _, err := e.execQuery(ctx, query); err != nil {
		return err
	}
	if val, ok := e.ownedRunningMigrations.Load(uuid); ok {
		if runningMigration, ok := val.(*schema.OnlineDDL); ok {
			var storeValue int64
			if isReady {
				storeValue = 1
				atomic.StoreInt64(&runningMigration.WasReadyToComplete, 1) // WasReadyToComplete is set once and never cleared
			}
			atomic.StoreInt64(&runningMigration.ReadyToComplete, storeValue)
		}
	}
	return nil
}

func (e *Executor) updateMigrationUserThrottleRatio(ctx context.Context, uuid string, ratio float64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationUserThrottleRatio,
		sqltypes.Float64BindVariable(ratio),
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
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
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
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in CLEANUP: %s", uuid)
	}
	log.Infof("CleanupMigration: request to cleanup migration %s", uuid)
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateReadyForCleanup,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	rs, err := e.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	log.Infof("CleanupMigration: migration %s marked as ready to clean up", uuid)
	defer e.triggerNextCheckInterval()
	return rs, nil
}

// ForceCutOverMigration markes the given migration for forced cut-over. This has two implications:
//   - No backoff for the given migration's cut-over (cut-over will be attempted at the next scheduler cycle,
//     irrespective of how many cut-over attempts have been made and when these attempts have been made).
//   - During the cut-over, Online DDL will try and temrinate all existing queries on the migrated table, and
//     transactions (killing their connections) holding a lock on the migrated table. This is likely to cause the
//     cut-over to succeed. Of course, it's not guaranteed, and it's possible that next cut-over will fail.
//     The force_cutover flag, once set, remains set, and so all future cut-over attempts will again KILL interfering
//     queries and connections.
func (e *Executor) ForceCutOverMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in FORCE_CUTOVER: %s", uuid)
	}
	log.Infof("ForceCutOverMigration: request to force cut-over migration %s", uuid)
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateForceCutOver,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	rs, err := e.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	e.triggerNextCheckInterval()
	log.Infof("ForceCutOverMigration: migration %s marked for forced cut-over", uuid)
	return rs, nil
}

// ForceCutOverPendingMigrations sets force_cutover flag for all pending migrations
func (e *Executor) ForceCutOverPendingMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("ForceCutOverPendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("ForceCutOverPendingMigrations: applying to %s", uuid)
		res, err := e.ForceCutOverMigration(ctx, uuid)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("ForceCutOverPendingMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

// CompleteMigration clears the postpone_completion flag for a given migration, assuming it was set in the first place
func (e *Executor) CompleteMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in COMPLETE: %s", uuid)
	}
	log.Infof("CompleteMigration: request to complete migration %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlClearPostponeCompletion,
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
	rs, err := e.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	log.Infof("CompleteMigration: migration %s marked as unpostponed", uuid)
	return rs, nil
}

// CompletePendingMigrations completes all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) CompletePendingMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("CompletePendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("CompletePendingMigrations: completing %s", uuid)
		res, err := e.CompleteMigration(ctx, uuid)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("CompletePendingMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

// LaunchMigration clears the postpone_launch flag for a given migration, assuming it was set in the first place
func (e *Executor) LaunchMigration(ctx context.Context, uuid string, shardsArg string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in EXECUTE: %s", uuid)
	}
	if !e.matchesShards(shardsArg) {
		// Does not apply  to this shard!
		return &sqltypes.Result{}, nil
	}
	log.Infof("LaunchMigration: request to launch migration %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateLaunchMigration,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	rs, err := e.execQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	log.Infof("LaunchMigration: migration %s marked as unpostponed", uuid)
	return rs, nil
}

// LaunchMigrations launches all launch-postponed queued migrations for this keyspace
func (e *Executor) LaunchMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	r, err := e.execQuery(ctx, sqlSelectQueuedMigrations)
	if err != nil {
		return result, err
	}
	rows := r.Named().Rows
	log.Infof("LaunchMigrations: iterating %v migrations %s", len(rows))
	result = &sqltypes.Result{}
	for _, row := range rows {
		uuid := row["migration_uuid"].ToString()
		log.Infof("LaunchMigrations: unpostponing %s", uuid)
		res, err := e.LaunchMigration(ctx, uuid, "")
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("LaunchMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

func (e *Executor) submittedMigrationConflictsWithPendingMigrationInSingletonContext(
	ctx context.Context, submittedMigration, pendingOnlineDDL *schema.OnlineDDL,
) bool {
	if pendingOnlineDDL.MigrationContext == submittedMigration.MigrationContext {
		// same migration context. this is obviously allowed
		return false
	}
	// Let's see if the pending migration is a revert:
	if _, err := pendingOnlineDDL.GetRevertUUID(e.env.Environment().Parser()); err != nil {
		// Not a revert. So the pending migration definitely conflicts with our migration.
		return true
	}

	// The pending migration is a revert
	if !pendingOnlineDDL.StrategySetting().IsSingletonContext() {
		// Aha! So, our "conflict" is with a REVERT migration, which does _not_ have a -singleton-context
		// flag. Because we want to allow REVERT migrations to run as concurrently as possible, we allow this scenario.
		return false
	}
	return true
}

// submitCallbackIfNonConflicting is called internally by SubmitMigration, and is given a callback to execute
// if the given migration does not conflict any terms. Specifically, this function looks for singleton or
// singleton-context conflicts.
// The call back can be an insertion of a new migration, or a retry of an existing migration, or whatnot.
func (e *Executor) submitCallbackIfNonConflicting(
	ctx context.Context,
	onlineDDL *schema.OnlineDDL,
	callback func() (*sqltypes.Result, error),
) (
	result *sqltypes.Result, err error,
) {
	if !onlineDDL.StrategySetting().IsSingleton() && !onlineDDL.StrategySetting().IsSingletonContext() {
		// not a singleton. No conflict
		return callback()
	}
	// This is either singleton or singleton-context

	// This entire next logic is wrapped in an anonymous func just to get the migrationMutex released
	// before calling the callback function. Reason is: the callback function itself may need to acquire
	// the mutex. And specifically, one of the callback functions used is e.RetryMigration(), which does
	// lock the mutex...
	err = func() error {
		e.migrationMutex.Lock()
		defer e.migrationMutex.Unlock()

		rs, err := e.execQuery(ctx, sqlSelectPendingMigrations)
		if err != nil {
			return err
		}
		rows := rs.Named().Rows

		switch {
		case onlineDDL.StrategySetting().IsSingleton():
			// We will reject this migration if there's any pending migration
			if len(rows) > 0 {
				samplePendingUUID := rows[0]["migration_uuid"].ToString()
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migrations [sample: %s]", samplePendingUUID)
			}
		case onlineDDL.StrategySetting().IsSingletonContext():
			// We will reject this migration if there's any pending migration within a different context
			for _, row := range rows {
				migrationContext := row["migration_context"].ToString()
				if onlineDDL.MigrationContext == migrationContext {
					// obviously no conflict here. We can skip the next checks, which are more expensive
					// as they require reading each migration separately.
					continue
				}
				pendingUUID := row["migration_uuid"].ToString()
				pendingOnlineDDL, _, err := e.readMigration(ctx, pendingUUID)
				if err != nil {
					return vterrors.Wrapf(err, "validateSingleton() migration: %s", pendingUUID)
				}
				if e.submittedMigrationConflictsWithPendingMigrationInSingletonContext(ctx, onlineDDL, pendingOnlineDDL) {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton-context migration rejected: found pending migration: %s in different context: %s", pendingUUID, pendingOnlineDDL.MigrationContext)
				}
				// no conflict? continue looking for other pending migrations
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	// OK to go!
	return callback()
}

// SubmitMigration inserts a new migration request
func (e *Executor) SubmitMigration(
	ctx context.Context,
	stmt sqlparser.Statement,
) (*sqltypes.Result, error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}

	log.Infof("SubmitMigration: request to submit migration with statement: %0.50s...", sqlparser.CanonicalString(stmt))
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

	// The logic below has multiple steps. We hence protect the rest of the code with a mutex, only used by this function.
	e.submitMutex.Lock()
	defer e.submitMutex.Unlock()

	// Is there already a migration by this same UUID?
	storedMigration, _, err := e.readMigration(ctx, onlineDDL.UUID)
	if err != nil && err != ErrMigrationNotFound {
		return nil, vterrors.Wrapf(err, "while checking whether migration %s exists", onlineDDL.UUID)
	}
	if storedMigration != nil {
		log.Infof("SubmitMigration: migration %s already exists with migration_context=%s, table=%s", onlineDDL.UUID, storedMigration.MigrationContext, onlineDDL.Table)
		// A migration already exists with the same UUID. This is fine, we allow re-submitting migrations
		// with the same UUID, as we provide idempotency.
		// So we will _mostly_ ignore the request: we will not submit a new migration. However, we will do
		// these things:

		// 1. Check that the requested submitted migration matches the existing one's migration-context, otherwise
		//    this doesn't seem right, not the idempotency we were looking for
		if storedMigration.MigrationContext != onlineDDL.MigrationContext {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "migration rejected: found migration %s with different context: %s than submitted migration's context: %s", onlineDDL.UUID, storedMigration.MigrationContext, onlineDDL.MigrationContext)
		}
		// 2. Possibly, the existing migration is in 'failed' or 'cancelled' state, in which case this
		//    resubmission should retry the migration.
		return e.submitCallbackIfNonConflicting(
			ctx, onlineDDL,
			func() (*sqltypes.Result, error) { return e.RetryMigration(ctx, onlineDDL.UUID) },
		)
	}

	// OK, this is a new UUID

	_, actionStr, err := onlineDDL.GetActionStr(e.env.Environment().Parser())
	if err != nil {
		return nil, err
	}
	log.Infof("SubmitMigration: request to submit migration %s; action=%s, table=%s", onlineDDL.UUID, actionStr, onlineDDL.Table)

	revertedUUID, _ := onlineDDL.GetRevertUUID(e.env.Environment().Parser()) // Empty value if the migration is not actually a REVERT. Safe to ignore error.
	retainArtifactsSeconds := int64((retainOnlineDDLTables).Seconds())
	if retainArtifacts, _ := onlineDDL.StrategySetting().RetainArtifactsDuration(); retainArtifacts != 0 {
		// Explicit retention indicated by `--retain-artifact` DDL strategy flag for this migration. Override!
		retainArtifactsSeconds = int64((retainArtifacts).Seconds())
	}

	_, allowConcurrentMigration := e.allowConcurrentMigration(onlineDDL)
	submitQuery, err := sqlparser.ParseAndBind(sqlInsertMigration,
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
		sqltypes.BoolBindVariable(onlineDDL.StrategySetting().IsPostponeLaunch()),
		sqltypes.BoolBindVariable(onlineDDL.StrategySetting().IsPostponeCompletion()),
		sqltypes.BoolBindVariable(allowConcurrentMigration),
		sqltypes.StringBindVariable(revertedUUID),
		sqltypes.BoolBindVariable(onlineDDL.IsView(e.env.Environment().Parser())),
	)
	if err != nil {
		return nil, err
	}
	result, err := e.submitCallbackIfNonConflicting(
		ctx, onlineDDL,
		func() (*sqltypes.Result, error) { return e.execQuery(ctx, submitQuery) },
	)
	if err != nil {
		return nil, vterrors.Wrapf(err, "submitting migration %v", onlineDDL.UUID)

	}
	log.Infof("SubmitMigration: migration %s submitted", onlineDDL.UUID)

	defer e.triggerNextCheckInterval()

	return result, nil
}

// ShowMigrations shows migrations, optionally filtered by a condition
func (e *Executor) ShowMigrations(ctx context.Context, show *sqlparser.Show) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
	}
	showBasic, ok := show.Internal.(*sqlparser.ShowBasic)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] ShowMigrations expects a ShowBasic statement. Got: %s", sqlparser.String(show))
	}
	if showBasic.Command != sqlparser.VitessMigrations {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] ShowMigrations expects a VitessMigrations command, got %+v. Statement: %s", showBasic.Command, sqlparser.String(show))
	}
	whereExpr := ""
	if showBasic.Filter != nil {
		if showBasic.Filter.Filter != nil {
			whereExpr = fmt.Sprintf(" where %s", sqlparser.String(showBasic.Filter.Filter))
		} else if showBasic.Filter.Like != "" {
			lit := sqlparser.String(sqlparser.NewStrLiteral(showBasic.Filter.Like))
			whereExpr = fmt.Sprintf(" where migration_uuid LIKE %s OR migration_context LIKE %s OR migration_status LIKE %s", lit, lit, lit)
		}
	}
	query := sqlparser.BuildParsedQuery(sqlShowMigrationsWhere, whereExpr).Query
	return e.execQuery(ctx, query)
}

// ShowMigrationLogs reads the migration log for a given migration
func (e *Executor) ShowMigrationLogs(ctx context.Context, stmt *sqlparser.ShowMigrationLogs) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, schema.ErrOnlineDDLDisabled.Error())
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
