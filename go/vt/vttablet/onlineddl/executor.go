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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/vexec"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"github.com/google/shlex"
)

var (
	// ErrExecutorNotWritableTablet  is generated when executor is asked to run gh-ost on a read-only server
	ErrExecutorNotWritableTablet = errors.New("Cannot run gh-ost migration on non-writable tablet")
	// ErrExecutorMigrationAlreadyRunning is generated when an attempt is made to run an operation that conflicts with a running migration
	ErrExecutorMigrationAlreadyRunning = errors.New("Cannot run gh-ost migration since a migration is already running")
	// ErrMigrationNotFound is returned by readMigration when given UUI cannot be found
	ErrMigrationNotFound = errors.New("Migration not found")
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

var emptyResult = &sqltypes.Result{
	RowsAffected: 0,
}

var ghostOverridePath = flag.String("gh-ost-path", "", "override default gh-ost binary full path")
var ptOSCOverridePath = flag.String("pt-osc-path", "", "override default pt-online-schema-change binary full path")
var migrationCheckInterval = flag.Duration("migration_check_interval", 1*time.Minute, "Interval between migration checks")
var migrationNextCheckInterval = 5 * time.Second

const (
	maxPasswordLength             = 32 // MySQL's *replication* password may not exceed 32 characters
	staleMigrationMinutes         = 10
	progressPctFull       float64 = 100.0
)

var (
	migrationLogFileName = "migration.log"
	onlineDDLUser        = "vt-online-ddl-internal"
	onlineDDLGrant       = fmt.Sprintf("'%s'@'%s'", onlineDDLUser, "%")
)

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

	initMutex          sync.Mutex
	migrationMutex     sync.Mutex
	migrationRunning   int64
	lastMigrationUUID  string
	tickReentranceFlag int64

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

// NewExecutor creates a new gh-ost executor.
func NewExecutor(env tabletenv.Env, tabletAlias topodatapb.TabletAlias, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType) *Executor {
	return &Executor{
		env:         env,
		tabletAlias: &tabletAlias,

		pool: connpool.NewPool(env, "ExecutorPool", tabletenv.ConnPoolConfig{
			Size:               1,
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

	for _, ddl := range applyDDL {
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
	if e.isOpen {
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
	e.ticks.TriggerAfter(migrationNextCheckInterval)
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
func (e *Executor) readMySQLVariables(ctx context.Context) (host string, port int, readOnly bool, err error) {
	conn, err := e.pool.Get(ctx)
	if err != nil {
		return host, port, readOnly, err
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, "select @@global.hostname as hostname, @@global.port as port, @@global.read_only as read_only from dual", 1, true)
	if err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read MySQL variables: %v", err)
	}
	row := tm.Named().Row()
	if row == nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL variables: %+v", tm.Rows)
	}
	host = row["hostname"].ToString()

	p, err := row.ToInt64("port")
	if err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.port %v: %v", tm, err)
	}
	port = int(p)

	if readOnly, err = row.ToBool("read_only"); err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.read_only %v: %v", tm, err)
	}
	return host, port, readOnly, nil
}

// createOnlineDDLUser creates a gh-ost user account with all neccessary privileges and with a random password
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
	conn, err := e.pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	tableName = strings.ReplaceAll(tableName, `_`, `\_`)
	parsed := sqlparser.BuildParsedQuery(sqlShowTablesLike, tableName)
	rs, err := conn.Exec(ctx, parsed.Query, 1, true)
	if err != nil {
		return false, err
	}
	row := rs.Named().Row()
	return (row != nil), nil
}

// executeDirectly runs a DDL query directly on the backend MySQL server
func (e *Executor) executeDirectly(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch(onlineDDL.SQL, 0, false); err != nil {
		return err
	}
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull)

	return nil
}

// ExecuteWithGhost validates and runs a gh-ost process.
// Validation included testing the backend MySQL server and the gh-ost binary itself
// Execution runs first a dry run, then an actual migration
func (e *Executor) ExecuteWithGhost(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if atomic.LoadInt64(&e.migrationRunning) > 0 {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
		return ErrExecutorNotWritableTablet
	}
	mysqlHost, mysqlPort, readOnly, err := e.readMySQLVariables(ctx)
	if err != nil {
		log.Errorf("Error before running gh-ost: %+v", err)
		return err
	}
	if readOnly {
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

mkdir -p "$ghost_log_path"

export ONLINE_DDL_PASSWORD
%s "$@" > "$ghost_log_path/$ghost_log_file" 2>&1
	`, tempDir, migrationLogFileName, binaryFileName,
	)
	wrapperScriptFileName, err := createTempScript(tempDir, "gh-ost-wrapper.sh", wrapperScriptContent)
	if err != nil {
		log.Errorf("Error creating wrapper script: %+v", err)
		return err
	}
	onHookContent := func(status schema.OnlineDDLStatus) string {
		return fmt.Sprintf(`#!/bin/bash
curl -s 'http://localhost:%d/schema-migration/report-status?uuid=%s&status=%s&dryrun='"$GH_OST_DRY_RUN"'&progress='"$GH_OST_PROGRESS"
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
	log.Infof("+ OK")

	if err := e.updateMigrationLogPath(ctx, onlineDDL.UUID, mysqlHost, tempDir); err != nil {
		return err
	}

	runGhost := func(execute bool) error {
		// Temporary hack (2020-08-11)
		// Because sqlparser does not do full blown ALTER TABLE parsing,
		// and because we don't want gh-ost to know about WITH_GHOST and WITH_PT syntax,
		// we resort to regexp-based parsing of the query.
		// TODO(shlomi): generate _alter options_ via sqlparser when it full supports ALTER TABLE syntax.
		_, _, alterOptions := schema.ParseAlterTableOptions(onlineDDL.SQL)
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
			fmt.Sprintf(`--host=%s`, mysqlHost),
			fmt.Sprintf(`--port=%d`, mysqlPort),
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
		opts, _ := shlex.Split(onlineDDL.Options)
		args = append(args, opts...)
		_, err := execCmd("bash", args, os.Environ(), "/tmp", nil, nil)
		return err
	}

	atomic.StoreInt64(&e.migrationRunning, 1)
	e.lastMigrationUUID = onlineDDL.UUID

	go func() error {
		defer atomic.StoreInt64(&e.migrationRunning, 0)
		defer e.dropOnlineDDLUser(ctx)
		defer e.gcArtifacts(ctx)

		log.Infof("Will now dry-run gh-ost on: %s:%d", mysqlHost, mysqlPort)
		if err := runGhost(false); err != nil {
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			log.Errorf("Error executing gh-ost dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run gh-ost on: %s:%d", mysqlHost, mysqlPort)
		startedMigrations.Add(1)
		if err := runGhost(true); err != nil {
			// perhaps gh-ost was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			failedMigrations.Add(1)
			log.Errorf("Error running gh-ost: %+v", err)
			return err
		}
		// Migration successful!
		os.RemoveAll(tempDir)
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
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if atomic.LoadInt64(&e.migrationRunning) > 0 {
		return ErrExecutorMigrationAlreadyRunning
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
		return ErrExecutorNotWritableTablet
	}
	mysqlHost, mysqlPort, readOnly, err := e.readMySQLVariables(ctx)
	if err != nil {
		log.Errorf("Error before running pt-online-schema-change: %+v", err)
		return err
	}
	if readOnly {
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

	if err := e.updateMigrationLogPath(ctx, onlineDDL.UUID, mysqlHost, tempDir); err != nil {
		return err
	}

	// Temporary hack (2020-08-11)
	// Because sqlparser does not do full blown ALTER TABLE parsing,
	// and because pt-online-schema-change requires only the table options part of the ALTER TABLE statement,
	// we resort to regexp-based parsing of the query.
	// TODO(shlomi): generate _alter options_ via sqlparser when it full supports ALTER TABLE syntax.
	_, _, alterOptions := schema.ParseAlterTableOptions(onlineDDL.SQL)

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
			fmt.Sprintf(`h=%s,P=%d,D=%s,t=%s,u=%s`, mysqlHost, mysqlPort, e.dbName, onlineDDL.Table, onlineDDLUser),
			executeFlag,
			fmt.Sprintf(`h=%s,P=%d,D=%s,t=%s,u=%s`, mysqlHost, mysqlPort, e.dbName, onlineDDL.Table, onlineDDLUser),
		}

		if execute {
			args = append(args,
				`--no-drop-new-table`,
				`--no-drop-old-table`,
			)
		}
		opts, _ := shlex.Split(onlineDDL.Options)
		args = append(args, opts...)
		_, err = execCmd("bash", args, os.Environ(), "/tmp", nil, nil)
		return err
	}

	atomic.StoreInt64(&e.migrationRunning, 1)
	e.lastMigrationUUID = onlineDDL.UUID

	go func() error {
		defer atomic.StoreInt64(&e.migrationRunning, 0)
		defer e.dropOnlineDDLUser(ctx)
		defer e.gcArtifacts(ctx)

		log.Infof("Will now dry-run pt-online-schema-change on: %s:%d", mysqlHost, mysqlPort)
		if err := runPTOSC(false); err != nil {
			// perhaps pt-osc was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			log.Errorf("Error executing pt-online-schema-change dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run pt-online-schema-change on: %s:%d", mysqlHost, mysqlPort)
		startedMigrations.Add(1)
		if err := runPTOSC(true); err != nil {
			// perhaps pt-osc was interrupted midway and didn't have the chance to send a "failes" status
			_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
			_ = e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID)
			_ = e.dropPTOSCMigrationTriggers(ctx, onlineDDL)
			failedMigrations.Add(1)
			log.Errorf("Error running pt-online-schema-change: %+v", err)
			return err
		}
		// Migration successful!
		os.RemoveAll(tempDir)
		successfulMigrations.Add(1)
		log.Infof("+ OK")
		return nil
	}()
	return nil
}

func (e *Executor) readMigration(ctx context.Context, uuid string) (onlineDDL *schema.OnlineDDL, err error) {

	parsed := sqlparser.BuildParsedQuery(sqlSelectMigration, "_vt", ":migration_uuid")
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return onlineDDL, err
	}
	r, err := e.execQuery(ctx, bound)
	if err != nil {
		return onlineDDL, err
	}
	row := r.Named().Row()
	if row == nil {
		// No results
		return nil, ErrMigrationNotFound
	}
	onlineDDL = &schema.OnlineDDL{
		Keyspace:    row["keyspace"].ToString(),
		Table:       row["mysql_table"].ToString(),
		Schema:      row["mysql_schema"].ToString(),
		SQL:         row["migration_statement"].ToString(),
		UUID:        row["migration_uuid"].ToString(),
		Strategy:    schema.DDLStrategy(row["strategy"].ToString()),
		Options:     row["options"].ToString(),
		Status:      schema.OnlineDDLStatus(row["migration_status"].ToString()),
		Retries:     row.AsInt64("retries", 0),
		TabletAlias: row["tablet"].ToString(),
	}
	return onlineDDL, nil
}

// terminateMigration attempts to interrupt and hard-stop a running migration
func (e *Executor) terminateMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, lastMigrationUUID string) (foundRunning bool, err error) {
	if atomic.LoadInt64(&e.migrationRunning) > 0 {
		// double check: is the running migration the very same one we wish to cancel?
		if onlineDDL.UUID == lastMigrationUUID {
			// assuming all goes well in next steps, we can already report that there has indeed been a migration
			foundRunning = true
		}
	}
	switch onlineDDL.Strategy {
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
		// gh-ost migrations are easy to kill: just touch their specific panic flag files. We trust
		// gh-ost to terminate. No need to KILL it. And there's no trigger cleanup.
		if err := e.createGhostPanicFlagFile(onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error cancelling migration, flag file error: %+v", err)
		}
	}
	return foundRunning, nil
}

// cancelMigration attempts to abort a scheduled or a running migration
func (e *Executor) cancelMigration(ctx context.Context, uuid string, terminateRunningMigration bool) (result *sqltypes.Result, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var rowsAffected uint64

	onlineDDL, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady:
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusCancelled); err != nil {
			return nil, err
		}
		rowsAffected = 1
	}

	if terminateRunningMigration {
		migrationFound, err := e.terminateMigration(ctx, onlineDDL, e.lastMigrationUUID)
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
func (e *Executor) cancelMigrations(ctx context.Context, uuids []string) (err error) {
	for _, uuid := range uuids {
		log.Infof("cancelMigrations: cancelling %s", uuid)
		if _, err := e.cancelMigration(ctx, uuid, true); err != nil {
			return err
		}
	}
	return nil
}

// cancelPendingMigrations cancels all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) cancelPendingMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlSelectPendingMigrations, "_vt")
	r, err := e.execQuery(ctx, parsed.Query)
	if err != nil {
		return result, err
	}
	var uuids []string
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		uuids = append(uuids, uuid)
	}

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("cancelPendingMigrations: cancelling %s", uuid)
		res, err := e.cancelMigration(ctx, uuid, true)
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

	if atomic.LoadInt64(&e.migrationRunning) > 0 {
		return ErrExecutorMigrationAlreadyRunning
	}

	{
		parsed := sqlparser.BuildParsedQuery(sqlSelectCountReadyMigrations, "_vt")
		r, err := e.execQuery(ctx, parsed.Query)
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

	parsed := sqlparser.BuildParsedQuery(sqlScheduleSingleMigration, "_vt")
	_, err := e.execQuery(ctx, parsed.Query)

	return err
}

func (e *Executor) executeMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		_ = e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed)
		return err
	}

	ddlAction, err := onlineDDL.GetAction()
	if err != nil {
		return failMigration(err)
	}
	switch ddlAction {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction:
		go func() {
			if err := e.executeDirectly(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	case sqlparser.AlterDDLAction:
		switch onlineDDL.Strategy {
		case schema.DDLStrategyGhost:
			go func() {
				if err := e.ExecuteWithGhost(ctx, onlineDDL); err != nil {
					failMigration(err)
				}
			}()
		case schema.DDLStrategyPTOSC:
			go func() {
				if err := e.ExecuteWithPTOSC(ctx, onlineDDL); err != nil {
					failMigration(err)
				}
			}()
		default:
			{
				return failMigration(fmt.Errorf("Unsupported strategy: %+v", onlineDDL.Strategy))
			}
		}
	}
	return nil
}

func (e *Executor) runNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if atomic.LoadInt64(&e.migrationRunning) > 0 {
		return ErrExecutorMigrationAlreadyRunning
	}

	parsed := sqlparser.BuildParsedQuery(sqlSelectReadyMigration, "_vt")
	r, err := e.execQuery(ctx, parsed.Query)
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

// reviewRunningMigrations iterates migrations in 'running' state (there really should just be one that is
// actually running).
func (e *Executor) reviewRunningMigrations(ctx context.Context) (countRunnning int, runningNotByThisProcess []string, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	parsed := sqlparser.BuildParsedQuery(sqlSelectRunningMigrations, "_vt", ":strategy")
	bindVars := map[string]*querypb.BindVariable{
		"strategy": sqltypes.StringBindVariable(string(schema.DDLStrategyPTOSC)),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return countRunnning, runningNotByThisProcess, err
	}
	r, err := e.execQuery(ctx, bound)
	if err != nil {
		return countRunnning, runningNotByThisProcess, err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		// Since pt-osc doesn't have a "liveness" plugin entry point, we do it externally:
		// if the process is alive, we update the `liveness_timestamp` for this migration.
		if running, _, _ := e.isPTOSCMigrationRunning(ctx, uuid); running {
			_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
		}
		countRunnning++

		if uuid != e.lastMigrationUUID {
			// This executor can only run one migration at a time. And that
			// migration is identified by e.lastMigrationUUID.
			// If we find a _running_ migration that does not have this UUID, it _must_
			// mean the migration was started by a former vttablet (ie vttablet crashed and restarted)
			runningNotByThisProcess = append(runningNotByThisProcess, uuid)
		}
	}
	return countRunnning, runningNotByThisProcess, err
}

// reviewStaleMigrations marks as 'failed' migrations whose status is 'running' but which have
// shown no liveness in past X minutes
func (e *Executor) reviewStaleMigrations(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	parsed := sqlparser.BuildParsedQuery(sqlSelectStaleMigrations, "_vt", ":minutes")
	bindVars := map[string]*querypb.BindVariable{
		"minutes": sqltypes.Int64BindVariable(staleMigrationMinutes),
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
		uuid := row["migration_uuid"].ToString()

		onlineDDL, err := e.readMigration(ctx, uuid)
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
	}

	return nil
}

// retryTabletFailureMigrations looks for migrations failed by tablet failure (e.g. by failover)
// and retry them (put them back in the queue)
func (e *Executor) retryTabletFailureMigrations(ctx context.Context) error {
	_, err := e.retryMigration(ctx, sqlWhereTabletFailure)
	return err
}

// gcArtifacts garbage-collects migration artifacts from completed/failed migrations
func (e *Executor) gcArtifactTable(ctx context.Context, artifactTable string) error {
	tableExists, err := e.tableExists(ctx, artifactTable)
	if err != nil {
		return err
	}
	if !tableExists {
		return nil
	}
	renameStatement, _, err := schema.GenerateRenameStatement(artifactTable, schema.PurgeTableGCState, time.Now().UTC())
	if err != nil {
		return err
	}
	conn, err := e.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	_, err = conn.Exec(ctx, renameStatement, 1, true)
	return err
}

// gcArtifacts garbage-collects migration artifacts from completed/failed migrations
func (e *Executor) gcArtifacts(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	parsed := sqlparser.BuildParsedQuery(sqlSelectUncollectedArtifacts, "_vt")
	r, err := e.execQuery(ctx, parsed.Query)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		artifacts := row["artifacts"].ToString()

		artifactTables := textutil.SplitDelimitedList(artifacts)
		for _, artifactTable := range artifactTables {
			if err := e.gcArtifactTable(ctx, artifactTable); err != nil {
				return err
			}
			log.Infof("Executor.gcArtifacts: renamed away artifact %s", artifactTable)
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
	if _, runningNotByThisProcess, err := e.reviewRunningMigrations(ctx); err != nil {
		log.Error(err)
	} else if err := e.cancelMigrations(ctx, runningNotByThisProcess); err != nil {
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
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationStartedTimestamp, "_vt",
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
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationTimestamp, "_vt", timestampColumn,
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

func (e *Executor) updateMigrationLogPath(ctx context.Context, uuid string, hostname, path string) error {
	logPath := fmt.Sprintf("%s:%s", hostname, path)
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationLogPath, "_vt",
		":log_path",
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"log_path":       sqltypes.StringBindVariable(logPath),
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) updateArtifacts(ctx context.Context, uuid string, artifacts ...string) error {
	bindArtifacts := strings.Join(artifacts, ",")
	parsed := sqlparser.BuildParsedQuery(sqlUpdateArtifacts, "_vt",
		":artifacts",
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"artifacts":      sqltypes.StringBindVariable(bindArtifacts),
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

// updateTabletFailure marks a given migration as "tablet_failed"
func (e *Executor) updateTabletFailure(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateTabletFailure, "_vt",
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
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationStatus, "_vt",
		":migration_status",
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_status": sqltypes.StringBindVariable(string(status)),
		"migration_uuid":   sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) updateMigrationProgress(ctx context.Context, uuid string, progress float64) error {
	if progress <= 0 {
		// progress starts at 0, and can only increase.
		// A value of "0" either means "This is the actual current progress" or "No information"
		// In both cases there's nothing to update
		return nil
	}
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationProgress, "_vt",
		":migration_progress",
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_progress": sqltypes.Float64BindVariable(progress),
		"migration_uuid":     sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, bound)
	return err
}

func (e *Executor) retryMigration(ctx context.Context, whereExpr string) (result *sqltypes.Result, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()
	parsed := sqlparser.BuildParsedQuery(sqlRetryMigration, "_vt", ":tablet", whereExpr)
	bindVars := map[string]*querypb.BindVariable{
		"tablet": sqltypes.StringBindVariable(e.TabletAliasString()),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	result, err = e.execQuery(ctx, bound)
	return result, err
}

// onSchemaMigrationStatus is called when a status is set/changed for a running migration
func (e *Executor) onSchemaMigrationStatus(ctx context.Context, uuid string, status schema.OnlineDDLStatus, dryRun bool, progressPct float64) (err error) {
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

	if !dryRun {
		switch status {
		case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed:
			e.triggerNextCheckInterval()
		}
	}

	return nil
}

// OnSchemaMigrationStatus is called by TabletServer's API, which is invoked by a running gh-ost migration's hooks.
func (e *Executor) OnSchemaMigrationStatus(ctx context.Context, uuidParam, statusParam, dryrunParam, progressParam string) (err error) {
	status := schema.OnlineDDLStatus(statusParam)
	dryRun := (dryrunParam == "true")
	var progressPct float64
	if pct, err := strconv.ParseFloat(progressParam, 32); err == nil {
		progressPct = pct
	}

	return e.onSchemaMigrationStatus(ctx, uuidParam, status, dryRun, progressPct)
}

// VExec is called by a VExec invocation
func (e *Executor) VExec(ctx context.Context, vx *vexec.TabletVExec) (qr *querypb.QueryResult, err error) {
	response := func(result *sqltypes.Result, err error) (*querypb.QueryResult, error) {
		if err != nil {
			return nil, err
		}
		return sqltypes.ResultToProto3(result), nil
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
			return response(e.retryMigration(ctx, sqlparser.String(stmt.Where.Expr)))
		case cancelMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.cancelMigration(ctx, uuid, true))
		case cancelAllMigrationHint:
			uuid, _ := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if uuid != "" {
				return nil, fmt.Errorf("Unexpetced UUID: %s", uuid)
			}
			return response(e.cancelPendingMigrations(ctx))
		default:
			return nil, fmt.Errorf("Unexpected value for migration_status: %v. Supported values are: %s, %s",
				statusVal, retryMigrationHint, cancelMigrationHint)
		}
	default:
		return nil, fmt.Errorf("No handler for this query: %s", vx.Query)
	}
}
