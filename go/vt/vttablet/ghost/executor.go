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

package ghost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/withddl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// ErrGhostExecutorNotWritableTablet error is thrown when executor is asked to run gh-ost on a read-only server
	ErrGhostExecutorNotWritableTablet = errors.New("Cannot run gh-ost migration on non-writable tablet")
)

const (
	sqlCreateSidecarDB             = "create database if not exists %s"
	sqlCreateSchemaMigrationsTable = `CREATE TABLE IF NOT EXISTS %s.schema_migrations (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		migration_uuid varchar(64) NOT NULL,
		keyspace varchar(256) NOT NULL,
		shard varchar(256) NOT NULL,
		mysql_table varchar(128) NOT NULL,
		migration_statement text NOT NULL,
		strategy varchar(128) NOT NULL,
		added_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ready_timestamp timestamp NULL DEFAULT NULL,
		assigned_timestamp timestamp NULL DEFAULT NULL,
		started_timestamp timestamp NULL DEFAULT NULL,
		liveness_timestamp timestamp NULL DEFAULT NULL,
		completed_timestamp timestamp NULL DEFAULT NULL,
		migration_status varchar(128) NOT NULL,
		PRIMARY KEY (id),
		KEY uuid_idx (migration_uuid),
		KEY keyspace_shard_idx (keyspace,shard),
		KEY status_idx (migration_status, liveness_timestamp)
	) engine=InnoDB DEFAULT CHARSET=utf8mb4`
	sqlValidationQuery = `select 1 from schema_migrations limit 1`
)

var withDDL = withddl.New([]string{
	fmt.Sprintf(sqlCreateSidecarDB, "_vt"),
	fmt.Sprintf(sqlCreateSchemaMigrationsTable, "_vt"),
})

// GhostExecutor wraps and manages the execution of a gh-ost migration.
type GhostExecutor struct {
	hash string

	env  tabletenv.Env
	pool *connpool.Pool

	mu     sync.Mutex
	isOpen bool
}

// NewGhostExecutor creates a new gh-ost executor.
func NewGhostExecutor(env tabletenv.Env) *GhostExecutor {
	return &GhostExecutor{
		hash: ShortRandomHash(),
		env:  env,
		pool: connpool.NewPool(env, "GhostExecutorPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}
}

func (e *GhostExecutor) initSchema(ctx context.Context) error {
	defer e.env.LogError()

	conn, err := e.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	_, err = withDDL.Exec(ctx, sqlValidationQuery, conn.Exec)
	if err != nil {
		return err
	}
	return nil
}

// Open opens database pool and initializes the schema
func (e *GhostExecutor) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isOpen {
		return nil
	}
	e.pool.Open(e.env.Config().DB.AppWithDB(), e.env.Config().DB.DbaWithDB(), e.env.Config().DB.AppDebugWithDB())
	e.isOpen = true

	return nil
}

// Close frees resources
func (e *GhostExecutor) Close() {
	e.pool.Close()
}

func (e *GhostExecutor) panicFlagFileName() string {
	return fmt.Sprintf("/tmp/ghost.%s.panic.flag", e.hash)
}

// readMySQLVariables contacts the backend MySQL server to read some of its configuration
func (e *GhostExecutor) readMySQLVariables(ctx context.Context) (host string, port int, readOnly bool, err error) {
	e.initSchema(ctx)
	conn, err := e.pool.Get(ctx)
	if err != nil {
		return host, port, readOnly, err
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, "select @@global.hostname, @@global.port, @@global.read_only", 1, false)
	if err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read MySQL variables: %v", err)
	}
	if len(tm.Rows) != 1 {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL variables: %+v", tm.Rows)
	}
	host = tm.Rows[0][0].ToString()
	if p, err := evalengine.ToInt64(tm.Rows[0][1]); err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.port %v: %v", tm, err)
	} else {
		port = int(p)
	}
	if rd, err := evalengine.ToInt64(tm.Rows[0][2]); err != nil {
		return host, port, readOnly, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.read_only %v: %v", tm, err)
	} else {
		readOnly = (rd != 0)
	}
	return host, port, readOnly, nil
}

// Execute validates and runs a gh-ost process.
// Validation included testing the backend MySQL server and the gh-ost binray itself
// Execution runs first a dry run, then an actual migration
func (e *GhostExecutor) Execute(ctx context.Context, target querypb.Target, alias topodatapb.TabletAlias, schema, table, alter string) error {
	if target.TabletType != topodatapb.TabletType_MASTER {
		return ErrGhostExecutorNotWritableTablet
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
	tempDir, err := createTempDir()
	if err != nil {
		log.Errorf("Error creating temporary directory: %+v", err)
		return err
	}
	wrapperScriptContent := fmt.Sprintf(`#!/bin/bash
ghost_log_path="%s"
ghost_log_file=gh-ost.log

mkdir -p "$ghost_log_path"

echo "executing: gh-ost" "$@" > "$ghost_log_path/$ghost_log_file.exec"
gh-ost "$@" > "$ghost_log_path/$ghost_log_file" 2>&1
	`, tempDir,
	)
	wrapperScriptFileName, err := createTempFile(tempDir, "gh-ost-wrapper.sh", wrapperScriptContent)
	if err != nil {
		log.Errorf("Error creating wrapper script: %+v", err)
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

	runGhost := func(execute bool) error {
		// TODO[(shlomi, the code below assumes user+password are gh-ost:gh-ost)]: externalize credentials before submitting the PR

		_, err := execCmd(
			"bash",
			[]string{
				wrapperScriptFileName,
				fmt.Sprintf(`--host=%s`, mysqlHost),
				fmt.Sprintf(`--port=%d`, mysqlPort),
				`--user=gh-ost`,
				`--password=gh-ost`,
				`--allow-on-master`,
				`--max-load=Threads_running=100`,
				`--critical-load=Threads_running=200`,
				`--critical-load-hibernate-seconds=60`,
				`--approve-renamed-columns`,
				`--debug`,
				`--exact-rowcount`,
				`--timestamp-old-table`,
				`--initially-drop-ghost-table`,
				`--default-retries=120`,
				fmt.Sprintf(`--hooks-hint=%s`, e.hash),
				fmt.Sprintf(`--database=%s`, schema),
				fmt.Sprintf(`--table=%s`, table),
				fmt.Sprintf(`--alter=%s`, alter),
				fmt.Sprintf(`--panic-flag-file=%s`, e.panicFlagFileName()),
				fmt.Sprintf(`--execute=%t`, execute),
			},
			os.Environ(),
			"/tmp",
			nil,
			nil,
		)
		return err
	}

	go func() error {
		log.Infof("Will now dry-run gh-ost on: %s:%d", mysqlHost, mysqlPort)
		if err := runGhost(false); err != nil {
			log.Errorf("Error executing gh-ost dry run: %+v", err)
			return err
		}
		log.Infof("+ OK")

		log.Infof("Will now run gh-ost on: %s:%d", mysqlHost, mysqlPort)
		startedMigrations.Add(1)
		if err := runGhost(true); err != nil {
			failedMigrations.Add(1)
			log.Errorf("Error running gh-ost: %+v", err)
			return err
		}
		successfulMigrations.Add(1)
		log.Infof("+ OK")
		return nil
	}()
	return nil
}

// Cancel attempts to abort a running migration by touching the panic flag file
func (e *GhostExecutor) Cancel() error {
	file, err := os.OpenFile(e.panicFlagFileName(), os.O_RDONLY|os.O_CREATE, 0666)
	if file != nil {
		defer file.Close()
	}
	return err
}
