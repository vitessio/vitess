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
	"fmt"
	"os"
	"sync"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// ErrExecutorNotWritableTablet error is thrown when executor is asked to run gh-ost on a read-only server
	ErrExecutorNotWritableTablet = errors.New("Cannot run gh-ost migration on non-writable tablet")
)

// Executor wraps and manages the execution of a gh-ost migration.
type Executor struct {
	hash string

	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server

	keyspace string

	mu     sync.Mutex
	ticks  *timer.Timer
	isOpen bool
}

var (
	migrationCheckInterval = time.Second * 10
)

// NewExecutor creates a new gh-ost executor.
func NewExecutor(env tabletenv.Env, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType) *Executor {
	return &Executor{
		hash: ShortRandomHash(),
		env:  env,
		pool: connpool.NewPool(env, "ExecutorPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		ticks:          timer.NewTimer(migrationCheckInterval),
	}
}

func (e *Executor) initSchema(ctx context.Context) error {
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

// InitDBConfig initializes keysapce
func (e *Executor) InitDBConfig(keyspace string) {
	e.keyspace = keyspace
}

// Open opens database pool and initializes the schema
func (e *Executor) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isOpen {
		return nil
	}
	e.pool.Open(e.env.Config().DB.AppWithDB(), e.env.Config().DB.DbaWithDB(), e.env.Config().DB.AppDebugWithDB())
	e.ticks.Start(e.checkMigrations)
	e.isOpen = true

	return nil
}

// Close frees resources
func (e *Executor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.isOpen {
		return
	}

	e.ticks.Stop()
	e.pool.Close()
	e.isOpen = false
}

func (e *Executor) ghostPanicFlagFileName() string {
	return fmt.Sprintf("/tmp/ghost.%s.panic.flag", e.hash)
}

// readMySQLVariables contacts the backend MySQL server to read some of its configuration
func (e *Executor) readMySQLVariables(ctx context.Context) (host string, port int, readOnly bool, err error) {
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
func (e *Executor) Execute(ctx context.Context, target querypb.Target, alias topodatapb.TabletAlias, schema, table, alter string) error {
	if target.TabletType != topodatapb.TabletType_MASTER {
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
				fmt.Sprintf(`--panic-flag-file=%s`, e.ghostPanicFlagFileName()),
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
func (e *Executor) Cancel() error {
	file, err := os.OpenFile(e.ghostPanicFlagFileName(), os.O_RDONLY|os.O_CREATE, 0666)
	if file != nil {
		defer file.Close()
	}
	return err
}

func (e *Executor) checkMigrations() {
	if e.tabletTypeFunc() != topodatapb.TabletType_MASTER {
		return
	}
	if e.keyspace == "" {
		log.Errorf("Executor.checkMigrations(): empty keyspace")
		return
	}
	ctx := context.Background()
	e.initSchema(ctx)

	conn, err := e.ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		log.Errorf("Executor.checkMigrations ConnForCell error: %s", err.Error())
		return
	}

	dirPath := fmt.Sprintf("schema-migration/%s", e.keyspace)
	fmt.Printf("=============== listing dirPath=%s\n", dirPath)
	entries, err := conn.ListDir(ctx, dirPath, false)
	if err != nil {
		log.Errorf("Executor.checkMigrations listDir error: %s", err.Error())
		return
	}
	fmt.Printf("=============== ListDir=%+v\n", entries)
	for _, entry := range entries {
		entryPath := fmt.Sprintf("%s/%s/request", dirPath, entry.Name)
		bytes, _, err := conn.Get(ctx, entryPath)
		if err != nil {
			log.Errorf("Executor.checkMigrations Get %s error: %s", entryPath, err.Error())
			continue
		}
		fmt.Printf("===GET %s: %s\n", entryPath, string(bytes))
	}
}
