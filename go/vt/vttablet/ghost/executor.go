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
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	GhostExecutorNotWritableTablet error = errors.New("Cannot run gh-ost migration on non-writable tablet")
)

// Writer runs on master tablets and writes heartbeats to the _vt.heartbeat
// table at a regular interval, defined by heartbeat_interval.
type GhostExecutor struct {
	hash string

	env      tabletenv.Env
	errorLog *logutil.ThrottledLogger
	pool     *connpool.Pool

	mu     sync.Mutex
	isOpen bool
}

// NewGhostExecutor creates a new gh-ost executor.
func NewGhostExecutor(env tabletenv.Env) *GhostExecutor {
	return &GhostExecutor{
		hash:     RandomHash(),
		env:      env,
		errorLog: logutil.NewThrottledLogger("GhostExecutor", 60*time.Second),
		pool: connpool.NewPool(env, "GhostExecutorPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}
}

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

func (e *GhostExecutor) Close() {
	e.pool.Close()
}

func (e *GhostExecutor) panicFlagFileName() string {
	return fmt.Sprintf("/tmp/ghost.%s.panic.flag", e.hash)
}

func (e *GhostExecutor) readMySQLVariables(ctx context.Context) (host string, port int, readOnly bool, err error) {
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

// Init runs at tablet startup and last minute initialization of db settings, and
// creates the necessary tables for heartbeat.
func (e *GhostExecutor) Execute(ctx context.Context, target querypb.Target, alias topodatapb.TabletAlias, schema, table, alter string) error {
	if target.TabletType != topodatapb.TabletType_MASTER {
		return GhostExecutorNotWritableTablet
	}
	mysqlHost, mysqlPort, readOnly, err := e.readMySQLVariables(ctx)
	if err != nil {
		e.errorLog.Errorf("Error before running gh-ost: %+v", err)
		return err
	}
	if readOnly {
		err := fmt.Errorf("Error before running gh-ost: MySQL server is read_only")
		e.errorLog.Errorf(err.Error())
		return err
	}
	go func() {
		e.errorLog.Errorf("Will now run gh-ost on: %s:%d", mysqlHost, mysqlPort)
		_, _, err := execCmd(
			"gh-ost",
			[]string{
				fmt.Sprintf(`--host=%s`, mysqlHost),
				fmt.Sprintf(`--port=%d`, mysqlPort),
				`--user=gh-ost`,
				`--password=gh-ost`,
				`--allow-on-master`,
				`--max-load=Threads_running=100`,
				`--critical-load=Threads_running=200`,
				`--critical-load-hibernate-seconds=60`,
				`--approve-renamed-columns`,
				`--verbose`,
				`--exact-rowcount`,
				`--timestamp-old-table`,
				`--initially-drop-ghost-table`,
				`--default-retries=120`,
				fmt.Sprintf(`--database=%s`, schema),
				fmt.Sprintf(`--table=%s`, table),
				fmt.Sprintf(`--alter=%s`, alter),
				fmt.Sprintf(`--panic-flag-file=%s`, e.panicFlagFileName()),
				`--execute`,
			},
			[]string{},
			"/tmp",
			nil,
		)
		e.errorLog.Errorf("+ result: %+v", err)
		if err != nil {
			e.errorLog.Errorf("Error while running gh-ost: %+v", err)
		}
	}()
	return nil
}

func (e *GhostExecutor) Cancel() error {
	file, err := os.OpenFile(e.panicFlagFileName(), os.O_RDONLY|os.O_CREATE, 0666)
	if file != nil {
		defer file.Close()
	}
	return err
}
