// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	"golang.org/x/net/context"
)

// VtGateExecutor applies schema changes via VtGate
type VtGateExecutor struct {
	keyspace   string
	conn       vtgateconn.VTGateConn
	vtGateAddr string
	timeout    time.Duration
	isClosed   bool
}

// NewVtGateExecutor creates a new VtGateExecutor instance
func NewVtGateExecutor(
	keyspace string,
	addr string,
	conn vtgateconn.VTGateConn,
	timeout time.Duration) *VtGateExecutor {
	return &VtGateExecutor{
		keyspace:   keyspace,
		vtGateAddr: addr,
		conn:       conn,
		timeout:    timeout,
		isClosed:   true,
	}
}

// Open opens a connection to VtGate
func (exec *VtGateExecutor) Open() error {
	exec.isClosed = false
	return nil
}

// Validate validates a list of sql statements
func (exec *VtGateExecutor) Validate(sqls []string) error {
	for _, sql := range sqls {
		if _, err := sqlparser.Parse(sql); err != nil {
			return err
		}
	}
	return nil
}

// Execute applies schema changes through VtGate
func (exec *VtGateExecutor) Execute(sqls []string, shards []string) *ExecuteResult {
	execResult := ExecuteResult{}
	execResult.Sqls = sqls
	if exec.isClosed {
		execResult.ExecutorErr = "executor is closed"
		return &execResult
	}
	for index, sql := range sqls {
		execResult.CurSqlIndex = index
		stat, err := sqlparser.Parse(sql)
		if err != nil {
			execResult.ExecutorErr = err.Error()
			return &execResult
		}
		_, ok := stat.(*sqlparser.DDL)
		if !ok {
			exec.execute(&execResult, sql, shards, true)
		} else {
			exec.execute(&execResult, sql, shards, false)
		}
		if len(execResult.FailedShards) > 0 {
			break
		}
	}
	return &execResult
}

func (exec *VtGateExecutor) execute(execResult *ExecuteResult, sql string, shards []string, enableTx bool) {
	var wg sync.WaitGroup
	wg.Add(len(shards))
	errChan := make(chan ShardWithError, len(shards))
	resultChan := make(chan ShardResult, len(shards))
	for _, s := range shards {
		go func(keyspace string, shard string) {
			defer wg.Done()
			ctx := context.Background()
			if enableTx {
				exec.executeTx(ctx, sql, keyspace, shard, errChan, resultChan)
			} else {
				queryResult, err := exec.conn.ExecuteShard(ctx, sql, keyspace, []string{shard}, nil, topo.TYPE_MASTER)
				if err != nil {
					errChan <- ShardWithError{Shard: shard, Err: err.Error()}
				} else {
					resultChan <- ShardResult{Shard: shard, Result: queryResult}
				}
			}
		}(exec.keyspace, s)
	}
	wg.Wait()
	close(errChan)
	close(resultChan)
	execResult.FailedShards = make([]ShardWithError, 0, len(errChan))
	execResult.SuccessShards = make([]ShardResult, 0, len(resultChan))
	for e := range errChan {
		execResult.FailedShards = append(execResult.FailedShards, e)
	}
	for r := range resultChan {
		execResult.SuccessShards = append(execResult.SuccessShards, r)
	}
}

func (exec *VtGateExecutor) executeTx(ctx context.Context, sql string, keyspace string, shard string, errChan chan ShardWithError, resultChan chan ShardResult) {
	tx, err := exec.conn.Begin(ctx)
	if err != nil {
		errChan <- ShardWithError{Shard: shard, Err: err.Error()}
		return
	}
	queryResult, err := tx.ExecuteShard(ctx, sql, keyspace, []string{shard}, nil, topo.TYPE_MASTER)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		errChan <- ShardWithError{Shard: shard, Err: err.Error()}
		err = tx.Rollback(ctx)
		if err != nil {
			log.Errorf("failed to rollback transaction: %s on shard %s, error: %v", sql, shard, err)
		}
	} else {
		resultChan <- ShardResult{Shard: shard, Result: queryResult}
	}

}

// Close closes VtGate connection
func (exec *VtGateExecutor) Close() error {
	if !exec.isClosed {
		exec.conn.Close()
		exec.isClosed = true
	}
	return nil
}

var _ Executor = (*VtGateExecutor)(nil)
