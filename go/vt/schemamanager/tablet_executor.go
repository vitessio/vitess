// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// TabletExecutor applies schema changes to all tablets.
type TabletExecutor struct {
	keyspace    string
	tmClient    tmclient.TabletManagerClient
	topoServer  topo.Server
	tabletInfos []*topo.TabletInfo
	schemaDiffs []*proto.SchemaChangeResult
	isClosed    bool
}

// NewTabletExecutor creates a new TabletExecutor instance
func NewTabletExecutor(
	tmClient tmclient.TabletManagerClient,
	topoServer topo.Server,
	keyspace string) *TabletExecutor {
	return &TabletExecutor{
		keyspace:   keyspace,
		tmClient:   tmClient,
		topoServer: topoServer,
		isClosed:   true,
	}
}

// Open opens a connection to the master for every shard
func (exec *TabletExecutor) Open() error {
	if !exec.isClosed {
		return nil
	}
	shardNames, err := exec.topoServer.GetShardNames(exec.keyspace)
	if err != nil {
		return fmt.Errorf("unable to get shard names for keyspace: %s, error: %v", exec.keyspace, err)
	}
	log.Infof("Keyspace: %v, Shards: %v\n", exec.keyspace, shardNames)
	exec.tabletInfos = make([]*topo.TabletInfo, len(shardNames))
	for i, shardName := range shardNames {
		shardInfo, err := exec.topoServer.GetShard(exec.keyspace, shardName)
		log.Infof("\tShard: %s, ShardInfo: %v\n", shardName, shardInfo)
		if err != nil {
			return fmt.Errorf("unable to get shard info, keyspace: %s, shard: %s, error: %v", exec.keyspace, shardName, err)
		}
		tabletInfo, err := exec.topoServer.GetTablet(shardInfo.MasterAlias)
		if err != nil {
			return fmt.Errorf("unable to get master tablet info, keyspace: %s, shard: %s, error: %v", exec.keyspace, shardName, err)
		}
		exec.tabletInfos[i] = tabletInfo
		log.Infof("\t\tTabletInfo: %+v\n", tabletInfo)
	}
	exec.isClosed = false
	return nil
}

// Validate validates a list of sql statements
func (exec *TabletExecutor) Validate(sqls []string) error {
	for _, sql := range sqls {
		stat, err := sqlparser.Parse(sql)
		if err != nil {
			return err
		}
		_, ok := stat.(*sqlparser.DDL)
		if !ok {
			return fmt.Errorf("schema change works for DDLs only, but get non DDL statement: %s", sql)
		}
	}
	return nil
}

func (exec *TabletExecutor) preflightSchemaChanges(sqls []string) error {
	if len(exec.tabletInfos) == 0 {
		return nil
	}
	exec.schemaDiffs = make([]*proto.SchemaChangeResult, len(sqls))
	for i := range sqls {
		schemaDiff, err := exec.tmClient.PreflightSchema(
			context.Background(), exec.tabletInfos[0], sqls[i])
		if err != nil {
			return err
		}
		exec.schemaDiffs[i] = schemaDiff
		diffs := proto.DiffSchemaToArray(
			"BeforeSchema",
			exec.schemaDiffs[i].BeforeSchema,
			"AfterSchema",
			exec.schemaDiffs[i].AfterSchema)
		if len(diffs) == 0 {
			return fmt.Errorf("Schema change: '%s' does not introduce any table definition change.", sqls[i])
		}
	}
	return nil
}

// Execute applies schema changes
func (exec *TabletExecutor) Execute(sqls []string) *ExecuteResult {
	execResult := ExecuteResult{}
	execResult.Sqls = sqls
	if exec.isClosed {
		execResult.ExecutorErr = "executor is closed"
		return &execResult
	}

	// make sure every schema change introduces a table definition change
	if err := exec.preflightSchemaChanges(sqls); err != nil {
		execResult.ExecutorErr = err.Error()
		return &execResult
	}

	for index, sql := range sqls {
		execResult.CurSqlIndex = index
		exec.executeOnAllTablets(&execResult, sql)
		if len(execResult.FailedShards) > 0 {
			break
		}
	}
	return &execResult
}

func (exec *TabletExecutor) executeOnAllTablets(execResult *ExecuteResult, sql string) {
	var wg sync.WaitGroup
	numOfMasterTablets := len(exec.tabletInfos)
	wg.Add(numOfMasterTablets)
	errChan := make(chan ShardWithError, numOfMasterTablets)
	successChan := make(chan ShardResult, numOfMasterTablets)
	for i := range exec.tabletInfos {
		go exec.executeOneTablet(&wg, exec.tabletInfos[i], sql, errChan, successChan)
	}
	wg.Wait()
	close(errChan)
	close(successChan)
	execResult.FailedShards = make([]ShardWithError, 0, len(errChan))
	execResult.SuccessShards = make([]ShardResult, 0, len(successChan))
	for e := range errChan {
		execResult.FailedShards = append(execResult.FailedShards, e)
	}
	for r := range successChan {
		execResult.SuccessShards = append(execResult.SuccessShards, r)
	}
}

func (exec *TabletExecutor) executeOneTablet(
	wg *sync.WaitGroup,
	tabletInfo *topo.TabletInfo,
	sql string,
	errChan chan ShardWithError,
	successChan chan ShardResult) {
	defer wg.Done()
	ctx := context.Background()
	result, err := exec.tmClient.ExecuteFetchAsDba(ctx, tabletInfo, sql, 10, false, false, true)
	if err != nil {
		errChan <- ShardWithError{Shard: tabletInfo.Shard, Err: err.Error()}
	} else {
		successChan <- ShardResult{Shard: tabletInfo.Shard, Result: result}
	}
}

// Close clears tablet executor states
func (exec *TabletExecutor) Close() error {
	if !exec.isClosed {
		exec.tabletInfos = nil
		exec.isClosed = true
	}
	return nil
}

var _ Executor = (*TabletExecutor)(nil)
