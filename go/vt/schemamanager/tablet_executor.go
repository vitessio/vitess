// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// TabletExecutor applies schema changes to all tablets.
type TabletExecutor struct {
	tmClient    tmclient.TabletManagerClient
	topoServer  topo.Server
	tabletInfos []*topo.TabletInfo
	schemaDiffs []*proto.SchemaChangeResult
	isClosed    bool
}

// NewTabletExecutor creates a new TabletExecutor instance
func NewTabletExecutor(
	tmClient tmclient.TabletManagerClient,
	topoServer topo.Server) *TabletExecutor {
	return &TabletExecutor{
		tmClient:   tmClient,
		topoServer: topoServer,
		isClosed:   true,
	}
}

// Open opens a connection to the master for every shard
func (exec *TabletExecutor) Open(ctx context.Context, keyspace string) error {
	if !exec.isClosed {
		return nil
	}
	shardNames, err := exec.topoServer.GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("unable to get shard names for keyspace: %s, error: %v", keyspace, err)
	}
	log.Infof("Keyspace: %v, Shards: %v\n", keyspace, shardNames)
	exec.tabletInfos = make([]*topo.TabletInfo, len(shardNames))
	for i, shardName := range shardNames {
		shardInfo, err := exec.topoServer.GetShard(ctx, keyspace, shardName)
		log.Infof("\tShard: %s, ShardInfo: %v\n", shardName, shardInfo)
		if err != nil {
			return fmt.Errorf("unable to get shard info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		tabletInfo, err := exec.topoServer.GetTablet(ctx, topo.ProtoToTabletAlias(shardInfo.MasterAlias))
		if err != nil {
			return fmt.Errorf("unable to get master tablet info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		exec.tabletInfos[i] = tabletInfo
		log.Infof("\t\tTabletInfo: %+v\n", tabletInfo)
	}

	if len(exec.tabletInfos) == 0 {
		return fmt.Errorf("keyspace: %s does not contain any master tablets", keyspace)
	}
	exec.isClosed = false
	return nil
}

// Validate validates a list of sql statements
func (exec *TabletExecutor) Validate(ctx context.Context, sqls []string) error {
	if exec.isClosed {
		return fmt.Errorf("executor is closed")
	}
	parsedDDLs := make([]*sqlparser.DDL, len(sqls))
	for i, sql := range sqls {
		stat, err := sqlparser.Parse(sql)
		if err != nil {
			return fmt.Errorf("failed to parse sql: %s, got error: %v", sql, err)
		}
		ddl, ok := stat.(*sqlparser.DDL)
		if !ok {
			return fmt.Errorf("schema change works for DDLs only, but get non DDL statement: %s", sql)
		}
		parsedDDLs[i] = ddl
	}
	return exec.detectBigSchemaChanges(ctx, parsedDDLs)
}

// a schema change that satisfies any following condition is considered
// to be a big schema change and will be rejected.
//   1. Alter more than 100,000 rows.
//   2. Change a table with more than 2,000,000 rows (Drops are fine).
func (exec *TabletExecutor) detectBigSchemaChanges(ctx context.Context, parsedDDLs []*sqlparser.DDL) error {
	// exec.tabletInfos is guaranteed to have at least one element;
	// Otherwise, Open should fail and executor should fail.
	masterTabletInfo := exec.tabletInfos[0]
	// get database schema, excluding views.
	dbSchema, err := exec.tmClient.GetSchema(
		ctx, masterTabletInfo, []string{}, []string{}, false)
	if err != nil {
		return fmt.Errorf("unable to get database schema, error: %v", err)
	}
	tableWithCount := make(map[string]uint64, dbSchema.TableDefinitions.Len())
	for _, tableSchema := range dbSchema.TableDefinitions {
		tableWithCount[tableSchema.Name] = tableSchema.RowCount
	}
	for _, ddl := range parsedDDLs {
		if ddl.Action == sqlparser.AST_DROP {
			continue
		}
		tableName := string(ddl.Table)
		if rowCount, ok := tableWithCount[tableName]; ok {
			if rowCount > 100000 && ddl.Action == sqlparser.AST_ALTER {
				return fmt.Errorf(
					"big schema change, ddl: %v alters a table with more than 100 thousand rows", ddl)
			}
			if rowCount > 2000000 {
				return fmt.Errorf(
					"big schema change, ddl: %v changes a table with more than 2 million rows", ddl)
			}
		}
	}
	return nil
}

func (exec *TabletExecutor) preflightSchemaChanges(ctx context.Context, sqls []string) error {
	exec.schemaDiffs = make([]*proto.SchemaChangeResult, len(sqls))
	for i := range sqls {
		schemaDiff, err := exec.tmClient.PreflightSchema(
			ctx, exec.tabletInfos[0], sqls[i])
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
func (exec *TabletExecutor) Execute(ctx context.Context, sqls []string) *ExecuteResult {
	execResult := ExecuteResult{}
	execResult.Sqls = sqls
	if exec.isClosed {
		execResult.ExecutorErr = "executor is closed"
		return &execResult
	}
	startTime := time.Now()
	defer func() { execResult.TotalTimeSpent = time.Since(startTime) }()

	// make sure every schema change introduces a table definition change
	if err := exec.preflightSchemaChanges(ctx, sqls); err != nil {
		execResult.ExecutorErr = err.Error()
		return &execResult
	}

	for index, sql := range sqls {
		execResult.CurSqlIndex = index
		exec.executeOnAllTablets(ctx, &execResult, sql)
		if len(execResult.FailedShards) > 0 {
			break
		}
	}
	return &execResult
}

func (exec *TabletExecutor) executeOnAllTablets(ctx context.Context, execResult *ExecuteResult, sql string) {
	var wg sync.WaitGroup
	numOfMasterTablets := len(exec.tabletInfos)
	wg.Add(numOfMasterTablets)
	errChan := make(chan ShardWithError, numOfMasterTablets)
	successChan := make(chan ShardResult, numOfMasterTablets)
	for i := range exec.tabletInfos {
		go exec.executeOneTablet(ctx, &wg, exec.tabletInfos[i], sql, errChan, successChan)
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
	ctx context.Context,
	wg *sync.WaitGroup,
	tabletInfo *topo.TabletInfo,
	sql string,
	errChan chan ShardWithError,
	successChan chan ShardResult) {
	defer wg.Done()
	result, err := exec.tmClient.ExecuteFetchAsDba(ctx, tabletInfo, sql, 10, false, false, true)
	if err != nil {
		errChan <- ShardWithError{Shard: tabletInfo.Shard, Err: err.Error()}
	} else {
		successChan <- ShardResult{Shard: tabletInfo.Shard, Result: result}
	}
}

// Close clears tablet executor states
func (exec *TabletExecutor) Close() {
	if !exec.isClosed {
		exec.tabletInfos = nil
		exec.isClosed = true
	}
}

var _ Executor = (*TabletExecutor)(nil)
