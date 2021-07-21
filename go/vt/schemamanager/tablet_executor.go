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

package schemamanager

import (
	"fmt"
	"sync"
	"time"

	"context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletExecutor applies schema changes to all tablets.
type TabletExecutor struct {
	requestContext       string
	wr                   *wrangler.Wrangler
	tablets              []*topodatapb.Tablet
	isClosed             bool
	allowBigSchemaChange bool
	keyspace             string
	waitReplicasTimeout  time.Duration
	ddlStrategy          string
	skipPreflight        bool
}

// NewTabletExecutor creates a new TabletExecutor instance
func NewTabletExecutor(requestContext string, wr *wrangler.Wrangler, waitReplicasTimeout time.Duration) *TabletExecutor {
	return &TabletExecutor{
		wr:                   wr,
		isClosed:             true,
		allowBigSchemaChange: false,
		waitReplicasTimeout:  waitReplicasTimeout,
		requestContext:       requestContext,
	}
}

// AllowBigSchemaChange changes TabletExecutor such that big schema changes
// will no longer be rejected.
func (exec *TabletExecutor) AllowBigSchemaChange() {
	exec.allowBigSchemaChange = true
}

// DisallowBigSchemaChange enables the check for big schema changes such that
// TabletExecutor will reject these.
func (exec *TabletExecutor) DisallowBigSchemaChange() {
	exec.allowBigSchemaChange = false
}

// SetDDLStrategy applies ddl_strategy from command line flags
func (exec *TabletExecutor) SetDDLStrategy(ddlStrategy string) error {
	if _, _, err := schema.ParseDDLStrategy(ddlStrategy); err != nil {
		return err
	}
	exec.ddlStrategy = ddlStrategy
	return nil
}

// SkipPreflight disables preflight checks
func (exec *TabletExecutor) SkipPreflight() {
	exec.skipPreflight = true
}

// Open opens a connection to the master for every shard.
func (exec *TabletExecutor) Open(ctx context.Context, keyspace string) error {
	if !exec.isClosed {
		return nil
	}
	exec.keyspace = keyspace
	shardNames, err := exec.wr.TopoServer().GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("unable to get shard names for keyspace: %s, error: %v", keyspace, err)
	}
	exec.tablets = make([]*topodatapb.Tablet, len(shardNames))
	for i, shardName := range shardNames {
		shardInfo, err := exec.wr.TopoServer().GetShard(ctx, keyspace, shardName)
		if err != nil {
			return fmt.Errorf("unable to get shard info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		if !shardInfo.HasMaster() {
			return fmt.Errorf("shard: %s does not have a master", shardName)
		}
		tabletInfo, err := exec.wr.TopoServer().GetTablet(ctx, shardInfo.MasterAlias)
		if err != nil {
			return fmt.Errorf("unable to get master tablet info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		exec.tablets[i] = tabletInfo.Tablet
	}

	if len(exec.tablets) == 0 {
		return fmt.Errorf("keyspace: %s does not contain any master tablets", keyspace)
	}
	exec.isClosed = false
	return nil
}

// Validate validates a list of sql statements.
func (exec *TabletExecutor) Validate(ctx context.Context, sqls []string) error {
	if exec.isClosed {
		return fmt.Errorf("executor is closed")
	}

	// We ignore DATABASE-level DDLs here because detectBigSchemaChanges doesn't
	// look at them anyway.
	parsedDDLs, _, err := exec.parseDDLs(sqls)
	if err != nil {
		return err
	}

	bigSchemaChange, err := exec.detectBigSchemaChanges(ctx, parsedDDLs)
	if bigSchemaChange && exec.allowBigSchemaChange {
		exec.wr.Logger().Warningf("Processing big schema change. This may cause visible MySQL downtime.")
		return nil
	}
	return err
}

func (exec *TabletExecutor) parseDDLs(sqls []string) ([]sqlparser.DDLStatement, []sqlparser.DBDDLStatement, error) {
	parsedDDLs := make([]sqlparser.DDLStatement, 0)
	parsedDBDDLs := make([]sqlparser.DBDDLStatement, 0)
	for _, sql := range sqls {
		stat, err := sqlparser.Parse(sql)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse sql: %s, got error: %v", sql, err)
		}
		switch ddl := stat.(type) {
		case sqlparser.DDLStatement:
			parsedDDLs = append(parsedDDLs, ddl)
		case sqlparser.DBDDLStatement:
			parsedDBDDLs = append(parsedDBDDLs, ddl)
		default:
			if len(exec.tablets) != 1 {
				return nil, nil, fmt.Errorf("non-ddl statements can only be executed for single shard keyspaces: %s", sql)
			}
		}
	}
	return parsedDDLs, parsedDBDDLs, nil
}

// IsOnlineSchemaDDL returns true if we expect to run a online schema change DDL
func (exec *TabletExecutor) isOnlineSchemaDDL(ddlStmt sqlparser.DDLStatement) (isOnline bool, strategy schema.DDLStrategy, options string) {
	switch ddlStmt.GetAction() {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction, sqlparser.AlterDDLAction:
	default:
		return false, strategy, options
	}
	strategy, options, err := schema.ParseDDLStrategy(exec.ddlStrategy)
	if err != nil {
		return false, strategy, options
	}
	if strategy.IsDirect() {
		return false, strategy, options
	}
	return true, strategy, options
}

// a schema change that satisfies any following condition is considered
// to be a big schema change and will be rejected.
//   1. Alter more than 100,000 rows.
//   2. Change a table with more than 2,000,000 rows (Drops are fine).
func (exec *TabletExecutor) detectBigSchemaChanges(ctx context.Context, parsedDDLs []sqlparser.DDLStatement) (bool, error) {
	// exec.tablets is guaranteed to have at least one element;
	// Otherwise, Open should fail and executor should fail.
	masterTabletInfo := exec.tablets[0]
	// get database schema, excluding views.
	dbSchema, err := exec.wr.TabletManagerClient().GetSchema(
		ctx, masterTabletInfo, []string{}, []string{}, false)
	if err != nil {
		return false, fmt.Errorf("unable to get database schema, error: %v", err)
	}
	tableWithCount := make(map[string]uint64, len(dbSchema.TableDefinitions))
	for _, tableSchema := range dbSchema.TableDefinitions {
		tableWithCount[tableSchema.Name] = tableSchema.RowCount
	}
	for _, ddl := range parsedDDLs {
		if isOnline, _, _ := exec.isOnlineSchemaDDL(ddl); isOnline {
			// Since this is an online schema change, there is no need to worry about big changes
			continue
		}
		switch ddl.GetAction() {
		case sqlparser.DropDDLAction, sqlparser.CreateDDLAction, sqlparser.TruncateDDLAction, sqlparser.RenameDDLAction:
			continue
		}
		tableName := ddl.GetTable().Name.String()
		if rowCount, ok := tableWithCount[tableName]; ok {
			if rowCount > 100000 && ddl.GetAction() == sqlparser.AlterDDLAction {
				return true, fmt.Errorf(
					"big schema change detected. Disable check with -allow_long_unavailability. ddl: %s alters a table with more than 100 thousand rows", sqlparser.String(ddl))
			}
			if rowCount > 2000000 {
				return true, fmt.Errorf(
					"big schema change detected. Disable check with -allow_long_unavailability. ddl: %s changes a table with more than 2 million rows", sqlparser.String(ddl))
			}
		}
	}
	return false, nil
}

func (exec *TabletExecutor) preflightSchemaChanges(ctx context.Context, sqls []string) error {
	if exec.skipPreflight {
		return nil
	}
	_, err := exec.wr.TabletManagerClient().PreflightSchema(ctx, exec.tablets[0], sqls)
	return err
}

// executeSQL executes a single SQL statement either as online DDL or synchronously on all tablets.
// In online DDL case, the query may be exploded into multiple queries during
func (exec *TabletExecutor) executeSQL(ctx context.Context, sql string, execResult *ExecuteResult) error {
	stat, err := sqlparser.Parse(sql)
	if err != nil {
		return err
	}
	switch stat := stat.(type) {
	case sqlparser.DDLStatement:
		if isOnlineDDL, strategy, options := exec.isOnlineSchemaDDL(stat); isOnlineDDL {
			exec.wr.Logger().Infof("Received DDL request. strategy=%+v", strategy)
			normalizedQueries, err := schema.NormalizeOnlineDDL(sql)
			if err != nil {
				return err
			}
			for _, normalized := range normalizedQueries {
				exec.executeOnlineDDL(ctx, execResult, normalized.SQL, normalized.TableName.Name.String(), strategy, options)
			}
			return nil
		}
	}
	exec.wr.Logger().Infof("Received DDL request. strategy=%+v", schema.DDLStrategyDirect)
	exec.executeOnAllTablets(ctx, execResult, sql)
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

	// Lock the keyspace so our schema change doesn't overlap with other
	// keyspace-wide operations like resharding migrations.
	ctx, unlock, lockErr := exec.wr.TopoServer().LockKeyspace(ctx, exec.keyspace, "ApplySchemaKeyspace")
	if lockErr != nil {
		execResult.ExecutorErr = lockErr.Error()
		return &execResult
	}
	defer func() {
		// This is complicated because execResult.ExecutorErr
		// is not of type error.
		var unlockErr error
		unlock(&unlockErr)
		if execResult.ExecutorErr == "" && unlockErr != nil {
			execResult.ExecutorErr = unlockErr.Error()
		}
	}()

	// Make sure the schema changes introduce a table definition change.
	if err := exec.preflightSchemaChanges(ctx, sqls); err != nil {
		execResult.ExecutorErr = err.Error()
		return &execResult
	}

	for index, sql := range sqls {
		execResult.CurSQLIndex = index
		if err := exec.executeSQL(ctx, sql, &execResult); err != nil {
			execResult.ExecutorErr = err.Error()
			return &execResult
		}
		if len(execResult.FailedShards) > 0 {
			break
		}
	}
	return &execResult
}

// executeOnlineDDL submits an online DDL request; this runs on topo, not on tablets, and is a quick operation.
func (exec *TabletExecutor) executeOnlineDDL(
	ctx context.Context, execResult *ExecuteResult, sql string,
	tableName string, strategy schema.DDLStrategy, options string,
) {
	if strategy.IsDirect() {
		execResult.ExecutorErr = "Not an online DDL strategy"
		return
	}
	onlineDDL, err := schema.NewOnlineDDL(exec.keyspace, tableName, sql, strategy, options, exec.requestContext)
	if err != nil {
		execResult.ExecutorErr = err.Error()
		return
	}
	conn, err := exec.wr.TopoServer().ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		execResult.ExecutorErr = fmt.Sprintf("online DDL ConnForCell error:%s", err.Error())
		return
	}
	err = onlineDDL.WriteTopo(ctx, conn, schema.MigrationRequestsPath())
	if err != nil {
		execResult.ExecutorErr = err.Error()
	}
	exec.wr.Logger().Infof("UUID=%+v", onlineDDL.UUID)
	exec.wr.Logger().Printf("%s\n", onlineDDL.UUID)
}

// executeOnAllTablets runs a query on all tablets, synchronously. This can be a long running operation.
func (exec *TabletExecutor) executeOnAllTablets(
	ctx context.Context, execResult *ExecuteResult, sql string,
) {
	var wg sync.WaitGroup
	numOfMasterTablets := len(exec.tablets)
	wg.Add(numOfMasterTablets)
	errChan := make(chan ShardWithError, numOfMasterTablets)
	successChan := make(chan ShardResult, numOfMasterTablets)
	for _, tablet := range exec.tablets {
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			exec.executeOneTablet(ctx, tablet, sql, errChan, successChan)
		}(tablet)
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

	if len(execResult.FailedShards) > 0 {
		return
	}

	// If all shards succeeded, wait (up to waitReplicasTimeout) for replicas to
	// execute the schema change via replication. This is best-effort, meaning
	// we still return overall success if the timeout expires.
	concurrency := sync2.NewSemaphore(10, 0)
	reloadCtx, cancel := context.WithTimeout(ctx, exec.waitReplicasTimeout)
	defer cancel()
	for _, result := range execResult.SuccessShards {
		wg.Add(1)
		go func(result ShardResult) {
			defer wg.Done()
			exec.wr.ReloadSchemaShard(reloadCtx, exec.keyspace, result.Shard, result.Position, concurrency, false /* includeMaster */)
		}(result)
	}
	wg.Wait()
}

func (exec *TabletExecutor) executeOneTablet(
	ctx context.Context,
	tablet *topodatapb.Tablet,
	sql string,
	errChan chan ShardWithError,
	successChan chan ShardResult) {
	result, err := exec.wr.TabletManagerClient().ExecuteFetchAsDba(ctx, tablet, false, []byte(sql), 10, false, true)
	if err != nil {
		errChan <- ShardWithError{Shard: tablet.Shard, Err: err.Error()}
		return
	}
	// Get a replication position that's guaranteed to be after the schema change
	// was applied on the master.
	pos, err := exec.wr.TabletManagerClient().MasterPosition(ctx, tablet)
	if err != nil {
		errChan <- ShardWithError{
			Shard: tablet.Shard,
			Err:   fmt.Sprintf("couldn't get replication position after applying schema change on master: %v", err),
		}
		return
	}
	successChan <- ShardResult{
		Shard:    tablet.Shard,
		Result:   result,
		Position: pos,
	}
}

// Close clears tablet executor states
func (exec *TabletExecutor) Close() {
	if !exec.isClosed {
		exec.tablets = nil
		exec.isClosed = true
	}
}

var _ Executor = (*TabletExecutor)(nil)
