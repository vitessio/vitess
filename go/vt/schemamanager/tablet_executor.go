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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// TabletExecutor applies schema changes to all tablets.
type TabletExecutor struct {
	migrationContext    string
	ts                  *topo.Server
	tmc                 tmclient.TabletManagerClient
	logger              logutil.Logger
	tablets             []*topodatapb.Tablet
	isClosed            bool
	keyspace            string
	waitReplicasTimeout time.Duration
	ddlStrategySetting  *schema.DDLStrategySetting
	uuids               []string
	batchSize           int64
	parser              *sqlparser.Parser
}

// NewTabletExecutor creates a new TabletExecutor instance
func NewTabletExecutor(migrationContext string, ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger, waitReplicasTimeout time.Duration, batchSize int64, parser *sqlparser.Parser) *TabletExecutor {
	return &TabletExecutor{
		ts:                  ts,
		tmc:                 tmc,
		logger:              logger,
		isClosed:            true,
		waitReplicasTimeout: waitReplicasTimeout,
		migrationContext:    migrationContext,
		batchSize:           batchSize,
		parser:              parser,
	}
}

// SetDDLStrategy applies ddl_strategy from command line flags
func (exec *TabletExecutor) SetDDLStrategy(ddlStrategy string) error {
	ddlStrategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	if err != nil {
		return err
	}
	exec.ddlStrategySetting = ddlStrategySetting
	return nil
}

// SetUUIDList sets a (possibly empty) list of provided UUIDs for schema migrations
func (exec *TabletExecutor) SetUUIDList(uuids []string) error {
	uuidsMap := map[string]bool{}
	for _, uuid := range uuids {
		if !schema.IsOnlineDDLUUID(uuid) {
			return fmt.Errorf("Not a valid UUID: %s", uuid)
		}
		uuidsMap[uuid] = true
	}
	if len(uuidsMap) != len(uuids) {
		return fmt.Errorf("UUID values must be unique")
	}
	exec.uuids = uuids
	return nil
}

// hasProvidedUUIDs returns true when UUIDs were provided
func (exec *TabletExecutor) hasProvidedUUIDs() bool {
	return len(exec.uuids) != 0
}

// Open opens a connection to the primary for every shard.
func (exec *TabletExecutor) Open(ctx context.Context, keyspace string) error {
	if !exec.isClosed {
		return nil
	}
	exec.keyspace = keyspace
	shards, err := exec.ts.FindAllShardsInKeyspace(ctx, keyspace, nil)
	if err != nil {
		return fmt.Errorf("unable to get shards for keyspace: %s, error: %v", keyspace, err)
	}
	exec.tablets = make([]*topodatapb.Tablet, 0, len(shards))
	for shardName, shardInfo := range shards {
		if !shardInfo.HasPrimary() {
			return fmt.Errorf("shard: %s does not have a primary", shardName)
		}
		tabletInfo, err := exec.ts.GetTablet(ctx, shardInfo.PrimaryAlias)
		if err != nil {
			return fmt.Errorf("unable to get primary tablet info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		exec.tablets = append(exec.tablets, tabletInfo.Tablet)
	}

	if len(exec.tablets) == 0 {
		return fmt.Errorf("keyspace: %s does not contain any primary tablets", keyspace)
	}
	exec.isClosed = false
	return nil
}

// Validate validates a list of sql statements.
func (exec *TabletExecutor) Validate(ctx context.Context, sqls []string) error {
	if exec.isClosed {
		return fmt.Errorf("executor is closed")
	}
	if err := exec.parseDDLs(sqls); err != nil {
		return err
	}

	return nil
}

func (exec *TabletExecutor) parseDDLs(sqls []string) error {
	for _, sql := range sqls {
		stmt, err := exec.parser.Parse(sql)
		if err != nil {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "failed to parse sql: %s, got error: %v", sql, err)
		}
		switch stmt.(type) {
		case sqlparser.DDLStatement:
		case sqlparser.DBDDLStatement:
		case *sqlparser.RevertMigration:
		case *sqlparser.AlterMigration:
		default:
			if len(exec.tablets) != 1 {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "non-ddl statements can only be executed for single shard keyspaces: %s", sql)
			}
		}
	}
	return nil
}

// isDirectStrategy returns 'true' when the ddl_strategy configuration implies 'direct'
func (exec *TabletExecutor) isDirectStrategy() (isDirect bool) {
	if exec.ddlStrategySetting == nil {
		return true
	}
	if exec.ddlStrategySetting.Strategy.IsDirect() {
		return true
	}
	return false
}

// IsOnlineSchemaDDL returns true if we expect to run a online schema change DDL
func (exec *TabletExecutor) isOnlineSchemaDDL(stmt sqlparser.Statement) (isOnline bool) {
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		if exec.isDirectStrategy() {
			return false
		}
		switch stmt.GetAction() {
		case sqlparser.CreateDDLAction, sqlparser.DropDDLAction, sqlparser.AlterDDLAction:
			return true
		}
	case *sqlparser.RevertMigration:
		return true
	}
	return false
}

// executeSQL executes a single SQL statement either as online DDL or synchronously on all tablets.
// In online DDL case, the query may be exploded into multiple queries during
func (exec *TabletExecutor) executeSQL(ctx context.Context, sql string, providedUUID string, execResult *ExecuteResult) (executedAsynchronously bool, err error) {
	executeViaFetch := func() (bool, error) {
		exec.executeOnAllTablets(ctx, execResult, sql, false)
		return false, nil
	}
	if exec.batchSize > 1 {
		// Batched writes only ever work with 'direct' strategy and appleid directly to the mysql servers
		return executeViaFetch()
	}
	// Analyze what type of query this is:
	stmt, err := exec.parser.Parse(sql)
	if err != nil {
		return false, err
	}
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		if exec.isOnlineSchemaDDL(stmt) {
			onlineDDLs, err := schema.NewOnlineDDLs(exec.keyspace, sql, stmt, exec.ddlStrategySetting, exec.migrationContext, providedUUID, exec.parser)
			if err != nil {
				execResult.ExecutorErr = err.Error()
				return false, err
			}
			for _, onlineDDL := range onlineDDLs {
				exec.executeOnAllTablets(ctx, execResult, onlineDDL.SQL, true)
				if len(execResult.SuccessShards) > 0 {
					execResult.UUIDs = append(execResult.UUIDs, onlineDDL.UUID)
					exec.logger.Printf("%s\n", onlineDDL.UUID)
				}
			}
			return true, nil
		}
	case *sqlparser.RevertMigration:
		strategySetting := schema.NewDDLStrategySetting(schema.DDLStrategyOnline, exec.ddlStrategySetting.Options)
		onlineDDL, err := schema.NewOnlineDDL(exec.keyspace, "", sqlparser.String(stmt), strategySetting, exec.migrationContext, providedUUID, exec.parser)
		if err != nil {
			execResult.ExecutorErr = err.Error()
			return false, err
		}
		exec.executeOnAllTablets(ctx, execResult, onlineDDL.SQL, true)
		execResult.UUIDs = append(execResult.UUIDs, onlineDDL.UUID)
		exec.logger.Printf("%s\n", onlineDDL.UUID)
		return true, nil
	case *sqlparser.AlterMigration:
		exec.executeOnAllTablets(ctx, execResult, sql, true)
		return true, nil
	}
	// Got here? The statement needs to be executed directly.
	return executeViaFetch()
}

// batchSQLs combines SQLs into batches, delimited by ';'
func batchSQLs(sqls []string, batchSize int) (batchedSQLs []string) {
	if batchSize <= 1 {
		return sqls
	}
	for len(sqls) > 0 {
		nextBatchSize := batchSize
		if nextBatchSize > len(sqls) {
			nextBatchSize = len(sqls)
		}
		nextBatch := sqls[0:nextBatchSize]
		nextBatchSql := strings.Join(nextBatch, ";")
		batchedSQLs = append(batchedSQLs, nextBatchSql)
		sqls = sqls[nextBatchSize:]
	}
	return batchedSQLs
}

// allSQLsAreCreateQueries returns 'true' when all given queries are CREATE TABLE|VIEW
// This function runs pretty fast even for thousands of tables (its overhead is insignificant compared with
// the time it would take to apply the changes).
func allSQLsAreCreateQueries(sqls []string, parser *sqlparser.Parser) (bool, error) {
	for _, sql := range sqls {
		stmt, err := parser.Parse(sql)
		if err != nil {
			return false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "failed to parse sql: %s, got error: %v", sql, err)
		}
		switch stmt.(type) {
		case *sqlparser.CreateTable, *sqlparser.CreateView:
		default:
			return false, nil
		}
	}
	return true, nil
}

// Execute applies schema changes
func (exec *TabletExecutor) Execute(ctx context.Context, sqls []string) *ExecuteResult {
	execResult := ExecuteResult{}

	// errorExecResult is a utility function that populates the execResult with the given error, and returns it. Used to quickly bail out of
	// this function.
	errorExecResult := func(err error) *ExecuteResult {
		if err != nil {
			execResult.ExecutorErr = err.Error()
		}
		return &execResult
	}
	execResult.Sqls = sqls
	if exec.isClosed {
		return errorExecResult(fmt.Errorf("executor is closed"))
	}
	startTime := time.Now()
	defer func() { execResult.TotalTimeSpent = time.Since(startTime) }()

	// Lock the keyspace so our schema change doesn't overlap with other
	// keyspace-wide operations like resharding migrations.
	ctx, unlock, lockErr := exec.ts.LockKeyspace(ctx, exec.keyspace, "ApplySchemaKeyspace")
	if lockErr != nil {
		return errorExecResult(vterrors.Wrapf(lockErr, "lockErr in ApplySchemaKeyspace %v", exec.keyspace))
	}
	defer func() {
		// This is complicated because execResult.ExecutorErr
		// is not of type error.
		var unlockErr error
		unlock(&unlockErr)
		if execResult.ExecutorErr == "" && unlockErr != nil {
			execResult.ExecutorErr = vterrors.Wrapf(unlockErr, "unlockErr in ApplySchemaKeyspace %v", exec.keyspace).Error()
		}
	}()

	if exec.hasProvidedUUIDs() && len(exec.uuids) != len(sqls) {
		return errorExecResult(fmt.Errorf("provided %v UUIDs do not match number of DDLs %v", len(exec.uuids), len(sqls)))
	}
	providedUUID := ""

	rl := timer.NewRateLimiter(topo.RemoteOperationTimeout / 4)
	defer rl.Stop()

	syncOperationExecuted := false

	// ReloadSchema once. Do it even if we do an early return on error
	defer func() {
		if !syncOperationExecuted {
			exec.logger.Infof("Skipped ReloadSchema since all SQLs executed asynchronously")
			return
		}
		// same shards will appear multiple times in execResult.SuccessShards when there are
		// multiple SQLs
		uniqueShards := map[string]*ShardResult{}
		for i := range execResult.SuccessShards {
			// Please do not change the above iteration to "for result := range ...".
			// This is because we want to end up grabbing a pointer to the result. But golang's "for"
			// implementation reuses the iteration parameter, and we end up reusing the same pointer.
			result := &execResult.SuccessShards[i]
			uniqueShards[result.Shard] = result
		}
		var wg sync.WaitGroup
		// If all shards succeeded, wait (up to waitReplicasTimeout) for replicas to
		// execute the schema change via replication. This is best-effort, meaning
		// we still return overall success if the timeout expires.
		concurrency := semaphore.NewWeighted(10)
		reloadCtx, cancel := context.WithTimeout(ctx, exec.waitReplicasTimeout)
		defer cancel()
		for _, result := range uniqueShards {
			wg.Add(1)
			go func(result *ShardResult) {
				defer wg.Done()
				exec.logger.Infof("ReloadSchema on shard: %s", result.Shard)
				schematools.ReloadShard(
					reloadCtx,
					exec.ts,
					exec.tmc,
					exec.logger,
					exec.keyspace,
					result.Shard,
					result.Position,
					concurrency,
					true, /* includePrimary */
				)
			}(result)
		}
		wg.Wait()
	}()

	if exec.batchSize > 1 {
		// Before we proceed to batch, we need to validate there's no conflicts.
		if !exec.isDirectStrategy() {
			return errorExecResult(fmt.Errorf("--batch-size requires 'direct' ddl-strategy"))
		}
		if exec.hasProvidedUUIDs() {
			return errorExecResult(fmt.Errorf("--batch-size conflicts with --uuid-list. Batching does not support UUIDs."))
		}
		allSQLsAreCreate, err := allSQLsAreCreateQueries(sqls, exec.parser)
		if err != nil {
			return errorExecResult(err)
		}
		if !allSQLsAreCreate {
			return errorExecResult(fmt.Errorf("--batch-size only allowed when all queries are CREATE TABLE|VIEW"))
		}

		sqls = batchSQLs(sqls, int(exec.batchSize))
	}
	for index, sql := range sqls {
		// Attempt to renew lease:
		if err := rl.Do(func() error { return topo.CheckKeyspaceLockedAndRenew(ctx, exec.keyspace) }); err != nil {
			return errorExecResult(vterrors.Wrapf(err, "CheckKeyspaceLocked in ApplySchemaKeyspace %v", exec.keyspace))
		}
		execResult.CurSQLIndex = index
		if exec.hasProvidedUUIDs() {
			providedUUID = exec.uuids[index]
		}
		executedAsynchronously, err := exec.executeSQL(ctx, sql, providedUUID, &execResult)
		if err != nil {
			return errorExecResult(err)
		}
		if !executedAsynchronously {
			syncOperationExecuted = true
		}
		if len(execResult.FailedShards) > 0 {
			break
		}
	}

	return &execResult
}

// executeOnAllTablets runs a query on all tablets, synchronously. This can be a long running operation.
func (exec *TabletExecutor) executeOnAllTablets(ctx context.Context, execResult *ExecuteResult, sql string, viaQueryService bool) {
	var wg sync.WaitGroup
	numOfPrimaryTablets := len(exec.tablets)
	wg.Add(numOfPrimaryTablets)
	errChan := make(chan ShardWithError, numOfPrimaryTablets)
	successChan := make(chan ShardResult, numOfPrimaryTablets)
	for _, tablet := range exec.tablets {
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			exec.executeOneTablet(ctx, tablet, sql, viaQueryService, errChan, successChan)
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
}

// applyAllowZeroInDate takes a SQL string which may contain one or more statements,
// and, assuming those are DDLs, adds a /*vt+ allowZeroInDate=true */ directive to all of them,
// returning the result again as one long SQL.
func applyAllowZeroInDate(sql string, parser *sqlparser.Parser) (string, error) {
	// sql may be a batch of multiple statements
	sqls, err := parser.SplitStatementToPieces(sql)
	if err != nil {
		return sql, err
	}
	var modifiedSqls []string
	for _, singleSQL := range sqls {
		// --allow-zero-in-date Applies to DDLs
		stmt, err := parser.Parse(singleSQL)
		if err != nil {
			return sql, err
		}
		if ddlStmt, ok := stmt.(sqlparser.DDLStatement); ok {
			// Add comments directive to allow zero in date
			const directive = `/*vt+ allowZeroInDate=true */`
			ddlStmt.SetComments(ddlStmt.GetParsedComments().Prepend(directive))
			singleSQL = sqlparser.String(ddlStmt)
		}
		modifiedSqls = append(modifiedSqls, singleSQL)
	}
	return strings.Join(modifiedSqls, ";"), err
}

func (exec *TabletExecutor) executeOneTablet(
	ctx context.Context,
	tablet *topodatapb.Tablet,
	sql string,
	viaQueryService bool,
	errChan chan ShardWithError,
	successChan chan ShardResult) {

	var result *querypb.QueryResult
	var err error
	if viaQueryService {
		result, err = exec.tmc.ExecuteQuery(ctx, tablet, &tabletmanagerdatapb.ExecuteQueryRequest{
			Query:   []byte(sql),
			MaxRows: 10,
		})
	} else {
		if exec.ddlStrategySetting != nil && exec.ddlStrategySetting.IsAllowZeroInDateFlag() {
			// --allow-zero-in-date Applies to DDLs
			sql, err = applyAllowZeroInDate(sql, exec.parser)
			if err != nil {
				errChan <- ShardWithError{Shard: tablet.Shard, Err: err.Error()}
				return
			}
		}
		request := &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(sql),
			MaxRows: 10,
		}
		if exec.ddlStrategySetting != nil && exec.ddlStrategySetting.IsAllowForeignKeysFlag() {
			request.DisableForeignKeyChecks = true
		}
		result, err = exec.tmc.ExecuteFetchAsDba(ctx, tablet, false, request)

	}
	if err != nil {
		errChan <- ShardWithError{Shard: tablet.Shard, Err: err.Error()}
		return
	}
	// Get a replication position that's guaranteed to be after the schema change
	// was applied on the primary.
	pos, err := exec.tmc.PrimaryPosition(ctx, tablet)
	if err != nil {
		errChan <- ShardWithError{
			Shard: tablet.Shard,
			Err:   fmt.Sprintf("couldn't get replication position after applying schema change on primary: %v", err),
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
