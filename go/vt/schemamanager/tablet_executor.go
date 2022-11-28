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
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
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
)

// TabletExecutor applies schema changes to all tablets.
type TabletExecutor struct {
	migrationContext     string
	ts                   *topo.Server
	tmc                  tmclient.TabletManagerClient
	logger               logutil.Logger
	tablets              []*topodatapb.Tablet
	isClosed             bool
	allowBigSchemaChange bool
	keyspace             string
	waitReplicasTimeout  time.Duration
	ddlStrategySetting   *schema.DDLStrategySetting
	uuids                []string
	skipPreflight        bool
}

// NewTabletExecutor creates a new TabletExecutor instance
func NewTabletExecutor(migrationContext string, ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger, waitReplicasTimeout time.Duration) *TabletExecutor {
	return &TabletExecutor{
		ts:                   ts,
		tmc:                  tmc,
		logger:               logger,
		isClosed:             true,
		allowBigSchemaChange: false,
		waitReplicasTimeout:  waitReplicasTimeout,
		migrationContext:     migrationContext,
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

// SkipPreflight disables preflight checks
func (exec *TabletExecutor) SkipPreflight() {
	exec.skipPreflight = true
}

// Open opens a connection to the primary for every shard.
func (exec *TabletExecutor) Open(ctx context.Context, keyspace string) error {
	if !exec.isClosed {
		return nil
	}
	exec.keyspace = keyspace
	shardNames, err := exec.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("unable to get shard names for keyspace: %s, error: %v", keyspace, err)
	}
	exec.tablets = make([]*topodatapb.Tablet, len(shardNames))
	for i, shardName := range shardNames {
		shardInfo, err := exec.ts.GetShard(ctx, keyspace, shardName)
		if err != nil {
			return fmt.Errorf("unable to get shard info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		if !shardInfo.HasPrimary() {
			return fmt.Errorf("shard: %s does not have a primary", shardName)
		}
		tabletInfo, err := exec.ts.GetTablet(ctx, shardInfo.PrimaryAlias)
		if err != nil {
			return fmt.Errorf("unable to get primary tablet info, keyspace: %s, shard: %s, error: %v", keyspace, shardName, err)
		}
		exec.tablets[i] = tabletInfo.Tablet
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

	// We ignore DATABASE-level DDLs here because detectBigSchemaChanges doesn't
	// look at them anyway.
	parsedDDLs, _, _, _, err := exec.parseDDLs(sqls)
	if err != nil {
		return err
	}

	bigSchemaChange, err := exec.detectBigSchemaChanges(ctx, parsedDDLs)
	if bigSchemaChange && exec.allowBigSchemaChange {
		exec.logger.Warningf("Processing big schema change. This may cause visible MySQL downtime.")
		return nil
	}
	return err
}

func (exec *TabletExecutor) parseDDLs(sqls []string) ([]sqlparser.DDLStatement, []sqlparser.DBDDLStatement, [](*sqlparser.RevertMigration), [](*sqlparser.AlterMigration), error) {
	parsedDDLs := make([]sqlparser.DDLStatement, 0)
	parsedDBDDLs := make([]sqlparser.DBDDLStatement, 0)
	revertStatements := make([](*sqlparser.RevertMigration), 0)
	alterMigrationStatements := make([](*sqlparser.AlterMigration), 0)
	for _, sql := range sqls {
		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to parse sql: %s, got error: %v", sql, err)
		}
		switch stmt := stmt.(type) {
		case sqlparser.DDLStatement:
			parsedDDLs = append(parsedDDLs, stmt)
		case sqlparser.DBDDLStatement:
			parsedDBDDLs = append(parsedDBDDLs, stmt)
		case *sqlparser.RevertMigration:
			revertStatements = append(revertStatements, stmt)
		case *sqlparser.AlterMigration:
			alterMigrationStatements = append(alterMigrationStatements, stmt)
		default:
			if len(exec.tablets) != 1 {
				return nil, nil, nil, nil, fmt.Errorf("non-ddl statements can only be executed for single shard keyspaces: %s", sql)
			}
		}
	}
	return parsedDDLs, parsedDBDDLs, revertStatements, alterMigrationStatements, nil
}

// IsOnlineSchemaDDL returns true if we expect to run a online schema change DDL
func (exec *TabletExecutor) isOnlineSchemaDDL(stmt sqlparser.Statement) (isOnline bool) {
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		if exec.ddlStrategySetting == nil {
			return false
		}
		if exec.ddlStrategySetting.Strategy.IsDirect() {
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

// a schema change that satisfies any following condition is considered
// to be a big schema change and will be rejected.
//  1. Alter more than 100,000 rows.
//  2. Change a table with more than 2,000,000 rows (Drops are fine).
func (exec *TabletExecutor) detectBigSchemaChanges(ctx context.Context, parsedDDLs []sqlparser.DDLStatement) (bool, error) {
	// We want to avoid any overhead if possible. If all DDLs are online schema changes, then we want to
	// skip GetSchema altogether.
	foundAnyNonOnlineDDL := false
	for _, ddl := range parsedDDLs {
		if !exec.isOnlineSchemaDDL(ddl) {
			foundAnyNonOnlineDDL = true
		}
	}
	if !foundAnyNonOnlineDDL {
		return false, nil
	}
	// exec.tablets is guaranteed to have at least one element;
	// Otherwise, Open should fail and executor should fail.
	primaryTabletInfo := exec.tablets[0]
	// get database schema, excluding views.
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{}, ExcludeTables: []string{}, TableSchemaOnly: true}
	dbSchema, err := exec.tmc.GetSchema(ctx, primaryTabletInfo, req)
	if err != nil {
		return false, fmt.Errorf("unable to get database schema, error: %v", err)
	}
	tableWithCount := make(map[string]uint64, len(dbSchema.TableDefinitions))
	for _, tableSchema := range dbSchema.TableDefinitions {
		tableWithCount[tableSchema.Name] = tableSchema.RowCount
	}
	for _, ddl := range parsedDDLs {
		if exec.isOnlineSchemaDDL(ddl) {
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
	_, err := exec.tmc.PreflightSchema(ctx, exec.tablets[0], sqls)
	return err
}

// executeSQL executes a single SQL statement either as online DDL or synchronously on all tablets.
// In online DDL case, the query may be exploded into multiple queries during
func (exec *TabletExecutor) executeSQL(ctx context.Context, sql string, providedUUID string, execResult *ExecuteResult) (executedAsynchronously bool, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return false, err
	}
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		if exec.isOnlineSchemaDDL(stmt) {
			onlineDDLs, err := schema.NewOnlineDDLs(exec.keyspace, sql, stmt, exec.ddlStrategySetting, exec.migrationContext, providedUUID)
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
		onlineDDL, err := schema.NewOnlineDDL(exec.keyspace, "", sqlparser.String(stmt), strategySetting, exec.migrationContext, providedUUID)
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
	exec.executeOnAllTablets(ctx, execResult, sql, false)
	return false, nil
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
	ctx, unlock, lockErr := exec.ts.LockKeyspace(ctx, exec.keyspace, "ApplySchemaKeyspace")
	if lockErr != nil {
		execResult.ExecutorErr = vterrors.Wrapf(lockErr, "lockErr in ApplySchemaKeyspace %v", exec.keyspace).Error()
		return &execResult
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

	// Make sure the schema changes introduce a table definition change.
	if err := exec.preflightSchemaChanges(ctx, sqls); err != nil {
		execResult.ExecutorErr = err.Error()
		return &execResult
	}

	if exec.hasProvidedUUIDs() && len(exec.uuids) != len(sqls) {
		execResult.ExecutorErr = fmt.Sprintf("provided %v UUIDs do not match number of DDLs %v", len(exec.uuids), len(sqls))
		return &execResult
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
		concurrency := sync2.NewSemaphore(10, 0)
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

	for index, sql := range sqls {
		// Attempt to renew lease:
		if err := rl.Do(func() error { return topo.CheckKeyspaceLockedAndRenew(ctx, exec.keyspace) }); err != nil {
			execResult.ExecutorErr = vterrors.Wrapf(err, "CheckKeyspaceLocked in ApplySchemaKeyspace %v", exec.keyspace).Error()
			return &execResult
		}
		execResult.CurSQLIndex = index
		if exec.hasProvidedUUIDs() {
			providedUUID = exec.uuids[index]
		}
		executedAsynchronously, err := exec.executeSQL(ctx, sql, providedUUID, &execResult)
		if err != nil {
			execResult.ExecutorErr = err.Error()
			return &execResult
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
			stmt, err := sqlparser.Parse(string(sql))
			if err != nil {
				errChan <- ShardWithError{Shard: tablet.Shard, Err: err.Error()}
				return
			}
			if ddlStmt, ok := stmt.(sqlparser.DDLStatement); ok {
				// Add comments directive to allow zero in date
				const directive = `/*vt+ allowZeroInDate=true */`
				ddlStmt.SetComments(ddlStmt.GetParsedComments().Prepend(directive))
				sql = sqlparser.String(ddlStmt)
			}
		}
		result, err = exec.tmc.ExecuteFetchAsDba(ctx, tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(sql),
			MaxRows: 10,
		})
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
