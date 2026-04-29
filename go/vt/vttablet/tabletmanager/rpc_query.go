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

package tabletmanager

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// schemaEngine returns the schema engine if available, or nil. It nil-guards
// the QueryServiceControl reference so callers — particularly tests that
// build TabletManager without a controller wired in — don't panic. The
// schema-package gate helpers all treat a nil engine as a no-op.
func (tm *TabletManager) schemaEngine() *schema.Engine {
	if tm.QueryServiceControl == nil {
		return nil
	}
	return tm.QueryServiceControl.SchemaEngine()
}

// checkCreateTableLimitForSQL splits the given multi-statement SQL into
// individual statements and applies the schema engine's CREATE TABLE
// table-count gate to the batch. When some part of the input cannot be
// parsed, the function falls back to RejectIfAtLimitWithUnparseable so
// that unparseable CREATE TABLE syntax cannot silently bypass the gate
// when the engine is already at the limit.
func checkCreateTableLimitForSQL(parser *sqlparser.Parser, se *schema.Engine, sql string) error {
	queries, splitErr := parser.SplitStatementToPieces(sql)
	if splitErr != nil {
		// We could not split the SQL at all; treat the whole input as a
		// single unparseable unit and apply the at-limit safety net.
		return schema.RejectIfAtLimitWithUnparseable(se, true)
	}
	hadParseFailure := false
	stmts := make([]sqlparser.Statement, 0, len(queries))
	for _, query := range queries {
		stmt, parseErr := parser.Parse(query)
		if parseErr != nil {
			hadParseFailure = true
			continue
		}
		stmts = append(stmts, stmt)
	}
	if err := schema.CheckCreateTableLimit(se, stmts...); err != nil {
		return err
	}
	return schema.RejectIfAtLimitWithUnparseable(se, hadParseFailure)
}

// analyzeExecuteFetchAsDbaMultiQuery reutrns 'true' when at least one of the queries
// in the given SQL has a `/*vt+ allowZeroInDate=true */` directive.
func analyzeExecuteFetchAsDbaMultiQuery(sql string, parser *sqlparser.Parser) (queries []string, parseable bool, countCreate int, allowZeroInDate bool, err error) {
	queries, err = parser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, false, 0, false, err
	}
	if len(queries) == 0 {
		return nil, false, 0, false, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "no statements found in query: %s", sql)
	}
	parseable = true
	for _, query := range queries {
		// Some of the queries we receive here are legitimately non-parseable by our
		// current parser, such as `CHANGE REPLICATION SOURCE TO...`. We must allow
		// them and so we skip parsing errors.
		stmt, err := parser.Parse(query)
		if err != nil {
			parseable = false
			continue
		}
		switch stmt.(type) {
		case *sqlparser.CreateTable, *sqlparser.CreateView:
			countCreate++
		default:
		}

		if cmnt, ok := stmt.(sqlparser.Commented); ok {
			directives := cmnt.GetParsedComments().Directives()
			if directives.IsSet("allowZeroInDate") {
				allowZeroInDate = true
			}
		}
	}
	return queries, parseable, countCreate, allowZeroInDate, nil
}

// ExecuteMultiFetchAsDba will execute the given queries, possibly disabling binlogs and reload schema.
func (tm *TabletManager) executeMultiFetchAsDba(
	ctx context.Context,
	dbName string,
	sql string,
	maxRows int,
	reloadSchema bool,
	disableBinlogs bool,
	disableForeignKeyChecks bool,
	validateQueries func(queries []string, countCreate int) error,
) ([]*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// Get a connection.
	conn, err := tm.MysqlDaemon.GetDbaConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Disable binlogs if necessary.
	if disableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	// Disable FK checks if requested.
	if disableForeignKeyChecks {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if dbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(dbName), 1, false)
	}

	queries, parseable, countCreate, allowZeroInDate, err := analyzeExecuteFetchAsDbaMultiQuery(sql, tm.Env.Parser())
	if err != nil {
		return nil, err
	}
	if validateQueries != nil {
		if err := validateQueries(queries, countCreate); err != nil {
			return nil, err
		}
	}
	// Reject any CREATE TABLEs that would collectively exceed the schema
	// engine's table-count limit before we open a transaction with mysqld.
	// countCreate also includes CREATE VIEW; CheckCreateTableLimit ignores
	// non-table statements internally.
	if countCreate > 0 || !parseable {
		se := tm.schemaEngine()
		hadParseFailure := !parseable
		stmts := make([]sqlparser.Statement, 0, len(queries))
		for _, query := range queries {
			stmt, parseErr := tm.Env.Parser().Parse(query)
			if parseErr != nil {
				hadParseFailure = true
				continue
			}
			stmts = append(stmts, stmt)
		}
		if err := schema.CheckCreateTableLimit(se, stmts...); err != nil {
			return nil, err
		}
		// Defense in depth: if any statement could not be parsed and we are
		// already at the limit, refuse to proceed — the unparseable
		// statement may be a CREATE TABLE that would silently bypass the gate.
		if err := schema.RejectIfAtLimitWithUnparseable(se, hadParseFailure); err != nil {
			return nil, err
		}
	}
	if allowZeroInDate {
		if _, err := conn.ExecuteFetch("set @@session.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", 1, false); err != nil {
			return nil, err
		}
	}
	uq, err := tm.Env.Parser().ReplaceTableQualifiersMultiQuery(sql, sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	results := make([]*querypb.QueryResult, 0, len(queries))
	result, more, err := conn.ExecuteFetchMulti(uq, maxRows, true /*wantFields*/)
	if err == nil {
		results = append(results, sqltypes.ResultToProto3(result))
	}
	for more {
		result, more, _, err = conn.ReadQueryResult(maxRows, true /*wantFields*/)
		if err != nil {
			return nil, err
		}
		results = append(results, sqltypes.ResultToProto3(result))
	}

	// Re-enable FK checks if necessary.
	if disableForeignKeyChecks && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = ON", 0, false)
		if err != nil {
			// If we can't reset the FK checks flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	// Re-enable binlogs if necessary.
	if disableBinlogs && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	if err == nil && reloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Error(fmt.Sprintf("failed to reload the schema %v", reloadErr))
		}
	}
	return results, err
}

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	results, err := tm.executeMultiFetchAsDba(
		ctx,
		req.DbName,
		string(req.Query),
		int(req.MaxRows),
		req.ReloadSchema,
		req.DisableBinlogs,
		req.DisableForeignKeyChecks,
		func(queries []string, countCreate int) error {
			// As of v23 we do not allow multi-statement SQL in ExecuteFetchAsDba at all, and
			// ExecuteMultiFetchAsDba will be the only way to execute multiple statements.
			// See https://github.com/vitessio/vitess/issues/15505
			if len(queries) > 1 {
				return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "multi statement queries are not supported in ExecuteFetchAsDba")
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "received no query results in ExecuteFetchAsDba. Expcted at least 1")
	}
	return results[0], nil
}

// ExecuteMultiFetchAsDba will execute the given queries, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteMultiFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest) ([]*querypb.QueryResult, error) {
	results, err := tm.executeMultiFetchAsDba(
		ctx,
		req.DbName,
		string(req.Sql),
		int(req.MaxRows),
		req.ReloadSchema,
		req.DisableBinlogs,
		req.DisableForeignKeyChecks,
		nil, // Validation query is not needed for ExecuteMultiFetchAsDba
	)
	return results, err
}

// ExecuteFetchAsAllPrivs will execute the given query, possibly reloading schema.
func (tm *TabletManager) ExecuteFetchAsAllPrivs(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get a connection
	conn, err := tm.MysqlDaemon.GetAllPrivsConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.Env.Parser().ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	// Reject any CREATE TABLE that would push the schema engine past its
	// configured table-count limit before reaching mysqld.
	if err := checkCreateTableLimitForSQL(tm.Env.Parser(), tm.schemaEngine(), uq); err != nil {
		return nil, err
	}
	result, err := conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)

	if err == nil && req.ReloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Error(fmt.Sprintf("failed to reload the schema %v", reloadErr))
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsApp will execute the given query.
func (tm *TabletManager) ExecuteFetchAsApp(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get a connection
	conn, err := tm.MysqlDaemon.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.Env.Parser().ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	// Reject any CREATE TABLE that would push the schema engine past its
	// configured table-count limit before reaching mysqld.
	if err := checkCreateTableLimitForSQL(tm.Env.Parser(), tm.schemaEngine(), uq); err != nil {
		return nil, err
	}
	result, err := conn.Conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
}

// MysqlHostMetrics gets system metrics from mysqlctl[d]
func (tm *TabletManager) MysqlHostMetrics(ctx context.Context, req *tabletmanagerdatapb.MysqlHostMetricsRequest) (*tabletmanagerdatapb.MysqlHostMetricsResponse, error) {
	if tm.Cnf == nil {
		return &tabletmanagerdatapb.MysqlHostMetricsResponse{}, nil
	}
	mysqlResp, err := tm.MysqlDaemon.HostMetrics(ctx, tm.Cnf)
	if err != nil {
		return nil, err
	}
	resp := &tabletmanagerdatapb.MysqlHostMetricsResponse{
		HostMetrics: mysqlResp,
	}
	return resp, nil
}

// ExecuteQuery submits a new online DDL request
func (tm *TabletManager) ExecuteQuery(ctx context.Context, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	// get the db name from the tablet
	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.Env.Parser().ReplaceTableQualifiers(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	result, err := tm.QueryServiceControl.QueryService().Execute(ctx, nil, target, uq, nil, 0, 0, nil)
	return sqltypes.ResultToProto3(result), err
}
