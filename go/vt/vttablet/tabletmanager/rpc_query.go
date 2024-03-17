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

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

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

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
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
	if req.DisableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	// Disable FK checks if requested.
	if req.DisableForeignKeyChecks {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	statements, _, countCreate, allowZeroInDate, err := analyzeExecuteFetchAsDbaMultiQuery(string(req.Query), tm.Env.Parser())
	if err != nil {
		return nil, err
	}
	if len(statements) > 1 {
		// Up to v19, we allow multi-statement SQL in ExecuteFetchAsDba, but only for the specific case
		// where all statements are CREATE TABLE or CREATE VIEW. This is to support `ApplySchema --batch-size`.
		// In v20, we will not support multi statements whatsoever.
		// v20 will throw an error by virtua of using ExecuteFetch instead of ExecuteFetchMulti.
		if countCreate != len(statements) {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "multi statement queries are not supported in ExecuteFetchAsDba unless all are CREATE TABLE or CREATE VIEW")
		}
	}
	if allowZeroInDate {
		if _, err := conn.ExecuteFetch("set @@session.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", 1, false); err != nil {
			return nil, err
		}
	}
	// Replace any provided sidecar database qualifiers with the correct one.
	// TODO(shlomi): we use ReplaceTableQualifiersMultiQuery for backwards compatibility. In v20 we will not accept
	// multi statement queries in ExecuteFetchAsDBA. This will be rewritten as ReplaceTableQualifiers()
	uq, err := tm.Env.Parser().ReplaceTableQualifiersMultiQuery(string(req.Query), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	// TODO(shlomi): we use ExecuteFetchMulti for backwards compatibility. In v20 we will not accept
	// multi statement queries in ExecuteFetchAsDBA. This will be rewritten as:
	//  (in v20): result, err := ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
	result, more, err := conn.ExecuteFetchMulti(uq, int(req.MaxRows), true /*wantFields*/)
	for more {
		_, more, _, err = conn.ReadQueryResult(0, false)
		if err != nil {
			return nil, err
		}
	}

	// Re-enable FK checks if necessary.
	if req.DisableForeignKeyChecks && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = ON", 0, false)
		if err != nil {
			// If we can't reset the FK checks flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	// Re-enable binlogs if necessary.
	if req.DisableBinlogs && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	if err == nil && req.ReloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Errorf("failed to reload the schema %v", reloadErr)
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteMultiFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest) ([]*querypb.QueryResult, error) {
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
	if req.DisableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	// Disable FK checks if requested.
	if req.DisableForeignKeyChecks {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	queries, _, _, allowZeroInDate, err := analyzeExecuteFetchAsDbaMultiQuery(string(req.Sql), tm.Env.Parser())
	if err != nil {
		return nil, err
	}
	if allowZeroInDate {
		if _, err := conn.ExecuteFetch("set @@session.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", 1, false); err != nil {
			return nil, err
		}
	}
	// Replace any provided sidecar database qualifiers with the correct one.
	// TODO(shlomi): we use ReplaceTableQualifiersMultiQuery for backwards compatibility. In v20 we will not accept
	// multi statement queries in ExecuteFetchAsDBA. This will be rewritten as ReplaceTableQualifiers()
	uq, err := tm.Env.Parser().ReplaceTableQualifiersMultiQuery(string(req.Sql), sidecar.DefaultName, sidecar.GetName())
	if err != nil {
		return nil, err
	}
	// TODO(shlomi): we use ExecuteFetchMulti for backwards compatibility. In v20 we will not accept
	// multi statement queries in ExecuteFetchAsDBA. This will be rewritten as:
	//  (in v20): result, err := ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
	results := make([]*querypb.QueryResult, 0, len(queries))
	result, more, err := conn.ExecuteFetchMulti(uq, int(req.MaxRows), true /*wantFields*/)
	results = append(results, sqltypes.ResultToProto3(result))
	for more {
		result, more, _, err = conn.ReadQueryResult(0, false)
		results = append(results, sqltypes.ResultToProto3(result))
		if err != nil {
			return nil, err
		}
	}

	// Re-enable FK checks if necessary.
	if req.DisableForeignKeyChecks && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET SESSION foreign_key_checks = ON", 0, false)
		if err != nil {
			// If we can't reset the FK checks flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	// Re-enable binlogs if necessary.
	if req.DisableBinlogs && !conn.IsClosed() {
		_, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	if err == nil && req.ReloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Errorf("failed to reload the schema %v", reloadErr)
		}
	}
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
	result, err := conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)

	if err == nil && req.ReloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Errorf("failed to reload the schema %v", reloadErr)
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
	result, err := conn.Conn.ExecuteFetch(uq, int(req.MaxRows), true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
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
	result, err := tm.QueryServiceControl.QueryService().Execute(ctx, target, uq, nil, 0, 0, nil)
	return sqltypes.ResultToProto3(result), err
}
