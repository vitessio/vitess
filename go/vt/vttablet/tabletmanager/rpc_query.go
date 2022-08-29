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

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteFetchAsDba(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := tm.MysqlDaemon.GetDbaConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// disable binlogs if necessary
	if req.DisableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if req.DbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		_, _ = conn.ExecuteFetch("USE "+sqlescape.EscapeID(req.DbName), 1, false)
	}

	// Handle special possible directives
	var directives *sqlparser.CommentDirectives
	if stmt, err := sqlparser.Parse(string(req.Query)); err == nil {
		if cmnt, ok := stmt.(sqlparser.Commented); ok {
			directives = cmnt.GetParsedComments().Directives()
		}
	}
	if directives.IsSet("allowZeroInDate") {
		if _, err := conn.ExecuteFetch("set @@session.sql_mode=REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", 1, false); err != nil {
			return nil, err
		}
	}
	// run the query
	result, err := conn.ExecuteFetch(string(req.Query), int(req.MaxRows), true /*wantFields*/)

	// re-enable binlogs if necessary
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

// ExecuteFetchAsAllPrivs will execute the given query, possibly reloading schema.
func (tm *TabletManager) ExecuteFetchAsAllPrivs(ctx context.Context, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
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

	// run the query
	result, err := conn.ExecuteFetch(string(req.Query), int(req.MaxRows), true /*wantFields*/)

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
	// get a connection
	conn, err := tm.MysqlDaemon.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	result, err := conn.ExecuteFetch(string(req.Query), int(req.MaxRows), true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
}

// ExecuteQuery submits a new online DDL request
func (tm *TabletManager) ExecuteQuery(ctx context.Context, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	// get the db name from the tablet
	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	result, err := tm.QueryServiceControl.QueryService().Execute(ctx, target, string(req.Query), nil, 0, 0, nil)
	return sqltypes.ResultToProto3(result), err
}
