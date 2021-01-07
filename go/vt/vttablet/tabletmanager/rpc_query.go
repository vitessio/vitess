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

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (tm *TabletManager) ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := tm.MysqlDaemon.GetDbaConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// disable binlogs if necessary
	if disableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	if dbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		conn.ExecuteFetch("USE "+sqlescape.EscapeID(dbName), 1, false)
	}

	// run the query
	result, err := conn.ExecuteFetch(string(query), maxrows, true /*wantFields*/)

	// re-enable binlogs if necessary
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
			log.Errorf("failed to reload the schema %v", reloadErr)
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsAllPrivs will execute the given query, possibly reloading schema.
func (tm *TabletManager) ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := tm.MysqlDaemon.GetAllPrivsConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if dbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		conn.ExecuteFetch("USE "+sqlescape.EscapeID(dbName), 1, false)
	}

	// run the query
	result, err := conn.ExecuteFetch(string(query), maxrows, true /*wantFields*/)

	if err == nil && reloadSchema {
		reloadErr := tm.QueryServiceControl.ReloadSchema(ctx)
		if reloadErr != nil {
			log.Errorf("failed to reload the schema %v", reloadErr)
		}
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsApp will execute the given query.
func (tm *TabletManager) ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := tm.MysqlDaemon.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	result, err := conn.ExecuteFetch(string(query), maxrows, true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
}
