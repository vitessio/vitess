/*
Copyright 2017 Google Inc.

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
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// ExecuteFetchAsDba will execute the given query, possibly disabling binlogs and reload schema.
func (agent *ActionAgent) ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetDbaConnection()
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
		conn.ExecuteFetch("USE "+dbName, 1, false)
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
		agent.QueryServiceControl.ReloadSchema(ctx)
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsAllPrivs will execute the given query, possibly reloading schema.
func (agent *ActionAgent) ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetAllPrivsConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if dbName != "" {
		// This execute might fail if db does not exist.
		// Error is ignored because given query might create this database.
		conn.ExecuteFetch("USE "+dbName, 1, false)
	}

	// run the query
	result, err := conn.ExecuteFetch(string(query), maxrows, true /*wantFields*/)

	if err == nil && reloadSchema {
		agent.QueryServiceControl.ReloadSchema(ctx)
	}
	return sqltypes.ResultToProto3(result), err
}

// ExecuteFetchAsApp will execute the given query.
func (agent *ActionAgent) ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error) {
	// get a connection
	conn, err := agent.MysqlDaemon.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	result, err := conn.ExecuteFetch(string(query), maxrows, true /*wantFields*/)
	return sqltypes.ResultToProto3(result), err
}
