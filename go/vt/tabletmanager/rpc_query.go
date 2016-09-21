// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
