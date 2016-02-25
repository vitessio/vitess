// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
)

// getPoolReconnect gets a connection from a pool, tests it, and reconnects if
// it gets errno 2006.
func getPoolReconnect(pool *dbconnpool.ConnectionPool, timeout time.Duration) (dbconnpool.PoolConnection, error) {
	conn, err := pool.Get(timeout)
	if err != nil {
		return conn, err
	}
	// Run a test query to see if this connection is still good.
	if _, err := conn.ExecuteFetch("SELECT 1", 1, false); err != nil {
		// If we get "MySQL server has gone away (errno 2006)", try to reconnect.
		if sqlErr, ok := err.(*sqldb.SQLError); ok && sqlErr.Number() == 2006 {
			if err := conn.Reconnect(); err != nil {
				conn.Recycle()
				return conn, err
			}
		}
	}
	return conn, nil
}

// ExecuteSuperQuery allows the user to execute a query as a super user.
func (mysqld *Mysqld) ExecuteSuperQuery(query string) error {
	return mysqld.ExecuteSuperQueryList([]string{query})
}

// ExecuteSuperQueryList alows the user to execute queries as a super user.
func (mysqld *Mysqld) ExecuteSuperQueryList(queryList []string) error {
	conn, connErr := getPoolReconnect(mysqld.dbaPool, 0)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()
	for _, query := range queryList {
		log.Infof("exec %v", redactMasterPassword(query))
		if _, err := conn.ExecuteFetch(query, 10000, false); err != nil {
			return fmt.Errorf("ExecuteFetch(%v) failed: %v", redactMasterPassword(query), err.Error())
		}
	}
	return nil
}

// FetchSuperQuery returns the results of executing a query as a super user.
func (mysqld *Mysqld) FetchSuperQuery(query string) (*sqltypes.Result, error) {
	conn, connErr := getPoolReconnect(mysqld.dbaPool, 0)
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Recycle()
	log.V(6).Infof("fetch %v", query)
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// fetchSuperQueryMap returns a map from column names to cell data for a query
// that should return exactly 1 row.
func (mysqld *Mysqld) fetchSuperQueryMap(query string) (map[string]string, error) {
	qr, err := mysqld.FetchSuperQuery(query)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("query %#v returned %d rows, expected 1", query, len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, fmt.Errorf("query %#v returned %d column names, expected %d", query, len(qr.Fields), len(qr.Rows[0]))
	}

	rowMap := make(map[string]string, len(qr.Rows[0]))
	for i, value := range qr.Rows[0] {
		rowMap[qr.Fields[i].Name] = value.String()
	}
	return rowMap, nil
}

// fetchVariables returns a map from MySQL variable names to variable value
// for variables that match the given pattern.
func (mysqld *Mysqld) fetchVariables(pattern string) (map[string]string, error) {
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", pattern)
	qr, err := mysqld.FetchSuperQuery(query)
	if err != nil {
		return nil, err
	}
	if len(qr.Fields) != 2 {
		return nil, fmt.Errorf("query %#v returned %d columns, expected 2", query, len(qr.Fields))
	}
	varMap := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		varMap[row[0].String()] = row[1].String()
	}
	return varMap, nil
}

const masterPasswordStart = "  MASTER_PASSWORD = '"
const masterPasswordEnd = "',\n"

func redactMasterPassword(input string) string {
	i := strings.Index(input, masterPasswordStart)
	if i == -1 {
		return input
	}
	j := strings.Index(input[i+len(masterPasswordStart):], masterPasswordEnd)
	if j == -1 {
		return input
	}
	return input[:i+len(masterPasswordStart)] + strings.Repeat("*", j) + input[i+len(masterPasswordStart)+j:]
}
