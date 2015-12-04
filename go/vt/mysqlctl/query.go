// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
)

// ExecuteSuperQuery allows the user to execute a query as a super user.
func (mysqld *Mysqld) ExecuteSuperQuery(query string) error {
	return mysqld.ExecuteSuperQueryList([]string{query})
}

// ExecuteSuperQueryList alows the user to execute queries as a super user.
func (mysqld *Mysqld) ExecuteSuperQueryList(queryList []string) error {
	conn, connErr := mysqld.dbaPool.Get(0)
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
	conn, connErr := mysqld.dbaPool.Get(0)
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

	rowMap := make(map[string]string)
	for i, value := range qr.Rows[0] {
		rowMap[qr.Fields[i].Name] = value.String()
	}
	return rowMap, nil
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
