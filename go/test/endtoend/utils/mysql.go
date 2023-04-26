/*
Copyright 2022 The Vitess Authors.

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

package utils

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

// NewMySQL creates a new MySQL server using the local mysqld binary. The name of the database
// will be set to `dbName`. SQL queries that need to be executed on the new MySQL instance
// can be passed through the `schemaSQL` argument.
// The mysql.ConnParams to connect to the new database is returned, along with a function to
// teardown the database.
func NewMySQL(cluster *cluster.LocalProcessCluster, dbName string, schemaSQL ...string) (mysql.ConnParams, func(), error) {
	return NewMySQLWithDetails(cluster.GetAndReservePort(), cluster.Hostname, dbName, schemaSQL...)
}

func NewMySQLWithDetails(port int, hostname, dbName string, schemaSQL ...string) (mysql.ConnParams, func(), error) {
	mysqlDir, err := createMySQLDir()
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}
	initMySQLFile, err := createInitSQLFile(mysqlDir, dbName)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	mysqlPort := port
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(0, "", int32(mysqlPort))
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}
	err = initMysqld(mysqld, mycnf, initMySQLFile)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	params := mysql.ConnParams{
		UnixSocket: mycnf.SocketFile,
		Host:       hostname,
		Uname:      "root",
		DbName:     dbName,
	}
	for _, sql := range schemaSQL {
		err = prepareMySQLWithSchema(params, sql)
		if err != nil {
			return mysql.ConnParams{}, nil, err
		}
	}
	return params, func() {
		ctx := context.Background()
		_ = mysqld.Teardown(ctx, mycnf, true)
	}, nil
}

func createMySQLDir() (string, error) {
	mysqlDir := mysqlctl.TabletDir(0)
	err := os.Mkdir(mysqlDir, 0700)
	if err != nil {
		return "", err
	}
	return mysqlDir, nil
}

func createInitSQLFile(mysqlDir, ksName string) (string, error) {
	initSQLFile := path.Join(mysqlDir, "init.sql")
	f, err := os.Create(initSQLFile)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", ksName))
	if err != nil {
		return "", err
	}
	return initSQLFile, nil
}

func initMysqld(mysqld *mysqlctl.Mysqld, mycnf *mysqlctl.Mycnf, initSQLFile string) error {
	f, err := os.CreateTemp(path.Dir(mycnf.Path), "my.cnf")
	if err != nil {
		return err
	}
	f.Close()

	ctx := context.Background()
	err = mysqld.Init(ctx, mycnf, initSQLFile)
	if err != nil {
		return err
	}
	return nil
}

func prepareMySQLWithSchema(params mysql.ConnParams, sql string) error {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return err
	}
	_, err = conn.ExecuteFetch(sql, 1, false)
	if err != nil {
		return err
	}
	return nil
}

func compareVitessAndMySQLResults(t *testing.T, query string, vtQr, mysqlQr *sqltypes.Result, compareColumns bool) {
	if vtQr == nil && mysqlQr == nil {
		return
	}
	if vtQr == nil {
		t.Error("Vitess result is 'nil' while MySQL's is not.")
		return
	}
	if mysqlQr == nil {
		t.Error("MySQL result is 'nil' while Vitess' is not.")
		return
	}
	if compareColumns {
		vtColCount := len(vtQr.Fields)
		myColCount := len(mysqlQr.Fields)
		if vtColCount > 0 && myColCount > 0 {
			if vtColCount != myColCount {
				t.Errorf("column count does not match: %d vs %d", vtColCount, myColCount)
			}

			var vtCols []string
			var myCols []string
			for i, vtField := range vtQr.Fields {
				vtCols = append(vtCols, vtField.Name)
				myCols = append(myCols, mysqlQr.Fields[i].Name)
			}
			assert.Equal(t, myCols, vtCols, "column names do not match - the expected values are what mysql produced")
		}
	}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		t.Error(err)
		return
	}
	orderBy := false
	if selStmt, isSelStmt := stmt.(sqlparser.SelectStatement); isSelStmt {
		orderBy = selStmt.GetOrderBy() != nil
	}

	if orderBy && sqltypes.ResultsEqual([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr}) {
		return
	} else if sqltypes.ResultsEqualUnordered([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr}) {
		return
	}

	errStr := "Query (" + query + ") results mismatched.\nVitess Results:\n"
	for _, row := range vtQr.Rows {
		errStr += fmt.Sprintf("%s\n", row)
	}
	errStr += "MySQL Results:\n"
	for _, row := range mysqlQr.Rows {
		errStr += fmt.Sprintf("%s\n", row)
	}
	t.Error(errStr)
}

func compareVitessAndMySQLErrors(t *testing.T, vtErr, mysqlErr error) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		return
	}
	out := fmt.Sprintf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
	t.Error(out)
}
