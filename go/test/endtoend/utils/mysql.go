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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

const (
	Ordered int = 1 << iota
	IgnoreDifference
	DontCompareResultsOnError
)

func NewMySQL(cluster *cluster.LocalProcessCluster, ksName string, schemaSQL string) (mysql.ConnParams, func(), error) {
	mysqlDir, err := createMySQLDir()
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	initMySQLFile, err := createInitSQLFile(mysqlDir, ksName)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	mysqlPort := cluster.GetAndReservePort()
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
		Host:       cluster.Hostname,
		Uname:      "root",
		DbName:     ksName,
	}

	err = prepareMySQLWithSchema(err, params, schemaSQL)
	if err != nil {
		return mysql.ConnParams{}, nil, err
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

func prepareMySQLWithSchema(err error, params mysql.ConnParams, sql string) error {
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

func compareVitessAndMySQLResults(t *testing.T, query string, vtQr *sqltypes.Result, mysqlQr *sqltypes.Result, options int) {
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
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		t.Error(err)
		return
	}
	orderBy := false
	if selStmt, isSelStmt := stmt.(sqlparser.SelectStatement); isSelStmt {
		sel := sqlparser.GetFirstSelect(selStmt)
		orderBy = sel.OrderBy != nil
	}

	if options&Ordered != 0 || orderBy && sqltypes.ResultsEqual([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr}) {
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
	if options&IgnoreDifference != 0 {
		return
	}
	t.Error(errStr)
}

func compareVitessAndMySQLErrors(t *testing.T, vtErr, mysqlErr error, allow bool) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		if vtErr != nil {
			fmt.Printf("Got an error from Vitess and MySQL:\n\tVitess error: %v\n\tMySQL error: %v\n", vtErr, mysqlErr)
		}
		return
	}
	out := fmt.Sprintf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
	if !allow {
		t.Error(out)
		return
	}
}
