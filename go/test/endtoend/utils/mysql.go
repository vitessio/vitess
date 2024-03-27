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
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

const mysqlShutdownTimeout = 1 * time.Minute

// NewMySQL creates a new MySQL server using the local mysqld binary. The name of the database
// will be set to `dbName`. SQL queries that need to be executed on the new MySQL instance
// can be passed through the `schemaSQL` argument.
// The mysql.ConnParams to connect to the new database is returned, along with a function to
// teardown the database.
func NewMySQL(cluster *cluster.LocalProcessCluster, dbName string, schemaSQL ...string) (mysql.ConnParams, func(), error) {
	// Even though we receive schemaSQL as a variadic argument, we ensure to further split it into singular statements.
	parser := sqlparser.NewTestParser()
	var sqls []string
	for _, sql := range schemaSQL {
		split, err := parser.SplitStatementToPieces(sql)
		if err != nil {
			return mysql.ConnParams{}, nil, err
		}
		sqls = append(sqls, split...)
	}
	mysqlParam, _, closer, error := NewMySQLWithMysqld(cluster.GetAndReservePort(), cluster.Hostname, dbName, sqls...)
	return mysqlParam, closer, error
}

// CreateMysqldAndMycnf returns a Mysqld and a Mycnf object to use for working with a MySQL
// installation that hasn't been set up yet.
func CreateMysqldAndMycnf(tabletUID uint32, mysqlSocket string, mysqlPort int) (*mysqlctl.Mysqld, *mysqlctl.Mycnf, error) {
	mycnf := mysqlctl.NewMycnf(tabletUID, mysqlPort)
	if err := mycnf.RandomizeMysqlServerID(); err != nil {
		return nil, nil, fmt.Errorf("couldn't generate random MySQL server_id: %v", err)
	}
	if mysqlSocket != "" {
		mycnf.SocketFile = mysqlSocket
	}
	var cfg dbconfigs.DBConfigs
	// ensure the DBA username is 'root' instead of the system's default username so that mysqladmin can shutdown
	cfg.Dba.User = "root"
	cfg.InitWithSocket(mycnf.SocketFile, collations.MySQL8())
	return mysqlctl.NewMysqld(&cfg), mycnf, nil
}

func NewMySQLWithMysqld(port int, hostname, dbName string, schemaSQL ...string) (mysql.ConnParams, *mysqlctl.Mysqld, func(), error) {
	mysqlDir, err := createMySQLDir()
	if err != nil {
		return mysql.ConnParams{}, nil, nil, err
	}
	initMySQLFile, err := createInitSQLFile(mysqlDir, dbName)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, err
	}

	mysqlPort := port
	mysqld, mycnf, err := CreateMysqldAndMycnf(0, "", mysqlPort)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, err
	}
	err = initMysqld(mysqld, mycnf, initMySQLFile)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, err
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
			return mysql.ConnParams{}, nil, nil, err
		}
	}
	return params, mysqld, func() {
		ctx := context.Background()
		_ = mysqld.Teardown(ctx, mycnf, true, mysqlShutdownTimeout)
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
	_, err = f.WriteString("SET GLOBAL super_read_only='OFF';")
	if err != nil {
		return "", err
	}
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

func compareVitessAndMySQLResults(t TestingT, query string, vtConn *mysql.Conn, vtQr, mysqlQr *sqltypes.Result, compareColumnNames bool) error {
	t.Helper()

	if vtQr == nil && mysqlQr == nil {
		return nil
	}
	if vtQr == nil {
		t.Errorf("Vitess result is 'nil' while MySQL's is not.")
		return errors.New("Vitess result is 'nil' while MySQL's is not.\n")
	}
	if mysqlQr == nil {
		t.Errorf("MySQL result is 'nil' while Vitess' is not.")
		return errors.New("MySQL result is 'nil' while Vitess' is not.\n")
	}

	vtColCount := len(vtQr.Fields)
	myColCount := len(mysqlQr.Fields)

	if vtColCount != myColCount {
		t.Errorf("column count does not match: %d vs %d", vtColCount, myColCount)
	}

	if vtColCount > 0 {
		var vtCols []string
		var myCols []string
		for i, vtField := range vtQr.Fields {
			myField := mysqlQr.Fields[i]
			checkFields(t, myField.Name, vtField, myField)

			vtCols = append(vtCols, vtField.Name)
			myCols = append(myCols, myField.Name)
		}

		if compareColumnNames && !assert.Equal(t, myCols, vtCols, "column names do not match - the expected values are what mysql produced") {
			t.Errorf("column names do not match - the expected values are what mysql produced\nNot equal: \nexpected: %v\nactual: %v\n", myCols, vtCols)
		}
	}

	stmt, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		t.Errorf(err.Error())
		return err
	}
	orderBy := false
	if selStmt, isSelStmt := stmt.(sqlparser.SelectStatement); isSelStmt {
		orderBy = selStmt.GetOrderBy() != nil
	}

	if (orderBy && sqltypes.ResultsEqual([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr})) || sqltypes.ResultsEqualUnordered([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr}) {
		return nil
	}

	errStr := "Query (" + query + ") results mismatched.\nVitess Results:\n"
	for _, row := range vtQr.Rows {
		errStr += fmt.Sprintf("%s\n", row)
	}
	errStr += fmt.Sprintf("Vitess RowsAffected: %v\n", vtQr.RowsAffected)
	errStr += "MySQL Results:\n"
	for _, row := range mysqlQr.Rows {
		errStr += fmt.Sprintf("%s\n", row)
	}
	errStr += fmt.Sprintf("MySQL RowsAffected: %v\n", mysqlQr.RowsAffected)
	if vtConn != nil {
		qr, _ := ExecAllowError(t, vtConn, fmt.Sprintf("vexplain plan %s", query))
		if qr != nil && len(qr.Rows) > 0 {
			errStr += fmt.Sprintf("query plan: \n%s\n", qr.Rows[0][0].ToString())
		}
	}
	t.Errorf(errStr)
	return errors.New(errStr)
}

func checkFields(t TestingT, columnName string, vtField, myField *querypb.Field) {
	t.Helper()
	if vtField.Type != myField.Type {
		t.Errorf("for column %s field types do not match\nNot equal: \nMySQL: %v\nVitess: %v\n", columnName, myField.Type.String(), vtField.Type.String())
	}

	// starting in Vitess 20, decimal types are properly sized in their field information
	if BinaryIsAtLeastAtVersion(20, "vtgate") && vtField.Type == sqltypes.Decimal {
		if vtField.Decimals != myField.Decimals {
			t.Errorf("for column %s field decimals count do not match\nNot equal: \nMySQL: %v\nVitess: %v\n", columnName, myField.Decimals, vtField.Decimals)
		}
	}
}

func compareVitessAndMySQLErrors(t TestingT, vtErr, mysqlErr error) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		return
	}
	t.Errorf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
}
