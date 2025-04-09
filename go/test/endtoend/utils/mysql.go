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
	"math/rand/v2"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
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
	mysqlParam, _, _, closer, err := NewMySQLWithMysqld(cluster.GetAndReservePort(), cluster.Hostname, dbName, sqls...)
	return mysqlParam, closer, err
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

func NewMySQLWithMysqld(port int, hostname, dbName string, schemaSQL ...string) (mysql.ConnParams, *mysqlctl.Mysqld, *mysqlctl.Mycnf, func(), error) {
	uid := rand.Uint32()
	mysqlDir, err := createMySQLDir(uid)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, nil, err
	}
	initMySQLFile, err := createInitSQLFile(mysqlDir, dbName)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, nil, err
	}

	mysqlPort := port
	mysqld, mycnf, err := CreateMysqldAndMycnf(uid, "", mysqlPort)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, nil, err
	}
	err = initMysqld(mysqld, mycnf, initMySQLFile)
	if err != nil {
		return mysql.ConnParams{}, nil, nil, nil, err
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
			return mysql.ConnParams{}, nil, nil, nil, err
		}
	}
	return params, mysqld, mycnf, func() {
		ctx := context.Background()
		_ = mysqld.Teardown(ctx, mycnf, true, mysqlShutdownTimeout)
	}, nil
}

func createMySQLDir(portNo uint32) (string, error) {
	mysqlDir := mysqlctl.TabletDir(portNo)
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
	_, err = fmt.Fprintf(f, "CREATE DATABASE IF NOT EXISTS %s;", ksName)
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

type CompareOptions struct {
	CompareColumnNames bool
	IgnoreRowsAffected bool
	AllowAnyFieldSize  bool
}

func CompareVitessAndMySQLResults(t TestingT, query string, vtConn *mysql.Conn, vtQr, mysqlQr *sqltypes.Result, opts CompareOptions) error {
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
			checkFields(t, myField.Name, vtField, myField, opts.AllowAnyFieldSize)

			vtCols = append(vtCols, vtField.Name)
			myCols = append(myCols, myField.Name)
		}

		if opts.CompareColumnNames && !assert.Equal(t, myCols, vtCols, "column names do not match - the expected values are what mysql produced") {
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

	if opts.IgnoreRowsAffected {
		vtQr.RowsAffected = 0
		mysqlQr.RowsAffected = 0
	}

	l := []*sqltypes.Result{vtQr}
	r := []*sqltypes.Result{mysqlQr}
	if (orderBy && sqltypes.ResultsEqual(l, r)) ||
		sqltypes.ResultsEqualUnordered(l, r, opts.AllowAnyFieldSize) {
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

// Parse the string representation of a type (i.e. "INT64") into a three elements slice.
// First element of the slice will contain the full expression, second element contains the
// type "INT" and the third element contains the size if there is any "64" or empty if we use
// "TIMESTAMP" for instance.
var checkFieldsRegExpr = regexp.MustCompile(`([a-zA-Z]*)(\d*)`)

func isBinary(t string) bool {
	return t == "BINARY" || t == "VARBINARY" || t == "BLOB" || t == "LONGBLOB"
}

func isChar(t string) bool {
	return t == "CHAR" || t == "VARCHAR" || t == "TEXT" || t == "LONGTEXT"
}

func checkFields(t TestingT, columnName string, vtField, myField *querypb.Field, allowAnyFieldSize bool) {
	t.Helper()

	fail := func() {
		t.Errorf("for column %s field types do not match\nNot equal: \nMySQL: %v\nVitess: %v\n", columnName, myField.Type.String(), vtField.Type.String())
	}

	if vtField.Type != myField.Type && compareFieldAttributes(vtField, myField, fail, allowAnyFieldSize) {
		return
	}

	// starting in Vitess 20, decimal types are properly sized in their field information
	if BinaryIsAtLeastAtVersion(20, "vtgate") && vtField.Type == sqltypes.Decimal {
		if vtField.Decimals != myField.Decimals {
			t.Errorf("for column %s field decimals count do not match\nNot equal: \nMySQL: %v\nVitess: %v\n", columnName, myField.Decimals, vtField.Decimals)
		}
	}
}

func compareFieldAttributes(vtField *querypb.Field, myField *querypb.Field, fail func(), allowAnyFieldSize bool) bool {
	vtMatches := checkFieldsRegExpr.FindStringSubmatch(vtField.Type.String())
	myMatches := checkFieldsRegExpr.FindStringSubmatch(myField.Type.String())

	// Here we want to fail if we have totally different types for instance: "INT64" vs "TIMESTAMP"
	// We do this by checking the length of the regexp slices and checking the second item of the slices (the real type i.e. "INT")
	if len(vtMatches) != 3 || len(vtMatches) != len(myMatches) {
		fail()
		return true
	}

	if myMatches[2] == "" {
		myMatches[2] = "0"
	}
	if vtMatches[2] == "" {
		vtMatches[2] = "0"
	}

	vtVal, vtErr := strconv.Atoi(vtMatches[2])
	myVal, myErr := strconv.Atoi(myMatches[2])
	if vtErr != nil || myErr != nil {
		fail()
		return true
	}

	if allowAnyFieldSize {
		failed, done := forgivingTypeComparison(myMatches, vtMatches, vtVal, myVal)
		if failed {
			fail()
		}
		if done {
			return true
		}
	} else if myMatches[1] != vtMatches[1] || vtVal < myVal {
		fail()
		return true
	}
	return false
}

func forgivingTypeComparison(myMatches, vtMatches []string, vtVal, myVal int) (failed bool, done bool) {
	switch {
	case strings.HasPrefix(myMatches[1], "U") && !strings.HasPrefix(vtMatches[1], "U"):
		if myMatches[1][1:] != vtMatches[1] || vtVal <= myVal {
			// Case 1:
			// This case applies when MySQL indicates an unsigned type (for example, "UINT")
			// while Vitess indicates a signed equivalent (for instance, "INT").
			// We remove the "U" from MySQL's type and then check that it exactly matches the Vitess type.
			// In addition, we require that the size reported by Vitess (vtVal) is equal or greater than the size from MySQL (myVal).
			// If either of these conditions isn't met, it means the types don't fully match and the comparison fails.
			return true, true
		}
	case isChar(vtMatches[1]) && isChar(myMatches[1]):
		// Case 2:
		// When both field types are character types (for example, "CHAR" and "VARCHAR"),
		// they are considered compatible regardless of the exact character type name.
		return false, true
	case isBinary(vtMatches[1]) && isBinary(myMatches[1]):
		// Case 3:
		// When both field types are binary types (for example, "BINARY" or "VARBINARY"),
		// they are considered equivalent, so we return success.
		return false, true
	case myMatches[1] != vtMatches[1]:
		// Case 4:
		// If the types do not match at all (for example, MySQL returns "TIME" while Vitess returns "INT"),
		// then the fields are incompatible and we flag a failure.
		return true, true
	}

	return false, false
}

func compareVitessAndMySQLErrors(t TestingT, vtErr, mysqlErr error) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		return
	}
	t.Errorf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
}
