/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CompareOptions struct {
	CompareColumnNames bool
	IgnoreRowsAffected bool
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
			checkFields(t, myField.Name, vtField, myField)

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

	if (orderBy && sqltypes.ResultsEqual([]*sqltypes.Result{vtQr}, []*sqltypes.Result{mysqlQr})) || sqltypes.ResultsEqualUnordered([]sqltypes.Result{*vtQr}, []sqltypes.Result{*mysqlQr}) {
		return nil
	}

	errStr := "Query (" + query + ") results mismatched.\nVitess Results:\n"
	var errStrSb236 strings.Builder
	for _, row := range vtQr.Rows {
		fmt.Fprintf(&errStrSb236, "%s\n", row)
	}
	errStr += errStrSb236.String()
	errStr += fmt.Sprintf("Vitess RowsAffected: %v\n", vtQr.RowsAffected)
	errStr += "MySQL Results:\n"
	var errStrSb241 strings.Builder
	for _, row := range mysqlQr.Rows {
		fmt.Fprintf(&errStrSb241, "%s\n", row)
	}
	errStr += errStrSb241.String()
	errStr += fmt.Sprintf("MySQL RowsAffected: %v\n", mysqlQr.RowsAffected)
	if vtConn != nil {
		qr, _ := ExecAllowError(t, vtConn, "vexplain plan "+query)
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

func checkFields(t TestingT, columnName string, vtField, myField *querypb.Field) {
	t.Helper()

	fail := func() {
		t.Errorf("for column %s field types do not match\nNot equal: \nMySQL: %v\nVitess: %v\n", columnName, myField.Type.String(), vtField.Type.String())
	}

	if vtField.Type != myField.Type {
		vtMatches := checkFieldsRegExpr.FindStringSubmatch(vtField.Type.String())
		myMatches := checkFieldsRegExpr.FindStringSubmatch(myField.Type.String())

		// Here we want to fail if we have totally different types for instance: "INT64" vs "TIMESTAMP"
		// We do this by checking the length of the regexp slices and checking the second item of the slices (the real type i.e. "INT")
		if len(vtMatches) != 3 || len(vtMatches) != len(myMatches) || vtMatches[1] != myMatches[1] {
			fail()
			return
		}
		vtVal, vtErr := strconv.Atoi(vtMatches[2])
		myVal, myErr := strconv.Atoi(myMatches[2])
		if vtErr != nil || myErr != nil {
			fail()
			return
		}

		// Types the same now, however, if the size of the type is smaller on Vitess compared to MySQL
		// we need to fail. We can allow superset but not the opposite.
		if vtVal < myVal {
			fail()
			return
		}
	}

	// decimal types are properly sized in their field information
	if vtField.Type == sqltypes.Decimal {
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
