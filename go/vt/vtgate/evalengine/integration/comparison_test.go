/*
Copyright 2021 The Vitess Authors.

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

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/evalengine/testcases"
)

var (
	collationEnv *collations.Environment

	debugPrintAll        bool
	debugNormalize       = true
	debugSimplify        = time.Now().UnixNano()&1 != 0
	debugCheckTypes      = true
	debugCheckCollations = true
)

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&debugPrintAll, "print-all", debugPrintAll, "print all matching tests")
	fs.BoolVar(&debugNormalize, "normalize", debugNormalize, "normalize comparisons against MySQL values")
	fs.BoolVar(&debugSimplify, "simplify", debugSimplify, "simplify expressions before evaluating them")
	fs.BoolVar(&debugCheckTypes, "check-types", debugCheckTypes, "check the TypeOf operator for all queries")
	fs.BoolVar(&debugCheckCollations, "check-collations", debugCheckCollations, "check the returned collations for all queries")
}

func init() {
	// We require MySQL 8.0 collations for the comparisons in the tests
	mySQLVersion := "8.0.0"
	servenv.SetMySQLServerVersionForTest(mySQLVersion)
	collationEnv = collations.NewEnvironment(mySQLVersion)
	servenv.OnParse(registerFlags)
}

func compareRemoteExprEnv(t *testing.T, env *evalengine.ExpressionEnv, conn *mysql.Conn, expr string) {
	t.Helper()

	localQuery := "SELECT " + expr
	remoteQuery := "SELECT " + expr
	if debugCheckCollations {
		remoteQuery = fmt.Sprintf("SELECT %s, COLLATION(%s)", expr, expr)
	}
	if len(env.Fields) > 0 {
		if _, err := conn.ExecuteFetch(`DROP TEMPORARY TABLE IF EXISTS vteval_test`, -1, false); err != nil {
			t.Fatalf("failed to drop temporary table: %v", err)
		}

		var schema strings.Builder
		schema.WriteString(`CREATE TEMPORARY TABLE vteval_test(autopk int primary key auto_increment, `)
		for i, field := range env.Fields {
			if i > 0 {
				schema.WriteString(", ")
			}
			_, _ = fmt.Fprintf(&schema, "%s %s", field.Name, field.ColumnType)
		}
		schema.WriteString(")")

		if _, err := conn.ExecuteFetch(schema.String(), -1, false); err != nil {
			t.Fatalf("failed to initialize temporary table: %v (sql=%s)", err, schema.String())
		}

		if len(env.Row) > 0 {
			var rowsql strings.Builder
			rowsql.WriteString(`INSERT INTO vteval_test(`)
			for i, field := range env.Fields {
				if i > 0 {
					rowsql.WriteString(", ")
				}
				rowsql.WriteString(field.Name)
			}

			rowsql.WriteString(`) VALUES (`)
			for i, row := range env.Row {
				if i > 0 {
					rowsql.WriteString(", ")
				}
				row.EncodeSQLStringBuilder(&rowsql)
			}
			rowsql.WriteString(")")

			if _, err := conn.ExecuteFetch(rowsql.String(), -1, false); err != nil {
				t.Fatalf("failed to insert data into temporary table: %v (sql=%s)", err, rowsql.String())
			}
		}

		remoteQuery = remoteQuery + " FROM vteval_test"
	}

	local, localType, localErr := evaluateLocalEvalengine(env, localQuery)
	remote, remoteErr := conn.ExecuteFetch(remoteQuery, 1, true)

	var localVal, remoteVal string
	var localCollation, remoteCollation collations.ID
	if localErr == nil {
		v := local.Value()
		if debugCheckCollations {
			if v.IsNull() {
				localCollation = collations.CollationBinaryID
			} else {
				localCollation = local.Collation()
			}
		}
		if debugNormalize {
			localVal = evalengine.NormalizeValue(v, local.Collation())
		} else {
			localVal = v.String()
		}
		if debugCheckTypes {
			tt := v.Type()
			if tt != sqltypes.Null && tt != localType {
				t.Errorf("evaluation type mismatch: eval=%v vs typeof=%v\nlocal: %s\nquery: %s (SIMPLIFY=%v)",
					tt, localType, localVal, localQuery, debugSimplify)
			}
		}
	}
	if remoteErr == nil {
		if debugNormalize {
			remoteVal = evalengine.NormalizeValue(remote.Rows[0][0], collations.ID(remote.Fields[0].Charset))
		} else {
			remoteVal = remote.Rows[0][0].String()
		}
		if debugCheckCollations {
			if remote.Rows[0][0].IsNull() {
				// TODO: passthrough proper collations for nullable fields
				remoteCollation = collations.CollationBinaryID
			} else {
				remoteCollation = collationEnv.LookupByName(remote.Rows[0][1].ToString()).ID()
			}
		}
	}

	if diff := compareResult(localErr, remoteErr, localVal, remoteVal, localCollation, remoteCollation); diff != "" {
		t.Errorf("%s\nquery: %s (SIMPLIFY=%v)\nrow: %v", diff, localQuery, debugSimplify, env.Row)
	} else if debugPrintAll {
		t.Logf("local=%s mysql=%s\nquery: %s\nrow: %v", localVal, remoteVal, localQuery, env.Row)
	}
}

func TestMySQL(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, tc := range testcases.Cases {
		t.Run(fmt.Sprintf("%T", tc), func(t *testing.T) {
			env := tc.Environment()
			tc.Test(func(query string, row []sqltypes.Value) {
				env.Row = row
				compareRemoteExprEnv(t, env, conn, query)
			})
		})
	}
}
