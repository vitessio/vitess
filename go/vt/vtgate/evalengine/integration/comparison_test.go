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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

// normalizeValue returns a normalized form of this value that matches the output
// of the evaluation engine. This is used to mask quirks in the way MySQL sends SQL
// values over the wire, to allow comparing our implementation against MySQL's in
// integration tests.
func normalizeValue(v sqltypes.Value, coll collations.ID) sqltypes.Value {
	typ := v.Type()
	if typ == sqltypes.VarChar && coll == collations.CollationBinaryID {
		return sqltypes.NewVarBinary(string(v.Raw()))
	}
	if typ == sqltypes.Float32 || typ == sqltypes.Float64 {
		var bitsize = 64
		if typ == sqltypes.Float32 {
			bitsize = 32
		}
		f, err := strconv.ParseFloat(v.RawStr(), bitsize)
		if err != nil {
			panic(err)
		}
		return sqltypes.MakeTrusted(typ, mysql.FormatFloat(typ, f))
	}
	return v
}

func compareRemoteExprEnv(t *testing.T, env *evalengine.ExpressionEnv, conn *mysql.Conn, expr string, fields []*querypb.Field) {
	t.Helper()

	localQuery := "SELECT " + expr
	remoteQuery := "SELECT " + expr
	if debugCheckCollations {
		remoteQuery = fmt.Sprintf("SELECT %s, COLLATION(%s)", expr, expr)
	}
	if len(fields) > 0 {
		if _, err := conn.ExecuteFetch(`DROP TEMPORARY TABLE IF EXISTS vteval_test`, -1, false); err != nil {
			t.Fatalf("failed to drop temporary table: %v", err)
		}

		var schema strings.Builder
		schema.WriteString(`CREATE TEMPORARY TABLE vteval_test(autopk int primary key auto_increment, `)
		for i, field := range fields {
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
			for i, field := range fields {
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

	local, localType, localErr := evaluateLocalEvalengine(env, localQuery, fields)
	remote, remoteErr := conn.ExecuteFetch(remoteQuery, 1, true)

	var localVal, remoteVal sqltypes.Value
	var localCollation, remoteCollation collations.ID
	var decimals uint32
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
			localVal = normalizeValue(v, local.Collation())
		} else {
			localVal = v
		}
		if debugCheckTypes && localType != -1 {
			tt := v.Type()
			if tt != sqltypes.Null && tt != localType {
				t.Errorf("evaluation type mismatch: eval=%v vs typeof=%v\nlocal: %s\nquery: %s (SIMPLIFY=%v)",
					tt, localType, localVal, localQuery, debugSimplify)
			}
		}
	}
	if remoteErr == nil {
		if debugNormalize {
			remoteVal = normalizeValue(remote.Rows[0][0], collations.ID(remote.Fields[0].Charset))
			decimals = remote.Fields[0].Decimals
		} else {
			remoteVal = remote.Rows[0][0]
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

	if err := compareResult(localErr, remoteErr, localVal, remoteVal, localCollation, remoteCollation, decimals); err != nil {
		t.Errorf("%s\nquery: %s (SIMPLIFY=%v)\nrow: %v", err, localQuery, debugSimplify, env.Row)
	} else if debugPrintAll {
		t.Logf("local=%s mysql=%s\nquery: %s\nrow: %v", localVal.String(), remoteVal.String(), localQuery, env.Row)
	}
}

type vcursor struct {
}

func (vc *vcursor) GetKeyspace() string {
	return "vttest"
}

func (vc *vcursor) TimeZone() *time.Location {
	return time.Local
}

func TestMySQL(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	// We require MySQL 8.0 collations for the comparisons in the tests

	servenv.SetMySQLServerVersionForTest(conn.ServerVersion)
	collationEnv = collations.NewEnvironment(conn.ServerVersion)
	servenv.OnParse(registerFlags)

	for _, tc := range testcases.Cases {
		t.Run(tc.Name(), func(t *testing.T) {
			ctx := callerid.NewContext(context.Background(), &vtrpc.CallerID{Principal: "testuser"}, &querypb.VTGateCallerID{
				Username: "vt_dba",
			})
			env := evalengine.NewExpressionEnv(ctx, nil, &vcursor{})
			tc.Run(func(query string, row []sqltypes.Value) {
				env.Row = row
				compareRemoteExprEnv(t, env, conn, query, tc.Schema)
			})
		})
	}
}
