//go:build !race

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
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/format"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/evalengine/testcases"
)

var (
	debugGolden          = false
	debugNormalize       = true
	debugSimplify        = time.Now().UnixNano()&1 != 0
	debugCheckCollations = true
)

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&debugGolden, "golden", debugGolden, "print golden test files")
	fs.BoolVar(&debugNormalize, "normalize", debugNormalize, "normalize comparisons against MySQL values")
	fs.BoolVar(&debugSimplify, "simplify", debugSimplify, "simplify expressions before evaluating them")
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
		return sqltypes.MakeTrusted(typ, format.FormatFloat(f))
	}
	return v
}

func compareRemoteExprEnv(t *testing.T, collationEnv *collations.Environment, env *evalengine.ExpressionEnv, conn *mysql.Conn, expr string, fields []*querypb.Field, cmp *testcases.Comparison) {
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
	if cmp == nil {
		cmp = &testcases.Comparison{}
	}

	local, localErr := evaluateLocalEvalengine(env, localQuery, fields)
	remote, remoteErr := conn.ExecuteFetch(remoteQuery, 1, true)

	var localVal, remoteVal sqltypes.Value
	var localCollation, remoteCollation collations.ID
	if localErr == nil {
		v := local.Value(collations.MySQL8().DefaultConnectionCharset())
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
	}
	if remoteErr == nil {
		if debugNormalize {
			remoteVal = normalizeValue(remote.Rows[0][0], collations.ID(remote.Fields[0].Charset))
			cmp.Decimals = remote.Fields[0].Decimals
		} else {
			remoteVal = remote.Rows[0][0]
		}
		if debugCheckCollations {
			if remote.Rows[0][0].IsNull() {
				// TODO: passthrough proper collations for nullable fields
				remoteCollation = collations.CollationBinaryID
			} else {
				remoteCollation = collationEnv.LookupByName(remote.Rows[0][1].ToString())
			}
		}
	}

	localResult := Result{
		Error:     localErr,
		Value:     localVal,
		Collation: localCollation,
	}
	remoteResult := Result{
		Error:     remoteErr,
		Value:     remoteVal,
		Collation: remoteCollation,
	}

	if debugGolden {
		g := GoldenTest{Query: localQuery}
		if remoteErr != nil {
			g.Error = remoteErr.Error()
		} else {
			g.Value = remoteVal.String()
		}
		seenGoldenTests = append(seenGoldenTests, g)
		return
	}

	if err := compareResult(localResult, remoteResult, cmp); err != nil {
		t.Errorf("%s\nquery: %s (SIMPLIFY=%v)\nrow: %v", err, localQuery, debugSimplify, env.Row)
	}
}

var seenGoldenTests []GoldenTest

type vcursor struct {
	env *vtenv.Environment
}

func (vc *vcursor) GetKeyspace() string {
	return "vttest"
}

func (vc *vcursor) TimeZone() *time.Location {
	return time.Local
}

func (vc *vcursor) SQLMode() string {
	return config.DefaultSQLMode
}

func (vc *vcursor) Environment() *vtenv.Environment {
	return vc.env
}

func initTimezoneData(t *testing.T, conn *mysql.Conn) {
	// We load the timezone information into MySQL. The evalengine assumes
	// our backend MySQL is configured with the timezone information as well
	// for functions like CONVERT_TZ.
	out, err := exec.Command("mysql_tzinfo_to_sql", "/usr/share/zoneinfo").Output()
	if err != nil {
		t.Fatalf("failed to retrieve timezone info: %v", err)
	}

	_, more, err := conn.ExecuteFetchMulti(fmt.Sprintf("USE mysql; %s\n", string(out)), -1, false)
	if err != nil {
		t.Fatalf("failed to insert timezone info: %v", err)
	}
	for more {
		_, more, _, err = conn.ReadQueryResult(-1, false)
		if err != nil {
			t.Fatalf("failed to insert timezone info: %v", err)
		}
	}
	_, err = conn.ExecuteFetch(fmt.Sprintf("USE %s", connParams.DbName), -1, false)
	if err != nil {
		t.Fatalf("failed to switch back to database: %v", err)
	}
}

func TestMySQL(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	var conn = mysqlconn(t)
	defer conn.Close()

	// We require MySQL 8.0 collations for the comparisons in the tests

	collationEnv := collations.NewEnvironment(conn.ServerVersion)
	servenv.OnParse(registerFlags)
	initTimezoneData(t, conn)

	venv, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: conn.ServerVersion,
	})
	require.NoError(t, err)
	for _, tc := range testcases.Cases {
		t.Run(tc.Name(), func(t *testing.T) {
			ctx := callerid.NewContext(context.Background(), &vtrpc.CallerID{Principal: "testuser"}, &querypb.VTGateCallerID{
				Username: "vt_dba",
			})
			env := evalengine.NewExpressionEnv(ctx, nil, &vcursor{env: venv})
			tc.Run(func(query string, row []sqltypes.Value) {
				env.Row = row
				compareRemoteExprEnv(t, collationEnv, env, conn, query, tc.Schema, tc.Compare)
			})
		})
	}

	if debugGolden {
		writeGolden(t, seenGoldenTests)
	}
}
