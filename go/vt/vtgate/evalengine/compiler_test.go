/*
Copyright 2023 The Vitess Authors.

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

package evalengine_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/evalengine/testcases"
)

func makeFields(values []sqltypes.Value) (fields []*querypb.Field) {
	for i, v := range values {
		field := &querypb.Field{
			Name: fmt.Sprintf("column%d", i),
			Type: v.Type(),
		}
		if sqltypes.IsText(field.Type) {
			field.Charset = uint32(collations.CollationUtf8mb4ID)
		} else {
			field.Charset = uint32(collations.CollationBinaryID)
		}
		fields = append(fields, field)
	}
	return
}

type Tracker struct {
	buf              strings.Builder
	tbl              *tablewriter.Table
	supported, total int
}

func NewTracker() *Tracker {
	track := &Tracker{}
	track.tbl = tablewriter.NewTable(
		&track.buf,
		tablewriter.WithAlignment(tw.Alignment{
			tw.AlignLeft,
			tw.AlignRight,
			tw.AlignRight,
			tw.AlignRight,
		}),
		tablewriter.WithFooterAlignmentConfig(tw.CellAlignment{
			Global: tw.AlignRight,
		}),
		tablewriter.WithRendition(tw.Rendition{
			Borders: tw.BorderNone,
		}),
	)
	return track
}

func (s *Tracker) Add(name string, supported, total int) {
	s.tbl.Append([]string{
		name,
		strconv.Itoa(supported),
		strconv.Itoa(total),
		fmt.Sprintf("%.02f%%", 100*float64(supported)/float64(total)),
	})
	s.supported += supported
	s.total += total
}

func (s *Tracker) String() string {
	s.tbl.Footer(
		"",
		strconv.Itoa(s.supported),
		strconv.Itoa(s.total),
		fmt.Sprintf("%.02f%%", 100*float64(s.supported)/float64(s.total)),
	)
	_ = s.tbl.Render() // Ignore render error as this is output formatting
	return s.buf.String()
}

func TestOneCase(t *testing.T) {
	query := ``
	if query == "" {
		t.Skip("no query to test")
	}
	venv := vtenv.NewTestEnv()
	env := evalengine.EmptyExpressionEnv(venv)
	testCompilerCase(t, query, venv, nil, env)
}

func TestCompilerReference(t *testing.T) {
	// This test runs a lot of queries and compares the results of the evalengine in eval mode to the results of the compiler.
	now := time.Now()
	evalengine.SystemTime = func() time.Time { return now }
	defer func() { evalengine.SystemTime = time.Now }()

	track := NewTracker()
	venv := vtenv.NewTestEnv()
	for _, tc := range testcases.Cases {
		t.Run(tc.Name(), func(t *testing.T) {
			var supported, total int
			env := evalengine.EmptyExpressionEnv(venv)

			tc.Run(func(query string, row []sqltypes.Value, _ bool) {
				env.Row = row
				total++
				testCompilerCase(t, query, venv, tc.Schema, env)
				if !t.Failed() {
					supported++
				}
			})

			track.Add(tc.Name(), supported, total)
		})
	}

	t.Logf("\n%s", track.String())
}

func testCompilerCase(t *testing.T, query string, venv *vtenv.Environment, schema []*querypb.Field, env *evalengine.ExpressionEnv) {
	stmt, err := venv.Parser().ParseExpr(query)
	if err != nil {
		// no need to test un-parseable queries
		return
	}

	fields := evalengine.FieldResolver(schema)
	cfg := &evalengine.Config{
		ResolveColumn:     fields.Column,
		ResolveType:       fields.Type,
		Collation:         collations.CollationUtf8mb4ID,
		Environment:       venv,
		NoConstantFolding: true,
	}

	converted, err := evalengine.Translate(stmt, cfg)
	if err != nil {
		return
	}

	var expected evalengine.EvalResult
	var evalErr error
	assert.NotPanics(t, func() {
		expected, evalErr = env.EvaluateAST(converted)
	})
	var res evalengine.EvalResult
	var vmErr error
	assert.NotPanics(t, func() {
		res, vmErr = env.Evaluate(converted)
	})
	switch {
	case vmErr == nil && evalErr == nil:
		eval := expected.String()
		comp := res.String()
		assert.Equalf(t, eval, comp, "bad evaluation from compiler:\nSQL:  %s\nEval: %s\nComp: %s", query, eval, comp)
		assert.Equalf(t, expected.Collation(), res.Collation(), "bad collation from compiler:\nSQL:  %s\nEval: %s\nComp: %s", query, colldata.Lookup(expected.Collation()).Name(), colldata.Lookup(res.Collation()).Name())
	case vmErr == nil:
		assert.Failf(t, "failed evaluation from evalengine", "failed evaluation from evalengine:\nSQL:  %s\nError: %s", query, evalErr)
	case evalErr == nil:
		assert.Failf(t, "failed evaluation from compiler", "failed evaluation from compiler:\nSQL:  %s\nError: %s", query, vmErr)
	case evalErr.Error() != vmErr.Error():
		assert.Failf(t, "error mismatch", "error mismatch:\nSQL:  %s\nError eval: %s\nError comp: %s", query, evalErr, vmErr)
	}
}

func TestCompilerSingle(t *testing.T) {
	testCases := []struct {
		expression string
		values     []sqltypes.Value
		result     string
		collation  collations.ID
		typeWanted evalengine.Type
	}{
		{
			expression: "1 + column0",
			values:     []sqltypes.Value{sqltypes.NewInt64(1)},
			result:     "INT64(2)",
		},
		{
			expression: "1 + column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(2)",
		},
		{
			expression: "1.0e0 - column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(0)",
		},
		{
			expression: "128 - column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(127)",
		},
		{
			expression: "(128 - column0) * 3",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(381)",
		},
		{
			expression: "ROUND(column0)",
			values:     []sqltypes.Value{sqltypes.NewFloat64(2.5)},
			result:     "FLOAT64(2)",
		},
		{
			expression: "ROUND(column0)",
			values:     []sqltypes.Value{sqltypes.NewFloat64(3.5)},
			result:     "FLOAT64(4)",
		},
		{
			expression: "ROUND(column0)",
			values:     []sqltypes.Value{sqltypes.NewFloat64(-2.5)},
			result:     "FLOAT64(-2)",
		},
		{
			expression: "ROUND(column0, 2)",
			values:     []sqltypes.Value{sqltypes.NewFloat64(0.125)},
			result:     "FLOAT64(0.12)",
		},
		{
			expression: "ROUND(column0, -1)",
			values:     []sqltypes.Value{sqltypes.NewFloat64(25)},
			result:     "FLOAT64(20)",
		},
		{
			expression: "ROUND(2.5)",
			result:     "DECIMAL(3)",
		},
		{
			expression: "ROUND(-2.5)",
			result:     "DECIMAL(-3)",
		},
		{
			expression: "1.0e0 < column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(2)},
			result:     "INT64(1)",
		},
		{
			expression: "1.0e0 < column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(-1)},
			result:     "INT64(0)",
		},
		{
			expression: `'foo' = 'FOO' collate utf8mb4_0900_as_cs`,
			result:     "INT64(0)",
		},
		{
			expression: `'foo' < 'bar'`,
			result:     "INT64(0)",
		},
		{
			expression: `case when false then 0 else 18446744073709551615 end`,
			result:     `DECIMAL(18446744073709551615)`,
		},
		{
			expression: `case when true then _binary "foobar" else 'foo' collate utf8mb4_0900_as_cs end`,
			result:     `VARCHAR("foobar")`,
		},
		{
			expression: `- 18446744073709551615`,
			result:     `DECIMAL(-18446744073709551615)`,
		},
		{
			expression: `CAST(CAST(true AS JSON) AS BINARY)`,
			result:     `BLOB("true")`,
		},
		{
			expression: `JSON_ARRAY(true, 1.0)`,
			result:     `JSON("[true, 1.0]")`,
		},
		{
			expression: `cast(true as json) + 0`,
			result:     `FLOAT64(1)`,
		},
		{
			expression: `CAST(CAST(0 AS JSON) AS CHAR(16))`,
			result:     `VARCHAR("0")`,
		},
		{
			expression: `1 OR cast('invalid' as json)`,
			result:     `INT64(1)`,
		},
		{
			expression: `NULL AND 1`,
			result:     `NULL`,
		},
		{
			expression: `CONV(-1.5e0, 1.5e0, 1.5e0)`,
			result:     `VARCHAR("1111111111111111111111111111111111111111111111111111111111111111")`,
		},
		{
			expression: `CONV(9223372036854775810.4, 13, 7)`,
			result:     `VARCHAR("45012021522523134134601")`,
		},
		{
			expression: `CONV(-9223372036854775809, 13e0, 13e0)`,
			result:     `VARCHAR("0")`,
		},
		{
			expression: `0 + time '10:04:58'`,
			result:     `INT64(100458)`,
		},
		{
			expression: `0 + time '101:34:58'`,
			result:     `INT64(1013458)`,
		},
		{
			expression: `time '10:04:58' < '101:34:58'`,
			result:     `INT64(1)`,
		},
		{
			expression: `1.7 / 173458`,
			result:     `DECIMAL(0.00001)`,
		},
		{
			expression: `cast(time '5 12:34:58' as json)`,
			result:     `JSON("\"04:34:58.000000\"")`,
		},
		{
			expression: `CAST(20000229235959.999950 AS DATETIME(4))`,
			result:     `DATETIME("2000-03-01 00:00:00.0000")`,
		},
		{
			expression: `CAST(1.5678 AS TIME(2))`,
			result:     `TIME("00:00:01.57")`,
		},
		{
			expression: `CAST(235959.995 AS TIME(2))`,
			result:     `TIME("24:00:00.00")`,
		},
		{
			expression: `CAST(-235959.995 AS TIME(2))`,
			result:     `TIME("-24:00:00.00")`,
		},
		{
			expression: `WEEK('2000-01-02', 6)`,
			result:     `INT64(1)`,
		},
		{
			expression: `WEEK(date '2000-01-01', 4)`,
			result:     `INT64(0)`,
		},
		{
			// This is the day of DST change in Europe/Amsterdam when
			// the year started on a Wednesday. Regression test for
			// using 24 hour time diffing instead of days.
			expression: `WEEK(date '2014-10-26', 6)`,
			result:     `INT64(44)`,
		},
		{
			expression: `MAKEDATE(cast('invalid' as json), NULL)`,
			result:     `NULL`,
		},
		{
			expression: `MAKETIME(NULL, '', cast('invalid' as json))`,
			result:     `NULL`,
		},
		{
			expression: `1 = ' 1 '`,
			result:     `INT64(1)`,
		},
		{
			expression: `CAST(' 0 ' AS TIME)`,
			result:     `TIME("00:00:00")`,
		},
		{
			expression: `CAST('0' AS TIME)`,
			result:     `TIME("00:00:00")`,
		},
		{
			expression: `timestamp '2000-01-01 10:34:58.978654' DIV '\t1 foo\t'`,
			result:     `INT64(20000101103458)`,
		},
		{
			expression: `UNHEX('f')`,
			result:     `VARBINARY("\x0f")`,
		},
		{
			expression: `STRCMP(1234, '12_4')`,
			result:     `INT64(-1)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 1, 0)`,
			result:     `INT64(1)`,
		},
		{
			expression: `INTERVAL(0, 1, 0, 0)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, -1, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 1, 1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, -1, -1, -1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, 1)`,
			result:     `INT64(2)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, NULL, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(NULL, 0, 0, 0)`,
			result:     `INT64(-1)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, NULL)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, NULL, 1, 1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 2, NULL, 1, 1)`,
			result:     `INT64(1)`,
		},
		{
			expression: `INTERVAL(0, 2, -1, NULL, -1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, 2, NULL, NULL, -1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, NULL, NULL, NULL, -1, 1)`,
			result:     `INT64(4)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1, NULL, 1)`,
			result:     `INT64(4)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1, NULL, NULL, 1)`,
			result:     `INT64(5)`,
		},
		{
			expression: `REGEXP_REPLACE(1234, 12, 6, 1)`,
			result:     `TEXT("634")`,
		},
		{
			expression: `_latin1 0xFF`,
			result:     `VARCHAR("ÿ")`,
		},
		{
			expression: `TRIM(_latin1 0xA078A0 FROM _utf8mb4 0xC2A078C2A0)`,
			result:     `VARCHAR("")`,
		},
		{
			expression: `CONCAT_WS("😊😂🤢", date '2000-01-01', _latin1 0xFF)`,
			result:     `VARCHAR("2000-01-01😊😂🤢ÿ")`,
		},
		{
			expression: `concat('test', _latin1 0xff)`,
			result:     `VARCHAR("testÿ")`,
		},
		{
			expression: `WEIGHT_STRING('foobar' as char(3))`,
			result:     `VARBINARY("\x1c\xe5\x1d\xdd\x1d\xdd")`,
		},
		{
			expression: `CAST(time '5 10:34:58' AS DATETIME)`,
			result:     `DATETIME("2023-10-29 10:34:58")`,
		},
		{
			expression: `CAST(time '130:34:58' AS DATETIME)`,
			result:     `DATETIME("2023-10-29 10:34:58")`,
		},
		{
			expression: `UNIX_TIMESTAMP(time '5 10:34:58')`,
			result:     `INT64(1698572098)`,
		},
		{
			expression: `CONV(-1, -1.5e0, 3.141592653589793)`,
			result:     `VARCHAR("11112220022122120101211020120210210211220")`,
		},
		{
			expression: `column0 between 10 and 20`,
			values:     []sqltypes.Value{sqltypes.NewInt16(15)},
			result:     `INT64(1)`,
		},
		{
			expression: `column0 between 10 and 20`,
			values:     []sqltypes.Value{sqltypes.NULL},
			result:     `NULL`,
		},
		{
			expression: `1 + 0b1001`,
			result:     `INT64(10)`,
		},
		{
			expression: `1 + 0x6`,
			result:     `UINT64(7)`,
		},
		{
			expression: `0 DIV 0b1001`,
			result:     `INT64(0)`,
		},
		{
			expression: `0 & 0b1001`,
			result:     `UINT64(0)`,
		},
		{
			expression: `CAST(0b1001 AS DECIMAL)`,
			result:     `DECIMAL(9)`,
		},
		{
			expression: `-0b1001`,
			result:     `FLOAT64(-9)`,
		},
		{
			expression: `'2020-01-01' + interval month(date_sub(FROM_UNIXTIME(1234), interval 1 month))-1 month`,
			result:     `CHAR("2020-12-01")`,
		},
		{
			expression: `case column0 when 1 then column1 else column2 end`,
			values:     []sqltypes.Value{sqltypes.NewInt64(42), sqltypes.NewVarChar("sole"), sqltypes.NewInt64(0)},
			result:     `VARCHAR("0")`,
			collation:  collations.CollationUtf8mb4ID,
		},
		{
			expression: `UNIX_TIMESTAMP(0.0) + 1`,
			result:     `DECIMAL(1.0)`,
		},
		{
			expression: `UNIX_TIMESTAMP(0.000) + 1`,
			result:     `DECIMAL(1.000)`,
		},
		{
			expression: `UNIX_TIMESTAMP(-1.5) + 1`,
			result:     `DECIMAL(1.0)`,
		},
		{
			expression: `UNIX_TIMESTAMP(-1.500) + 1`,
			result:     `DECIMAL(1.000)`,
		},
		{
			expression: `UNIX_TIMESTAMP(0x0) + 1`,
			result:     `INT64(1)`,
		},
		{
			expression: `UNIX_TIMESTAMP(timestamp '2000-01-01 10:34:58.123456') + 1`,
			result:     `DECIMAL(946719299.123456)`,
		},
		{
			expression: `UNIX_TIMESTAMP('200001011034581111111') + 1`,
			result:     `INT64(946719299)`,
		},
		{
			expression: `CONV(-0x1, 13e0, 13e0)`,
			result:     `VARCHAR("219505A9511A867B72")`,
		},
		{
			expression: `UNIX_TIMESTAMP('20000101103458.111111') + 1`,
			result:     `DECIMAL(946719299.111111)`,
		},
		{
			expression: `cast(null * 1 as CHAR)`,
			result:     `NULL`,
		},
		{
			expression: `cast(null + 1 as CHAR)`,
			result:     `NULL`,
		},
		{
			expression: `cast(null - 1 as CHAR)`,
			result:     `NULL`,
		},
		{
			expression: `cast(null / 1 as CHAR)`,
			result:     `NULL`,
		},
		{
			expression: `cast(null % 1 as CHAR)`,
			result:     `NULL`,
		},
		{
			expression: `1 AND NULL * 1`,
			result:     `NULL`,
		},
		{
			expression: `case 0 when NULL then 1 else 0 end`,
			result:     `INT64(0)`,
		},
		{
			expression: `case when null is null then 23 else null end`,
			result:     `INT64(23)`,
		},
		{
			expression: `CAST(0 AS DATE)`,
			result:     `NULL`,
		},
		{
			expression: `DAYOFMONTH(0)`,
			result:     `INT64(0)`,
		},
		{
			expression: `week('2023-12-31', 4)`,
			result:     `INT64(53)`,
		},
		{
			expression: `week('2023-12-31', 2)`,
			result:     `INT64(53)`,
		},
		{
			expression: `week('2024-12-31', 1)`,
			result:     `INT64(53)`,
		},
		{
			expression: `week('2024-12-31', 5)`,
			result:     `INT64(53)`,
		},
		{
			expression: `FROM_UNIXTIME(time '10:04:58.5')`,
			result:     `DATETIME("1970-01-02 04:54:18.5")`,
		},
		{
			expression: `0 = time '10:04:58.1'`,
			result:     `INT64(0)`,
		},
		{
			expression: `CAST(time '32:34:58.5' AS TIME)`,
			result:     `TIME("32:34:59")`,
		},
		{
			expression: `now(6) + interval 1 day`,
			result:     `DATETIME("2023-10-25 12:00:00.123456")`,
		},
		{
			expression: `now() + interval 654321 microsecond`,
			result:     `DATETIME("2023-10-24 12:00:00.654321")`,
		},
		{
			expression: `time('1111:66:56')`,
			result:     `NULL`,
		},
		{
			expression: `locate('Å', 'a')`,
			result:     `INT64(1)`,
		},
		{
			expression: `locate('a', 'Å')`,
			result:     `INT64(1)`,
		},
		{
			expression: `locate("", "😊😂🤢", 3)`,
			result:     `INT64(3)`,
		},
		{
			expression: `REPLACE('www.mysql.com', '', 'Ww')`,
			result:     `VARCHAR("www.mysql.com")`,
		},
		{
			expression: `1 * unix_timestamp(utc_timestamp(1))`,
			result:     `DECIMAL(1698134400.1)`,
		},
		{
			expression: `1 * unix_timestamp(CONVERT_TZ(20040101120000.10e0,'+00:00','+10:00'))`,
			result:     `DECIMAL(1072990800.101563)`,
		},
		{
			expression: `1 * unix_timestamp(CONVERT_TZ(20040101120000.10,'+00:00','+10:00'))`,
			result:     `DECIMAL(1072990800.10)`,
		},
		{
			expression: `1 * unix_timestamp(CONVERT_TZ(timestamp'2004-01-01 12:00:00.10','+00:00','+10:00'))`,
			result:     `DECIMAL(1072990800.10)`,
		},
		{
			expression: `1 * unix_timestamp(CONVERT_TZ('2004-01-01 12:00:00.10','+00:00','+10:00'))`,
			result:     `DECIMAL(1072990800.10)`,
		},
		{
			expression: `1 * unix_timestamp('2004-01-01 12:00:00.10')`,
			result:     `DECIMAL(1072954800.10)`,
		},
		{
			expression: `1 * unix_timestamp(from_unixtime(1447430881.123))`,
			result:     `DECIMAL(1447430881.123)`,
		},
		{
			expression: `1 * unix_timestamp(from_unixtime('1447430881.123'))`,
			result:     `DECIMAL(1447430881.123000)`,
		},
		{
			expression: `1 * unix_timestamp(from_unixtime(time '31:34:58'))`,
			result:     `INT64(313458)`,
		},
		{
			expression: `1 * unix_timestamp(from_unixtime(time '31:34:58.123'))`,
			result:     `DECIMAL(313458.123)`,
		},
		{
			expression: `1 * unix_timestamp(time('1.0000'))`,
			result:     `DECIMAL(1698098401.0000)`,
		},
		{
			expression: `(case
               when 'PROMOTION' like 'PROMO%'
                   then 0.01
               else 0
    end) * 0.01`,
			result:     `DECIMAL(0.0001)`,
			typeWanted: evalengine.NewTypeEx(sqltypes.Decimal, collations.CollationBinaryID, false, 4, 4, nil),
		},
		{
			expression: `case when true then 0.02 else 1.000 end`,
			result:     `DECIMAL(0.02)`,
		},
		{
			expression: `case
				when false
				then timestamp'2023-10-24 12:00:00.123456'
				else timestamp'2023-10-24 12:00:00'
			end`,
			result:     `DATETIME("2023-10-24 12:00:00.000000")`,
			typeWanted: evalengine.NewTypeEx(sqltypes.Datetime, collations.CollationBinaryID, false, 6, 0, nil),
		},
		{
			expression: `convert(0xFF using utf16)`,
			result:     `VARCHAR("ÿ")`,
		},
		{
			expression: `_utf16 0xFF`,
			result:     `VARCHAR("ÿ")`,
		},
		{
			expression: `convert(0xFF using utf32)`,
			result:     `NULL`,
		},
		{
			expression: `cast(_utf32 0xFF as binary)`,
			result:     `VARBINARY("\x00\x00\x00\xff")`,
		},
		{
			expression: `cast(_utf32 0x00FF as binary)`,
			result:     `VARBINARY("\x00\x00\x00\xff")`,
		},
		{
			expression: `cast(_utf32 0x0000FF as binary)`,
			result:     `VARBINARY("\x00\x00\x00\xff")`,
		},
		{
			expression: `DATE_FORMAT(timestamp '2024-12-30 10:34:58', "%u")`,
			result:     `VARCHAR("53")`,
		},
		{
			expression: `WEEK(timestamp '2024-12-30 10:34:58', 0)`,
			result:     `INT64(52)`,
		},
		{
			expression: `WEEK(timestamp '2024-12-30 10:34:58', 1)`,
			result:     `INT64(53)`,
		},
		{
			expression: `WEEK(timestamp '2024-01-01 10:34:58', 0)`,
			result:     `INT64(0)`,
		},
		{
			expression: `WEEK(timestamp '2024-01-01 10:34:58', 1)`,
			result:     `INT64(1)`,
		},
		{
			expression: `column0 + 1`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte("foo"))},
			// Returns 0, as unknown enums evaluate here to -1. We have this test to
			// exercise the path to push enums onto the stack.
			result: `FLOAT64(0)`,
		},
		{
			expression: `column0 + 1`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Set, []byte("foo"))},
			// Returns 1, as unknown sets evaluate here to 0. We have this test to
			// exercise the path to push sets onto the stack.
			result: `FLOAT64(1)`,
		},
		{
			expression: `GREATEST(NULL, '2023-10-24')`,
			result:     `NULL`,
		},
		{
			expression: `GREATEST(NULL, 1)`,
			result:     `NULL`,
		},
		{
			expression: `GREATEST(NULL, 1.0)`,
			result:     `NULL`,
		},
		{
			expression: `GREATEST(NULL, 1.0e0)`,
			result:     `NULL`,
		},
		{
			expression: `GREATEST(column0, 1.0e0)`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte("foo"))},
			// Enum and set are treated as strings in this case.
			result: `VARCHAR("foo")`,
		},
		{
			expression: `GREATEST(JSON_OBJECT(), JSON_ARRAY())`,
			result:     `VARCHAR("{}")`,
		},
	}

	tz, _ := time.LoadLocation("Europe/Madrid")
	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			fields := evalengine.FieldResolver(makeFields(tc.values))
			cfg := &evalengine.Config{
				ResolveColumn:     fields.Column,
				ResolveType:       fields.Type,
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			}

			converted, err := evalengine.Translate(expr, cfg)
			require.NoError(t, err)

			env := evalengine.NewExpressionEnv(t.Context(), nil, evalengine.NewEmptyVCursor(venv, tz))
			env.SetTime(time.Date(2023, 10, 24, 12, 0, 0, 123456000, tz))
			env.Row = tc.values

			expected, err := env.EvaluateAST(converted)
			require.NoError(t, err)
			require.Equalf(t, tc.result, expected.String(), "bad evaluation from eval engine: got %s, want %s", expected.String(), tc.result)
			if tc.collation != collations.Unknown {
				require.Equalf(t, tc.collation, expected.Collation(), "bad collation evaluation from eval engine: got %d, want %d", expected.Collation(), tc.collation)
			}

			if tc.typeWanted.Type() != sqltypes.Unknown {
				typ, err := env.TypeOf(converted)
				require.NoError(t, err)
				require.True(t, tc.typeWanted.Equal(&typ))
			}

			// re-run the same evaluation multiple times to ensure results are always consistent
			for i := range 8 {
				res, err := env.Evaluate(converted)
				require.NoError(t, err)

				assert.Equalf(t, tc.result, res.String(), "bad evaluation from compiler: got %s, want %s (iteration %d)", res, tc.result, i)
				if tc.collation != collations.Unknown {
					require.Equalf(t, tc.collation, res.Collation(), "bad collation evaluation from compiler: got %d, want %d", res.Collation(), tc.collation)
				}
			}
		})
	}
}

// TestJSONComparisonDomains checks that IN, BETWEEN and simple CASE match
// MySQL's comparison-domain coercion when a JSON value participates: IN
// compares every pair as JSON, while BETWEEN and simple CASE never use the
// JSON comparator. Non-JSON and NULL rows pin existing behavior.
func TestJSONComparisonDomains(t *testing.T) {
	testCases := []struct {
		expression string
		values     []sqltypes.Value
		result     string
		// skipFolding skips the constant-folding run: folding produces
		// standalone JSON literals, which compilation cannot push yet — a
		// pre-existing translation gap fixed separately.
		skipFolding bool
	}{
		// IN/NOT IN with a JSON value in the comparison domain.
		{expression: `'[]' IN (JSON_ARRAY(), 0)`, result: `INT64(0)`},
		{expression: `0 IN (JSON_ARRAY(), '0')`, result: `INT64(0)`},
		{expression: `1 IN (JSON_ARRAY(), '1')`, result: `INT64(0)`},
		{expression: `'[]' NOT IN (JSON_ARRAY(), 0)`, result: `INT64(1)`},
		{expression: `JSON_ARRAY(1) IN ('[1]')`, result: `INT64(0)`},
		{expression: `0 IN (JSON_EXTRACT('[1]', '$'), '0')`, result: `INT64(0)`},
		{expression: `0 NOT IN (JSON_ARRAY(), '0')`, result: `INT64(1)`},
		{expression: `1 NOT IN (JSON_ARRAY(), '1')`, result: `INT64(1)`},
		{expression: `CAST('[]' AS JSON) IN (JSON_ARRAY(), 0)`, result: `INT64(1)`},
		{expression: `0 IN (JSON_ARRAY(), 0)`, result: `INT64(1)`},
		{expression: `'[]' IN (JSON_ARRAY(), '0')`, result: `INT64(0)`},
		{expression: `'0' IN (CAST('[]' AS JSON), '0')`, result: `INT64(1)`},

		// ENUM and SET values convert to JSON string scalars of their
		// textual value when a JSON value participates in the IN domain.
		{
			expression:  `column0 IN ('foo', JSON_ARRAY())`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte(`foo`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 IN (JSON_ARRAY(), 'foo')`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte(`foo`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT IN ('foo', JSON_ARRAY())`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte(`foo`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT IN (JSON_ARRAY(), 'foo')`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Enum, []byte(`foo`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `column0 IN ('foo', JSON_ARRAY())`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Set, []byte(`foo`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 IN (JSON_ARRAY(), 'foo')`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Set, []byte(`foo`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT IN ('foo', JSON_ARRAY())`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Set, []byte(`foo`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT IN (JSON_ARRAY(), 'foo')`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Set, []byte(`foo`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},

		// BETWEEN/NOT BETWEEN with a JSON value in the comparison domain,
		// compared as DOUBLE when a non-textual operand participates.
		{expression: `'[]' BETWEEN JSON_ARRAY() AND 0`, result: `INT64(1)`},
		{expression: `0 BETWEEN JSON_ARRAY() AND '0'`, result: `INT64(1)`},
		{expression: `1 BETWEEN JSON_ARRAY() AND '1'`, result: `INT64(1)`},
		{expression: `JSON_ARRAY() BETWEEN 0 AND '[]'`, result: `INT64(1)`},
		{expression: `0 BETWEEN 0 AND JSON_ARRAY()`, result: `INT64(1)`},
		{expression: `'[]' NOT BETWEEN JSON_ARRAY() AND 0`, result: `INT64(0)`},
		{expression: `1 NOT BETWEEN JSON_ARRAY() AND '1'`, result: `INT64(0)`},
		{expression: `1 BETWEEN CAST('"1"' AS JSON) AND 2`, result: `INT64(1)`},

		// BETWEEN/NOT BETWEEN with only JSON and textual operands, compared
		// as strings on the serialized text forms.
		{expression: `JSON_ARRAY() BETWEEN CAST('0' AS JSON) AND JSON_OBJECT()`, result: `INT64(1)`},
		{expression: `JSON_OBJECT() BETWEEN JSON_ARRAY() AND JSON_ARRAY()`, result: `INT64(0)`},
		{expression: `CAST(2 AS JSON) BETWEEN CAST(10 AS JSON) AND CAST(3 AS JSON)`, result: `INT64(1)`},
		{expression: `'[]' BETWEEN JSON_ARRAY() AND '0'`, result: `INT64(0)`},
		{expression: `JSON_ARRAY() BETWEEN NULL AND CAST(0 AS JSON)`, result: `INT64(0)`},

		// BETWEEN/NOT BETWEEN with a JSON operand and a statically selected
		// temporal domain: a DATE/DATETIME/TIMESTAMP participant compares the
		// whole domain as DATETIME; a TIME column selects the TIME domain,
		// but a CAST(... AS TIME) expression does not.
		{expression: `CAST('2020-01-01' AS DATE) BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(1)`},
		{expression: `CAST('2020-01-01' AS DATE) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(0)`},
		{expression: `CAST('2020-01-01' AS DATETIME) BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(1)`},
		{expression: `CAST('2020-01-01' AS DATETIME) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(0)`},
		{expression: `'2020-01-01' BETWEEN JSON_ARRAY() AND CAST('2021-01-01' AS DATE)`, result: `INT64(1)`},
		{expression: `CAST('2020-01-01' AS DATE) BETWEEN NULL AND JSON_ARRAY()`, result: `INT64(0)`},
		{expression: `CAST('2020-01-01' AS DATE) BETWEEN JSON_ARRAY() AND NULL`, result: `NULL`},
		{expression: `CAST('12:00:00' AS TIME) BETWEEN JSON_ARRAY() AND '13:00:00'`, result: `INT64(0)`},
		{expression: `CAST('12:00:00' AS TIME) NOT BETWEEN JSON_ARRAY() AND '13:00:00'`, result: `INT64(1)`},
		{
			expression:  `column0 BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `column0 BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Datetime, []byte(`2020-01-01 12:00:00`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Timestamp, []byte(`2020-01-01 12:00:00`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 BETWEEN JSON_ARRAY() AND '13:00:00'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Time, []byte(`12:00:00`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `column0 NOT BETWEEN JSON_ARRAY() AND '13:00:00'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Time, []byte(`12:00:00`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},

		// Composite operands whose static result type resolves to DATE select
		// the DATETIME domain like the column itself would.
		{
			expression:  `COALESCE(column0, column0) BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `COALESCE(column0, column0) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `IFNULL(column0, column0) BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `IFNULL(column0, column0) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `IF(1, column0, column0) BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `IF(1, column0, column0) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `CASE WHEN 1 THEN column0 ELSE column0 END BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `CASE WHEN 1 THEN column0 ELSE column0 END NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `GREATEST(column0, column0) BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `GREATEST(column0, column0) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `DATE_ADD(column0, INTERVAL 0 DAY) BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},
		{
			expression:  `DATE_ADD(column0, INTERVAL 0 DAY) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},

		// Constant composite operands resolve their static type the same way.
		// 8.0.x constant-folds them before selecting the domain while 8.4.x
		// does not; we match 8.4.x.
		{expression: `COALESCE(CAST('2020-01-01' AS DATE), NULL) BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(1)`},
		{expression: `COALESCE(CAST('2020-01-01' AS DATE), NULL) NOT BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(0)`},
		{expression: `GREATEST(CAST('2020-01-01' AS DATE), CAST('2020-06-01' AS DATE)) BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(1)`},

		// A TIME-typed composite does not select the TIME domain: COALESCE
		// over a TIME column falls back to the string domain.
		{
			expression:  `COALESCE(column0, column0) BETWEEN JSON_ARRAY() AND '13:00:00'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Time, []byte(`12:00:00`))},
			result:      `INT64(0)`,
			skipFolding: true,
		},
		{
			expression:  `COALESCE(column0, column0) NOT BETWEEN JSON_ARRAY() AND '13:00:00'`,
			values:      []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Time, []byte(`12:00:00`))},
			result:      `INT64(1)`,
			skipFolding: true,
		},

		// The string and DOUBLE fallbacks still apply when no operand is
		// date-carrying.
		{expression: `'2020-01-01' BETWEEN JSON_ARRAY() AND '2021-01-01'`, result: `INT64(0)`},
		{expression: `1.5 BETWEEN JSON_ARRAY() AND '2'`, result: `INT64(1)`},

		// JSON-free temporal BETWEEN is unchanged.
		{expression: `CAST('2020-01-01' AS DATE) BETWEEN '2019-01-01' AND '2021-01-01'`, result: `INT64(1)`},
		{expression: `CAST('2020-01-01' AS DATE) NOT BETWEEN '2019-01-01' AND '2021-01-01'`, result: `INT64(0)`},
		{expression: `CAST('12:00:00' AS TIME) BETWEEN '11:00:00' AND '13:00:00'`, result: `INT64(1)`},
		{
			expression: `column0 BETWEEN '2019-01-01' AND '2021-01-01'`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Date, []byte(`2020-01-01`))},
			result:     `INT64(1)`,
		},
		{
			expression: `column0 BETWEEN '11:00:00' AND '13:00:00'`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Time, []byte(`12:00:00`))},
			result:     `INT64(1)`,
		},

		// Simple CASE with a JSON value in a (base, WHEN) pair.
		{expression: `case JSON_ARRAY() when '[]' then 1 else 0 end`, result: `INT64(1)`},
		{expression: `case 0 when JSON_ARRAY() then 1 when '0' then 2 else 3 end`, result: `INT64(1)`},
		{expression: `case '[]' when JSON_ARRAY() then 1 when 0 then 2 else 3 end`, result: `INT64(1)`},
		{expression: `case JSON_ARRAY() when 0 then 1 when '[]' then 2 else 3 end`, result: `INT64(1)`},
		{expression: `case when JSON_ARRAY() = '[]' then 1 else 0 end`, result: `INT64(0)`},
		{expression: `case JSON_ARRAY(1) when CAST('[1]' AS JSON) then 1 else 0 end`, result: `INT64(1)`},
		{expression: `case JSON_OBJECT() when JSON_ARRAY() then 1 else 0 end`, result: `INT64(0)`},
		{expression: `case CAST(1 AS JSON) when CAST('1.0' AS JSON) then 1 else 0 end`, result: `INT64(0)`},
		{expression: `case '[1]' when 3 then 1 when JSON_ARRAY(2) then 2 else 0 end`, result: `INT64(0)`},

		// JSON-free domains are unchanged.
		{expression: `0 IN ('0', 2)`, result: `INT64(1)`},
		{expression: `1 BETWEEN 0 AND '2'`, result: `INT64(1)`},
		{expression: `2 NOT BETWEEN 5 AND 20`, result: `INT64(1)`},
		{expression: `'b' BETWEEN 'a' AND 'c'`, result: `INT64(1)`},
		{expression: `case 0 when '0' then 1 else 2 end`, result: `INT64(1)`},
		{expression: `case 'b' when 'b' then 1 else 2 end`, result: `INT64(1)`},

		// NULL semantics are unchanged, with or without JSON in the domain.
		{expression: `NULL IN (JSON_ARRAY(), 1)`, result: `NULL`},
		{expression: `1 IN (NULL, JSON_ARRAY())`, result: `NULL`},
		{expression: `0 IN (NULL, '0')`, result: `INT64(1)`},
		{expression: `3 IN (NULL, '0')`, result: `NULL`},
		{expression: `NULL NOT IN (1, 2)`, result: `NULL`},
		{expression: `NULL BETWEEN 1 AND 2`, result: `NULL`},
		{expression: `1 BETWEEN NULL AND 2`, result: `NULL`},
		{expression: `1 BETWEEN 2 AND NULL`, result: `INT64(0)`},
		{expression: `1 NOT BETWEEN 2 AND NULL`, result: `INT64(1)`},
		{expression: `1 BETWEEN NULL AND 0`, result: `INT64(0)`},
		{expression: `0 BETWEEN NULL AND JSON_ARRAY()`, result: `NULL`},
		{expression: `NULL BETWEEN JSON_ARRAY() AND 1`, result: `NULL`},
		{expression: `case NULL when NULL then 1 else 2 end`, result: `INT64(2)`},
		{expression: `case 1 when NULL then 1 else 2 end`, result: `INT64(2)`},
		{expression: `case NULL when JSON_ARRAY() then 1 else 2 end`, result: `INT64(2)`},

		// The comparison domain is decided from runtime types.
		{
			expression: `0 IN (column0, '0')`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`[]`))},
			result:     `INT64(0)`,
		},
		{
			expression: `0 BETWEEN column0 AND '0'`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`[]`))},
			result:     `INT64(1)`,
		},
		{
			expression: `case column0 when '[]' then 1 else 0 end`,
			values:     []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`[]`))},
			result:     `INT64(1)`,
		},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			fields := evalengine.FieldResolver(makeFields(tc.values))
			for _, folding := range []bool{false, true} {
				if folding && tc.skipFolding {
					continue
				}
				cfg := &evalengine.Config{
					ResolveColumn:     fields.Column,
					ResolveType:       fields.Type,
					Collation:         collations.CollationUtf8mb4ID,
					Environment:       venv,
					NoConstantFolding: !folding,
				}

				converted, err := evalengine.Translate(expr, cfg)
				require.NoError(t, err)

				env := evalengine.EmptyExpressionEnv(venv)
				env.Row = tc.values

				expected, err := env.EvaluateAST(converted)
				require.NoError(t, err)
				require.Equalf(t, tc.result, expected.String(), "bad evaluation from eval engine (folding=%v)", folding)

				res, err := env.Evaluate(converted)
				require.NoError(t, err)
				assert.Equalf(t, tc.result, res.String(), "bad evaluation from compiler (folding=%v)", folding)
			}
		})
	}
}

func TestBindVarLiteral(t *testing.T) {
	testCases := []struct {
		expression string
		bindType   func(expr sqlparser.Expr)
		bindVar    *querypb.BindVariable
		result     string
	}{
		{
			expression: `_latin1 :vtg1 /* HEXNUM */`,
			bindType: func(expr sqlparser.Expr) {
				expr.(*sqlparser.IntroducerExpr).Expr.(*sqlparser.Argument).Type = sqltypes.HexNum
			},
			bindVar: sqltypes.HexNumBindVariable([]byte("0xFF")),
			result:  `VARCHAR("ÿ")`,
		},
		{
			expression: `cast(:vtg1 /* HEXVAL */ as char character set latin1)`,
			bindType: func(expr sqlparser.Expr) {
				expr.(*sqlparser.CastExpr).Expr.(*sqlparser.Argument).Type = sqltypes.HexVal
			},
			bindVar: sqltypes.HexValBindVariable([]byte("0'FF'")),
			result:  `VARCHAR("ÿ")`,
		},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			tc.bindType(expr)

			fields := evalengine.FieldResolver(makeFields(nil))
			cfg := &evalengine.Config{
				ResolveColumn:     fields.Column,
				ResolveType:       fields.Type,
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			}

			converted, err := evalengine.Translate(expr, cfg)
			require.NoError(t, err)

			result := `VARCHAR("ÿ")`

			env := evalengine.EmptyExpressionEnv(venv)
			env.BindVars = map[string]*querypb.BindVariable{
				"vtg1": tc.bindVar,
			}

			expected, err := env.EvaluateAST(converted)
			require.NoError(t, err)
			require.Equalf(t, result, expected.String(), "bad evaluation from eval engine: got %s, want %s", expected.String(), result)

			// re-run the same evaluation multiple times to ensure results are always consistent
			for i := range 8 {
				res, err := env.EvaluateVM(converted.(*evalengine.CompiledExpr))
				require.NoError(t, err)
				assert.Equalf(t, result, res.String(), "bad evaluation from compiler: got %s, want %s (iteration %d)", res, result, i)
			}
		})
	}
}

type testVcursor struct {
	lastInsertID *uint64
	env          *vtenv.Environment
}

func (t *testVcursor) TimeZone() *time.Location {
	return time.UTC
}

func (t *testVcursor) GetKeyspace() string {
	return "apa"
}

func (t *testVcursor) SQLMode() string {
	return "oltp"
}

func (t *testVcursor) Environment() *vtenv.Environment {
	return t.env
}

func (t *testVcursor) SetLastInsertID(id uint64) {
	t.lastInsertID = &id
}

var _ evalengine.VCursor = (*testVcursor)(nil)

func TestLastInsertID(t *testing.T) {
	testCases := []struct {
		expression string
		result     uint64
		missing    bool
	}{
		{
			expression: `last_insert_id(1)`,
			result:     1,
		}, {
			expression: `12`,
			missing:    true,
		}, {
			expression: `last_insert_id(666)`,
			result:     666,
		}, {
			expression: `last_insert_id(null)`,
			result:     0,
		},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			cfg := &evalengine.Config{
				Collation:         collations.CollationUtf8mb4ID,
				NoConstantFolding: true,
				NoCompilation:     false,
				Environment:       venv,
			}
			t.Run("eval", func(t *testing.T) {
				cfg.NoCompilation = true
				runTest(t, expr, cfg, tc)
			})
			t.Run("compiled", func(t *testing.T) {
				cfg.NoCompilation = false
				runTest(t, expr, cfg, tc)
			})
		})
	}
}

func runTest(t *testing.T, expr sqlparser.Expr, cfg *evalengine.Config, tc struct {
	expression string
	result     uint64
	missing    bool
},
) {
	converted, err := evalengine.Translate(expr, cfg)
	require.NoError(t, err)

	vc := &testVcursor{env: vtenv.NewTestEnv()}
	env := evalengine.NewExpressionEnv(context.Background(), nil, vc)

	_, err = env.Evaluate(converted)
	require.NoError(t, err)
	if tc.missing {
		require.Nil(t, vc.lastInsertID)
	} else {
		require.NotNil(t, vc.lastInsertID)
		require.Equal(t, tc.result, *vc.lastInsertID)
	}
}

func TestCompilerNonConstant(t *testing.T) {
	testCases := []struct {
		expression string
	}{
		{
			expression: "RANDOM_BYTES(4)",
		},
		{
			expression: "UUID()",
		},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			cfg := &evalengine.Config{
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			}

			converted, err := evalengine.Translate(expr, cfg)
			require.NoError(t, err)

			env := evalengine.EmptyExpressionEnv(venv)
			var prev string
			for range 1000 {
				expected, err := env.EvaluateAST(converted)
				require.NoError(t, err)
				require.NotEqualf(t, prev, expected.String(), "constant evaluation from eval engine: got %s multiple times", expected.String())
				prev = expected.String()
			}

			// re-run the same evaluation multiple times to ensure results are always consistent
			for range 1000 {
				res, err := env.EvaluateVM(converted.(*evalengine.CompiledExpr))
				require.NoError(t, err)
				require.NotEqualf(t, prev, res.String(), "constant evaluation from eval engine: got %s multiple times", res.String())
				prev = res.String()
			}
		})
	}
}
