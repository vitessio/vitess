/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sysvars"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestNormalize(t *testing.T) {
	prefix := "bv"
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]*querypb.BindVariable
	}{{
		// str val
		in:      "select * from t where foobar = 'aa'",
		outstmt: "select * from t where foobar = :foobar /* VARCHAR */",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.StringBindVariable("aa"),
		},
	}, {
		// placeholder
		in:      "select * from t where col=?",
		outstmt: "select * from t where col = :v1",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// qualified table name
		in:      "select * from `t` where col=?",
		outstmt: "select * from t where col = :v1",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// str val in select
		in:      "select 'aa' from t",
		outstmt: "select :bv1 /* VARCHAR */ from t",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.StringBindVariable("aa"),
		},
	}, {
		// int val
		in:      "select * from t where foobar = 1",
		outstmt: "select * from t where foobar = :foobar /* INT64 */",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.Int64BindVariable(1),
		},
	}, {
		// float val
		in:      "select * from t where foobar = 1.2",
		outstmt: "select * from t where foobar = :foobar /* DECIMAL(2,1) */",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.DecimalBindVariable("1.2"),
		},
	}, {
		// datetime val
		in:      "select * from t where foobar = timestamp'2012-02-29 12:34:56.123456'",
		outstmt: "select * from t where foobar = CAST(:foobar AS DATETIME(6))",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.ValueBindVariable(sqltypes.NewDatetime("2012-02-29 12:34:56.123456")),
		},
	}, {
		// time val
		in:      "select * from t where foobar = time'12:34:56.123456'",
		outstmt: "select * from t where foobar = CAST(:foobar AS TIME(6))",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.ValueBindVariable(sqltypes.NewTime("12:34:56.123456")),
		},
	}, {
		// time val
		in:      "select * from t where foobar = time'12:34:56'",
		outstmt: "select * from t where foobar = CAST(:foobar AS TIME)",
		outbv: map[string]*querypb.BindVariable{
			"foobar": sqltypes.ValueBindVariable(sqltypes.NewTime("12:34:56")),
		},
	}, {
		// multiple vals
		in:      "select * from t where foo = 1.2 and bar = 2",
		outstmt: "select * from t where foo = :foo /* DECIMAL(2,1) */ and bar = :bar /* INT64 */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.DecimalBindVariable("1.2"),
			"bar": sqltypes.Int64BindVariable(2),
		},
	}, {
		// bv collision
		in:      "select * from t where foo = :bar and bar = 12",
		outstmt: "select * from t where foo = :bar and bar = :bar1 /* INT64 */",
		outbv: map[string]*querypb.BindVariable{
			"bar1": sqltypes.Int64BindVariable(12),
		},
	}, {
		// val reuse
		in:      "select * from t where foo = 1 and bar = 1",
		outstmt: "select * from t where foo = :foo /* INT64 */ and bar = :foo /* INT64 */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.Int64BindVariable(1),
		},
	}, {
		// ints and strings are different
		in:      "select * from t where foo = 1 and bar = '1'",
		outstmt: "select * from t where foo = :foo /* INT64 */ and bar = :bar /* VARCHAR */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.Int64BindVariable(1),
			"bar": sqltypes.StringBindVariable("1"),
		},
	}, {
		// val should not be reused for non-select statements
		in:      "insert into a values(1, 1)",
		outstmt: "insert into a values (:bv1 /* INT64 */, :bv2 /* INT64 */)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(1),
		},
	}, {
		// val should be reused only in subqueries of DMLs
		in:      "update a set v1=(select 5 from t), v2=5, v3=(select 5 from t), v4=5",
		outstmt: "update a set v1 = (select :bv1 /* INT64 */ from t), v2 = :bv1 /* INT64 */, v3 = (select :bv1 /* INT64 */ from t), v4 = :bv1 /* INT64 */",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(5),
		},
	}, {
		// list vars should work for DMLs also
		in:      "update a set v1=5 where v2 in (1, 4, 5)",
		outstmt: "update a set v1 = :v1 /* INT64 */ where v2 in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"v1":  sqltypes.Int64BindVariable(5),
			"bv1": sqltypes.TestBindVariable([]any{1, 4, 5}),
		},
	}, {
		// Hex number values should work for selects
		in:      "select * from t where foo = 0x1234",
		outstmt: "select * from t where foo = :foo /* HEXNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexNumBindVariable([]byte("0x1234")),
		},
	}, {
		// Hex number values are normalized to a consistent case
		in:      "select * from t where foo = 0xdeadbeef",
		outstmt: "select * from t where foo = :foo /* HEXNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexNumBindVariable([]byte("0xDEADBEEF")),
		},
	}, {
		// Hex number values are normalized to a consistent case
		in:      "select * from t where foo = 0xDEADBEEF",
		outstmt: "select * from t where foo = :foo /* HEXNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexNumBindVariable([]byte("0xDEADBEEF")),
		},
	}, {
		// Hex encoded string values should work for selects
		in:      "select * from t where foo = x'7b7d'",
		outstmt: "select * from t where foo = :foo /* HEXVAL */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexValBindVariable([]byte("x'7B7D'")),
		},
	}, {
		// Hex encoded string are converted to a consistent case
		in:      "select * from t where foo = x'7b7D'",
		outstmt: "select * from t where foo = :foo /* HEXVAL */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexValBindVariable([]byte("x'7B7D'")),
		},
	}, {
		// Hex encoded string values should work for selects
		in:      "select * from t where foo = x'7B7D'",
		outstmt: "select * from t where foo = :foo /* HEXVAL */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexValBindVariable([]byte("x'7B7D'")),
		},
	}, {
		// Ensure that hex notation bind vars work with collation based conversions
		in:      "select convert(x'7b7d' using utf8mb4) from dual",
		outstmt: "select convert(:bv1 /* HEXVAL */ using utf8mb4) from dual",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.HexValBindVariable([]byte("x'7B7D'")),
		},
	}, {
		// Hex number values should work for DMLs
		in:      "update a set foo = 0x12",
		outstmt: "update a set foo = :foo /* HEXNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.HexNumBindVariable([]byte("0x12")),
		},
	}, {
		// Bin values work fine
		in:      "select * from t where foo = b'11'",
		outstmt: "select * from t where foo = :foo /* BITNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.BitNumBindVariable([]byte("0b11")),
		},
	}, {
		// Large bin values work fine
		in:      "select * from t where foo = b'11101010100101010010101010101010101010101000100100100100100101001101010101010101000001'",
		outstmt: "select * from t where foo = :foo /* BITNUM */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.BitNumBindVariable([]byte("0b11101010100101010010101010101010101010101000100100100100100101001101010101010101000001")),
		},
	}, {
		// Bin value does not convert for DMLs
		in:      "update a set v1 = b'11'",
		outstmt: "update a set v1 = :v1 /* BITNUM */",
		outbv: map[string]*querypb.BindVariable{
			"v1": sqltypes.BitNumBindVariable([]byte("0b11")),
		},
	}, {
		// json value in insert
		in:      "insert into t values ('{\"k\", \"v\"}')",
		outstmt: "insert into t values (:bv1 /* VARCHAR */)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.StringBindVariable("{\"k\", \"v\"}"),
		},
	}, {
		// json function in insert
		in:      "insert into t values (JSON_OBJECT('_id', 27, 'name', 'carrot'))",
		outstmt: "insert into t values (json_object(:bv1 /* VARCHAR */, :bv2 /* INT64 */, :bv3 /* VARCHAR */, :bv4 /* VARCHAR */))",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.StringBindVariable("_id"),
			"bv2": sqltypes.Int64BindVariable(27),
			"bv3": sqltypes.StringBindVariable("name"),
			"bv4": sqltypes.StringBindVariable("carrot"),
		},
	}, {
		// ORDER BY column_position
		in:      "select a, b from t order by 1 asc",
		outstmt: "select a, b from t order by 1 asc",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// GROUP BY column_position
		in:      "select a, b from t group by 1",
		outstmt: "select a, b from t group by 1",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// ORDER BY with literal inside complex expression
		in:      "select a, b from t order by field(a,1,2,3) asc",
		outstmt: "select a, b from t order by field(a, :bv1 /* INT64 */, :bv2 /* INT64 */, :bv3 /* INT64 */) asc",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(2),
			"bv3": sqltypes.Int64BindVariable(3),
		},
	}, {
		// ORDER BY variable
		in:      "select a, b from t order by c asc",
		outstmt: "select a, b from t order by c asc",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Values up to len 256 will reuse.
		in:      fmt.Sprintf("select * from t where foo = '%256s' and bar = '%256s'", "a", "a"),
		outstmt: "select * from t where foo = :foo /* VARCHAR */ and bar = :foo /* VARCHAR */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.StringBindVariable(fmt.Sprintf("%256s", "a")),
		},
	}, {
		// Values greater than len 256 will not reuse.
		in:      fmt.Sprintf("select * from t where foo = '%257s' and bar = '%257s'", "b", "b"),
		outstmt: "select * from t where foo = :foo /* VARCHAR */ and bar = :bar /* VARCHAR */",
		outbv: map[string]*querypb.BindVariable{
			"foo": sqltypes.StringBindVariable(fmt.Sprintf("%257s", "b")),
			"bar": sqltypes.StringBindVariable(fmt.Sprintf("%257s", "b")),
		},
	}, {
		// bad int
		in:      "select * from t where v1 = 12345678901234567890",
		outstmt: "select * from t where v1 = 12345678901234567890",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// comparison with no vals
		in:      "select * from t where v1 = v2",
		outstmt: "select * from t where v1 = v2",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with existing bv
		in:      "select * from t where v1 in ::list",
		outstmt: "select * from t where v1 in ::list",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with non-val values
		in:      "select * from t where v1 in (1, a)",
		outstmt: "select * from t where v1 in (:bv1 /* INT64 */, a)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// IN clause with vals
		in:      "select * from t where v1 in (1, '2')",
		outstmt: "select * from t where v1 in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]any{1, "2"}),
		},
	}, {
		// repeated IN clause with vals
		in:      "select * from t where v1 in (1, '2') OR v2 in (1, '2')",
		outstmt: "select * from t where v1 in ::bv1 or v2 in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]any{1, "2"}),
		},
	}, { // EXPLAIN query will be normalized and not parameterized
		in:      "explain select @x from t where v1 in (1, '2')",
		outstmt: "explain select :__vtudvx as `@x` from t where v1 in (1, '2')",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// NOT IN clause
		in:      "select * from t where v1 not in (1, '2')",
		outstmt: "select * from t where v1 not in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]any{1, "2"}),
		},
	}, {
		// Do not normalize cast/convert types
		in:      `select CAST("test" AS CHAR(60))`,
		outstmt: `select cast(:bv1 /* VARCHAR */ as CHAR(60)) from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.StringBindVariable("test"),
		},
	}, {
		// insert syntax
		in:      "insert into a (v1, v2, v3) values (1, '2', 3)",
		outstmt: "insert into a(v1, v2, v3) values (:bv1 /* INT64 */, :bv2 /* VARCHAR */, :bv3 /* INT64 */)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.StringBindVariable("2"),
			"bv3": sqltypes.Int64BindVariable(3),
		},
	}, {
		// BitNum should also be normalized
		in:      `select b'1', 0b01, b'1010', 0b1111111`,
		outstmt: `select :bv1 /* BITNUM */, :bv2 /* BITNUM */, :bv3 /* BITNUM */, :bv4 /* BITNUM */ from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BitNumBindVariable([]byte("0b1")),
			"bv2": sqltypes.BitNumBindVariable([]byte("0b01")),
			"bv3": sqltypes.BitNumBindVariable([]byte("0b1010")),
			"bv4": sqltypes.BitNumBindVariable([]byte("0b1111111")),
		},
	}, {
		// DateVal should also be normalized
		in:      `select date'2022-08-06'`,
		outstmt: `select CAST(:bv1 AS DATE) from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(sqltypes.Date, []byte("2022-08-06"))),
		},
	}, {
		// TimeVal should also be normalized
		in:      `select time'17:05:12'`,
		outstmt: `select CAST(:bv1 AS TIME) from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(sqltypes.Time, []byte("17:05:12"))),
		},
	}, {
		// TimestampVal should also be normalized
		in:      `select timestamp'2022-08-06 17:05:12'`,
		outstmt: `select CAST(:bv1 AS DATETIME) from dual`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2022-08-06 17:05:12"))),
		},
	}, {
		// TimestampVal should also be parameterized
		in:      `select comms_by_companies.* from comms_by_companies where comms_by_companies.id = 'rjve634shXzaavKHbAH16ql6OrxJ' limit 1,1`,
		outstmt: `select comms_by_companies.* from comms_by_companies where comms_by_companies.id = :comms_by_companies_id /* VARCHAR */ limit :bv1 /* INT64 */, :bv2 /* INT64 */`,
		outbv: map[string]*querypb.BindVariable{
			"bv1":                   sqltypes.Int64BindVariable(1),
			"bv2":                   sqltypes.Int64BindVariable(1),
			"comms_by_companies_id": sqltypes.StringBindVariable("rjve634shXzaavKHbAH16ql6OrxJ"),
		},
	}, {
		// Int leading with zero should also be normalized
		in:      `select * from t where zipcode = 01001900`,
		outstmt: `select * from t where zipcode = :zipcode /* INT64 */`,
		outbv: map[string]*querypb.BindVariable{
			"zipcode": sqltypes.ValueBindVariable(sqltypes.MakeTrusted(sqltypes.Int64, []byte("01001900"))),
		},
	}, {
		// literals in limit and offset should not reuse bindvars
		in:      `select * from t where id = 10 limit 10 offset 10`,
		outstmt: `select * from t where id = :id /* INT64 */ limit :bv1 /* INT64 */, :bv2 /* INT64 */`,
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(10),
			"bv2": sqltypes.Int64BindVariable(10),
			"id":  sqltypes.Int64BindVariable(10),
		},
	}, {
		// we don't want to replace literals on the select expressions of a derived table
		// these expressions can be referenced from the outside,
		// and changing them to bindvars can change the meaning of the query
		// example of problematic query: select tmp.`1` from (select 1) as tmp
		in:      `select * from (select 12) as t`,
		outstmt: `select * from (select 12 from dual) as t`,
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// HexVal and Int should not share a bindvar just because they have the same value
		in:      `select * from t where v1 = x'31' and v2 = 31`,
		outstmt: `select * from t where v1 = :v1 /* HEXVAL */ and v2 = :v2 /* INT64 */`,
		outbv: map[string]*querypb.BindVariable{
			"v1": sqltypes.HexValBindVariable([]byte("x'31'")),
			"v2": sqltypes.Int64BindVariable(31),
		},
	}, {
		// ORDER BY and GROUP BY variable
		in:      "select a, b from t group by 1, field(a,1,2,3) order by 1 asc, field(a,1,2,3)",
		outstmt: "select a, b from t group by 1, field(a, :bv1 /* INT64 */, :bv2 /* INT64 */, :bv3 /* INT64 */) order by 1 asc, field(a, :bv1 /* INT64 */, :bv2 /* INT64 */, :bv3 /* INT64 */) asc",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(2),
			"bv3": sqltypes.Int64BindVariable(3),
		},
	}, {
		// list in on duplicate key update
		in:      "insert into t(a, b) values (1, 2) on duplicate key update b = if(values(b) in (1, 2), b, values(b))",
		outstmt: "insert into t(a, b) values (:bv1 /* INT64 */, :bv2 /* INT64 */) on duplicate key update b = if(values(b) in ::bv3, b, values(b))",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(2),
			"bv3": sqltypes.TestBindVariable([]any{1, 2}),
		},
	}, {
		in:      "SELECT 1 WHERE (~ (1||0)) IS NULL",
		outstmt: "select :bv1 /* INT64 */ from dual where ~(:bv1 /* INT64 */ or :bv2 /* INT64 */) is null",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(0),
		},
	}, {
		// Verify we don't change anything in the normalization of create procedures.
		in:      "CREATE PROCEDURE p2 (in x BIGINT) BEGIN declare y DECIMAL(14,2); START TRANSACTION; set y = 4.2; SELECT 128 from dual; COMMIT; END",
		outstmt: "create procedure p2 (in x BIGINT) begin declare y DECIMAL(14,2); start transaction; set y = 4.2; select 128 from dual; commit; end;",
		outbv:   map[string]*querypb.BindVariable{},
	}}
	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := parser.Parse(tc.in)
			require.NoError(t, err)
			known := getBindvars(stmt)
			bv := make(map[string]*querypb.BindVariable)
			out, err := Normalize(stmt, NewReservedVars(prefix, known), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.outstmt, String(out.AST))
			assert.Equal(t, tc.outbv, bv)
		})
	}
}

func TestNormalizeInvalidDates(t *testing.T) {
	testcases := []struct {
		in  string
		err error
	}{{
		in:  "select date'foo'",
		err: vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect DATE value: '%s'", "foo"),
	}, {
		in:  "select time'foo'",
		err: vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect TIME value: '%s'", "foo"),
	}, {
		in:  "select timestamp'foo'",
		err: vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect DATETIME value: '%s'", "foo"),
	}}
	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := parser.Parse(tc.in)
			require.NoError(t, err)
			known := getBindvars(stmt)
			bv := make(map[string]*querypb.BindVariable)
			_, err = Normalize(stmt, NewReservedVars("bv", known), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
			require.EqualError(t, err, tc.err.Error())
		})
	}
}

func TestNormalizeValidSQL(t *testing.T) {
	parser := NewTestParser()
	for _, tcase := range validSQL {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.partialDDL || tcase.ignoreNormalizerTest {
				return
			}
			tree, err := parser.Parse(tcase.input)
			require.NoError(t, err, tcase.input)
			// Skip the test for the queries that do not run the normalizer
			if !CanNormalize(tree) {
				return
			}
			bv := make(map[string]*querypb.BindVariable)
			known := make(BindVars)

			out, err := Normalize(tree, NewReservedVars("vtg", known), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
			require.NoError(t, err)
			normalizerOutput := String(out.AST)
			if normalizerOutput == "otheradmin" || normalizerOutput == "otherread" {
				return
			}
			_, err = parser.Parse(normalizerOutput)
			require.NoError(t, err, normalizerOutput)
		})
	}
}

func TestNormalizeOneCasae(t *testing.T) {
	testOne := struct {
		input, output string
	}{
		input:  "",
		output: "",
	}
	if testOne.input == "" {
		t.Skip("empty test case")
	}
	parser := NewTestParser()
	tree, err := parser.Parse(testOne.input)
	require.NoError(t, err, testOne.input)
	// Skip the test for the queries that do not run the normalizer
	if !CanNormalize(tree) {
		return
	}
	bv := make(map[string]*querypb.BindVariable)
	known := make(BindVars)
	out, err := Normalize(tree, NewReservedVars("vtg", known), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
	require.NoError(t, err)
	normalizerOutput := String(out.AST)
	require.EqualValues(t, testOne.output, normalizerOutput)
	if normalizerOutput == "otheradmin" || normalizerOutput == "otherread" {
		return
	}
	_, err = parser.Parse(normalizerOutput)
	require.NoError(t, err, normalizerOutput)
}

func TestGetBindVars(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("select * from t where :v1 = :v2 and :v2 = :v3 and :v4 in ::v5")
	if err != nil {
		t.Fatal(err)
	}
	got := getBindvars(stmt)
	want := map[string]struct{}{
		"v1": {},
		"v2": {},
		"v3": {},
		"v4": {},
		"v5": {},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetBindVars: %v, want: %v", got, want)
	}
}

type testCaseSetVar struct {
	in, expected, setVarComment string
}

type testCaseSysVar struct {
	in, expected string
	sysVar       map[string]string
}

type myTestCase struct {
	in, expected                                                                            string
	liid, db, foundRows, rowCount, rawGTID, rawTimeout, sessTrackGTID                       bool
	ddlStrategy, migrationContext, sessionUUID, sessionEnableSystemSettings                 bool
	udv                                                                                     int
	autocommit, foreignKeyChecks, clientFoundRows, skipQueryPlanCache, socket, queryTimeout bool
	sqlSelectLimit, transactionMode, workload, version, versionComment, transactionTimeout  bool
}

func TestRewrites(in *testing.T) {
	tests := []myTestCase{{
		in:       "SELECT 42",
		expected: "SELECT 42",
		// no bindvar needs
	}, {
		in:       "SELECT @@version",
		expected: "SELECT :__vtversion as `@@version`",
		version:  true,
	}, {
		in:           "SELECT @@query_timeout",
		expected:     "SELECT :__vtquery_timeout as `@@query_timeout`",
		queryTimeout: true,
	}, {
		in:                 "SELECT @@transaction_timeout",
		expected:           "SELECT :__vttransaction_timeout as `@@transaction_timeout`",
		transactionTimeout: true,
	}, {
		in:             "SELECT @@version_comment",
		expected:       "SELECT :__vtversion_comment as `@@version_comment`",
		versionComment: true,
	}, {
		in:                          "SELECT @@enable_system_settings",
		expected:                    "SELECT :__vtenable_system_settings as `@@enable_system_settings`",
		sessionEnableSystemSettings: true,
	}, {
		in:       "SELECT last_insert_id()",
		expected: "SELECT :__lastInsertId as `last_insert_id()`",
		liid:     true,
	}, {
		in:       "SELECT database()",
		expected: "SELECT :__vtdbname as `database()`",
		db:       true,
	}, {
		in:       "SELECT database() from test",
		expected: "SELECT database() from test",
		// no bindvar needs
	}, {
		in:       "SELECT last_insert_id() as test",
		expected: "SELECT :__lastInsertId as test",
		liid:     true,
	}, {
		in:       "SELECT last_insert_id() + database()",
		expected: "SELECT :__lastInsertId + :__vtdbname as `last_insert_id() + database()`",
		db:       true, liid: true,
	}, {
		// unnest database() call
		in:       "select (select database()) from test",
		expected: "select database() as `(select database() from dual)` from test",
		// no bindvar needs
	}, {
		// unnest database() call
		in:       "select (select database() from dual) from test",
		expected: "select database() as `(select database() from dual)` from test",
		// no bindvar needs
	}, {
		in:       "select (select database() from dual) from dual",
		expected: "select :__vtdbname as `(select database() from dual)` from dual",
		db:       true,
	}, {
		// don't unnest solo columns
		in:       "select 1 as foobar, (select foobar)",
		expected: "select 1 as foobar, (select foobar from dual) from dual",
	}, {
		in:       "select id from user where database()",
		expected: "select id from user where database()",
		// no bindvar needs
	}, {
		in:       "select table_name from information_schema.tables where table_schema = database()",
		expected: "select table_name from information_schema.tables where table_schema = database()",
		// no bindvar needs
	}, {
		in:       "select schema()",
		expected: "select :__vtdbname as `schema()`",
		db:       true,
	}, {
		in:        "select found_rows()",
		expected:  "select :__vtfrows as `found_rows()`",
		foundRows: true,
	}, {
		in:       "select @`x y`",
		expected: "select :__vtudvx_y as `@``x y``` from dual",
		udv:      1,
	}, {
		in:       "select id from t where id = @x and val = @y",
		expected: "select id from t where id = :__vtudvx and val = :__vtudvy",
		db:       false, udv: 2,
	}, {
		in:       "insert into t(id) values(@xyx)",
		expected: "insert into t(id) values(:__vtudvxyx)",
		db:       false, udv: 1,
	}, {
		in:       "select row_count()",
		expected: "select :__vtrcount as `row_count()`",
		rowCount: true,
	}, {
		in:       "SELECT lower(database())",
		expected: "SELECT lower(:__vtdbname) as `lower(database())`",
		db:       true,
	}, {
		in:         "SELECT @@autocommit",
		expected:   "SELECT :__vtautocommit as `@@autocommit`",
		autocommit: true,
	}, {
		in:              "SELECT @@client_found_rows",
		expected:        "SELECT :__vtclient_found_rows as `@@client_found_rows`",
		clientFoundRows: true,
	}, {
		in:                 "SELECT @@skip_query_plan_cache",
		expected:           "SELECT :__vtskip_query_plan_cache as `@@skip_query_plan_cache`",
		skipQueryPlanCache: true,
	}, {
		in:             "SELECT @@sql_select_limit",
		expected:       "SELECT :__vtsql_select_limit as `@@sql_select_limit`",
		sqlSelectLimit: true,
	}, {
		in:              "SELECT @@transaction_mode",
		expected:        "SELECT :__vttransaction_mode as `@@transaction_mode`",
		transactionMode: true,
	}, {
		in:       "SELECT @@workload",
		expected: "SELECT :__vtworkload as `@@workload`",
		workload: true,
	}, {
		in:       "SELECT @@socket",
		expected: "SELECT :__vtsocket as `@@socket`",
		socket:   true,
	}, {
		in:       "select (select 42) from dual",
		expected: "select 42 as `(select 42 from dual)` from dual",
	}, {
		in:       "select * from user where col = (select 42)",
		expected: "select * from user where col = 42",
	}, {
		in:       "select * from (select 42) as t", // this is not an expression, and should not be rewritten
		expected: "select * from (select 42) as t",
	}, {
		in:       `select (select (select (select (select (select last_insert_id()))))) as x`,
		expected: "select :__lastInsertId as x from dual",
		liid:     true,
	}, {
		in:       `select * from (select last_insert_id()) as t`,
		expected: "select * from (select :__lastInsertId as `last_insert_id()` from dual) as t",
		liid:     true,
	}, {
		in:          `select * from user where col = @@ddl_strategy`,
		expected:    "select * from user where col = :__vtddl_strategy",
		ddlStrategy: true,
	}, {
		in:               `select * from user where col = @@migration_context`,
		expected:         "select * from user where col = :__vtmigration_context",
		migrationContext: true,
	}, {
		in:       `select * from user where col = @@read_after_write_gtid OR col = @@read_after_write_timeout OR col = @@session_track_gtids`,
		expected: "select * from user where col = :__vtread_after_write_gtid or col = :__vtread_after_write_timeout or col = :__vtsession_track_gtids",
		rawGTID:  true, rawTimeout: true, sessTrackGTID: true,
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual)",
		expected: "SELECT * FROM tbl WHERE id IN (1)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT last_insert_id() FROM dual)",
		expected: "SELECT * FROM tbl WHERE id IN (:__lastInsertId)",
		liid:     true,
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT (SELECT 1 FROM dual WHERE 1 = 0) FROM dual)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual WHERE 1 = 0)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual WHERE 1 = 0)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual WHERE 1 = 0)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1,2 FROM dual)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1,2 FROM dual)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual ORDER BY 1)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual ORDER BY 1)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT id FROM user GROUP BY id)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT id FROM user GROUP BY id)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual, user)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual, user)",
	}, {
		in:       "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual limit 1)",
		expected: "SELECT * FROM tbl WHERE id IN (SELECT 1 FROM dual limit 1)",
	}, {
		// SELECT * behaves different depending the join type used, so if that has been used, we won't rewrite
		in:       "SELECT * FROM A JOIN B USING (id1,id2,id3)",
		expected: "SELECT * FROM A JOIN B USING (id1,id2,id3)",
	}, {
		in:       "CALL proc(@foo)",
		expected: "CALL proc(:__vtudvfoo)",
		udv:      1,
	}, {
		in:       "SELECT * FROM tbl WHERE NOT id = 42",
		expected: "SELECT * FROM tbl WHERE id != 42",
	}, {
		in:       "SELECT * FROM tbl WHERE not id < 12",
		expected: "SELECT * FROM tbl WHERE id >= 12",
	}, {
		in:       "SELECT * FROM tbl WHERE not id > 12",
		expected: "SELECT * FROM tbl WHERE id <= 12",
	}, {
		in:       "SELECT * FROM tbl WHERE not id <= 33",
		expected: "SELECT * FROM tbl WHERE id > 33",
	}, {
		in:       "SELECT * FROM tbl WHERE not id >= 33",
		expected: "SELECT * FROM tbl WHERE id < 33",
	}, {
		in:       "SELECT * FROM tbl WHERE not id != 33",
		expected: "SELECT * FROM tbl WHERE id = 33",
	}, {
		in:       "SELECT * FROM tbl WHERE not id in (1,2,3)",
		expected: "SELECT * FROM tbl WHERE id not in (1,2,3)",
	}, {
		in:       "SELECT * FROM tbl WHERE not id not in (1,2,3)",
		expected: "SELECT * FROM tbl WHERE id in (1,2,3)",
	}, {
		in:       "SELECT * FROM tbl WHERE not id not in (1,2,3)",
		expected: "SELECT * FROM tbl WHERE id in (1,2,3)",
	}, {
		in:       "SELECT * FROM tbl WHERE not id like '%foobar'",
		expected: "SELECT * FROM tbl WHERE id not like '%foobar'",
	}, {
		in:       "SELECT * FROM tbl WHERE not id not like '%foobar'",
		expected: "SELECT * FROM tbl WHERE id like '%foobar'",
	}, {
		in:       "SELECT * FROM tbl WHERE not id regexp '%foobar'",
		expected: "SELECT * FROM tbl WHERE id not regexp '%foobar'",
	}, {
		in:       "SELECT * FROM tbl WHERE not id not regexp '%foobar'",
		expected: "select * from tbl where id regexp '%foobar'",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select col1, col2 from other_table where foo > bar)",
		expected: "SELECT * FROM tbl WHERE exists(select 1 from other_table where foo > bar)",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select col1, col2 from other_table where foo > bar limit 100 offset 34)",
		expected: "SELECT * FROM tbl WHERE exists(select 1 from other_table where foo > bar limit 100 offset 34)",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select col1, col2, count(*) from other_table where foo > bar group by col1, col2)",
		expected: "SELECT * FROM tbl WHERE exists(select 1 from other_table where foo > bar)",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select col1, col2 from other_table where foo > bar group by col1, col2)",
		expected: "SELECT * FROM tbl WHERE exists(select 1 from other_table where foo > bar)",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select count(*) from other_table where foo > bar)",
		expected: "SELECT * FROM tbl WHERE true",
	}, {
		in:       "SELECT * FROM tbl WHERE exists(select col1, col2, count(*) from other_table where foo > bar group by col1, col2 having count(*) > 3)",
		expected: "SELECT * FROM tbl WHERE exists(select col1, col2, count(*) from other_table where foo > bar group by col1, col2 having count(*) > 3)",
	}, {
		in:       "SELECT id, name, salary FROM user_details",
		expected: "SELECT id, name, salary FROM (select user.id, user.name, user_extra.salary from user join user_extra where user.id = user_extra.user_id) as user_details",
	}, {
		in:       "select max(distinct c1), min(distinct c2), avg(distinct c3), sum(distinct c4), count(distinct c5), group_concat(distinct c6) from tbl",
		expected: "select max(c1) as `max(distinct c1)`, min(c2) as `min(distinct c2)`, avg(distinct c3), sum(distinct c4), count(distinct c5), group_concat(distinct c6) from tbl",
	}, {
		in:                          "SHOW VARIABLES",
		expected:                    "SHOW VARIABLES",
		autocommit:                  true,
		foreignKeyChecks:            true,
		clientFoundRows:             true,
		skipQueryPlanCache:          true,
		sqlSelectLimit:              true,
		transactionMode:             true,
		workload:                    true,
		version:                     true,
		versionComment:              true,
		ddlStrategy:                 true,
		migrationContext:            true,
		sessionUUID:                 true,
		sessionEnableSystemSettings: true,
		rawGTID:                     true,
		rawTimeout:                  true,
		sessTrackGTID:               true,
		socket:                      true,
		queryTimeout:                true,
		transactionTimeout:          true,
	}, {
		in:                          "SHOW GLOBAL VARIABLES",
		expected:                    "SHOW GLOBAL VARIABLES",
		autocommit:                  true,
		foreignKeyChecks:            true,
		clientFoundRows:             true,
		skipQueryPlanCache:          true,
		sqlSelectLimit:              true,
		transactionMode:             true,
		workload:                    true,
		version:                     true,
		versionComment:              true,
		ddlStrategy:                 true,
		migrationContext:            true,
		sessionUUID:                 true,
		sessionEnableSystemSettings: true,
		rawGTID:                     true,
		rawTimeout:                  true,
		sessTrackGTID:               true,
		socket:                      true,
		queryTimeout:                true,
		transactionTimeout:          true,
	}}
	parser := NewTestParser()
	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, known, err := parser.Parse2(tc.in)
			require.NoError(err)
			vars := NewReservedVars("v", known)
			result, err := Normalize(
				stmt,
				vars,
				map[string]*querypb.BindVariable{},
				false,
				"ks",
				0,
				"",
				map[string]string{},
				nil,
				&fakeViews{},
			)
			require.NoError(err)

			expected, err := parser.Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			s := String(expected)
			assert := assert.New(t)
			assert.Equal(s, String(result.AST))
			assert.Equal(tc.liid, result.NeedsFuncResult(LastInsertIDName), "should need last insert id")
			assert.Equal(tc.db, result.NeedsFuncResult(DBVarName), "should need database name")
			assert.Equal(tc.foundRows, result.NeedsFuncResult(FoundRowsName), "should need found rows")
			assert.Equal(tc.rowCount, result.NeedsFuncResult(RowCountName), "should need row count")
			assert.Equal(tc.udv, len(result.NeedUserDefinedVariables), "count of user defined variables")
			assert.Equal(tc.autocommit, result.NeedsSysVar(sysvars.Autocommit.Name), "should need :__vtautocommit")
			assert.Equal(tc.foreignKeyChecks, result.NeedsSysVar(sysvars.ForeignKeyChecks), "should need :__vtforeignKeyChecks")
			assert.Equal(tc.clientFoundRows, result.NeedsSysVar(sysvars.ClientFoundRows.Name), "should need :__vtclientFoundRows")
			assert.Equal(tc.skipQueryPlanCache, result.NeedsSysVar(sysvars.SkipQueryPlanCache.Name), "should need :__vtskipQueryPlanCache")
			assert.Equal(tc.sqlSelectLimit, result.NeedsSysVar(sysvars.SQLSelectLimit.Name), "should need :__vtsqlSelectLimit")
			assert.Equal(tc.transactionMode, result.NeedsSysVar(sysvars.TransactionMode.Name), "should need :__vttransactionMode")
			assert.Equal(tc.workload, result.NeedsSysVar(sysvars.Workload.Name), "should need :__vtworkload")
			assert.Equal(tc.queryTimeout, result.NeedsSysVar(sysvars.QueryTimeout.Name), "should need :__vtquery_timeout")
			assert.Equal(tc.transactionTimeout, result.NeedsSysVar(sysvars.TransactionTimeout.Name), "should need :__vttransaction_timeout")
			assert.Equal(tc.ddlStrategy, result.NeedsSysVar(sysvars.DDLStrategy.Name), "should need ddlStrategy")
			assert.Equal(tc.migrationContext, result.NeedsSysVar(sysvars.MigrationContext.Name), "should need migrationContext")
			assert.Equal(tc.sessionUUID, result.NeedsSysVar(sysvars.SessionUUID.Name), "should need sessionUUID")
			assert.Equal(tc.sessionEnableSystemSettings, result.NeedsSysVar(sysvars.SessionEnableSystemSettings.Name), "should need sessionEnableSystemSettings")
			assert.Equal(tc.rawGTID, result.NeedsSysVar(sysvars.ReadAfterWriteGTID.Name), "should need rawGTID")
			assert.Equal(tc.rawTimeout, result.NeedsSysVar(sysvars.ReadAfterWriteTimeOut.Name), "should need rawTimeout")
			assert.Equal(tc.sessTrackGTID, result.NeedsSysVar(sysvars.SessionTrackGTIDs.Name), "should need sessTrackGTID")
			assert.Equal(tc.version, result.NeedsSysVar(sysvars.Version.Name), "should need Vitess version")
			assert.Equal(tc.versionComment, result.NeedsSysVar(sysvars.VersionComment.Name), "should need Vitess version")
			assert.Equal(tc.socket, result.NeedsSysVar(sysvars.Socket.Name), "should need :__vtsocket")
		})
	}
}

type fakeViews struct{}

func (*fakeViews) FindView(name TableName) TableStatement {
	if name.Name.String() != "user_details" {
		return nil
	}
	parser := NewTestParser()
	statement, err := parser.Parse("select user.id, user.name, user_extra.salary from user join user_extra where user.id = user_extra.user_id")
	if err != nil {
		return nil
	}
	return statement.(TableStatement)
}

func TestRewritesWithSetVarComment(in *testing.T) {
	tests := []testCaseSetVar{{
		in:            "select 1",
		expected:      "select 1",
		setVarComment: "",
	}, {
		in:            "select 1",
		expected:      "select /*+ AA(a) */ 1",
		setVarComment: "AA(a)",
	}, {
		in:            "insert /* toto */ into t(id) values(1)",
		expected:      "insert /*+ AA(a) */ /* toto */ into t(id) values(1)",
		setVarComment: "AA(a)",
	}, {
		in:            "select  /* toto */ * from t union select * from s",
		expected:      "select /*+ AA(a) */ /* toto */ * from t union select /*+ AA(a) */ * from s",
		setVarComment: "AA(a)",
	}, {
		in:            "vstream /* toto */ * from t1",
		expected:      "vstream /*+ AA(a) */ /* toto */ * from t1",
		setVarComment: "AA(a)",
	}, {
		in:            "stream /* toto */ t from t1",
		expected:      "stream /*+ AA(a) */ /* toto */ t from t1",
		setVarComment: "AA(a)",
	}, {
		in:            "update /* toto */ t set id = 1",
		expected:      "update /*+ AA(a) */ /* toto */ t set id = 1",
		setVarComment: "AA(a)",
	}, {
		in:            "delete /* toto */ from t",
		expected:      "delete /*+ AA(a) */ /* toto */ from t",
		setVarComment: "AA(a)",
	}}

	parser := NewTestParser()
	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := parser.Parse(tc.in)
			require.NoError(err)
			vars := NewReservedVars("v", nil)
			result, err := Normalize(
				stmt,
				vars,
				map[string]*querypb.BindVariable{},
				false,
				"ks",
				0,
				tc.setVarComment,
				map[string]string{},
				nil,
				&fakeViews{},
			)

			require.NoError(err)

			expected, err := parser.Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			assert.Equal(t, String(expected), String(result.AST))
		})
	}
}

func TestRewritesSysVar(in *testing.T) {
	tests := []testCaseSysVar{{
		in:       "select @x = @@sql_mode",
		expected: "select :__vtudvx = @@sql_mode as `@x = @@sql_mode` from dual",
	}, {
		in:       "select @x = @@sql_mode",
		expected: "select :__vtudvx = :__vtsql_mode as `@x = @@sql_mode` from dual",
		sysVar:   map[string]string{"sql_mode": "' '"},
	}, {
		in:       "SELECT @@tx_isolation",
		expected: "select @@tx_isolation from dual",
	}, {
		in:       "SELECT @@transaction_isolation",
		expected: "select @@transaction_isolation from dual",
	}, {
		in:       "SELECT @@session.transaction_isolation",
		expected: "select @@session.transaction_isolation from dual",
	}, {
		in:       "SELECT @@tx_isolation",
		sysVar:   map[string]string{"tx_isolation": "'READ-COMMITTED'"},
		expected: "select :__vttx_isolation as `@@tx_isolation` from dual",
	}, {
		in:       "SELECT @@transaction_isolation",
		sysVar:   map[string]string{"transaction_isolation": "'READ-COMMITTED'"},
		expected: "select :__vttransaction_isolation as `@@transaction_isolation` from dual",
	}, {
		in:       "SELECT @@session.transaction_isolation",
		sysVar:   map[string]string{"transaction_isolation": "'READ-COMMITTED'"},
		expected: "select :__vttransaction_isolation as `@@session.transaction_isolation` from dual",
	}}

	parser := NewTestParser()
	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := parser.Parse(tc.in)
			require.NoError(err)
			vars := NewReservedVars("v", nil)
			result, err := Normalize(
				stmt,
				vars,
				map[string]*querypb.BindVariable{},
				false,
				"ks",
				0,
				"",
				tc.sysVar,
				nil,
				&fakeViews{},
			)

			require.NoError(err)

			expected, err := parser.Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			assert.Equal(t, String(expected), String(result.AST))
		})
	}
}

func TestRewritesWithDefaultKeyspace(in *testing.T) {
	tests := []myTestCase{{
		in:       "SELECT 1 from x.test",
		expected: "SELECT 1 from x.test", // no change
	}, {
		in:       "SELECT x.col as c from x.test",
		expected: "SELECT x.col as c from x.test", // no change
	}, {
		in:       "SELECT 1 from test",
		expected: "SELECT 1 from sys.test",
	}, {
		in:       "SELECT 1 from test as t",
		expected: "SELECT 1 from sys.test as t",
	}, {
		in:       "SELECT 1 from `test 24` as t",
		expected: "SELECT 1 from sys.`test 24` as t",
	}, {
		in:       "SELECT 1, (select 1 from test) from x.y",
		expected: "SELECT 1, (select 1 from sys.test) from x.y",
	}, {
		in:       "SELECT 1 from (select 2 from test) t",
		expected: "SELECT 1 from (select 2 from sys.test) t",
	}, {
		in:       "SELECT 1 from test where exists(select 2 from test)",
		expected: "SELECT 1 from sys.test where exists(select 1 from sys.test)",
	}, {
		in:       "SELECT 1 from dual",
		expected: "SELECT 1 from dual",
	}, {
		in:       "SELECT (select 2 from dual) from DUAL",
		expected: "SELECT 2 as `(select 2 from dual)` from DUAL",
	}}

	parser := NewTestParser()
	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := parser.Parse(tc.in)
			require.NoError(err)
			vars := NewReservedVars("v", nil)
			result, err := Normalize(
				stmt,
				vars,
				map[string]*querypb.BindVariable{},
				false,
				"sys",
				0,
				"",
				map[string]string{},
				nil,
				&fakeViews{},
			)

			require.NoError(err)

			expected, err := parser.Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			assert.Equal(t, String(expected), String(result.AST))
		})
	}
}

func TestReservedVars(t *testing.T) {
	for _, prefix := range []string{"vtg", "bv"} {
		t.Run("prefix_"+prefix, func(t *testing.T) {
			reserved := NewReservedVars(prefix, make(BindVars))
			for i := 1; i < 1000; i++ {
				require.Equal(t, fmt.Sprintf("%s%d", prefix, i), reserved.nextUnusedVar())
			}
		})
	}
}

/*
Skipping ColName, TableName:
BenchmarkNormalize-8     1000000              2205 ns/op             821 B/op         27 allocs/op
Prior to skip:
BenchmarkNormalize-8      500000              3620 ns/op            1461 B/op         55 allocs/op
*/
func BenchmarkNormalize(b *testing.B) {
	parser := NewTestParser()
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	ast, reservedVars, err := parser.Parse2(sql)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := Normalize(ast, NewReservedVars("", reservedVars), map[string]*querypb.BindVariable{}, true, "ks", 0, "", map[string]string{}, nil, nil)
		require.NoError(b, err)
	}
}

func BenchmarkNormalizeTraces(b *testing.B) {
	parser := NewTestParser()
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		b.Run(trace, func(b *testing.B) {
			queries := loadQueries(b, trace)
			if len(queries) > 10000 {
				queries = queries[:10000]
			}

			parsed := make([]Statement, 0, len(queries))
			reservedVars := make([]BindVars, 0, len(queries))
			for _, q := range queries {
				pp, kb, err := parser.Parse2(q)
				if err != nil {
					b.Fatal(err)
				}
				parsed = append(parsed, pp)
				reservedVars = append(reservedVars, kb)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for i, query := range parsed {
					_, err := Normalize(query, NewReservedVars("", reservedVars[i]), map[string]*querypb.BindVariable{}, true, "ks", 0, "", map[string]string{}, nil, nil)
					require.NoError(b, err)
				}
			}
		})
	}
}

func BenchmarkNormalizeVTGate(b *testing.B) {
	const keyspace = "main_keyspace"
	parser := NewTestParser()

	queries := loadQueries(b, "lobsters.sql.gz")
	if len(queries) > 10000 {
		queries = queries[:10000]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			stmt, reservedVars, err := parser.Parse2(sql)
			if err != nil {
				b.Fatal(err)
			}

			query := sql
			statement := stmt
			bindVarNeeds := &BindVarNeeds{}
			bindVars := make(map[string]*querypb.BindVariable)
			_ = IgnoreMaxMaxMemoryRowsDirective(stmt)

			// Normalize if possible and retry.
			if CanNormalize(stmt) || MustRewriteAST(stmt, false) {
				result, err := Normalize(
					stmt,
					NewReservedVars("vtg", reservedVars),
					bindVars,
					true,
					keyspace,
					SQLSelectLimitUnset,
					"",
					nil, /*sysvars*/
					nil,
					nil, /*views*/
				)
				if err != nil {
					b.Fatal(err)
				}
				statement = result.AST
				bindVarNeeds = result.BindVarNeeds
				query = String(statement)
			}

			_ = query
			_ = statement
			_ = bindVarNeeds
		}
	}
}

func randtmpl(template string) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const numberBytes = "0123456789"

	result := []byte(template)
	for i, c := range result {
		switch c {
		case '#':
			result[i] = numberBytes[rand.IntN(len(numberBytes))]
		case '@':
			result[i] = letterBytes[rand.IntN(len(letterBytes))]
		}
	}
	return string(result)
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.IntN(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkNormalizeTPCCBinds(b *testing.B) {
	query := `INSERT IGNORE INTO customer0
(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
values
(:c_id, :c_d_id, :c_w_id, :c_first, :c_middle, :c_last, :c_street_1, :c_street_2, :c_city, :c_state, :c_zip, :c_phone, :c_since, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_ytd_payment, :c_payment_cnt, :c_delivery_cnt, :c_data)`
	benchmarkNormalization(b, []string{query})
}

func BenchmarkNormalizeTPCCInsert(b *testing.B) {
	generateInsert := func(rows int) string {
		var query strings.Builder
		query.WriteString("INSERT IGNORE INTO customer0 (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) values ")
		for i := 0; i < rows; i++ {
			fmt.Fprintf(&query, "(%d, %d, %d, '%s','OE','%s','%s', '%s', '%s', '%s', '%s','%s',NOW(),'%s',50000,%f,-10,10,1,0,'%s' )",
				rand.Int(), rand.Int(), rand.Int(),
				"first-"+randString(rand.IntN(10)),
				randtmpl("last-@@@@"),
				randtmpl("street1-@@@@@@@@@@@@"),
				randtmpl("street2-@@@@@@@@@@@@"),
				randtmpl("city-@@@@@@@@@@@@"),
				randtmpl("@@"), randtmpl("zip-#####"),
				randtmpl("################"),
				"GC", rand.Float64(), randString(300+rand.IntN(200)),
			)
			if i < rows-1 {
				query.WriteString(", ")
			}
		}
		return query.String()
	}

	var queries []string

	for i := 0; i < 1024; i++ {
		queries = append(queries, generateInsert(4))
	}

	benchmarkNormalization(b, queries)
}

func BenchmarkNormalizeTPCC(b *testing.B) {
	templates := []string{
		`SELECT c_discount, c_last, c_credit, w_tax
FROM customer%d AS c
JOIN warehouse%d AS w ON c_w_id=w_id
WHERE w_id = %d
AND c_d_id = %d
AND c_id = %d`,
		`SELECT d_next_o_id, d_tax
FROM district%d
WHERE d_w_id = %d
AND d_id = %d FOR UPDATE`,
		`UPDATE district%d
SET d_next_o_id = %d
WHERE d_id = %d AND d_w_id= %d`,
		`INSERT INTO orders%d
(o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
VALUES (%d,%d,%d,%d,NOW(),%d,%d)`,
		`INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
VALUES (%d,%d,%d)`,
		`SELECT i_price, i_name, i_data
FROM item%d
WHERE i_id = %d`,
		`SELECT s_quantity, s_data, s_dist_%s s_dist
FROM stock%d
WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE`,
		`UPDATE stock%d
SET s_quantity = %d
WHERE s_i_id = %d
AND s_w_id= %d`,
		`INSERT INTO order_line%d
(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
VALUES (%d,%d,%d,%d,%d,%d,%d,%d,'%s')`,
		`UPDATE warehouse%d
SET w_ytd = w_ytd + %d
WHERE w_id = %d`,
		`SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
FROM warehouse%d
WHERE w_id = %d`,
		`UPDATE district%d
SET d_ytd = d_ytd + %d
WHERE d_w_id = %d
AND d_id= %d`,
		`SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
FROM district%d
WHERE d_w_id = %d
AND d_id = %d`,
		`SELECT count(c_id) namecnt
FROM customer%d
WHERE c_w_id = %d
AND c_d_id= %d
AND c_last='%s'`,
		`SELECT c_first, c_middle, c_last, c_street_1,
c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
FROM customer%d
WHERE c_w_id = %d
AND c_d_id= %d
AND c_id=%d FOR UPDATE`,
		`SELECT c_data
FROM customer%d
WHERE c_w_id = %d
AND c_d_id=%d
AND c_id= %d`,
		`UPDATE customer%d
SET c_balance=%f, c_ytd_payment=%f, c_data='%s'
WHERE c_w_id = %d
AND c_d_id=%d
AND c_id=%d`,
		`UPDATE customer%d
SET c_balance=%f, c_ytd_payment=%f
WHERE c_w_id = %d
AND c_d_id=%d
AND c_id=%d`,
		`INSERT INTO history%d
(h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)
VALUES (%d,%d,%d,%d,%d,NOW(),%d,'%s')`,
		`SELECT count(c_id) namecnt
FROM customer%d
WHERE c_w_id = %d
AND c_d_id= %d
AND c_last='%s'`,
		`SELECT c_balance, c_first, c_middle, c_id
FROM customer%d
WHERE c_w_id = %d
AND c_d_id= %d
AND c_last='%s' ORDER BY c_first`,
		`SELECT c_balance, c_first, c_middle, c_last
FROM customer%d
WHERE c_w_id = %d
AND c_d_id=%d
AND c_id=%d`,
		`SELECT o_id, o_carrier_id, o_entry_d
FROM orders%d
WHERE o_w_id = %d
AND o_d_id = %d
AND o_c_id = %d
ORDER BY o_id DESC`,
		`SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM order_line%d WHERE ol_w_id = %d AND ol_d_id = %d  AND ol_o_id = %d`,
		`SELECT no_o_id
FROM new_orders%d
WHERE no_d_id = %d
AND no_w_id = %d
ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE`,
		`DELETE FROM new_orders%d
WHERE no_o_id = %d
AND no_d_id = %d
AND no_w_id = %d`,
		`SELECT o_c_id
FROM orders%d
WHERE o_id = %d
AND o_d_id = %d
AND o_w_id = %d`,
		`UPDATE orders%d
SET o_carrier_id = %d
WHERE o_id = %d
AND o_d_id = %d
AND o_w_id = %d`,
		`UPDATE order_line%d
SET ol_delivery_d = NOW()
WHERE ol_o_id = %d
AND ol_d_id = %d
AND ol_w_id = %d`,
		`SELECT SUM(ol_amount) sm
FROM order_line%d
WHERE ol_o_id = %d
AND ol_d_id = %d
AND ol_w_id = %d`,
		`UPDATE customer%d
SET c_balance = c_balance + %f,
c_delivery_cnt = c_delivery_cnt + 1
WHERE c_id = %d
AND c_d_id = %d
AND c_w_id = %d`,
		`SELECT d_next_o_id
FROM district%d
WHERE d_id = %d AND d_w_id= %d`,
		`SELECT COUNT(DISTINCT(s.s_i_id))
FROM stock%d AS s
JOIN order_line%d AS ol ON ol.ol_w_id=s.s_w_id AND ol.ol_i_id=s.s_i_id
WHERE ol.ol_w_id = %d
AND ol.ol_d_id = %d
AND ol.ol_o_id < %d
AND ol.ol_o_id >= %d
AND s.s_w_id= %d
AND s.s_quantity < %d `,
		`SELECT DISTINCT ol_i_id FROM order_line%d
WHERE ol_w_id = %d AND ol_d_id = %d
AND ol_o_id < %d AND ol_o_id >= %d`,
		`SELECT count(*) FROM stock%d
WHERE s_w_id = %d AND s_i_id = %d
AND s_quantity < %d`,
		`SELECT min(no_o_id) mo
FROM new_orders%d
WHERE no_w_id = %d AND no_d_id = %d`,
		`SELECT o_id FROM orders%d o, (SELECT o_c_id,o_w_id,o_d_id,count(distinct o_id) FROM orders%d WHERE o_w_id=%d AND o_d_id=%d AND o_id > 2100 AND o_id < %d GROUP BY o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t WHERE t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1 `,
		`DELETE FROM order_line%d where ol_w_id=%d AND ol_d_id=%d AND ol_o_id=%d`,
		`DELETE FROM orders%d where o_w_id=%d AND o_d_id=%d and o_id=%d`,
		`DELETE FROM history%d where h_w_id=%d AND h_d_id=%d LIMIT 10`,
	}

	re := regexp.MustCompile(`%\w`)
	repl := func(m string) string {
		switch m {
		case "%s":
			return "RANDOM_STRING"
		case "%d":
			return strconv.Itoa(rand.Int())
		case "%f":
			return strconv.FormatFloat(rand.Float64(), 'f', 8, 64)
		default:
			panic(m)
		}
	}

	var queries []string

	for _, tmpl := range templates {
		for i := 0; i < 128; i++ {
			queries = append(queries, re.ReplaceAllStringFunc(tmpl, repl))
		}
	}

	benchmarkNormalization(b, queries)
}

func benchmarkNormalization(b *testing.B, sqls []string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	parser := NewTestParser()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			stmt, reserved, err := parser.Parse2(sql)
			if err != nil {
				b.Fatalf("%v: %q", err, sql)
			}

			reservedVars := NewReservedVars("vtg", reserved)
			_, err = Normalize(
				stmt,
				reservedVars,
				make(map[string]*querypb.BindVariable),
				true,
				"keyspace0",
				SQLSelectLimitUnset,
				"",
				nil,
				nil,
				nil,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
