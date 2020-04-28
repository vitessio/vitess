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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
)

func TestPreview(t *testing.T) {
	testcases := []struct {
		sql  string
		want StatementType
	}{
		{"select ...", StmtSelect},
		{"    select ...", StmtSelect},
		{"(select ...", StmtSelect},
		{"( select ...", StmtSelect},
		{"insert ...", StmtInsert},
		{"replace ....", StmtReplace},
		{"   update ...", StmtUpdate},
		{"Update", StmtUpdate},
		{"UPDATE ...", StmtUpdate},
		{"\n\t    delete ...", StmtDelete},
		{"", StmtUnknown},
		{" ", StmtUnknown},
		{"begin", StmtBegin},
		{" begin", StmtBegin},
		{" begin ", StmtBegin},
		{"\n\t begin ", StmtBegin},
		{"... begin ", StmtUnknown},
		{"begin ...", StmtUnknown},
		{"begin /* ... */", StmtBegin},
		{"begin /* ... *//*test*/", StmtBegin},
		{"begin;", StmtBegin},
		{"begin ;", StmtBegin},
		{"begin; /*...*/", StmtBegin},
		{"start transaction", StmtBegin},
		{"commit", StmtCommit},
		{"commit /*...*/", StmtCommit},
		{"rollback", StmtRollback},
		{"rollback /*...*/", StmtRollback},
		{"create", StmtDDL},
		{"alter", StmtDDL},
		{"rename", StmtDDL},
		{"drop", StmtDDL},
		{"set", StmtSet},
		{"show", StmtShow},
		{"use", StmtUse},
		{"analyze", StmtOther},
		{"describe", StmtExplain},
		{"desc", StmtExplain},
		{"explain", StmtExplain},
		{"repair", StmtOther},
		{"optimize", StmtOther},
		{"grant", StmtPriv},
		{"revoke", StmtPriv},
		{"truncate", StmtDDL},
		{"unknown", StmtUnknown},

		{"/* leading comment */ select ...", StmtSelect},
		{"/* leading comment */ (select ...", StmtSelect},
		{"/* leading comment */ /* leading comment 2 */ select ...", StmtSelect},
		{"/*! MySQL-specific comment */", StmtComment},
		{"/*!50708 MySQL-version comment */", StmtComment},
		{"-- leading single line comment \n select ...", StmtSelect},
		{"-- leading single line comment \n -- leading single line comment 2\n select ...", StmtSelect},

		{"/* leading comment no end select ...", StmtUnknown},
		{"-- leading single line comment no end select ...", StmtUnknown},
	}
	for _, tcase := range testcases {
		if got := Preview(tcase.sql); got != tcase.want {
			t.Errorf("Preview(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsDML(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"   update ...", true},
		{"Update", true},
		{"UPDATE ...", true},
		{"\n\t    delete ...", true},
		{"insert ...", true},
		{"replace ...", true},
		{"select ...", false},
		{"    select ...", false},
		{"", false},
		{" ", false},
	}
	for _, tcase := range testcases {
		if got := IsDML(tcase.sql); got != tcase.want {
			t.Errorf("IsDML(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestSplitAndExpression(t *testing.T) {
	testcases := []struct {
		sql string
		out []string
	}{{
		sql: "select * from t",
		out: nil,
	}, {
		sql: "select * from t where a = 1",
		out: []string{"a = 1"},
	}, {
		sql: "select * from t where a = 1 and b = 1",
		out: []string{"a = 1", "b = 1"},
	}, {
		sql: "select * from t where a = 1 and (b = 1 and c = 1)",
		out: []string{"a = 1", "b = 1", "c = 1"},
	}, {
		sql: "select * from t where a = 1 and (b = 1 or c = 1)",
		out: []string{"a = 1", "b = 1 or c = 1"},
	}, {
		sql: "select * from t where a = 1 and b = 1 or c = 1",
		out: []string{"a = 1 and b = 1 or c = 1"},
	}, {
		sql: "select * from t where a = 1 and b = 1 + (c = 1)",
		out: []string{"a = 1", "b = 1 + (c = 1)"},
	}, {
		sql: "select * from t where (a = 1 and ((b = 1 and c = 1)))",
		out: []string{"a = 1", "b = 1", "c = 1"},
	}}
	for _, tcase := range testcases {
		stmt, err := Parse(tcase.sql)
		assert.NoError(t, err)
		var expr Expr
		if where := stmt.(*Select).Where; where != nil {
			expr = where.Expr
		}
		splits := SplitAndExpression(nil, expr)
		var got []string
		for _, split := range splits {
			got = append(got, String(split))
		}
		assert.Equal(t, tcase.out, got)
	}
}

func TestTableFromStatement(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "select * from t",
		out: "t",
	}, {
		in:  "select * from t.t",
		out: "t.t",
	}, {
		in:  "select * from t1, t2",
		out: "table expression is complex",
	}, {
		in:  "select * from (t)",
		out: "table expression is complex",
	}, {
		in:  "select * from t1 join t2",
		out: "table expression is complex",
	}, {
		in:  "select * from (select * from t) as tt",
		out: "table expression is complex",
	}, {
		in:  "update t set a=1",
		out: "unrecognized statement: update t set a=1",
	}, {
		in:  "bad query",
		out: "syntax error at position 4 near 'bad'",
	}}

	for _, tc := range testcases {
		name, err := TableFromStatement(tc.in)
		var got string
		if err != nil {
			got = err.Error()
		} else {
			got = String(name)
		}
		if got != tc.out {
			t.Errorf("TableFromStatement('%s'): %s, want %s", tc.in, got, tc.out)
		}
	}
}

func TestGetTableName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "select * from t",
		out: "t",
	}, {
		in:  "select * from t.t",
		out: "",
	}, {
		in:  "select * from (select * from t) as tt",
		out: "",
	}}

	for _, tc := range testcases {
		tree, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		out := GetTableName(tree.(*Select).From[0].(*AliasedTableExpr).Expr)
		if out.String() != tc.out {
			t.Errorf("GetTableName('%s'): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestIsColName(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &ColName{},
		out: true,
	}, {
		in: newHexVal(""),
	}}
	for _, tc := range testcases {
		out := IsColName(tc.in)
		if out != tc.out {
			t.Errorf("IsColName(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsValue(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  newStrVal("aa"),
		out: true,
	}, {
		in:  newHexVal("3131"),
		out: true,
	}, {
		in:  newIntVal("1"),
		out: true,
	}, {
		in:  newValArg(":a"),
		out: true,
	}, {
		in:  &NullVal{},
		out: false,
	}}
	for _, tc := range testcases {
		out := IsValue(tc.in)
		if out != tc.out {
			t.Errorf("IsValue(%T): %v, want %v", tc.in, out, tc.out)
		}
		if tc.out {
			// NewPlanValue should not fail for valid values.
			if _, err := NewPlanValue(tc.in); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestIsNull(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &NullVal{},
		out: true,
	}, {
		in: newStrVal(""),
	}}
	for _, tc := range testcases {
		out := IsNull(tc.in)
		if out != tc.out {
			t.Errorf("IsNull(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsSimpleTuple(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  ValTuple{newStrVal("aa")},
		out: true,
	}, {
		in: ValTuple{&ColName{}},
	}, {
		in:  ListArg("::a"),
		out: true,
	}, {
		in: &ColName{},
	}}
	for _, tc := range testcases {
		out := IsSimpleTuple(tc.in)
		if out != tc.out {
			t.Errorf("IsSimpleTuple(%T): %v, want %v", tc.in, out, tc.out)
		}
		if tc.out {
			// NewPlanValue should not fail for valid tuples.
			if _, err := NewPlanValue(tc.in); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestNewPlanValue(t *testing.T) {
	tcases := []struct {
		in  Expr
		out sqltypes.PlanValue
		err string
	}{{
		in: &SQLVal{
			Type: ValArg,
			Val:  []byte(":valarg"),
		},
		out: sqltypes.PlanValue{Key: "valarg"},
	}, {
		in: &SQLVal{
			Type: IntVal,
			Val:  []byte("10"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewInt64(10)},
	}, {
		in: &SQLVal{
			Type: IntVal,
			Val:  []byte("1111111111111111111111111111111111111111"),
		},
		err: "value out of range",
	}, {
		in: &SQLVal{
			Type: StrVal,
			Val:  []byte("strval"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewVarBinary("strval")},
	}, {
		in: &SQLVal{
			Type: BitVal,
			Val:  []byte("01100001"),
		},
		err: "expression is too complex",
	}, {
		in: &SQLVal{
			Type: HexVal,
			Val:  []byte("3131"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewVarBinary("11")},
	}, {
		in: &SQLVal{
			Type: HexVal,
			Val:  []byte("313"),
		},
		err: "odd length hex string",
	}, {
		in:  ListArg("::list"),
		out: sqltypes.PlanValue{ListKey: "list"},
	}, {
		in: ValTuple{
			&SQLVal{
				Type: ValArg,
				Val:  []byte(":valarg"),
			},
			&SQLVal{
				Type: StrVal,
				Val:  []byte("strval"),
			},
		},
		out: sqltypes.PlanValue{
			Values: []sqltypes.PlanValue{{
				Key: "valarg",
			}, {
				Value: sqltypes.NewVarBinary("strval"),
			}},
		},
	}, {
		in: ValTuple{
			ListArg("::list"),
		},
		err: "unsupported: nested lists",
	}, {
		in:  &NullVal{},
		out: sqltypes.PlanValue{},
	}, {
		in: &SQLVal{
			Type: FloatVal,
			Val:  []byte("2.1"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewFloat64(2.1)},
	}}
	for _, tc := range tcases {
		got, err := NewPlanValue(tc.in)
		if tc.err != "" {
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("NewPlanValue(%s) error: %v, want '%s'", String(tc.in), err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(tc.out, got) {
			t.Errorf("NewPlanValue(%s): %v, want %v", String(tc.in), got, tc.out)
		}
	}
}

func newStrVal(in string) *SQLVal {
	return NewStrVal([]byte(in))
}

func newIntVal(in string) *SQLVal {
	return NewIntVal([]byte(in))
}

func newHexVal(in string) *SQLVal {
	return NewHexVal([]byte(in))
}

func newValArg(in string) *SQLVal {
	return NewValArg([]byte(in))
}
