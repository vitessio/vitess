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
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	parser := NewTestParser()
	query := "select * from t where a = 1"
	tree, err := parser.Parse(query)
	require.NoError(t, err)
	var b strings.Builder
	Append(&b, tree)
	got := b.String()
	want := query
	if got != want {
		t.Errorf("Append: %s, want %s", got, want)
	}
	Append(&b, tree)
	got = b.String()
	want = query + query
	if got != want {
		t.Errorf("Append: %s, want %s", got, want)
	}
}

func TestSelect(t *testing.T) {
	parser := NewTestParser()
	e1, err := parser.ParseExpr("a = 1")
	require.NoError(t, err)
	e2, err := parser.ParseExpr("b = 2")
	require.NoError(t, err)
	t.Run("single predicate where", func(t *testing.T) {
		sel := &Select{}
		sel.AddWhere(e1)
		assert.Equal(t, " where a = 1", String(sel.Where))
	})

	t.Run("single predicate having", func(t *testing.T) {
		sel := &Select{}
		sel.AddHaving(e1)
		assert.Equal(t, " having a = 1", String(sel.Having))
	})

	t.Run("double predicate where", func(t *testing.T) {
		sel := &Select{}
		sel.AddWhere(e1)
		sel.AddWhere(e2)
		assert.Equal(t, " where a = 1 and b = 2", String(sel.Where))
	})

	t.Run("double predicate having", func(t *testing.T) {
		sel := &Select{}
		sel.AddHaving(e1)
		sel.AddHaving(e2)
		assert.Equal(t, " having a = 1 and b = 2", String(sel.Having))
	})
}

func TestUpdate(t *testing.T) {
	parser := NewTestParser()
	tree, err := parser.Parse("update t set a = 1")
	require.NoError(t, err)

	upd, ok := tree.(*Update)
	require.True(t, ok)

	upd.AddWhere(&ComparisonExpr{
		Left:     &ColName{Name: NewIdentifierCI("b")},
		Operator: EqualOp,
		Right:    NewIntLiteral("2"),
	})
	assert.Equal(t, "update t set a = 1 where b = 2", String(upd))

	upd.AddWhere(&ComparisonExpr{
		Left:     &ColName{Name: NewIdentifierCI("c")},
		Operator: EqualOp,
		Right:    NewIntLiteral("3"),
	})
	assert.Equal(t, "update t set a = 1 where b = 2 and c = 3", String(upd))
}

func TestRemoveHints(t *testing.T) {
	parser := NewTestParser()
	for _, query := range []string{
		"select * from t use index (i)",
		"select * from t force index (i)",
	} {
		tree, err := parser.Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		sel := tree.(*Select)
		sel.From = TableExprs{
			sel.From[0].(*AliasedTableExpr).RemoveHints(),
		}
		buf := NewTrackedBuffer(nil)
		sel.Format(buf)
		if got, want := buf.String(), "select * from t"; got != want {
			t.Errorf("stripped query: %s, want %s", got, want)
		}
	}
}

func TestAddOrder(t *testing.T) {
	parser := NewTestParser()
	src, err := parser.Parse("select foo, bar from baz order by foo")
	require.NoError(t, err)
	order := src.(*Select).OrderBy[0]
	dst, err := parser.Parse("select * from t")
	require.NoError(t, err)
	dst.(*Select).AddOrder(order)
	buf := NewTrackedBuffer(nil)
	dst.Format(buf)
	require.Equal(t, "select * from t order by foo asc", buf.String())
	dst, err = parser.Parse("select * from t union select * from s")
	require.NoError(t, err)
	dst.(*Union).AddOrder(order)
	buf = NewTrackedBuffer(nil)
	dst.Format(buf)
	require.Equal(t, "select * from t union select * from s order by foo asc", buf.String())
}

func TestSetLimit(t *testing.T) {
	parser := NewTestParser()
	src, err := parser.Parse("select foo, bar from baz limit 4")
	require.NoError(t, err)
	limit := src.(*Select).Limit
	dst, err := parser.Parse("select * from t")
	require.NoError(t, err)
	dst.(*Select).SetLimit(limit)
	buf := NewTrackedBuffer(nil)
	dst.Format(buf)
	require.Equal(t, "select * from t limit 4", buf.String())
	dst, err = parser.Parse("select * from t union select * from s")
	require.NoError(t, err)
	dst.(*Union).SetLimit(limit)
	buf = NewTrackedBuffer(nil)
	dst.Format(buf)
	require.Equal(t, "select * from t union select * from s limit 4", buf.String())
}

func TestDDL(t *testing.T) {
	testcases := []struct {
		query    string
		output   DDLStatement
		affected []string
	}{{
		query: "create table a",
		output: &CreateTable{
			Table: TableName{Name: NewIdentifierCS("a")},
		},
		affected: []string{"a"},
	}, {
		query: "rename table a to b",
		output: &RenameTable{
			TablePairs: []*RenameTablePair{
				{
					FromTable: TableName{Name: NewIdentifierCS("a")},
					ToTable:   TableName{Name: NewIdentifierCS("b")},
				},
			},
		},
		affected: []string{"a", "b"},
	}, {
		query: "rename table a to b, c to d",
		output: &RenameTable{
			TablePairs: []*RenameTablePair{
				{
					FromTable: TableName{Name: NewIdentifierCS("a")},
					ToTable:   TableName{Name: NewIdentifierCS("b")},
				}, {
					FromTable: TableName{Name: NewIdentifierCS("c")},
					ToTable:   TableName{Name: NewIdentifierCS("d")},
				},
			},
		},
		affected: []string{"a", "b", "c", "d"},
	}, {
		query: "drop table a",
		output: &DropTable{
			FromTables: TableNames{
				TableName{Name: NewIdentifierCS("a")},
			},
		},
		affected: []string{"a"},
	}, {
		query: "drop table a, b",
		output: &DropTable{
			FromTables: TableNames{
				TableName{Name: NewIdentifierCS("a")},
				TableName{Name: NewIdentifierCS("b")},
			},
		},
		affected: []string{"a", "b"},
	}}
	parser := NewTestParser()
	for _, tcase := range testcases {
		got, err := parser.Parse(tcase.query)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tcase.output) {
			t.Errorf("%s: %v, want %v", tcase.query, got, tcase.output)
		}
		want := make(TableNames, 0, len(tcase.affected))
		for _, t := range tcase.affected {
			want = append(want, TableName{Name: NewIdentifierCS(t)})
		}
		if affected := got.(DDLStatement).AffectedTables(); !reflect.DeepEqual(affected, want) {
			t.Errorf("Affected(%s): %v, want %v", tcase.query, affected, want)
		}
	}
}

func TestSetAutocommitON(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("SET autocommit=ON")
	require.NoError(t, err)
	s, ok := stmt.(*Set)
	if !ok {
		t.Errorf("SET statement is not Set: %T", s)
	}

	if len(s.Exprs) < 1 {
		t.Errorf("SET statement has no expressions")
	}

	e := s.Exprs[0]
	switch v := e.Expr.(type) {
	case *Literal:
		if v.Type != StrVal {
			t.Errorf("SET statement value is not StrVal: %T", v)
		}

		if v.Val != "on" {
			t.Errorf("SET statement value want: on, got: %s", v.Val)
		}
	default:
		t.Errorf("SET statement expression is not Literal: %T", e.Expr)
	}

	stmt, err = parser.Parse("SET @@session.autocommit=ON")
	require.NoError(t, err)
	s, ok = stmt.(*Set)
	if !ok {
		t.Errorf("SET statement is not Set: %T", s)
	}

	if len(s.Exprs) < 1 {
		t.Errorf("SET statement has no expressions")
	}

	e = s.Exprs[0]
	switch v := e.Expr.(type) {
	case *Literal:
		if v.Type != StrVal {
			t.Errorf("SET statement value is not StrVal: %T", v)
		}

		if v.Val != "on" {
			t.Errorf("SET statement value want: on, got: %s", v.Val)
		}
	default:
		t.Errorf("SET statement expression is not Literal: %T", e.Expr)
	}
}

func TestSetAutocommitOFF(t *testing.T) {
	parser := NewTestParser()
	stmt, err := parser.Parse("SET autocommit=OFF")
	require.NoError(t, err)
	s, ok := stmt.(*Set)
	if !ok {
		t.Errorf("SET statement is not Set: %T", s)
	}

	if len(s.Exprs) < 1 {
		t.Errorf("SET statement has no expressions")
	}

	e := s.Exprs[0]
	switch v := e.Expr.(type) {
	case *Literal:
		if v.Type != StrVal {
			t.Errorf("SET statement value is not StrVal: %T", v)
		}

		if v.Val != "off" {
			t.Errorf("SET statement value want: on, got: %s", v.Val)
		}
	default:
		t.Errorf("SET statement expression is not Literal: %T", e.Expr)
	}

	stmt, err = parser.Parse("SET @@session.autocommit=OFF")
	require.NoError(t, err)
	s, ok = stmt.(*Set)
	if !ok {
		t.Errorf("SET statement is not Set: %T", s)
	}

	if len(s.Exprs) < 1 {
		t.Errorf("SET statement has no expressions")
	}

	e = s.Exprs[0]
	switch v := e.Expr.(type) {
	case *Literal:
		if v.Type != StrVal {
			t.Errorf("SET statement value is not StrVal: %T", v)
		}

		if v.Val != "off" {
			t.Errorf("SET statement value want: on, got: %s", v.Val)
		}
	default:
		t.Errorf("SET statement expression is not Literal: %T", e.Expr)
	}

}

func TestWhere(t *testing.T) {
	var w *Where
	buf := NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(nil): %q, want \"\"", buf.String())
	}
	w = NewWhere(WhereClause, nil)
	buf = NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(&Where{nil}: %q, want \"\"", buf.String())
	}
}

func TestIsImpossible(t *testing.T) {
	f := ComparisonExpr{
		Operator: NotEqualOp,
		Left:     NewIntLiteral("1"),
		Right:    NewIntLiteral("1"),
	}
	if !f.IsImpossible() {
		t.Error("IsImpossible: false, want true")
	}

	f = ComparisonExpr{
		Operator: EqualOp,
		Left:     NewIntLiteral("1"),
		Right:    NewIntLiteral("1"),
	}
	if f.IsImpossible() {
		t.Error("IsImpossible: true, want false")
	}

	f = ComparisonExpr{
		Operator: NotEqualOp,
		Left:     NewIntLiteral("1"),
		Right:    NewIntLiteral("2"),
	}
	if f.IsImpossible() {
		t.Error("IsImpossible: true, want false")
	}
}

func TestReplaceExpr(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "select * from t where (select a from b)",
		out: ":a",
	}, {
		in:  "select * from t where (select a from b) and b",
		out: ":a and b",
	}, {
		in:  "select * from t where a and (select a from b)",
		out: "a and :a",
	}, {
		in:  "select * from t where (select a from b) or b",
		out: ":a or b",
	}, {
		in:  "select * from t where a or (select a from b)",
		out: "a or :a",
	}, {
		in:  "select * from t where not (select a from b)",
		out: "not :a",
	}, {
		in:  "select * from t where ((select a from b))",
		out: ":a",
	}, {
		in:  "select * from t where (select a from b) = 1",
		out: ":a = 1",
	}, {
		in:  "select * from t where a = (select a from b)",
		out: "a = :a",
	}, {
		in:  "select * from t where a like b escape (select a from b)",
		out: "a like b escape :a",
	}, {
		in:  "select * from t where (select a from b) between a and b",
		out: ":a between a and b",
	}, {
		in:  "select * from t where a between (select a from b) and b",
		out: "a between :a and b",
	}, {
		in:  "select * from t where a between b and (select a from b)",
		out: "a between b and :a",
	}, {
		in:  "select * from t where (select a from b) is null",
		out: ":a is null",
	}, {
		// exists should not replace.
		in:  "select * from t where exists (select a from b)",
		out: "exists (select a from b)",
	}, {
		in:  "select * from t where a in ((select a from b), 1)",
		out: "a in (:a, 1)",
	}, {
		in:  "select * from t where a in (0, (select a from b), 1)",
		out: "a in (0, :a, 1)",
	}, {
		in:  "select * from t where (select a from b) + 1",
		out: ":a + 1",
	}, {
		in:  "select * from t where 1+(select a from b)",
		out: "1 + :a",
	}, {
		in:  "select * from t where -(select a from b)",
		out: "-:a",
	}, {
		in:  "select * from t where (select a from b) collate utf8",
		out: ":a collate utf8",
	}, {
		in:  "select * from t where func((select a from b), 1)",
		out: "func(:a, 1)",
	}, {
		in:  "select * from t where func(1, (select a from b), 1)",
		out: "func(1, :a, 1)",
	}, {
		in:  "select * from t where group_concat((select a from b), 1 order by a)",
		out: "group_concat(:a, 1 order by a asc)",
	}, {
		in:  "select * from t where group_concat(1 order by (select a from b), a)",
		out: "group_concat(1 order by :a asc, a asc)",
	}, {
		in:  "select * from t where group_concat(1 order by a, (select a from b))",
		out: "group_concat(1 order by a asc, :a asc)",
	}, {
		in:  "select * from t where substr(a, (select a from b), b)",
		out: "substr(a, :a, b)",
	}, {
		in:  "select * from t where substr(a, b, (select a from b))",
		out: "substr(a, b, :a)",
	}, {
		in:  "select * from t where convert((select a from b), json)",
		out: "convert(:a, json)",
	}, {
		in:  "select * from t where convert((select a from b) using utf8)",
		out: "convert(:a using utf8)",
	}, {
		in:  "select * from t where case (select a from b) when a then b when b then c else d end",
		out: "case :a when a then b when b then c else d end",
	}, {
		in:  "select * from t where case a when (select a from b) then b when b then c else d end",
		out: "case a when :a then b when b then c else d end",
	}, {
		in:  "select * from t where case a when b then (select a from b) when b then c else d end",
		out: "case a when b then :a when b then c else d end",
	}, {
		in:  "select * from t where case a when b then c when (select a from b) then c else d end",
		out: "case a when b then c when :a then c else d end",
	}, {
		in:  "select * from t where case a when b then c when d then c else (select a from b) end",
		out: "case a when b then c when d then c else :a end",
	}}
	to := NewArgument("a")
	parser := NewTestParser()
	for _, tcase := range tcases {
		t.Run(tcase.in, func(t *testing.T) {
			tree, err := parser.Parse(tcase.in)
			require.NoError(t, err)
			var from *Subquery
			_ = Walk(func(node SQLNode) (kontinue bool, err error) {
				if sq, ok := node.(*Subquery); ok {
					from = sq
					return false, nil
				}
				return true, nil
			}, tree)
			require.NotNilf(t, from, "from is nil for %s", tcase.in)
			expr := ReplaceExpr(tree.(*Select).Where.Expr, from, to)
			assert.Equal(t, tcase.out, String(expr))
		})
	}
}

func TestColNameEqual(t *testing.T) {
	var c1, c2 *ColName
	if c1.Equal(c2) {
		t.Error("nil columns equal, want unequal")
	}
	c1 = &ColName{
		Name: NewIdentifierCI("aa"),
	}
	c2 = &ColName{
		Name: NewIdentifierCI("bb"),
	}
	if c1.Equal(c2) {
		t.Error("columns equal, want unequal")
	}
	c2.Name = NewIdentifierCI("aa")
	if !c1.Equal(c2) {
		t.Error("columns unequal, want equal")
	}
}

func TestIdentifierCI(t *testing.T) {
	str := NewIdentifierCI("Ab")
	if str.String() != "Ab" {
		t.Errorf("String=%s, want Ab", str.String())
	}
	if str.String() != "Ab" {
		t.Errorf("Val=%s, want Ab", str.String())
	}
	if str.Lowered() != "ab" {
		t.Errorf("Val=%s, want ab", str.Lowered())
	}
	if !str.Equal(NewIdentifierCI("aB")) {
		t.Error("str.Equal(NewIdentifierCI(aB))=false, want true")
	}
	if !str.EqualString("ab") {
		t.Error("str.EqualString(ab)=false, want true")
	}
	str = NewIdentifierCI("")
	if str.Lowered() != "" {
		t.Errorf("Val=%s, want \"\"", str.Lowered())
	}
}

func TestIdentifierCIMarshal(t *testing.T) {
	str := NewIdentifierCI("Ab")
	b, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `"Ab"`
	if got != want {
		t.Errorf("json.Marshal()= %s, want %s", got, want)
	}
	var out IdentifierCI
	if err := json.Unmarshal(b, &out); err != nil {
		t.Errorf("Unmarshal err: %v, want nil", err)
	}
	if !reflect.DeepEqual(out, str) {
		t.Errorf("Unmarshal: %v, want %v", out, str)
	}
}

func TestIdentifierCISize(t *testing.T) {
	size := unsafe.Sizeof(NewIdentifierCI(""))
	want := 2 * unsafe.Sizeof("")
	assert.Equal(t, want, size, "size of IdentifierCI")
}

func TestIdentifierCSMarshal(t *testing.T) {
	str := NewIdentifierCS("Ab")
	b, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `"Ab"`
	if got != want {
		t.Errorf("json.Marshal()= %s, want %s", got, want)
	}
	var out IdentifierCS
	if err := json.Unmarshal(b, &out); err != nil {
		t.Errorf("Unmarshal err: %v, want nil", err)
	}
	if !reflect.DeepEqual(out, str) {
		t.Errorf("Unmarshal: %v, want %v", out, str)
	}
}

func TestHexDecode(t *testing.T) {
	testcase := []struct {
		in, out string
	}{{
		in:  "313233",
		out: "123",
	}, {
		in:  "ag",
		out: "encoding/hex: invalid byte: U+0067 'g'",
	}, {
		in:  "777",
		out: "encoding/hex: odd length hex string",
	}}
	for _, tc := range testcase {
		out, err := NewHexLiteral(tc.in).HexDecode()
		if err != nil {
			if err.Error() != tc.out {
				t.Errorf("Decode(%q): %v, want %s", tc.in, err, tc.out)
			}
			continue
		}
		if !bytes.Equal(out, []byte(tc.out)) {
			t.Errorf("Decode(%q): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestCompliantName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "aa",
		out: "aa",
	}, {
		in:  "1a",
		out: "_a",
	}, {
		in:  "a1",
		out: "a1",
	}, {
		in:  "a.b",
		out: "a_b",
	}, {
		in:  ".ab",
		out: "_ab",
	}}
	for _, tc := range testcases {
		out := NewIdentifierCI(tc.in).CompliantName()
		if out != tc.out {
			t.Errorf("IdentifierCI(%s).CompliantNamt: %s, want %s", tc.in, out, tc.out)
		}
		out = NewIdentifierCS(tc.in).CompliantName()
		if out != tc.out {
			t.Errorf("IdentifierCS(%s).CompliantNamt: %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestColumns_FindColumn(t *testing.T) {
	cols := Columns{NewIdentifierCI("a"), NewIdentifierCI("c"), NewIdentifierCI("b"), NewIdentifierCI("0")}

	testcases := []struct {
		in  string
		out int
	}{{
		in:  "a",
		out: 0,
	}, {
		in:  "b",
		out: 2,
	},
		{
			in:  "0",
			out: 3,
		},
		{
			in:  "f",
			out: -1,
		}}

	for _, tc := range testcases {
		val := cols.FindColumn(NewIdentifierCI(tc.in))
		if val != tc.out {
			t.Errorf("FindColumn(%s): %d, want %d", tc.in, val, tc.out)
		}
	}
}

func TestParseMultipleIgnoreEmpty(t *testing.T) {
	testcases := []struct {
		input   string
		stmts   int
		wantErr bool
	}{
		{
			input: "select * from table1; \t; \n; \n\t\t ;select * from table1;",
			stmts: 2,
		}, {
			input: "select 1; ; ; select 2; /* Comment only */; /* Comment */; select 3;;;; /* Comment */",
			stmts: 3,
		}, {
			input: "select * from table1",
			stmts: 1,
		}, {
			input: "select * from table1;",
			stmts: 1,
		}, {
			input: "select * from table1;   ",
			stmts: 1,
		}, {
			input: "select * from table1; select * from table2;",
			stmts: 2,
		}, {
			input: "create /*vt+ directive=true */ table t1 (id int); create table t2 (id int); create table t3 (id int)",
			stmts: 3,
		}, {
			input: "create /*vt+ directive=true */ table t1 (id int); create table t2 (id int); create table t3 (id int);",
			stmts: 3,
		}, {
			input: "select * from /* comment ; */ table1;",
			stmts: 1,
		}, {
			input: "select * from table1 where semi = ';';",
			stmts: 1,
		}, {
			input: "CREATE TABLE `total_data` (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id', " +
				"`region` varchar(32) NOT NULL COMMENT 'region name, like zh; th; kepler'," +
				"`data_size` bigint NOT NULL DEFAULT '0' COMMENT 'data size;'," +
				"`createtime` datetime NOT NULL DEFAULT NOW() COMMENT 'create time;'," +
				"`comment` varchar(100) NOT NULL DEFAULT '' COMMENT 'comment'," +
				"PRIMARY KEY (`id`))",
			stmts: 1,
		}, {
			input: "create table t1 (id int primary key); create table t2 (id int primary key);",
			stmts: 2,
		}, {
			input: ";;; create table t1 (id int primary key);;; ;create table t2 (id int primary key);",
			stmts: 2,
		}, {
			input:   ";create table t1 ;create table t2 (id;",
			wantErr: true,
		}, {
			// Ignore quoted semicolon
			input:   ";create table t1 ';';;;create table t2 (id;",
			wantErr: true,
		},
	}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			statements, err := parser.ParseMultipleIgnoreEmpty(tcase.input)
			if tcase.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.stmts, len(statements))
			}
		})
	}
}

func TestTypeConversion(t *testing.T) {
	ct1 := &ColumnType{Type: "BIGINT"}
	ct2 := &ColumnType{Type: "bigint"}
	assert.Equal(t, ct1.SQLType(), ct2.SQLType())
}

func TestDefaultStatus(t *testing.T) {
	assert.Equal(t,
		String(&Default{ColName: "status"}),
		"default(`status`)")
}

func TestShowTableStatus(t *testing.T) {
	parser := NewTestParser()
	query := "Show Table Status FROM customer"
	tree, err := parser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, tree)
}

func BenchmarkStringTraces(b *testing.B) {
	parser := NewTestParser()
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		b.Run(trace, func(b *testing.B) {
			queries := loadQueries(b, trace)
			if len(queries) > 10000 {
				queries = queries[:10000]
			}

			parsed := make([]Statement, 0, len(queries))
			for _, q := range queries {
				pp, err := parser.Parse(q)
				if err != nil {
					b.Fatal(err)
				}
				parsed = append(parsed, pp)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for _, stmt := range parsed {
					_ = String(stmt)
				}
			}
		})
	}
}

func TestCloneComments(t *testing.T) {
	c := []string{"/*vt+ a=b */"}
	parsedComments := Comments(c).Parsed()
	directives := parsedComments.Directives()
	{
		assert.NotEmpty(t, directives.m)
		val, ok := directives.m["a"]
		assert.Truef(t, ok, "directives map: %v", directives.m)
		assert.Equal(t, "b", val)
	}
	cloned := CloneRefOfParsedComments(parsedComments)
	cloned.ResetDirectives()
	clonedDirectives := cloned.Directives()
	{
		assert.NotEmpty(t, clonedDirectives.m)
		val, ok := clonedDirectives.m["a"]
		assert.Truef(t, ok, "directives map: %v", directives.m)
		assert.Equal(t, "b", val)
	}
	{
		delete(directives.m, "a")
		assert.Empty(t, directives.m)

		val, ok := clonedDirectives.m["a"]
		assert.Truef(t, ok, "directives map: %v", directives.m)
		assert.Equal(t, "b", val)
	}
}
