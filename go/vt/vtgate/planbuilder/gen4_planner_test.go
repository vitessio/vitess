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

package planbuilder

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBindingSubquery(t *testing.T) {
	testcases := []struct {
		query            string
		requiredTableSet semantics.TableSet
		extractor        func(p *sqlparser.Select) sqlparser.Expr
		rewrite          bool
	}{
		{
			query:            "select (select col from tabl limit 1) as a from foo join tabl order by a + 1",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return sel.OrderBy[0].Expr
			},
			rewrite: true,
		}, {
			query:            "select t.a from (select (select col from tabl limit 1) as a from foo join tabl) t",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: true,
		}, {
			query:            "select (select col from tabl where foo.id = 4 limit 1) as a from foo",
			requiredTableSet: semantics.SingleTableSet(0),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: false,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(testcase.query)
			require.NoError(t, err)
			selStmt := parse.(*sqlparser.Select)
			semTable, err := semantics.Analyze(selStmt, "d", &semantics.FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewTableIdent("tabl")},
					"foo":  {Name: sqlparser.NewTableIdent("foo")},
				},
			})
			require.NoError(t, err)
			if testcase.rewrite {
				err = queryRewrite(semTable, sqlparser.NewReservedVars("vt", make(sqlparser.BindVars)), selStmt)
				require.NoError(t, err)
			}
			expr := testcase.extractor(selStmt)
			tableset := semTable.RecursiveDeps(expr)
			require.Equal(t, testcase.requiredTableSet, tableset)
		})
	}
}

func extractExpr(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestOptimizeQuery(t *testing.T) {
	testcases := []struct {
		query  string
		result string
	}{
		{
			query: "select id from unsharded where exists(select user_id from user_extra where user_id = 3 and user_id < unsharded.id)",
			result: `SubqueryTree: {
	Outer:	RouteTree{
		Opcode: SelectUnsharded,
		Tables: unsharded,
		Predicates: <nil>,
		ColNames: unsharded.id,
		LeftJoins: 
	}
	Inner:	RouteTree{
		Opcode: SelectEqualUnique,
		Tables: user_extra,
		Predicates: user_id = 3 and user_id < :unsharded_id,
		ColNames: ,
		LeftJoins: 
	}
	ExtractedSubQuery::__sq_has_values1
	Vars:map[unsharded_id:0]
}`,
		}, {
			query: "select id from user_extra where 4 < user_extra.id",
			result: `RouteTree{
	Opcode: SelectScatter,
	Tables: user_extra,
	Predicates: 4 < user_extra.id,
	ColNames: ,
	LeftJoins: 
}`,
		}, {
			query: "select id from user_extra join unsharded where 4 < user_extra.id and unsharded.col = user_extra.id",
			result: `Join: {
	LHS: 	RouteTree{
		Opcode: SelectScatter,
		Tables: user_extra,
		Predicates: 4 < user_extra.id,
		ColNames: user_extra.id,
		LeftJoins: 
	}
	RHS: 	RouteTree{
		Opcode: SelectUnsharded,
		Tables: unsharded,
		Predicates: unsharded.col = :user_extra_id,
		ColNames: ,
		LeftJoins: 
	}
	JoinVars: map[user_extra_id:0]
	Columns: []
}`,
		}, {
			query: "select t.x from (select id from user_extra) t(x) where t.x = 4",
			result: `Derived t: {
	Inner:	RouteTree{
		Opcode: SelectScatter,
		Tables: user_extra,
		Predicates: id = 4,
		ColNames: ,
		LeftJoins: 
	}
	ColumnAliases:(x)
	Columns:
}`,
		}, {
			query: "select id from unsharded where exists(select user_id from user_extra where user_id = 3)",
			result: `SubqueryTree: {
	Outer:	RouteTree{
		Opcode: SelectUnsharded,
		Tables: unsharded,
		Predicates: :__sq_has_values1,
		ColNames: ,
		LeftJoins: 
	}
	Inner:	RouteTree{
		Opcode: SelectEqualUnique,
		Tables: user_extra,
		Predicates: user_id = 3,
		ColNames: ,
		LeftJoins: 
	}
	ExtractedSubQuery::__sq_has_values1
}`,
		}, {
			query: "select id, keyspace_id, range_start, range_end, hex_keyspace_id, shard from user_index where id = :id",
			result: `Vindex: {
	Opcode:1
	Table:user_index
	Vindex:user_index
	Solved:TableSet{0}
	Columns:
}`,
		}, {
			query: "select id from user_extra union select id from unsharded order by 1",
			result: `Concatenate(distinct) {
	RouteTree{
		Opcode: SelectScatter,
		Tables: user_extra,
		Predicates: <nil>,
		ColNames: ,
		LeftJoins: 
	},
	RouteTree{
		Opcode: SelectUnsharded,
		Tables: unsharded,
		Predicates: <nil>,
		ColNames: ,
		LeftJoins: 
	},
	order by id asc
}`,
		}, {
			query: "select unsharded_a.col from unsharded_a join unsharded_b on unsharded_a.col+(select col from user_extra)",
			result: `SubqueryTree: {
	Outer:	RouteTree{
		Opcode: SelectUnsharded,
		Tables: unsharded_a,unsharded_b,
		Predicates: unsharded_a.col + :__sq1,
		ColNames: ,
		LeftJoins: 
	}
	Inner:	RouteTree{
		Opcode: SelectScatter,
		Tables: user_extra,
		Predicates: <nil>,
		ColNames: ,
		LeftJoins: 
	}
	ExtractedSubQuery::__sq1
}`,
		},
	}

	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json"),
	}

	for _, testcase := range testcases {
		t.Run(testcase.query, func(t *testing.T) {
			node, err := sqlparser.Parse(testcase.query)
			require.NoError(t, err)
			selStmt := node.(sqlparser.SelectStatement)
			reservedVars := sqlparser.NewReservedVars("vt", make(sqlparser.BindVars))
			ksName := ""
			if ks, _ := vschema.DefaultKeyspace(); ks != nil {
				ksName = ks.Name
			}
			semTable, err := semantics.Analyze(selStmt, ksName, vschema)
			require.NoError(t, err)
			err = queryRewrite(semTable, reservedVars, selStmt)
			require.NoError(t, err)
			ctx := newPlanningContext(reservedVars, semTable, vschema)
			opTree, err := abstract.CreateOperatorFromAST(selStmt, semTable)
			require.NoError(t, err)
			err = opTree.CheckValid()
			require.NoError(t, err)

			tree, err := optimizeQuery(ctx, opTree)
			require.NoError(t, err)
			require.EqualValues(t, testcase.result, getQueryTreeString(tree))
			if t.Failed() {
				fmt.Println(getQueryTreeString(tree))
			}
		})
	}
}

func getQueryTreeString(tree queryTree) string {
	switch tree := tree.(type) {
	case *routeTree:
		return fmt.Sprintf(`RouteTree{
	Opcode: %s,
	Tables: %s,
	Predicates: %s,
	ColNames: %s,
	LeftJoins: %s
}`, tree.routeOpCode.String(), getRelationString(tree.tables), sqlparser.String(sqlparser.AndExpressions(tree.predicates...)), getColmnsString(tree.columns), getOuterTablesString(tree.leftJoins))
	case *joinTree:
		leftStr := indent(getQueryTreeString(tree.lhs))
		rightStr := indent(getQueryTreeString(tree.rhs))
		if tree.leftJoin {
			return fmt.Sprintf("OuterJoin: {\n\tInner: %s\n\tOuter: %s\n\tJoinVars: %v\n\tColumns: %v\n}", leftStr, rightStr, tree.vars, tree.columns)
		}
		return fmt.Sprintf("Join: {\n\tLHS: %s\n\tRHS: %s\n\tJoinVars: %v\n\tColumns: %v\n}", leftStr, rightStr, tree.vars, tree.columns)
	case *derivedTree:
		inner := indent(getQueryTreeString(tree.inner))
		return fmt.Sprintf("Derived %s: {\n\tInner:%s\n\tColumnAliases:%s\n\tColumns:%s\n}", tree.alias, inner, sqlparser.String(tree.columnAliases), getColmnsString(tree.columns))
	case *subqueryTree:
		inner := indent(getQueryTreeString(tree.inner))
		outer := indent(getQueryTreeString(tree.outer))
		return fmt.Sprintf("SubqueryTree: {\n\tOuter:%s\n\tInner:%s\n\tExtractedSubQuery:%s\n}", outer, inner, sqlparser.String(tree.extracted))
	case *correlatedSubqueryTree:
		inner := indent(getQueryTreeString(tree.inner))
		outer := indent(getQueryTreeString(tree.outer))
		return fmt.Sprintf("SubqueryTree: {\n\tOuter:%s\n\tInner:%s\n\tExtractedSubQuery:%s\n\tVars:%v\n}", outer, inner, sqlparser.String(tree.extracted), tree.vars)
	case *vindexTree:
		return fmt.Sprintf("Vindex: {\n\tOpcode:%d\n\tTable:%s\n\tVindex:%s\n\tSolved:%v\n\tColumns:%s\n}", tree.opCode, getRelationString(tree.table), tree.vindex.String(), tree.solved, getColmnsString(tree.columns))
	case *concatenateTree:
		var inners []string
		for _, source := range tree.sources {
			inners = append(inners, indent(getQueryTreeString(source)))
		}
		if len(tree.ordering) > 0 {
			inners = append(inners, indent(sqlparser.String(tree.ordering)[1:]))
		}
		if tree.limit != nil {
			inners = append(inners, indent(sqlparser.String(tree.limit)[1:]))
		}
		dist := ""
		if tree.distinct {
			dist = "(distinct)"
		}
		return fmt.Sprintf("Concatenate%s {\n%s\n}", dist, strings.Join(inners, ",\n"))
	}
	return fmt.Sprintf("implement me: %T", tree)
}

func getOuterTablesString(joins []*outerTable) string {
	var res []string
	for _, join := range joins {
		res = append(res, fmt.Sprintf("[Relation: %s, Pred: %s]", getRelationString(join.right), sqlparser.String(join.pred)))
	}
	return strings.Join(res, ",")
}

func getColmnsString(columns []*sqlparser.ColName) string {
	var res []string
	for _, col := range columns {
		res = append(res, sqlparser.String(col))
	}
	return strings.Join(res, ",")
}

func getRelationString(tables relation) string {
	return strings.Join(tables.tableNames(), ",")
}

func indent(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = "\t" + line
	}
	return strings.Join(lines, "\n")
}
