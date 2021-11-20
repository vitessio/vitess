package fuzzdata

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
)

var dtNameCounter = 1

func TestName(t *testing.T) {
	fooCol := &sqlparser.ColName{
		Name:      sqlparser.NewColIdent("col"),
		Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("foo")},
	}
	fooId := &sqlparser.ColName{
		Name:      sqlparser.NewColIdent("id"),
		Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("foo")},
	}
	countStar := &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("count"),
		Exprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{}},
	}
	barCol := &sqlparser.ColName{
		Name:      sqlparser.NewColIdent("col"),
		Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("bar")},
	}
	predicate := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     fooId,
		Right:    sqlparser.NewIntLiteral("12"),
	}
	joinCond := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     fooCol,
		Right:    barCol,
	}

	tests := []struct {
		op       operator
		expected string
	}{
		{
			op: &project{
				projections: []*AliasedExpr{
					{Expr: fooCol},
					{Expr: barCol},
				},
				src: &join{
					lhs: &filter{
						src:       &table{name: "foo"},
						predicate: predicate,
					},
					rhs:       &table{name: "bar"},
					condition: joinCond,
				},
			},
			expected: "select foo.col, bar.col from foo, bar where foo.id = 12 and foo.col = bar.col",
		}, {
			op: &vectorAggr{
				src: &join{
					lhs: &filter{
						src:       &table{name: "foo"},
						predicate: predicate,
					},
					rhs:       &table{name: "bar"},
					condition: joinCond,
				},
				groupBy: []*AliasedExpr{{Expr: fooCol}, {Expr: barCol}},
				aggrF:   []*AliasedExpr{{Expr: countStar}},
			},
			expected: "select count(*), foo.col, bar.col from foo, bar where foo.id = 12 and foo.col = bar.col group by foo.col, bar.col",
		}, {
			op: &filter{
				src: &vectorAggr{
					src: &join{
						lhs:       &table{name: "foo"},
						rhs:       &table{name: "bar"},
						condition: joinCond,
					},
					groupBy: []*AliasedExpr{{Expr: fooCol}, {Expr: barCol}},
					aggrF:   []*AliasedExpr{{Expr: countStar}},
				},
				predicate: predicate,
			},
			expected: "select count(*), foo.col, bar.col from foo, bar where foo.col = bar.col group by foo.col, bar.col having foo.id = 12",
		}, {
			op: &join{
				lhs: &vectorAggr{
					src:     &table{name: "foo"},
					groupBy: []*AliasedExpr{{Expr: fooCol}},
					aggrF:   []*AliasedExpr{{Expr: countStar}},
				},
				rhs:       &table{name: "bar"},
				condition: joinCond,
			},
			expected: "select * from (select count(*), dt1.col from foo group by dt1.col) as dt1, bar having dt1.col = bar.col",
		}, {
			op: &join{
				lhs: &vectorAggr{
					src:     &table{name: "foo"},
					groupBy: []*AliasedExpr{{Expr: fooCol}},
					aggrF:   []*AliasedExpr{{Expr: countStar}},
				},
				rhs: &vectorAggr{
					src:     &table{name: "bar"},
					groupBy: []*AliasedExpr{{Expr: barCol}},
					aggrF:   []*AliasedExpr{{Expr: countStar}},
				},
				condition: joinCond,
			},
			expected: "select * from (select count(*), dt2.col from foo group by dt2.col) as dt2, (select count(*), dt1.col from bar group by dt1.col) as dt1 having dt2.col = dt1.col",
		},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			dtNameCounter = 1
			assert.Equal(t, test.expected, sqlparser.String(toSQL(test.op)))
		})
	}
}

func toSQL(op operator) sqlparser.SelectStatement {
	q := &queryBuilder{}
	buildQuery(op, q)
	return q.produce()
}

func buildQuery(op operator, qb *queryBuilder) {
	switch op := op.(type) {
	case *table:
		qb.addTable(op.name)
	case *join:
		buildQuery(op.lhs, qb)
		qbR := &queryBuilder{}
		buildQuery(op.rhs, qbR)
		qb.joinWith(qbR, op.condition)
	case *project:
		buildQuery(op.src, qb)
		qb.project(op.projections)
	case *filter:
		buildQuery(op.src, qb)
		qb.addPredicate(op.predicate)
	case *vectorAggr:
		buildQuery(op.src, qb)
		qb.vectorGroupBy(op.groupBy, op.aggrF)
	default:
		panic(fmt.Sprintf("%T", op))
	}
}

func (qb *queryBuilder) produce() sqlparser.SelectStatement {
	query := qb.sel
	if sel, ok := query.(*sqlparser.Select); ok && len(sel.SelectExprs) == 0 {
		sel.SelectExprs = append(sel.SelectExprs, &sqlparser.StarExpr{})
	}
	return query
}

func (qb *queryBuilder) addTable(t string) {
	if qb.sel == nil {
		qb.sel = &sqlparser.Select{}
	}
	sel := qb.sel.(*sqlparser.Select)
	sel.From = append(sel.From, &sqlparser.AliasedTableExpr{
		Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent(t)},
	})
	qb.sel = sel
	qb.tableNames = append(qb.tableNames, t)
}

func (qb *queryBuilder) addPredicate(expr Expr) {
	sel := qb.sel.(*sqlparser.Select)
	if qb.grouped {
		if sel.Having == nil {
			sel.Having = &sqlparser.Where{
				Type: sqlparser.HavingClause,
				Expr: expr,
			}
		} else {
			sel.Having.Expr = sqlparser.AndExpressions(sel.Having.Expr, expr)
		}
	} else if sel.Where == nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: expr,
		}
	} else {
		sel.Where.Expr = sqlparser.AndExpressions(sel.Where.Expr, expr)
	}
}

func (qb *queryBuilder) vectorGroupBy(grouping []*AliasedExpr, aggrFuncs []*AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	var exprs []sqlparser.SelectExpr
	var groupBy sqlparser.GroupBy
	for _, expr := range aggrFuncs {
		exprs = append(exprs, expr)
	}
	for _, expr := range grouping {
		exprs = append(exprs, expr)
		groupBy = append(groupBy, expr.Expr)
	}
	sel.SelectExprs = append(sel.SelectExprs, exprs...)
	sel.GroupBy = groupBy
	qb.grouped = true
}

func (qb *queryBuilder) project(projections []*AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	var exprs []sqlparser.SelectExpr
	for _, expr := range projections {
		exprs = append(exprs, expr)
	}
	sel.SelectExprs = append(sel.SelectExprs, exprs...)
}

func (qb *queryBuilder) joinWith(other *queryBuilder, onCondition Expr) {

	if other.grouped {
		dtName := other.convertToDerivedTable()
		other.rewriteExprForDerivedTable(onCondition, dtName)
	}

	if qb.grouped {
		dtName := qb.convertToDerivedTable()
		qb.rewriteExprForDerivedTable(onCondition, dtName)
	}

	// neither of the inputs needs to be put into a derived table. we can just merge them together
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = sqlparser.AndExpressions(predicate, otherSel.Where.Expr)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	qb.addPredicate(onCondition)
}

func (qb *queryBuilder) rewriteExprForDerivedTable(expr Expr, dtName string) {
	sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			hasTable := qb.hasTable(node.Qualifier.Name.String())
			if hasTable {
				node.Qualifier = sqlparser.TableName{
					Name: sqlparser.NewTableIdent(dtName),
				}
			}
		}
		return true
	}, nil)
}

func (qb *queryBuilder) hasTable(tableName string) bool {
	for _, name := range qb.tableNames {
		if strings.EqualFold(tableName, name) {
			return true
		}
	}
	return false
}

func (qb *queryBuilder) convertToDerivedTable() string {
	dtName := fmt.Sprintf("dt%d", dtNameCounter)
	dtNameCounter++
	sel := &sqlparser.Select{
		From: []sqlparser.TableExpr{
			&sqlparser.AliasedTableExpr{
				Expr: &sqlparser.DerivedTable{
					Select: qb.sel,
				},
				// TODO: use number generators for dt1
				As: sqlparser.NewTableIdent(dtName),
				// TODO: also alias the column names to avoid collision with other tables
			},
		},
	}
	qb.sel = sel
	return dtName
}

type queryBuilder struct {
	sel        sqlparser.SelectStatement
	grouped    bool
	tableNames []string
}

type (
	Expr        = sqlparser.Expr
	AliasedExpr = sqlparser.AliasedExpr
	operator    interface {
		op()
	}

	table struct {
		name string
	}

	join struct {
		lhs, rhs  operator
		condition Expr
	}

	filter struct {
		src       operator
		predicate Expr
	}

	project struct {
		projections []*AliasedExpr
		src         operator
	}

	vectorAggr struct {
		src     operator
		groupBy []*AliasedExpr
		aggrF   []*AliasedExpr
	}
)

func (*table) op()      {}
func (*join) op()       {}
func (*filter) op()     {}
func (*project) op()    {}
func (*vectorAggr) op() {}
