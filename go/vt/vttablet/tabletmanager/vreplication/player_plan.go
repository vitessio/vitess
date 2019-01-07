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

package vreplication

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type playerPlan struct {
	vstreamFilter *binlogdatapb.Filter
	tablePlans    map[string]*tablePlan
}

type tablePlan struct {
	name       string
	colExprs   []*colExpr
	onInsert   insertType
	updateCols []int

	fields []*querypb.Field
	pkCols []*colExpr
}

func (tp *tablePlan) findCol(name sqlparser.ColIdent) *colExpr {
	for _, cExpr := range tp.colExprs {
		if cExpr.colname.Equal(name) {
			return cExpr
		}
	}
	return nil
}

type colExpr struct {
	colname sqlparser.ColIdent
	colnum  int
	op      operation
}

type operation int

const (
	opNone = operation(iota)
	opCount
	opSum
	opExclude
)

type insertType int

const (
	insertNormal = insertType(iota)
	insertOndup
	insertIgnore
)

func buildPlayerPlan(filter *binlogdatapb.Filter) (*playerPlan, error) {
	plan := &playerPlan{
		vstreamFilter: &binlogdatapb.Filter{
			Rules: make([]*binlogdatapb.Rule, len(filter.Rules)),
		},
		tablePlans: make(map[string]*tablePlan),
	}
	for i, rule := range filter.Rules {
		if strings.HasPrefix(rule.Match, "/") {
			plan.vstreamFilter.Rules[i] = rule
			continue
		}
		sendRule, tplan, err := buildTablePlan(rule)
		if err != nil {
			return nil, err
		}
		if tplan == nil {
			continue
		}
		plan.vstreamFilter.Rules[i] = sendRule
		plan.tablePlans[rule.Match] = tplan
	}
	return plan, nil
}

func buildTablePlan(rule *binlogdatapb.Rule) (*binlogdatapb.Rule, *tablePlan, error) {
	statement, err := sqlparser.Parse(rule.Filter)
	if err != nil {
		return nil, nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	if len(sel.From) > 1 {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	fromTable := sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}

	if _, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); ok {
		if len(sel.SelectExprs) != 1 {
			return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
		}
		sendRule := &binlogdatapb.Rule{
			Match:  fromTable.String(),
			Filter: rule.Filter,
		}
		return sendRule, &tablePlan{name: rule.Match}, nil
	}

	tplan := &tablePlan{}
	sendSelect := &sqlparser.Select{
		From:  sel.From,
		Where: sel.Where,
	}
	for _, expr := range sel.SelectExprs {
		selExpr, cExpr, err := analyzeExpr(expr)
		if err != nil {
			return nil, nil, err
		}
		if selExpr != nil {
			sendSelect.SelectExprs = append(sendSelect.SelectExprs, selExpr)
			cExpr.colnum = len(sendSelect.SelectExprs) - 1
		}
		tplan.colExprs = append(tplan.colExprs, cExpr)
	}

	if sel.GroupBy != nil {
		if err := analyzeGroupBy(sel.GroupBy, tplan); err != nil {
			return nil, nil, err
		}
	}
	sendRule := &binlogdatapb.Rule{
		Match:  rule.Match,
		Filter: sqlparser.String(sendSelect),
	}
	return sendRule, tplan, nil
}

func analyzeExpr(selExpr sqlparser.SelectExpr) (sqlparser.SelectExpr, *colExpr, error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(selExpr))
	}
	as := aliased.As
	if as.IsEmpty() {
		as = sqlparser.NewColIdent(sqlparser.String(aliased.Expr))
	}
	switch expr := aliased.Expr.(type) {
	case *sqlparser.ColName:
		return selExpr, &colExpr{colname: as}, nil
	case *sqlparser.FuncExpr:
		if expr.Distinct || len(expr.Exprs) != 1 {
			return nil, nil, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
		if aliased.As.IsEmpty() {
			return nil, nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "month", "day", "hour":
			return selExpr, &colExpr{colname: as}, nil
		case "count":
			if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); !ok {
				return nil, nil, fmt.Errorf("only count(*) is supported: %v", sqlparser.String(expr))
			}
			return nil, &colExpr{colname: as, op: opCount}, nil
		case "sum":
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, nil, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			return &sqlparser.AliasedExpr{Expr: innerCol}, &colExpr{colname: as, op: opSum}, nil
		default:
			return nil, nil, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
	default:
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
}

func analyzeGroupBy(groupBy sqlparser.GroupBy, tplan *tablePlan) error {
	for _, expr := range groupBy {
		colname, ok := expr.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
		cExpr := tplan.findCol(colname.Name)
		if cExpr == nil {
			return fmt.Errorf("group by expression does not reference an alias in the select list: %v", sqlparser.String(expr))
		}
		if cExpr.op != opNone {
			return fmt.Errorf("group by expression is not allowed to reference an aggregate expression: %v", sqlparser.String(expr))
		}
		cExpr.op = opExclude
	}
	return nil
}
