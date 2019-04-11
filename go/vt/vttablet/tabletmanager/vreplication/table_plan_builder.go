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
	"regexp"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	bvNone = bindvarMode(iota)
	bvBefore
	bvAfter

	// The following values are the various colExpr opcodes.
	opExpr = operation(iota)
	opCount
	opSum
	// The following values are the various insert types.
	insertNormal = insertType(iota)
	insertOndup
	insertIgnore
)

type tablePlanBuilder struct {
	name       sqlparser.TableIdent
	sendSelect *sqlparser.Select
	selColumns map[string]bool
	colExprs   []*colExpr
	onInsert   insertType
	pkCols     []*colExpr
}

// colExpr describes the processing to be performed to
// compute the value of the target table column.
type colExpr struct {
	colName sqlparser.ColIdent
	// operation==opExpr: full expression is set
	// operation==opCount: nothing is set.
	// operation==opSum: for 'sum(a)', expr is set to 'a'.
	operation operation
	expr      sqlparser.Expr
	// references contains all the column names referenced in the expression.
	references map[string]bool

	isGrouped bool
	isPK      bool
}

// operation is the opcode for the colExpr.
type operation int

// insertType describes the type of insert statement to generate.
type insertType int

// buildPlayerPlan builds a PlayerPlan from the input filter.
// The filter is matched against the target schema. For every table matched,
// a table-specific rule is built to be sent to the source. We don't send the
// original rule to the source because it may not match the same tables as the
// target.
func buildPlayerPlan(filter *binlogdatapb.Filter, tableKeys map[string][]string) (*PlayerPlan, error) {
	plan := &PlayerPlan{
		VStreamFilter: &binlogdatapb.Filter{},
		TargetTables:  make(map[string]*TablePlan),
		TablePlans:    make(map[string]*TablePlan),
	}
	for tableName := range tableKeys {
		for _, rule := range filter.Rules {
			switch {
			case strings.HasPrefix(rule.Match, "/"):
				expr := strings.Trim(rule.Match, "/")
				result, err := regexp.MatchString(expr, tableName)
				if err != nil {
					return nil, err
				}
				if !result {
					continue
				}
				sendRule := &binlogdatapb.Rule{
					Match:  tableName,
					Filter: buildQuery(tableName, rule.Filter),
				}
				plan.VStreamFilter.Rules = append(plan.VStreamFilter.Rules, sendRule)
				tablePlan := &TablePlan{
					Name:     tableName,
					SendRule: sendRule,
				}
				plan.TargetTables[tableName] = tablePlan
				plan.TablePlans[tableName] = tablePlan
			case rule.Match == tableName:
				sendRule, tablePlan, err := buildTablePlan(rule, tableKeys)
				if err != nil {
					return nil, err
				}
				if _, ok := plan.TablePlans[sendRule.Match]; ok {
					continue
				}
				plan.VStreamFilter.Rules = append(plan.VStreamFilter.Rules, sendRule)
				plan.TargetTables[tableName] = tablePlan
				plan.TablePlans[sendRule.Match] = tablePlan
			}
		}
	}
	return plan, nil
}

func buildQuery(tableName, filter string) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select * from %v", sqlparser.NewTableIdent(tableName))
	if filter != "" {
		buf.Myprintf(" where in_keyrange(%v)", sqlparser.NewStrVal([]byte(filter)))
	}
	return buf.String()
}

func buildTablePlan(rule *binlogdatapb.Rule, tableKeys map[string][]string) (*binlogdatapb.Rule, *TablePlan, error) {
	sel, fromTable, err := analyzeSelectFrom(rule.Filter)
	if err != nil {
		return nil, nil, err
	}
	sendRule := &binlogdatapb.Rule{
		Match: fromTable,
	}

	if expr, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); ok {
		if len(sel.SelectExprs) != 1 {
			return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
		}
		if !expr.TableName.IsEmpty() {
			return nil, nil, fmt.Errorf("unsupported qualifier for '*' expression: %v", sqlparser.String(expr))
		}
		sendRule.Filter = rule.Filter
		tablePlan := &TablePlan{
			Name:     rule.Match,
			SendRule: sendRule,
		}
		return sendRule, tablePlan, nil
	}

	tpb := &tablePlanBuilder{
		name: sqlparser.NewTableIdent(rule.Match),
		sendSelect: &sqlparser.Select{
			From:  sel.From,
			Where: sel.Where,
		},
		selColumns: make(map[string]bool),
	}

	if err := tpb.analyzeExprs(sel.SelectExprs); err != nil {
		return nil, nil, err
	}
	if err := tpb.analyzeGroupBy(sel.GroupBy); err != nil {
		return nil, nil, err
	}
	if err := tpb.analyzePK(tableKeys); err != nil {
		return nil, nil, err
	}

	sendRule.Filter = sqlparser.String(tpb.sendSelect)
	tablePlan := tpb.generate(tableKeys)
	tablePlan.SendRule = sendRule
	return sendRule, tablePlan, nil
}

func buildTablePlanFromFields(tableName string, fields []*querypb.Field, tableKeys map[string][]string) (*TablePlan, error) {
	tpb := &tablePlanBuilder{
		name: sqlparser.NewTableIdent(tableName),
	}
	for _, field := range fields {
		colName := sqlparser.NewColIdent(field.Name)
		cexpr := &colExpr{
			colName: colName,
			expr: &sqlparser.ColName{
				Name: colName,
			},
			references: map[string]bool{
				field.Name: true,
			},
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	if err := tpb.analyzePK(tableKeys); err != nil {
		return nil, err
	}
	return tpb.generate(tableKeys), nil
}

func (tpb *tablePlanBuilder) generate(tableKeys map[string][]string) *TablePlan {
	refmap := make(map[string]bool)
	for _, cexpr := range tpb.pkCols {
		for k := range cexpr.references {
			refmap[k] = true
		}
	}
	pkrefs := make([]string, 0, len(refmap))
	for k := range refmap {
		pkrefs = append(pkrefs, k)
	}
	sort.Strings(pkrefs)
	return &TablePlan{
		Name:         tpb.name.String(),
		PKReferences: pkrefs,
		Insert:       tpb.generateInsertStatement(),
		Update:       tpb.generateUpdateStatement(),
		Delete:       tpb.generateDeleteStatement(),
	}
}

func analyzeSelectFrom(query string) (sel *sqlparser.Select, from string, err error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, "", err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	if sel.Distinct != "" {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	if len(sel.From) > 1 {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	fromTable := sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	return sel, fromTable.String(), nil
}

func (tpb *tablePlanBuilder) analyzeExprs(selExprs sqlparser.SelectExprs) error {
	for _, selExpr := range selExprs {
		cexpr, err := tpb.analyzeExpr(selExpr)
		if err != nil {
			return err
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	return nil
}

func (tpb *tablePlanBuilder) analyzeExpr(selExpr sqlparser.SelectExpr) (*colExpr, error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(selExpr))
	}
	as := aliased.As
	if as.IsEmpty() {
		as = sqlparser.NewColIdent(sqlparser.String(aliased.Expr))
	}
	cexpr := &colExpr{
		colName:    as,
		references: make(map[string]bool),
	}
	if expr, ok := aliased.Expr.(*sqlparser.FuncExpr); ok {
		if expr.Distinct || len(expr.Exprs) != 1 {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		if aliased.As.IsEmpty() {
			return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "count":
			if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); !ok {
				return nil, fmt.Errorf("only count(*) is supported: %v", sqlparser.String(expr))
			}
			cexpr.operation = opCount
			return cexpr, nil
		case "sum":
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			if !innerCol.Qualifier.IsEmpty() {
				return nil, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(innerCol))
			}
			cexpr.operation = opSum
			cexpr.expr = innerCol
			tpb.addCol(innerCol.Name)
			cexpr.references[innerCol.Name.Lowered()] = true
			return cexpr, nil
		}
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !node.Qualifier.IsEmpty() {
				return false, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(node))
			}
			tpb.addCol(node.Name)
			cexpr.references[node.Name.Lowered()] = true
		case *sqlparser.Subquery:
			return false, fmt.Errorf("unsupported subquery: %v", sqlparser.String(node))
		case *sqlparser.FuncExpr:
			if node.IsAggregate() {
				return false, fmt.Errorf("unexpected: %v", sqlparser.String(node))
			}
		}
		return true, nil
	}, aliased.Expr)
	if err != nil {
		return nil, err
	}
	cexpr.expr = aliased.Expr
	return cexpr, nil
}

func (tpb *tablePlanBuilder) addCol(ident sqlparser.ColIdent) {
	if tpb.selColumns[ident.Lowered()] {
		return
	}
	tpb.selColumns[ident.Lowered()] = true
	tpb.sendSelect.SelectExprs = append(tpb.sendSelect.SelectExprs, &sqlparser.AliasedExpr{
		Expr: &sqlparser.ColName{Name: ident},
	})
}

func (tpb *tablePlanBuilder) analyzeGroupBy(groupBy sqlparser.GroupBy) error {
	if groupBy == nil {
		return nil
	}
	for _, expr := range groupBy {
		colname, ok := expr.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		cexpr := tpb.findCol(colname.Name)
		if cexpr == nil {
			return fmt.Errorf("group by expression does not reference an alias in the select list: %v", sqlparser.String(expr))
		}
		if cexpr.operation != opExpr {
			return fmt.Errorf("group by expression is not allowed to reference an aggregate expression: %v", sqlparser.String(expr))
		}
		cexpr.isGrouped = true
	}
	tpb.onInsert = insertIgnore
	for _, cExpr := range tpb.colExprs {
		if !cExpr.isGrouped {
			tpb.onInsert = insertOndup
			break
		}
	}
	return nil
}

func (tpb *tablePlanBuilder) analyzePK(tableKeys map[string][]string) error {
	pkcols, ok := tableKeys[tpb.name.String()]
	if !ok {
		return fmt.Errorf("table %s not found in schema", tpb.name)
	}
	for _, pkcol := range pkcols {
		cexpr := tpb.findCol(sqlparser.NewColIdent(pkcol))
		if cexpr == nil {
			return fmt.Errorf("primary key column %s not found in select list", pkcol)
		}
		if cexpr.operation != opExpr {
			return fmt.Errorf("primary key column %s is not allowed to reference an aggregate expression", pkcol)
		}
		cexpr.isPK = true
		tpb.pkCols = append(tpb.pkCols, cexpr)
	}
	return nil
}

func (tpb *tablePlanBuilder) findCol(name sqlparser.ColIdent) *colExpr {
	for _, cexpr := range tpb.colExprs {
		if cexpr.colName.Equal(name) {
			return cexpr
		}
	}
	return nil
}

func (tpb *tablePlanBuilder) generateInsertStatement() *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	if tpb.onInsert == insertIgnore {
		buf.Myprintf("insert ignore into %v set ", tpb.name)
	} else {
		buf.Myprintf("insert into %v set ", tpb.name)
	}
	tpb.generateInsertValues(buf, bvf)
	if tpb.onInsert == insertOndup {
		buf.Myprintf(" on duplicate key update ")
		tpb.generateUpdate(buf, bvf, false /* before */, true /* after */)
	}
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateUpdateStatement() *sqlparser.ParsedQuery {
	if tpb.onInsert == insertIgnore {
		return tpb.generateInsertStatement()
	}
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	buf.Myprintf("update %v set ", tpb.name)
	tpb.generateUpdate(buf, bvf, true /* before */, true /* after */)
	tpb.generateWhere(buf, bvf)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateDeleteStatement() *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	switch tpb.onInsert {
	case insertNormal:
		buf.Myprintf("delete from %v", tpb.name)
		tpb.generateWhere(buf, bvf)
	case insertOndup:
		buf.Myprintf("update %v set ", tpb.name)
		tpb.generateUpdate(buf, bvf, true /* before */, false /* after */)
		tpb.generateWhere(buf, bvf)
	case insertIgnore:
		return nil
	}
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateInsertValues(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	bvf.mode = bvAfter
	separator := ""
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s%s=", separator, cexpr.colName.String())
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			buf.Myprintf("%v", cexpr.expr)
		case opCount:
			buf.WriteString("1")
		case opSum:
			buf.Myprintf("ifnull(%v, 0)", cexpr.expr)
		}
	}
}

func (tpb *tablePlanBuilder) generateUpdate(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter, before, after bool) {
	separator := ""
	for _, cexpr := range tpb.colExprs {
		if cexpr.isGrouped || cexpr.isPK {
			continue
		}
		buf.Myprintf("%s%s=", separator, cexpr.colName.String())
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			if after {
				bvf.mode = bvAfter
				buf.Myprintf("%v", cexpr.expr)
			} else {
				buf.WriteString("null")
			}
		case opCount:
			switch {
			case before && after:
				buf.Myprintf("%s", cexpr.colName.String())
			case before:
				buf.Myprintf("%s-1", cexpr.colName.String())
			case after:
				buf.Myprintf("%s+1", cexpr.colName.String())
			}
		case opSum:
			buf.Myprintf("%s", cexpr.colName.String())
			if before {
				bvf.mode = bvBefore
				buf.Myprintf("-ifnull(%v, 0)", cexpr.expr)
			}
			if after {
				bvf.mode = bvAfter
				buf.Myprintf("+ifnull(%v, 0)", cexpr.expr)
			}
		}
	}
}

func (tpb *tablePlanBuilder) generateWhere(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	buf.WriteString(" where ")
	bvf.mode = bvBefore
	separator := ""
	for _, cexpr := range tpb.pkCols {
		if _, ok := cexpr.expr.(*sqlparser.ColName); ok {
			buf.Myprintf("%s%s=%v", separator, cexpr.colName.String(), cexpr.expr)
		} else {
			// Parenthesize non-trivial expressions.
			buf.Myprintf("%s%s=(%v)", separator, cexpr.colName.String(), cexpr.expr)
		}
		separator = " and "
	}
}

type bindvarFormatter struct {
	mode bindvarMode
}

type bindvarMode int

func (bvf *bindvarFormatter) formatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if node, ok := node.(*sqlparser.ColName); ok {
		switch bvf.mode {
		case bvBefore:
			buf.WriteArg(fmt.Sprintf(":b_%s", node.Name.String()))
			return
		case bvAfter:
			buf.WriteArg(fmt.Sprintf(":a_%s", node.Name.String()))
			return
		}
	}
	node.Format(buf)
}
