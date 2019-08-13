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

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
)

type tablePlanBuilder struct {
	name       sqlparser.TableIdent
	sendSelect *sqlparser.Select
	selColumns map[string]bool
	colExprs   []*colExpr
	onInsert   insertType
	pkCols     []*colExpr
	lastpk     *sqltypes.Result
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

// The following values are the various colExpr opcodes.
const (
	opExpr = operation(iota)
	opCount
	opSum
)

// insertType describes the type of insert statement to generate.
type insertType int

// The following values are the various insert types.
const (
	insertNormal = insertType(iota)
	insertOnDup
	insertIgnore
)

// buildReplicatorPlan builds a ReplicatorPlan for the tables that match the filter.
// The filter is matched against the target schema. For every table matched,
// a table-specific rule is built to be sent to the source. We don't send the
// original rule to the source because it may not match the same tables as the
// target.
// The TablePlan built is a partial plan. The full plan for a table is built
// when we receive field information from events or rows sent by the source.
// buildExecutionPlan is the function that builds the full plan.
func buildReplicatorPlan(filter *binlogdatapb.Filter, tableKeys map[string][]string, copyState map[string]*sqltypes.Result) (*ReplicatorPlan, error) {
	plan := &ReplicatorPlan{
		VStreamFilter: &binlogdatapb.Filter{},
		TargetTables:  make(map[string]*TablePlan),
		TablePlans:    make(map[string]*TablePlan),
		tableKeys:     tableKeys,
	}
nextTable:
	for tableName := range tableKeys {
		lastpk, ok := copyState[tableName]
		if ok && lastpk == nil {
			// Don't replicate uncopied tables.
			continue
		}
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
					TargetName: tableName,
					SendRule:   sendRule,
					Lastpk:     lastpk,
				}
				plan.TargetTables[tableName] = tablePlan
				plan.TablePlans[tableName] = tablePlan
				continue nextTable
			case rule.Match == tableName:
				tablePlan, err := buildTablePlan(rule, tableKeys, lastpk)
				if err != nil {
					return nil, err
				}
				if _, ok := plan.TablePlans[tablePlan.SendRule.Match]; ok {
					continue
				}
				plan.VStreamFilter.Rules = append(plan.VStreamFilter.Rules, tablePlan.SendRule)
				plan.TargetTables[tableName] = tablePlan
				plan.TablePlans[tablePlan.SendRule.Match] = tablePlan
				continue nextTable
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

func buildTablePlan(rule *binlogdatapb.Rule, tableKeys map[string][]string, lastpk *sqltypes.Result) (*TablePlan, error) {
	query := rule.Filter
	if query == "" {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v", sqlparser.NewTableIdent(rule.Match))
		query = buf.String()
	}
	sel, fromTable, err := analyzeSelectFrom(query)
	if err != nil {
		return nil, err
	}
	sendRule := &binlogdatapb.Rule{
		Match: fromTable,
	}

	if expr, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); ok {
		if len(sel.SelectExprs) != 1 {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
		}
		if !expr.TableName.IsEmpty() {
			return nil, fmt.Errorf("unsupported qualifier for '*' expression: %v", sqlparser.String(expr))
		}
		sendRule.Filter = query
		tablePlan := &TablePlan{
			TargetName: rule.Match,
			SendRule:   sendRule,
			Lastpk:     lastpk,
		}
		return tablePlan, nil
	}

	tpb := &tablePlanBuilder{
		name: sqlparser.NewTableIdent(rule.Match),
		sendSelect: &sqlparser.Select{
			From:  sel.From,
			Where: sel.Where,
		},
		selColumns: make(map[string]bool),
		lastpk:     lastpk,
	}

	if err := tpb.analyzeExprs(sel.SelectExprs); err != nil {
		return nil, err
	}
	if tpb.lastpk != nil {
		for _, f := range tpb.lastpk.Fields {
			tpb.addCol(sqlparser.NewColIdent(f.Name))
		}
	}
	if err := tpb.analyzeGroupBy(sel.GroupBy); err != nil {
		return nil, err
	}
	if err := tpb.analyzePK(tableKeys); err != nil {
		return nil, err
	}

	sendRule.Filter = sqlparser.String(tpb.sendSelect)
	tablePlan := tpb.generate(tableKeys)
	tablePlan.SendRule = sendRule
	return tablePlan, nil
}

func (tpb *tablePlanBuilder) generate(tableKeys map[string][]string) *TablePlan {
	refmap := make(map[string]bool)
	for _, cexpr := range tpb.pkCols {
		for k := range cexpr.references {
			refmap[k] = true
		}
	}
	if tpb.lastpk != nil {
		for _, f := range tpb.lastpk.Fields {
			refmap[f.Name] = true
		}
	}
	pkrefs := make([]string, 0, len(refmap))
	for k := range refmap {
		pkrefs = append(pkrefs, k)
	}
	sort.Strings(pkrefs)

	bvf := &bindvarFormatter{}

	return &TablePlan{
		TargetName:       tpb.name.String(),
		Lastpk:           tpb.lastpk,
		PKReferences:     pkrefs,
		BulkInsertFront:  tpb.generateInsertPart(sqlparser.NewTrackedBuffer(bvf.formatter)),
		BulkInsertValues: tpb.generateValuesPart(sqlparser.NewTrackedBuffer(bvf.formatter), bvf),
		BulkInsertOnDup:  tpb.generateOnDupPart(sqlparser.NewTrackedBuffer(bvf.formatter)),
		Insert:           tpb.generateInsertStatement(),
		Update:           tpb.generateUpdateStatement(),
		Delete:           tpb.generateDeleteStatement(),
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
		// Require all non-trivial expressions to have an alias.
		if colAs, ok := aliased.Expr.(*sqlparser.ColName); ok && colAs.Qualifier.IsEmpty() {
			as = colAs.Name
		} else {
			return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(aliased))
		}
	}
	cexpr := &colExpr{
		colName:    as,
		references: make(map[string]bool),
	}
	if expr, ok := aliased.Expr.(*sqlparser.FuncExpr); ok {
		if expr.Distinct {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "count":
			if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); !ok {
				return nil, fmt.Errorf("only count(*) is supported: %v", sqlparser.String(expr))
			}
			cexpr.operation = opCount
			return cexpr, nil
		case "sum":
			if len(expr.Exprs) != 1 {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
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
			tpb.onInsert = insertOnDup
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

	tpb.generateInsertPart(buf)
	if tpb.lastpk == nil {
		buf.Myprintf(" values ", tpb.name)
		tpb.generateValuesPart(buf, bvf)
	} else {
		tpb.generateSelectPart(buf, bvf)
	}
	tpb.generateOnDupPart(buf)

	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateInsertPart(buf *sqlparser.TrackedBuffer) *sqlparser.ParsedQuery {
	if tpb.onInsert == insertIgnore {
		buf.Myprintf("insert ignore into %v(", tpb.name)
	} else {
		buf.Myprintf("insert into %v(", tpb.name)
	}
	separator := ""
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s%v", separator, cexpr.colName)
		separator = ","
	}
	buf.Myprintf(")", tpb.name)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateValuesPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	separator := "("
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s", separator)
		separator = ","
		switch cexpr.operation {
		case opExpr:
			buf.Myprintf("%v", cexpr.expr)
		case opCount:
			buf.WriteString("1")
		case opSum:
			buf.Myprintf("ifnull(%v, 0)", cexpr.expr)
		}
	}
	buf.Myprintf(")")
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateSelectPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	buf.WriteString(" select ")
	separator := ""
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s", separator)
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
	buf.WriteString(" from dual where ")
	tpb.generatePKConstraint(buf, bvf)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateOnDupPart(buf *sqlparser.TrackedBuffer) *sqlparser.ParsedQuery {
	if tpb.onInsert != insertOnDup {
		return nil
	}
	buf.Myprintf(" on duplicate key update ")
	separator := ""
	for _, cexpr := range tpb.colExprs {
		if cexpr.isGrouped || cexpr.isPK {
			continue
		}
		buf.Myprintf("%s%v=", separator, cexpr.colName)
		separator = ", "
		// TODO: What to do here?
		switch cexpr.operation {
		case opExpr:
			buf.Myprintf("values(%v)", cexpr.colName)
		case opCount:
			buf.Myprintf("%v+1", cexpr.colName)
		case opSum:
			buf.Myprintf("%v", cexpr.colName)
			buf.Myprintf("+ifnull(values(%v), 0)", cexpr.colName)
		}
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
	separator := ""
	for _, cexpr := range tpb.colExprs {
		if cexpr.isGrouped || cexpr.isPK {
			continue
		}
		buf.Myprintf("%s%v=", separator, cexpr.colName)
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			bvf.mode = bvAfter
			buf.Myprintf("%v", cexpr.expr)
		case opCount:
			buf.Myprintf("%v", cexpr.colName)
		case opSum:
			buf.Myprintf("%v", cexpr.colName)
			bvf.mode = bvBefore
			buf.Myprintf("-ifnull(%v, 0)", cexpr.expr)
			bvf.mode = bvAfter
			buf.Myprintf("+ifnull(%v, 0)", cexpr.expr)
		}
	}
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
	case insertOnDup:
		bvf.mode = bvBefore
		buf.Myprintf("update %v set ", tpb.name)
		separator := ""
		for _, cexpr := range tpb.colExprs {
			if cexpr.isGrouped || cexpr.isPK {
				continue
			}
			buf.Myprintf("%s%v=", separator, cexpr.colName)
			separator = ", "
			switch cexpr.operation {
			case opExpr:
				buf.WriteString("null")
			case opCount:
				buf.Myprintf("%v-1", cexpr.colName)
			case opSum:
				buf.Myprintf("%v-ifnull(%v, 0)", cexpr.colName, cexpr.expr)
			}
		}
		tpb.generateWhere(buf, bvf)
	case insertIgnore:
		return nil
	}
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateWhere(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	buf.WriteString(" where ")
	bvf.mode = bvBefore
	separator := ""
	for _, cexpr := range tpb.pkCols {
		if _, ok := cexpr.expr.(*sqlparser.ColName); ok {
			buf.Myprintf("%s%v=%v", separator, cexpr.colName, cexpr.expr)
		} else {
			// Parenthesize non-trivial expressions.
			buf.Myprintf("%s%v=(%v)", separator, cexpr.colName, cexpr.expr)
		}
		separator = " and "
	}
	if tpb.lastpk != nil {
		buf.WriteString(" and ")
		tpb.generatePKConstraint(buf, bvf)
	}
}

func (tpb *tablePlanBuilder) generatePKConstraint(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	separator := "("
	for _, pkname := range tpb.lastpk.Fields {
		buf.Myprintf("%s%v", separator, &sqlparser.ColName{Name: sqlparser.NewColIdent(pkname.Name)})
		separator = ","
	}
	separator = ") <= ("
	for _, val := range tpb.lastpk.Rows[0] {
		buf.WriteString(separator)
		separator = ","
		val.EncodeSQL(buf)
	}
	buf.WriteString(")")
}

// bindvarFormatter is a dual mode formatter. Its behavior
// can be changed dynamically changed to generate bind vars
// for the 'before' row or 'after' row by setting its mode
// to 'bvBefore' or 'bvAfter'. For example, inserts will always
// use bvAfter, whereas deletes will always use bvBefore.
// For updates, values being set will use bvAfter, whereas
// the where clause will use bvBefore.
type bindvarFormatter struct {
	mode bindvarMode
}

type bindvarMode int

const (
	bvBefore = bindvarMode(iota)
	bvAfter
)

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
