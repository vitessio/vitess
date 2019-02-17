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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// PlayerPlan is the execution plan for a player stream.
type PlayerPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TablePlans    map[string]*TablePlan
}

// TablePlan is the execution plan for a table within a player stream.
// There are two incarantions of this per table. The first one is built
// while analyzing the inital stream request. A tentative plan is built
// without knowing the table info. The second incarnation is built when
// we receive the field info for a table. At that time, we copy the
// original TablePlan into a separate map and populate the Fields and
// PKCols members.
type TablePlan struct {
	Name     string
	ColExprs []*ColExpr `json:",omitempty"`
	OnInsert InsertType `json:",omitempty"`

	Fields []*querypb.Field `json:",omitempty"`
	PKCols []*ColExpr       `json:",omitempty"`
}

// ColExpr describes the processing to be performed to
// compute the value of the target table column.
type ColExpr struct {
	ColName   sqlparser.ColIdent
	ColNum    int
	Operation Operation `json:",omitempty"`
	IsGrouped bool      `json:",omitempty"`
}

// Operation is the opcode for the ColExpr.
type Operation int

// The following values are the various ColExpr opcodes.
const (
	OpNone = Operation(iota)
	OpCount
	OpSum
)

// InsertType describes the type of insert statement to generate.
type InsertType int

// The following values are the various insert types.
const (
	InsertNormal = InsertType(iota)
	InsertOndup
	InsertIgnore
)

func buildPlayerPlan(filter *binlogdatapb.Filter) (*PlayerPlan, error) {
	plan := &PlayerPlan{
		VStreamFilter: &binlogdatapb.Filter{
			Rules: make([]*binlogdatapb.Rule, len(filter.Rules)),
		},
		TablePlans: make(map[string]*TablePlan),
	}
	for i, rule := range filter.Rules {
		if strings.HasPrefix(rule.Match, "/") {
			plan.VStreamFilter.Rules[i] = rule
			continue
		}
		sendRule, tp, err := buildTablePlan(rule)
		if err != nil {
			return nil, err
		}
		plan.VStreamFilter.Rules[i] = sendRule
		plan.TablePlans[sendRule.Match] = tp
	}
	return plan, nil
}

func buildTablePlan(rule *binlogdatapb.Rule) (*binlogdatapb.Rule, *TablePlan, error) {
	statement, err := sqlparser.Parse(rule.Filter)
	if err != nil {
		return nil, nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	if sel.Distinct != "" {
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
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
		return sendRule, &TablePlan{Name: rule.Match}, nil
	}

	tp := &TablePlan{
		Name: rule.Match,
	}
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
			cExpr.ColNum = len(sendSelect.SelectExprs) - 1
		}
		tp.ColExprs = append(tp.ColExprs, cExpr)
	}

	if sel.GroupBy != nil {
		if err := analyzeGroupBy(sel.GroupBy, tp); err != nil {
			return nil, nil, err
		}
		tp.OnInsert = InsertIgnore
		for _, cExpr := range tp.ColExprs {
			if !cExpr.IsGrouped {
				tp.OnInsert = InsertOndup
				break
			}
		}
	}
	sendRule := &binlogdatapb.Rule{
		Match:  fromTable.String(),
		Filter: sqlparser.String(sendSelect),
	}
	return sendRule, tp, nil
}

func analyzeExpr(selExpr sqlparser.SelectExpr) (sqlparser.SelectExpr, *ColExpr, error) {
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
		return selExpr, &ColExpr{ColName: as}, nil
	case *sqlparser.FuncExpr:
		if expr.Distinct || len(expr.Exprs) != 1 {
			return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		if aliased.As.IsEmpty() {
			return nil, nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "month", "day", "hour":
			return selExpr, &ColExpr{ColName: as}, nil
		case "count":
			if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); !ok {
				return nil, nil, fmt.Errorf("only count(*) is supported: %v", sqlparser.String(expr))
			}
			return nil, &ColExpr{ColName: as, Operation: OpCount}, nil
		case "sum":
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			return &sqlparser.AliasedExpr{Expr: innerCol}, &ColExpr{ColName: as, Operation: OpSum}, nil
		default:
			return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
	default:
		return nil, nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
}

func analyzeGroupBy(groupBy sqlparser.GroupBy, tp *TablePlan) error {
	for _, expr := range groupBy {
		colname, ok := expr.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		cExpr := tp.FindCol(colname.Name)
		if cExpr == nil {
			return fmt.Errorf("group by expression does not reference an alias in the select list: %v", sqlparser.String(expr))
		}
		if cExpr.Operation != OpNone {
			return fmt.Errorf("group by expression is not allowed to reference an aggregate expression: %v", sqlparser.String(expr))
		}
		cExpr.IsGrouped = true
	}
	return nil
}

//--------------------------------------------------------------
// TablePlan support functions.

// FindCol finds the ColExpr. It returns nil if not found.
func (tp *TablePlan) FindCol(name sqlparser.ColIdent) *ColExpr {
	for _, cExpr := range tp.ColExprs {
		if cExpr.ColName.Equal(name) {
			return cExpr
		}
	}
	return nil
}

// GenerateStatement must be called only after Fields and PKCols have been populated.
func (tp *TablePlan) GenerateStatement(rowChange *binlogdatapb.RowChange) string {
	// MakeRowTrusted is needed here because because Proto3ToResult is not convenient.
	var before, after []sqltypes.Value
	if rowChange.Before != nil {
		before = sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
	}
	if rowChange.After != nil {
		after = sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
	}
	var query string
	switch {
	case before == nil && after != nil:
		query = tp.generateInsert(after)
	case before != nil && after != nil:
		query = tp.generateUpdate(before, after)
	case before != nil && after == nil:
		query = tp.generateDelete(before)
	case before == nil && after == nil:
		// unreachable
	}
	return query
}

func (tp *TablePlan) generateInsert(after []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	if tp.OnInsert == InsertIgnore {
		sql.Myprintf("insert ignore into %v set ", sqlparser.NewTableIdent(tp.Name))
	} else {
		sql.Myprintf("insert into %v set ", sqlparser.NewTableIdent(tp.Name))
	}
	tp.generateInsertValues(sql, after)
	if tp.OnInsert == InsertOndup {
		sql.Myprintf(" on duplicate key update ")
		_ = tp.generateUpdateValues(sql, nil, after)
	}
	return sql.String()
}

func (tp *TablePlan) generateUpdate(before, after []sqltypes.Value) string {
	if tp.OnInsert == InsertIgnore {
		return tp.generateInsert(after)
	}
	sql := sqlparser.NewTrackedBuffer(nil)
	sql.Myprintf("update %v set ", sqlparser.NewTableIdent(tp.Name))
	if ok := tp.generateUpdateValues(sql, before, after); !ok {
		return ""
	}
	sql.Myprintf(" where ")
	tp.generateWhereValues(sql, before)
	return sql.String()
}

func (tp *TablePlan) generateDelete(before []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	switch tp.OnInsert {
	case InsertOndup:
		return tp.generateUpdate(before, nil)
	case InsertIgnore:
		return ""
	default: // insertNormal
		sql.Myprintf("delete from %v where ", sqlparser.NewTableIdent(tp.Name))
		tp.generateWhereValues(sql, before)
	}
	return sql.String()
}

func (tp *TablePlan) generateInsertValues(sql *sqlparser.TrackedBuffer, after []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tp.ColExprs {
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = ", "
		if cExpr.Operation == OpCount {
			sql.WriteString("1")
		} else {
			if cExpr.Operation == OpSum && after[cExpr.ColNum].IsNull() {
				sql.WriteString("0")
			} else {
				encodeValue(sql, after[cExpr.ColNum])
			}
		}
	}
}

// generateUpdateValues returns true if at least one value was set. Otherwise, it returns false.
func (tp *TablePlan) generateUpdateValues(sql *sqlparser.TrackedBuffer, before, after []sqltypes.Value) bool {
	separator := ""
	hasSet := false
	for _, cExpr := range tp.ColExprs {
		if cExpr.IsGrouped {
			continue
		}
		if len(before) != 0 && len(after) != 0 {
			if cExpr.Operation == OpCount {
				continue
			}
			bef := before[cExpr.ColNum]
			aft := after[cExpr.ColNum]
			// If both are null, there's no change
			if bef.IsNull() && aft.IsNull() {
				continue
			}
			// If any one of them is null, something has changed.
			if bef.IsNull() || aft.IsNull() {
				goto mustSet
			}
			// Compare content only if none are null.
			if bef.ToString() == aft.ToString() {
				continue
			}
		}
	mustSet:
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = ", "
		hasSet = true
		if cExpr.Operation == OpCount || cExpr.Operation == OpSum {
			sql.Myprintf("%v", cExpr.ColName)
		}
		if len(before) != 0 {
			switch cExpr.Operation {
			case OpNone:
				if len(after) == 0 {
					sql.WriteString("NULL")
				}
			case OpCount:
				sql.WriteString("-1")
			case OpSum:
				if !before[cExpr.ColNum].IsNull() {
					sql.WriteString("-")
					encodeValue(sql, before[cExpr.ColNum])
				}
			}
		}
		if len(after) != 0 {
			switch cExpr.Operation {
			case OpNone:
				encodeValue(sql, after[cExpr.ColNum])
			case OpCount:
				sql.WriteString("+1")
			case OpSum:
				if !after[cExpr.ColNum].IsNull() {
					sql.WriteString("+")
					encodeValue(sql, after[cExpr.ColNum])
				}
			}
		}
	}
	return hasSet
}

func (tp *TablePlan) generateWhereValues(sql *sqlparser.TrackedBuffer, before []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tp.PKCols {
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = " and "
		encodeValue(sql, before[cExpr.ColNum])
	}
}
