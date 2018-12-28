/*
Copyright 2018 The Vitess Authors.

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

package vstreamer

import (
	"fmt"
	"regexp"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Plan represents the streaming plan for a table.
type Plan struct {
	Table        string
	ColExprs     []ColExpr
	VindexColumn int
	Vindex       vindexes.Vindex
	KeyRange     *topodatapb.KeyRange
}

// ColExpr represents a column expression.
type ColExpr struct {
	ColNum    int
	Operation Operation
}

// Operation represents the operation to be performed on a column.
type Operation int

// The following are the supported operations on a column.
const (
	OpNone = Operation(iota)
	OpYearMonth
	OpDay
	OpHour
)

// TableMap contains the metadata for a table. The
// name is dervied from mysql's Table_map_log_event.
type TableMap struct {
	*mysql.TableMap
	ColumnNames []sqlparser.ColIdent
}

func buildPlan(tm *TableMap, kschema *vindexes.KeyspaceSchema, filter *binlogdatapb.Filter) (*Plan, error) {
	for _, rule := range filter.Rules {
		switch {
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, tm.Name)
			if err != nil {
				return nil, err
			}
			if !result {
				continue
			}
			return buildREPlan(tm, kschema, rule.Filter)
		case rule.Match == tm.Name:
		}
	}
	return nil, nil
}

func buildREPlan(tm *TableMap, kschema *vindexes.KeyspaceSchema, filter string) (*Plan, error) {
	plan := &Plan{
		Table: tm.Name,
	}
	plan.ColExprs = make([]ColExpr, len(tm.ColumnNames))
	for i := range tm.ColumnNames {
		plan.ColExprs[i].ColNum = i
	}
	if filter != "" {
		// We need to additionally set VindexColumn, Vindex and KeyRange
		// based on the Primary Vindex of the table.
		// Find table in kschema.
		table := kschema.Tables[tm.Name]
		if table == nil {
			return nil, fmt.Errorf("no vschema definition for table %s", tm.Name)
		}
		// Get Primary Vindex.
		if len(table.ColumnVindexes) == 0 {
			return nil, fmt.Errorf("table %s has no primary vindex", tm.Name)
		}
		colnum, err := findColumn(tm, table.ColumnVindexes[0].Columns[0])
		if err != nil {
			return nil, err
		}
		plan.VindexColumn = colnum
		plan.Vindex = table.ColumnVindexes[0].Vindex

		// Parse keyrange.
		keyranges, err := key.ParseShardingSpec(filter)
		if err != nil {
			return nil, err
		}
		if len(keyranges) != 1 {
			return nil, fmt.Errorf("error parsing keyrange: %v", filter)
		}
		plan.KeyRange = keyranges[0]
	}
	return plan, nil
}

func buildTablePlan(tm *TableMap, kschema *vindexes.KeyspaceSchema, query string) (*Plan, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Table: tm.Name,
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	if len(sel.From) > 1 {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	fromTable := sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	if fromTable.String() == tm.Name {
		return nil, fmt.Errorf("unexpected: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), tm.Name)
	}

	if _, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range sel.SelectExprs {
			cExpr, err := analyzeExpr(tm, expr)
			if err != nil {
				return nil, err
			}
			plan.ColExprs = append(plan.ColExprs, cExpr)
		}
	}

	if sel.Where == nil {
		return plan, nil
	}
	funcExpr, ok := sel.Where.Expr.(*sqlparser.FuncExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected where clause: %v", sqlparser.String(sel.Where))
	}
	if !funcExpr.Name.EqualString("in_keyrange") {
		return nil, fmt.Errorf("unexpected where clause: %v", sqlparser.String(sel.Where))
	}
	if len(funcExpr.Exprs) != 3 {
		return nil, fmt.Errorf("unexpected where clause: %v", sqlparser.String(sel.Where))
	}
	cExpr, err := analyzeExpr(tm, funcExpr.Exprs[0])
	if err != nil {
		return nil, err
	}
	if cExpr.Operation != OpNone {
		return nil, fmt.Errorf("unexpected operaion on vindex column: %v", funcExpr.Exprs[0])
	}
	plan.VindexColumn = cExpr.ColNum
	vtype, err := selString(funcExpr.Exprs[1])
	if err != nil {
		return nil, err
	}
	plan.Vindex, err = vindexes.CreateVindex(vtype, vtype, map[string]string{})
	if err != nil {
		return nil, err
	}
	kr, err := selString(funcExpr.Exprs[2])
	if err != nil {
		return nil, err
	}
	keyranges, err := key.ParseShardingSpec(kr)
	if err != nil {
		return nil, err
	}
	if len(keyranges) != 1 {
		return nil, fmt.Errorf("unexpected where clause: %v", sqlparser.String(sel.Where))
	}
	plan.KeyRange = keyranges[0]
	return plan, nil
}

func analyzeExpr(tm *TableMap, expr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
	aexpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	switch expr := aexpr.Expr.(type) {
	case *sqlparser.ColName:
		colnum, err := findColumn(tm, expr.Name)
		if err != nil {
			return ColExpr{}, err
		}
		return ColExpr{ColNum: colnum}, nil
	case *sqlparser.FuncExpr:
		if expr.Distinct || len(expr.Exprs) != 1 {
			return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "yearmonth", "day", "hour":
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			colnum, err := findColumn(tm, innerCol.Name)
			if err != nil {
				return ColExpr{}, err
			}
			switch fname {
			case "yearmonth":
				return ColExpr{ColNum: colnum, Operation: OpYearMonth}, nil
			case "day":
				return ColExpr{ColNum: colnum, Operation: OpDay}, nil
			case "hour":
				return ColExpr{ColNum: colnum, Operation: OpHour}, nil
			default:
				panic("unreachable")
			}
		default:
			return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
	default:
		return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
}

func selString(expr sqlparser.SelectExpr) (string, error) {
	aexpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return "", fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	val, ok := aexpr.Expr.(*sqlparser.SQLVal)
	if !ok {
		return "", fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	return string(val.Val), nil
}

func findColumn(tm *TableMap, name sqlparser.ColIdent) (int, error) {
	for i, cname := range tm.ColumnNames {
		if name.Equal(cname) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %s not found in table %s", sqlparser.String(name), tm.Name)
}
