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
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Plan represents the streaming plan for a table.
type Plan struct {
	Table        *Table
	ColExprs     []ColExpr
	VindexColumn int
	Vindex       vindexes.Vindex
	KeyRange     *topodatapb.KeyRange
}

// ColExpr represents a column expression.
type ColExpr struct {
	ColNum    int
	Alias     sqlparser.ColIdent
	Type      querypb.Type
	Operation Operation
}

// Operation represents the operation to be performed on a column.
type Operation int

// The following are the supported operations on a column.
const (
	OpNone = Operation(iota)
	OpMonth
	OpDay
	OpHour
)

// Table contains the metadata for a table. The
// name is dervied from mysql's Table_map_log_event.
type Table struct {
	*mysql.TableMap
	Columns []schema.TableColumn
}

// The filter function needs the ability to perform expression evaluations. This is
// because the consumer of vstream is not just VPlayer. It can also be a dumb client
// like a mysql client that's subscribing to changes. This ability to allow users
// to directly pull events by sending a complex select query. The same reasoning
// applies to where clauses. For now, only simple functions like hour are supported,
// but this can be expanded in the future.
func (plan *Plan) filter(values []sqltypes.Value) (bool, []sqltypes.Value, error) {
	result := make([]sqltypes.Value, len(plan.ColExprs))
	for i, colExpr := range plan.ColExprs {
		switch colExpr.Operation {
		case OpMonth:
			v, _ := sqltypes.ToInt64(values[colExpr.ColNum])
			t := time.Unix(v, 0).UTC()
			s := fmt.Sprintf("%d%02d", t.Year(), t.Month())
			result[i] = sqltypes.NewVarBinary(s)
		case OpDay:
			v, _ := sqltypes.ToInt64(values[colExpr.ColNum])
			t := time.Unix(v, 0).UTC()
			s := fmt.Sprintf("%d%02d%02d", t.Year(), t.Month(), t.Day())
			result[i] = sqltypes.NewVarBinary(s)
		case OpHour:
			v, _ := sqltypes.ToInt64(values[colExpr.ColNum])
			t := time.Unix(v, 0).UTC()
			s := fmt.Sprintf("%d%02d%02d%02d", t.Year(), t.Month(), t.Day(), t.Hour())
			result[i] = sqltypes.NewVarBinary(s)
		default:
			result[i] = values[colExpr.ColNum]
		}
	}
	if plan.Vindex == nil {
		return true, result, nil
	}

	// Filter by Vindex.
	destinations, err := plan.Vindex.Map(nil, []sqltypes.Value{result[plan.VindexColumn]})
	if err != nil {
		return false, nil, err
	}
	if len(destinations) != 1 {
		return false, nil, fmt.Errorf("mapping row to keyspace id returned an invalid array of destinations: %v", key.DestinationsString(destinations))
	}
	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) == 0 {
		return false, nil, fmt.Errorf("could not map %v to a keyspace id, got destination %v", result[plan.VindexColumn], destinations[0])
	}
	if !key.KeyRangeContains(plan.KeyRange, ksid) {
		return false, nil, nil
	}
	return true, result, nil
}

func buildPlan(ti *Table, kschema *vindexes.KeyspaceSchema, filter *binlogdatapb.Filter) (*Plan, error) {
	for _, rule := range filter.Rules {
		switch {
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, ti.Name)
			if err != nil {
				return nil, err
			}
			if !result {
				continue
			}
			return buildREPlan(ti, kschema, rule.Filter)
		case rule.Match == ti.Name:
			return buildTablePlan(ti, kschema, rule.Filter)
		}
	}
	return nil, nil
}

func buildREPlan(ti *Table, kschema *vindexes.KeyspaceSchema, filter string) (*Plan, error) {
	plan := &Plan{
		Table: ti,
	}
	plan.ColExprs = make([]ColExpr, len(ti.Columns))
	for i, col := range ti.Columns {
		plan.ColExprs[i].ColNum = i
		plan.ColExprs[i].Alias = col.Name
		plan.ColExprs[i].Type = col.Type
	}
	if filter == "" {
		return plan, nil
	}

	// We need to additionally set VindexColumn, Vindex and KeyRange
	// based on the Primary Vindex of the table.
	// Find table in kschema.
	table := kschema.Tables[ti.Name]
	if table == nil {
		return nil, fmt.Errorf("no vschema definition for table %s", ti.Name)
	}
	// Get Primary Vindex.
	if len(table.ColumnVindexes) == 0 {
		return nil, fmt.Errorf("table %s has no primary vindex", ti.Name)
	}
	// findColumn can be used here because result column list is same
	// as source.
	colnum, err := findColumn(ti, table.ColumnVindexes[0].Columns[0])
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
	return plan, nil
}

func buildTablePlan(ti *Table, kschema *vindexes.KeyspaceSchema, query string) (*Plan, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Table: ti,
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
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
	if fromTable.String() != ti.Name {
		return nil, fmt.Errorf("unexpected: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
	}

	if _, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range sel.SelectExprs {
			cExpr, err := analyzeExpr(ti, expr)
			if err != nil {
				return nil, err
			}
			plan.ColExprs = append(plan.ColExprs, cExpr)
		}
	} else {
		if len(sel.SelectExprs) != 1 {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
		}
		plan.ColExprs = make([]ColExpr, len(ti.Columns))
		for i, col := range ti.Columns {
			plan.ColExprs[i].ColNum = i
			plan.ColExprs[i].Alias = col.Name
			plan.ColExprs[i].Type = col.Type
		}
	}

	if sel.Where == nil {
		return plan, nil
	}

	// Filter by Vindex.
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
	aexpr, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(funcExpr))
	}
	colname, ok := aexpr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unsupported: %v", sqlparser.String(funcExpr))
	}
	found := false
	for i, cExpr := range plan.ColExprs {
		if cExpr.Alias.Equal(colname.Name) {
			found = true
			plan.VindexColumn = i
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("keyrange expression does not reference a column in the select list: %v", sqlparser.String(colname))
	}
	vtype, err := selString(funcExpr.Exprs[1])
	if err != nil {
		return nil, err
	}
	plan.Vindex, err = vindexes.CreateVindex(vtype, vtype, map[string]string{})
	if err != nil {
		return nil, err
	}
	if !plan.Vindex.IsUnique() || !plan.Vindex.IsFunctional() {
		return nil, fmt.Errorf("vindex must be Unique and Functional to be used for VReplication: %s", vtype)
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

func analyzeExpr(ti *Table, selExpr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(selExpr))
	}
	switch expr := aliased.Expr.(type) {
	case *sqlparser.ColName:
		colnum, err := findColumn(ti, expr.Name)
		if err != nil {
			return ColExpr{}, err
		}
		return ColExpr{ColNum: colnum, Alias: expr.Name, Type: ti.Columns[colnum].Type}, nil
	case *sqlparser.FuncExpr:
		if expr.Distinct || len(expr.Exprs) != 1 {
			return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "month", "day", "hour":
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(expr))
			}
			as := aliased.As
			if as.IsEmpty() {
				as = sqlparser.NewColIdent(sqlparser.String(expr))
			}
			colnum, err := findColumn(ti, innerCol.Name)
			if err != nil {
				return ColExpr{}, err
			}
			switch fname {
			case "month":
				return ColExpr{ColNum: colnum, Alias: as, Type: sqltypes.VarBinary, Operation: OpMonth}, nil
			case "day":
				return ColExpr{ColNum: colnum, Alias: as, Type: sqltypes.VarBinary, Operation: OpDay}, nil
			case "hour":
				return ColExpr{ColNum: colnum, Alias: as, Type: sqltypes.VarBinary, Operation: OpHour}, nil
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

func findColumn(ti *Table, name sqlparser.ColIdent) (int, error) {
	for i, col := range ti.Columns {
		if name.Equal(col.Name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %s not found in table %s", sqlparser.String(name), ti.Name)
}
