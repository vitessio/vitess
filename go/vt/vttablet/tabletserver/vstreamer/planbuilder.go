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
	ColNum int
	Alias  sqlparser.ColIdent
	Type   querypb.Type
}

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
		result[i] = values[colExpr.ColNum]
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

func mustSendDDL(query mysql.Query, dbname string, filter *binlogdatapb.Filter) bool {
	if query.Database != "" && query.Database != dbname {
		return false
	}
	ast, err := sqlparser.Parse(query.SQL)
	// If there was a parsing error, we send it through. Hopefully,
	// recipient can handle it.
	if err != nil {
		return true
	}
	switch stmt := ast.(type) {
	case *sqlparser.DBDDL:
		return false
	case *sqlparser.DDL:
		if !stmt.Table.IsEmpty() {
			return tableMatches(stmt.Table, dbname, filter)
		}
		for _, table := range stmt.FromTables {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		for _, table := range stmt.ToTables {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		return false
	}
	return true
}

func tableMatches(table sqlparser.TableName, dbname string, filter *binlogdatapb.Filter) bool {
	if !table.Qualifier.IsEmpty() && table.Qualifier.String() != dbname {
		return false
	}
	for _, rule := range filter.Rules {
		switch {
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, table.Name.String())
			if err != nil {
				return true
			}
			if !result {
				continue
			}
			return true
		case table.Name.String() == rule.Match:
			return true
		}
	}
	return false
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
		return nil, fmt.Errorf("unsupported: %v", sqlparser.String(statement))
	}
	if len(sel.From) > 1 {
		return nil, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	fromTable := sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	if fromTable.String() != ti.Name {
		return nil, fmt.Errorf("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
	}

	if err := plan.analyzeExprs(ti, sel.SelectExprs); err != nil {
		return nil, err
	}

	if sel.Where == nil {
		return plan, nil
	}

	funcExpr, ok := sel.Where.Expr.(*sqlparser.FuncExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported where clause: %v", sqlparser.String(sel.Where))
	}
	if !funcExpr.Name.EqualString("in_keyrange") {
		return nil, fmt.Errorf("unsupported where clause: %v", sqlparser.String(sel.Where))
	}
	if err := plan.analyzeInKeyRange(ti, kschema, funcExpr.Exprs); err != nil {
		return nil, err
	}
	return plan, nil
}

func (plan *Plan) analyzeExprs(ti *Table, selExprs sqlparser.SelectExprs) error {
	if _, ok := selExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range selExprs {
			cExpr, err := plan.analyzeExpr(ti, expr)
			if err != nil {
				return err
			}
			plan.ColExprs = append(plan.ColExprs, cExpr)
		}
	} else {
		if len(selExprs) != 1 {
			return fmt.Errorf("unsupported: %v", sqlparser.String(selExprs))
		}
		plan.ColExprs = make([]ColExpr, len(ti.Columns))
		for i, col := range ti.Columns {
			plan.ColExprs[i].ColNum = i
			plan.ColExprs[i].Alias = col.Name
			plan.ColExprs[i].Type = col.Type
		}
	}
	return nil
}

func (plan *Plan) analyzeExpr(ti *Table, selExpr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(selExpr))
	}
	as := aliased.As
	if as.IsEmpty() {
		as = sqlparser.NewColIdent(sqlparser.String(aliased.Expr))
	}
	colname, ok := aliased.Expr.(*sqlparser.ColName)
	if !ok {
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(aliased.Expr))
	}
	if !colname.Qualifier.IsEmpty() {
		return ColExpr{}, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(colname))
	}
	colnum, err := findColumn(ti, colname.Name)
	if err != nil {
		return ColExpr{}, err
	}
	return ColExpr{ColNum: colnum, Alias: as, Type: ti.Columns[colnum].Type}, nil
}

func (plan *Plan) analyzeInKeyRange(ti *Table, kschema *vindexes.KeyspaceSchema, exprs sqlparser.SelectExprs) error {
	var colname sqlparser.ColIdent
	var krExpr sqlparser.SelectExpr
	switch len(exprs) {
	case 1:
		table := kschema.Tables[ti.Name]
		if table == nil {
			return fmt.Errorf("no vschema definition for table %s", ti.Name)
		}
		// Get Primary Vindex.
		if len(table.ColumnVindexes) == 0 {
			return fmt.Errorf("table %s has no primary vindex", ti.Name)
		}
		colname = table.ColumnVindexes[0].Columns[0]
		plan.Vindex = table.ColumnVindexes[0].Vindex
		krExpr = exprs[0]
	case 3:
		aexpr, ok := exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return fmt.Errorf("unexpected: %v", sqlparser.String(exprs[0]))
		}
		qualifiedName, ok := aexpr.Expr.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unexpected: %v", sqlparser.String(exprs[0]))
		}
		if !qualifiedName.Qualifier.IsEmpty() {
			return fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(colname))
		}
		colname = qualifiedName.Name
		vtype, err := selString(exprs[1])
		if err != nil {
			return err
		}
		plan.Vindex, err = vindexes.CreateVindex(vtype, vtype, map[string]string{})
		if err != nil {
			return err
		}
		if !plan.Vindex.IsUnique() || !plan.Vindex.IsFunctional() {
			return fmt.Errorf("vindex must be Unique and Functional to be used for VReplication: %s", vtype)
		}
		krExpr = exprs[2]
	default:
		return fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(exprs))
	}
	found := false
	for i, cExpr := range plan.ColExprs {
		if cExpr.Alias.Equal(colname) {
			found = true
			plan.VindexColumn = i
			break
		}
	}
	if !found {
		return fmt.Errorf("keyrange expression does not reference a column in the select list: %v", sqlparser.String(colname))
	}
	kr, err := selString(krExpr)
	if err != nil {
		return err
	}
	keyranges, err := key.ParseShardingSpec(kr)
	if err != nil {
		return err
	}
	if len(keyranges) != 1 {
		return fmt.Errorf("unexpected in_keyrange parameter: %v", sqlparser.String(krExpr))
	}
	plan.KeyRange = keyranges[0]
	return nil
}

func selString(expr sqlparser.SelectExpr) (string, error) {
	aexpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
	}
	val, ok := aexpr.Expr.(*sqlparser.SQLVal)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
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
