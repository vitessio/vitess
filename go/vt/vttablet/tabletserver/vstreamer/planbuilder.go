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

// Plan represents the plan for a table.
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

// Table contains the metadata for a table.
type Table struct {
	Name    string
	Columns []schema.TableColumn
}

// fields returns the fields for the plan.
func (plan *Plan) fields() []*querypb.Field {
	fields := make([]*querypb.Field, len(plan.ColExprs))
	for i, ce := range plan.ColExprs {
		fields[i] = &querypb.Field{
			Name: ce.Alias.String(),
			Type: ce.Type,
		}
	}
	return fields
}

// filter filters the row against the plan. It returns false if the row did not match.
// If the row matched, it returns the columns to be sent.
func (plan *Plan) filter(values []sqltypes.Value) (bool, []sqltypes.Value, error) {
	result := make([]sqltypes.Value, len(plan.ColExprs))
	for i, colExpr := range plan.ColExprs {
		if colExpr.ColNum >= len(values) {
			return false, nil, fmt.Errorf("index out of range. colExpr.ColNum: %d   len(values):%d!!", colExpr.ColNum, len(values))
		}
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

// tableMatches is similar to the one defined in vreplication.
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
	sel, fromTable, err := analyzeSelect(query)
	if err != nil {
		return nil, err
	}
	if fromTable.String() != ti.Name {
		return nil, fmt.Errorf("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
	}

	plan := &Plan{
		Table: ti,
	}
	if err := plan.analyzeExprs(sel.SelectExprs); err != nil {
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
	if err := plan.analyzeInKeyRange(kschema, funcExpr.Exprs); err != nil {
		return nil, err
	}
	return plan, nil
}

func analyzeSelect(query string) (sel *sqlparser.Select, fromTable sqlparser.TableIdent, err error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fromTable, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(statement))
	}
	if len(sel.From) > 1 {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	fromTable = sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	return sel, fromTable, nil
}

func (plan *Plan) analyzeExprs(selExprs sqlparser.SelectExprs) error {
	if _, ok := selExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range selExprs {
			cExpr, err := plan.analyzeExpr(expr)
			if err != nil {
				return err
			}
			plan.ColExprs = append(plan.ColExprs, cExpr)
		}
	} else {
		if len(selExprs) != 1 {
			return fmt.Errorf("unsupported: %v", sqlparser.String(selExprs))
		}
		plan.ColExprs = make([]ColExpr, len(plan.Table.Columns))
		for i, col := range plan.Table.Columns {
			plan.ColExprs[i].ColNum = i
			plan.ColExprs[i].Alias = col.Name
			plan.ColExprs[i].Type = col.Type
		}
	}
	return nil
}

func (plan *Plan) analyzeExpr(selExpr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
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
	colnum, err := findColumn(plan.Table, colname.Name)
	if err != nil {
		return ColExpr{}, err
	}
	return ColExpr{ColNum: colnum, Alias: as, Type: plan.Table.Columns[colnum].Type}, nil
}

func (plan *Plan) analyzeInKeyRange(kschema *vindexes.KeyspaceSchema, exprs sqlparser.SelectExprs) error {
	var colname sqlparser.ColIdent
	var krExpr sqlparser.SelectExpr
	switch len(exprs) {
	case 1:
		table := kschema.Tables[plan.Table.Name]
		if table == nil {
			return fmt.Errorf("no vschema definition for table %s", plan.Table.Name)
		}
		// Get Primary Vindex.
		if len(table.ColumnVindexes) == 0 {
			return fmt.Errorf("table %s has no primary vindex", plan.Table.Name)
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
