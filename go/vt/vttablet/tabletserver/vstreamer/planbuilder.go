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
	Table    *Table
	ColExprs []ColExpr

	// Vindex, VindexColumns and KeyRange, if set, will be used
	// to filter the row.
	Vindex        vindexes.Vindex
	VindexColumns []int
	KeyRange      *topodatapb.KeyRange
}

// ColExpr represents a column expression.
type ColExpr struct {
	// ColNum specifies the source column value.
	ColNum int

	// Vindex and VindexColumns, if set, will be used to generate
	// a keyspace_id. If so, ColNum is ignored.
	Vindex        vindexes.Vindex
	VindexColumns []int

	Alias sqlparser.ColIdent
	Type  querypb.Type
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
	if plan.Vindex != nil {
		vindexValues := make([]sqltypes.Value, 0, len(plan.VindexColumns))
		for _, col := range plan.VindexColumns {
			vindexValues = append(vindexValues, values[col])
		}
		ksid, err := getKeyspaceID(vindexValues, plan.Vindex)
		if err != nil {
			return false, nil, err
		}
		if !key.KeyRangeContains(plan.KeyRange, ksid) {
			return false, nil, nil
		}
	}

	result := make([]sqltypes.Value, len(plan.ColExprs))
	for i, colExpr := range plan.ColExprs {
		if colExpr.ColNum >= len(values) {
			return false, nil, fmt.Errorf("index out of range, colExpr.ColNum: %d, len(values): %d", colExpr.ColNum, len(values))
		}
		if colExpr.Vindex == nil {
			result[i] = values[colExpr.ColNum]
		} else {
			vindexValues := make([]sqltypes.Value, 0, len(colExpr.VindexColumns))
			for _, col := range colExpr.VindexColumns {
				vindexValues = append(vindexValues, values[col])
			}
			ksid, err := getKeyspaceID(vindexValues, colExpr.Vindex)
			if err != nil {
				return false, nil, err
			}
			result[i] = sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(ksid))
		}
	}
	return true, result, nil
}

func getKeyspaceID(values []sqltypes.Value, vindex vindexes.Vindex) (key.DestinationKeyspaceID, error) {
	destinations, err := vindexes.Map(vindex, nil, [][]sqltypes.Value{values})
	if err != nil {
		return nil, err
	}
	if len(destinations) != 1 {
		return nil, fmt.Errorf("mapping row to keyspace id returned an invalid array of destinations: %v", key.DestinationsString(destinations))
	}
	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) == 0 {
		return nil, fmt.Errorf("could not map %v to a keyspace id, got destination %v", values, destinations[0])
	}
	return ksid, nil
}

func mustSendStmt(query mysql.Query, dbname string) bool {
	if query.Database != "" && query.Database != dbname {
		return false
	}
	return true
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

func buildPlan(ti *Table, vschema *localVSchema, filter *binlogdatapb.Filter) (*Plan, error) {
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
			return buildREPlan(ti, vschema, rule.Filter)
		case rule.Match == ti.Name:
			return buildTablePlan(ti, vschema, rule.Filter)
		}
	}
	return nil, nil
}

func buildREPlan(ti *Table, vschema *localVSchema, filter string) (*Plan, error) {
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
	table, err := vschema.FindTable(ti.Name)
	if err != nil {
		return nil, err
	}
	// Get Primary Vindex.
	if len(table.ColumnVindexes) == 0 {
		return nil, fmt.Errorf("table %s has no primary vindex", ti.Name)
	}
	plan.Vindex = table.ColumnVindexes[0].Vindex
	plan.VindexColumns, err = buildVindexColumns(plan.Table, table.ColumnVindexes[0].Columns)
	if err != nil {
		return nil, err
	}

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

func buildTablePlan(ti *Table, vschema *localVSchema, query string) (*Plan, error) {
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
	if err := plan.analyzeExprs(vschema, sel.SelectExprs); err != nil {
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
	if err := plan.analyzeInKeyRange(vschema, funcExpr.Exprs); err != nil {
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

func (plan *Plan) analyzeExprs(vschema *localVSchema, selExprs sqlparser.SelectExprs) error {
	if _, ok := selExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range selExprs {
			cExpr, err := plan.analyzeExpr(vschema, expr)
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

func (plan *Plan) analyzeExpr(vschema *localVSchema, selExpr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(selExpr))
	}
	switch inner := aliased.Expr.(type) {
	case *sqlparser.ColName:
		if !inner.Qualifier.IsEmpty() {
			return ColExpr{}, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(inner))
		}
		colnum, err := findColumn(plan.Table, inner.Name)
		if err != nil {
			return ColExpr{}, err
		}
		as := aliased.As
		if as.IsEmpty() {
			as = sqlparser.NewColIdent(sqlparser.String(aliased.Expr))
		}
		return ColExpr{
			ColNum: colnum,
			Alias:  as,
			Type:   plan.Table.Columns[colnum].Type,
		}, nil
	case *sqlparser.FuncExpr:
		if inner.Name.Lowered() != "keyspace_id" {
			return ColExpr{}, fmt.Errorf("unsupported function: %v", sqlparser.String(inner))
		}
		if len(inner.Exprs) != 0 {
			return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(inner))
		}
		table, err := vschema.FindTable(plan.Table.Name)
		if err != nil {
			return ColExpr{}, err
		}
		// Get Primary Vindex.
		if len(table.ColumnVindexes) == 0 {
			return ColExpr{}, fmt.Errorf("table %s has no primary vindex", plan.Table.Name)
		}
		vindexColumns, err := buildVindexColumns(plan.Table, table.ColumnVindexes[0].Columns)
		if err != nil {
			return ColExpr{}, err
		}
		return ColExpr{
			Vindex:        table.ColumnVindexes[0].Vindex,
			VindexColumns: vindexColumns,
			Alias:         sqlparser.NewColIdent("keyspace_id"),
			Type:          sqltypes.VarBinary,
		}, nil
	default:
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(aliased.Expr))
	}
}

func (plan *Plan) analyzeInKeyRange(vschema *localVSchema, exprs sqlparser.SelectExprs) error {
	var colnames []sqlparser.ColIdent
	var krExpr sqlparser.SelectExpr
	switch {
	case len(exprs) == 1:
		table, err := vschema.FindTable(plan.Table.Name)
		if err != nil {
			return err
		}
		// Get Primary Vindex.
		if len(table.ColumnVindexes) == 0 {
			return fmt.Errorf("table %s has no primary vindex", plan.Table.Name)
		}
		colnames = table.ColumnVindexes[0].Columns
		plan.Vindex = table.ColumnVindexes[0].Vindex
		krExpr = exprs[0]
	case len(exprs) >= 3:
		for _, expr := range exprs[:len(exprs)-2] {
			aexpr, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			qualifiedName, ok := aexpr.Expr.(*sqlparser.ColName)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			if !qualifiedName.Qualifier.IsEmpty() {
				return fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(qualifiedName))
			}
			colnames = append(colnames, qualifiedName.Name)
		}

		vtype, err := selString(exprs[len(exprs)-2])
		if err != nil {
			return err
		}
		plan.Vindex, err = vindexes.CreateVindex(vtype, vtype, map[string]string{})
		if err != nil {
			return err
		}
		if !plan.Vindex.IsUnique() {
			return fmt.Errorf("vindex must be Unique to be used for VReplication: %s", vtype)
		}

		krExpr = exprs[len(exprs)-1]
	default:
		return fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(exprs))
	}
	var err error
	plan.VindexColumns, err = buildVindexColumns(plan.Table, colnames)
	if err != nil {
		return err
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

func buildVindexColumns(ti *Table, colnames []sqlparser.ColIdent) ([]int, error) {
	vindexColumns := make([]int, 0, len(colnames))
	for _, colname := range colnames {
		colnum, err := findColumn(ti, colname)
		if err != nil {
			return nil, err
		}
		vindexColumns = append(vindexColumns, colnum)
	}
	return vindexColumns, nil
}

func findColumn(ti *Table, name sqlparser.ColIdent) (int, error) {
	for i, col := range ti.Columns {
		if name.Equal(col.Name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %s not found in table %s", sqlparser.String(name), ti.Name)
}
