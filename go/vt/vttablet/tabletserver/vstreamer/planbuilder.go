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
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Plan represents the plan for a table.
type Plan struct {
	Table *Table

	// ColExprs is the list of column expressions to be sent
	// in the stream.
	ColExprs []ColExpr

	convertUsingUTF8Columns map[string]bool

	// Any columns that require a function expression in the
	// stream.
	columnFuncExprs map[string]*sqlparser.FuncExpr

	// Filters is the list of filters to be applied to the columns
	// of the table.
	Filters []Filter
}

// Opcode enumerates the operators supported in a where clause
type Opcode int

const (
	// Equal is used to filter a comparable column on a specific value
	Equal = Opcode(iota)
	// VindexMatch is used for an in_keyrange() construct
	VindexMatch
	// LessThan is used to filter a comparable column if < specific value
	LessThan
	// LessThanEqual is used to filter a comparable column if <= specific value
	LessThanEqual
	// GreaterThan is used to filter a comparable column if > specific value
	GreaterThan
	// GreaterThanEqual is used to filter a comparable column if >= specific value
	GreaterThanEqual
	// NotEqual is used to filter a comparable column if != specific value
	NotEqual
)

// Filter contains opcodes for filtering.
type Filter struct {
	Opcode Opcode
	ColNum int
	Value  sqltypes.Value

	// Parameters for VindexMatch.
	// Vindex, VindexColumns and KeyRange, if set, will be used
	// to filter the row.
	// VindexColumns contains the column numbers of the table,
	// and not the column numbers of the stream to be sent.
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
	// VindexColumns contains the column numbers of the table,
	// and not the column numbers of the stream to be sent.
	Vindex        vindexes.Vindex
	VindexColumns []int

	Field *querypb.Field

	FixedValue sqltypes.Value
}

// Table contains the metadata for a table.
type Table struct {
	Name   string
	Fields []*querypb.Field
}

// FindColumn finds a column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindColumn(name sqlparser.IdentifierCI) int {
	for i, col := range ta.Fields {
		if name.EqualString(col.Name) {
			return i
		}
	}
	return -1
}

// fields returns the fields for the plan.
func (plan *Plan) fields() []*querypb.Field {
	fields := make([]*querypb.Field, len(plan.ColExprs))
	for i, ce := range plan.ColExprs {
		fields[i] = ce.Field
	}
	return fields
}

// getOpcode returns the equivalent planbuilder opcode for operators that are supported in Filters
func getOpcode(comparison *sqlparser.ComparisonExpr) (Opcode, error) {
	var opcode Opcode
	switch comparison.Operator {
	case sqlparser.EqualOp:
		opcode = Equal
	case sqlparser.LessThanOp:
		opcode = LessThan
	case sqlparser.LessEqualOp:
		opcode = LessThanEqual
	case sqlparser.GreaterThanOp:
		opcode = GreaterThan
	case sqlparser.GreaterEqualOp:
		opcode = GreaterThanEqual
	case sqlparser.NotEqualOp:
		opcode = NotEqual
	default:
		return -1, fmt.Errorf("comparison operator %s not supported", comparison.Operator.ToString())
	}
	return opcode, nil
}

// compare returns true after applying the comparison specified in the Filter to the actual data in the column
func compare(comparison Opcode, columnValue, filterValue sqltypes.Value, charset collations.ID) (bool, error) {
	// use null semantics: return false if either value is null
	if columnValue.IsNull() || filterValue.IsNull() {
		return false, nil
	}
	// at this point neither values can be null
	// NullsafeCompare returns 0 if values match, -1 if columnValue < filterValue, 1 if columnValue > filterValue
	result, err := evalengine.NullsafeCompare(columnValue, filterValue, charset)
	if err != nil {
		return false, err
	}

	switch comparison {
	case Equal:
		if result == 0 {
			return true, nil
		}
	case NotEqual:
		if result != 0 {
			return true, nil
		}
	case LessThan:
		if result == -1 {
			return true, nil
		}
	case LessThanEqual:
		if result <= 0 {
			return true, nil
		}
	case GreaterThan:
		if result == 1 {
			return true, nil
		}
	case GreaterThanEqual:
		if result >= 0 {
			return true, nil
		}
	default:
		return false, fmt.Errorf("comparison operator %d not supported", comparison)
	}
	return false, nil
}

// filter filters the row against the plan. It returns false if the row did not match.
// The output of the filtering operation is stored in the 'result' argument because
// filtering cannot be performed in-place. The result argument must be a slice of
// length equal to ColExprs
func (plan *Plan) filter(values, result []sqltypes.Value, charsets []collations.ID) (bool, error) {
	if len(result) != len(plan.ColExprs) {
		return false, fmt.Errorf("expected %d values in result slice", len(plan.ColExprs))
	}
	for _, filter := range plan.Filters {
		switch filter.Opcode {
		case VindexMatch:
			ksid, err := getKeyspaceID(values, filter.Vindex, filter.VindexColumns, plan.Table.Fields)
			if err != nil {
				return false, err
			}
			if !key.KeyRangeContains(filter.KeyRange, ksid) {
				return false, nil
			}
		default:
			match, err := compare(filter.Opcode, values[filter.ColNum], filter.Value, charsets[filter.ColNum])
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
	}
	for i, colExpr := range plan.ColExprs {
		if colExpr.ColNum == -1 {
			result[i] = colExpr.FixedValue
			continue
		}
		if colExpr.ColNum >= len(values) {
			return false, fmt.Errorf("index out of range, colExpr.ColNum: %d, len(values): %d", colExpr.ColNum, len(values))
		}
		if colExpr.Vindex == nil {
			result[i] = values[colExpr.ColNum]
		} else {
			ksid, err := getKeyspaceID(values, colExpr.Vindex, colExpr.VindexColumns, plan.Table.Fields)
			if err != nil {
				return false, err
			}
			result[i] = sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(ksid))
		}
	}
	return true, nil
}

func getKeyspaceID(values []sqltypes.Value, vindex vindexes.Vindex, vindexColumns []int, fields []*querypb.Field) (key.DestinationKeyspaceID, error) {
	vindexValues := make([]sqltypes.Value, 0, len(vindexColumns))
	for _, col := range vindexColumns {
		vindexValues = append(vindexValues, values[col])
	}
	destinations, err := vindexes.Map(context.TODO(), vindex, nil, [][]sqltypes.Value{vindexValues})
	if err != nil {
		return nil, err
	}
	if len(destinations) != 1 {
		return nil, fmt.Errorf("mapping row to keyspace id returned an invalid array of destinations: %v", key.DestinationsString(destinations))
	}
	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) == 0 {
		return nil, fmt.Errorf("could not map %v to a keyspace id, got destination %v", vindexValues, destinations[0])
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
	case sqlparser.DBDDLStatement:
		return false
	case sqlparser.DDLStatement:
		if !stmt.GetTable().IsEmpty() {
			return tableMatches(stmt.GetTable(), dbname, filter)
		}
		for _, table := range stmt.GetFromTables() {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		for _, table := range stmt.GetToTables() {
			if tableMatches(table, dbname, filter) {
				return true
			}
		}
		return false
	}
	return true
}

func ruleMatches(tableName string, filter *binlogdatapb.Filter) bool {
	for _, rule := range filter.Rules {
		switch {
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, tableName)
			if err != nil {
				return false
			}
			if !result {
				continue
			}
			return true
		case tableName == rule.Match:
			return true
		}
	}
	return false
}

// tableMatches is similar to buildPlan below and MatchTable in vreplication/table_plan_builder.go.
func tableMatches(table sqlparser.TableName, dbname string, filter *binlogdatapb.Filter) bool {
	if !table.Qualifier.IsEmpty() && table.Qualifier.String() != dbname {
		return false
	}
	return ruleMatches(table.Name.String(), filter)
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

// buildREPlan handles cases where Match has a regular expression.
// If so, the Filter can be an empty string or a keyrange, like "-80".
func buildREPlan(ti *Table, vschema *localVSchema, filter string) (*Plan, error) {
	plan := &Plan{
		Table: ti,
	}
	plan.ColExprs = make([]ColExpr, len(ti.Fields))
	for i, col := range ti.Fields {
		plan.ColExprs[i].ColNum = i
		plan.ColExprs[i].Field = col
	}
	if filter == "" {
		return plan, nil
	}

	// We need to additionally set VindexColumn, Vindex and KeyRange
	// based on the Primary Vindex of the table.
	cv, err := vschema.FindColVindex(ti.Name)
	if err != nil {
		return nil, err
	}
	whereFilter := Filter{
		Opcode: VindexMatch,
		Vindex: cv.Vindex,
	}
	whereFilter.VindexColumns, err = buildVindexColumns(plan.Table, cv.Columns)
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
	whereFilter.KeyRange = keyranges[0]
	plan.Filters = append(plan.Filters, whereFilter)
	return plan, nil
}

// BuildTablePlan handles cases where a specific table name is specified.
// The filter must be a select statement.
func buildTablePlan(ti *Table, vschema *localVSchema, query string) (*Plan, error) {
	sel, fromTable, err := analyzeSelect(query)
	if err != nil {
		log.Errorf("%s", err.Error())
		return nil, err
	}
	if fromTable.String() != ti.Name {
		log.Errorf("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
		return nil, fmt.Errorf("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
	}

	plan := &Plan{
		Table: ti,
	}
	if err := plan.analyzeWhere(vschema, sel.Where); err != nil {
		log.Errorf("%s", err.Error())
		return nil, err
	}
	if err := plan.analyzeExprs(vschema, sel.SelectExprs); err != nil {
		log.Errorf("%s", err.Error())
		return nil, err
	}

	if sel.Where == nil {
		return plan, nil
	}

	return plan, nil
}

func analyzeSelect(query string) (sel *sqlparser.Select, fromTable sqlparser.IdentifierCS, err error) {
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

// isConvertColumnUsingUTF8 returns 'true' when given column needs to be converted as UTF8
// while read from source table
func (plan *Plan) isConvertColumnUsingUTF8(columnName string) bool {
	if plan.convertUsingUTF8Columns == nil {
		return false
	}
	return plan.convertUsingUTF8Columns[columnName]
}

// setConvertColumnUsingUTF8 marks given column as needs to be converted as UTF8
// while read from source table
func (plan *Plan) setConvertColumnUsingUTF8(columnName string) {
	if plan.convertUsingUTF8Columns == nil {
		plan.convertUsingUTF8Columns = map[string]bool{}
	}
	plan.convertUsingUTF8Columns[columnName] = true
}

// setColumnFuncExpr sets the function expression for the column, which
// can then be used when building the streamer's query.
func (plan *Plan) setColumnFuncExpr(columnName string, funcExpr *sqlparser.FuncExpr) {
	if plan.columnFuncExprs == nil {
		plan.columnFuncExprs = map[string]*sqlparser.FuncExpr{}
	}
	plan.columnFuncExprs[columnName] = funcExpr
}

// getColumnFuncExpr returns a function expression if the column needs
// one when building the streamer's query.
func (plan *Plan) getColumnFuncExpr(columnName string) *sqlparser.FuncExpr {
	if plan.columnFuncExprs == nil {
		return nil
	}
	if val, ok := plan.columnFuncExprs[columnName]; ok {
		return val
	}
	return nil
}

func (plan *Plan) analyzeWhere(vschema *localVSchema, where *sqlparser.Where) error {
	if where == nil {
		return nil
	}
	exprs := splitAndExpression(nil, where.Expr)
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			opcode, err := getOpcode(expr)
			if err != nil {
				return err
			}
			qualifiedName, ok := expr.Left.(*sqlparser.ColName)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			if !qualifiedName.Qualifier.IsEmpty() {
				return fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(qualifiedName))
			}
			colnum, err := findColumn(plan.Table, qualifiedName.Name)
			if err != nil {
				return err
			}
			val, ok := expr.Right.(*sqlparser.Literal)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			//StrVal is varbinary, we do not support varchar since we would have to implement all collation types
			if val.Type != sqlparser.IntVal && val.Type != sqlparser.StrVal {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			pv, err := evalengine.Translate(val, semantics.EmptySemTable())
			if err != nil {
				return err
			}
			env := evalengine.EmptyExpressionEnv()
			resolved, err := env.Evaluate(pv)
			if err != nil {
				return err
			}
			plan.Filters = append(plan.Filters, Filter{
				Opcode: opcode,
				ColNum: colnum,
				Value:  resolved.Value(),
			})
		case *sqlparser.FuncExpr:
			if !expr.Name.EqualString("in_keyrange") {
				return fmt.Errorf("unsupported constraint: %v", sqlparser.String(expr))
			}
			if err := plan.analyzeInKeyRange(vschema, expr.Exprs); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported constraint: %v", sqlparser.String(expr))
		}
	}
	return nil
}

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
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
		plan.ColExprs = make([]ColExpr, len(plan.Table.Fields))
		for i, col := range plan.Table.Fields {
			plan.ColExprs[i].ColNum = i
			plan.ColExprs[i].Field = col
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
		return ColExpr{
			ColNum: colnum,
			Field:  plan.Table.Fields[colnum],
		}, nil
	case sqlparser.AggrFunc:
		if strings.ToLower(inner.AggrName()) != "keyspace_id" {
			return ColExpr{}, fmt.Errorf("unsupported function: %v", sqlparser.String(inner))
		}
		if len(inner.GetArgs()) != 0 {
			return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(inner))
		}
		cv, err := vschema.FindColVindex(plan.Table.Name)
		if err != nil {
			return ColExpr{}, err
		}
		vindexColumns, err := buildVindexColumns(plan.Table, cv.Columns)
		if err != nil {
			return ColExpr{}, err
		}
		return ColExpr{
			Field: &querypb.Field{
				Name: "keyspace_id",
				Type: sqltypes.VarBinary,
			},
			Vindex:        cv.Vindex,
			VindexColumns: vindexColumns,
		}, nil
	case *sqlparser.FuncExpr:
		switch inner.Name.Lowered() {
		case "keyspace_id":
			// This function is used internally to route queries and records properly
			// in sharded keyspaces using vindexes.
			if len(inner.Exprs) != 0 {
				return ColExpr{}, fmt.Errorf("unexpected: %v", sqlparser.String(inner))
			}
			cv, err := vschema.FindColVindex(plan.Table.Name)
			if err != nil {
				return ColExpr{}, err
			}
			vindexColumns, err := buildVindexColumns(plan.Table, cv.Columns)
			if err != nil {
				return ColExpr{}, err
			}
			return ColExpr{
				Field: &querypb.Field{
					Name: "keyspace_id",
					Type: sqltypes.VarBinary,
				},
				Vindex:        cv.Vindex,
				VindexColumns: vindexColumns,
			}, nil
		case "convert_tz":
			// This function is used when transforming datetime
			// values between the source and target.
			colnum, err := findColumn(plan.Table, aliased.As)
			if err != nil {
				return ColExpr{}, err
			}
			field := plan.Table.Fields[colnum]
			plan.setColumnFuncExpr(field.Name, inner)
			return ColExpr{
				ColNum: colnum,
				Field:  field,
			}, nil
		default:
			return ColExpr{}, fmt.Errorf("unsupported function: %v", sqlparser.String(inner))
		}
	case *sqlparser.Literal:
		//allow only intval 1
		if inner.Type != sqlparser.IntVal {
			return ColExpr{}, fmt.Errorf("only integer literals are supported")
		}
		num, err := strconv.ParseInt(string(inner.Val), 0, 64)
		if err != nil {
			return ColExpr{}, err
		}
		if num != 1 {
			return ColExpr{}, fmt.Errorf("only the integer literal 1 is supported")
		}
		return ColExpr{
			Field: &querypb.Field{
				Name: "1",
				Type: querypb.Type_INT64,
			},
			ColNum:     -1,
			FixedValue: sqltypes.NewInt64(num),
		}, nil
	case *sqlparser.ConvertUsingExpr:
		colnum, err := findColumn(plan.Table, aliased.As)
		if err != nil {
			return ColExpr{}, err
		}
		field := plan.Table.Fields[colnum]
		plan.setConvertColumnUsingUTF8(field.Name)
		return ColExpr{
			ColNum: colnum,
			Field:  field,
		}, nil
	default:
		log.Infof("Unsupported expression: %v", inner)
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(aliased.Expr))
	}
}

// analyzeInKeyRange allows the following constructs: "in_keyrange('-80')",
// "in_keyrange(col, 'hash', '-80')", "in_keyrange(col, 'local_vindex', '-80')", or
// "in_keyrange(col, 'ks.external_vindex', '-80')".
func (plan *Plan) analyzeInKeyRange(vschema *localVSchema, exprs sqlparser.SelectExprs) error {
	var colnames []sqlparser.IdentifierCI
	var krExpr sqlparser.SelectExpr
	whereFilter := Filter{
		Opcode: VindexMatch,
	}
	switch {
	case len(exprs) == 1:
		cv, err := vschema.FindColVindex(plan.Table.Name)
		if err != nil {
			return err
		}
		colnames = cv.Columns
		whereFilter.Vindex = cv.Vindex
		krExpr = exprs[0]
	case len(exprs) >= 3:
		for _, expr := range exprs[:len(exprs)-2] {
			aexpr, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected: %T %s", expr, sqlparser.String(expr))
			}
			qualifiedName, ok := aexpr.Expr.(*sqlparser.ColName)
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected: %T %s", aexpr.Expr, sqlparser.String(aexpr.Expr))
			}
			if !qualifiedName.Qualifier.IsEmpty() {
				return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported qualifier for column: %v", sqlparser.String(qualifiedName))
			}
			colnames = append(colnames, qualifiedName.Name)
		}

		vtype, err := selString(exprs[len(exprs)-2])
		if err != nil {
			return err
		}
		whereFilter.Vindex, err = vschema.FindOrCreateVindex(vtype)
		if err != nil {
			return err
		}
		if !whereFilter.Vindex.IsUnique() {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex must be Unique to be used for VReplication: %s", vtype)
		}

		krExpr = exprs[len(exprs)-1]
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected in_keyrange parameters: %v", sqlparser.String(exprs))
	}
	var err error
	whereFilter.VindexColumns, err = buildVindexColumns(plan.Table, colnames)
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
	whereFilter.KeyRange = keyranges[0]
	plan.Filters = append(plan.Filters, whereFilter)
	return nil
}

func selString(expr sqlparser.SelectExpr) (string, error) {
	aexpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
	}
	val, ok := aexpr.Expr.(*sqlparser.Literal)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
	}
	return string(val.Val), nil
}

// buildVindexColumns builds the list of column numbers of the table
// that will be the input to the vindex function.
func buildVindexColumns(ti *Table, colnames []sqlparser.IdentifierCI) ([]int, error) {
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

func findColumn(ti *Table, name sqlparser.IdentifierCI) (int, error) {
	for i, col := range ti.Fields {
		if name.EqualString(col.Name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %s not found in table %s", sqlparser.String(name), ti.Name)
}
