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

package planbuilder

import (
	"errors"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

//-------------------------------------------------------------------------

// builder defines the interface that a primitive must
// satisfy.
type builder interface {
	// Order is the execution order of the primitive. If there are subprimitives,
	// the order is one above the order of the subprimitives.
	// This is because the primitive executes its subprimitives first and
	// processes their results to generate its own values.
	// Please copy code from an existing primitive to define this function.
	Order() int

	// ResultColumns returns the list of result columns the
	// primitive returns.
	// Please copy code from an existing primitive to define this function.
	ResultColumns() []*resultColumn

	// Reorder reassigns order for the primitive and its sub-primitives.
	// The input is the order of the previous primitive that should
	// execute before this one.
	Reorder(int)

	// First returns the first builder of the tree,
	// which is usually the left most.
	First() builder

	// PushFilter pushes a WHERE or HAVING clause expression
	// to the specified origin.
	PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error

	// PushSelect pushes the select expression to the specified
	// originator. If successful, the originator must create
	// a resultColumn entry and return it. The top level caller
	// must accumulate these result columns and set the symtab
	// after analysis.
	PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error)

	// MakeDistinct makes the primitive handle the distinct clause.
	MakeDistinct() error
	// PushGroupBy makes the primitive handle the GROUP BY clause.
	PushGroupBy(sqlparser.GroupBy) error

	// PushOrderBy pushes the ORDER BY clause. It returns the
	// the current primitive or a replacement if a new one was
	// created.
	PushOrderBy(sqlparser.OrderBy) (builder, error)

	// SetUpperLimit is an optimization hint that tells that primitive
	// that it does not need to return more than the specified number of rows.
	// A primitive that cannot perform this can ignore the request.
	SetUpperLimit(count *sqlparser.SQLVal)

	// PushMisc pushes miscelleaneous constructs to all the primitives.
	PushMisc(sel *sqlparser.Select)

	// Wireup performs the wire-up work. Nodes should be traversed
	// from right to left because the rhs nodes can request vars from
	// the lhs nodes.
	Wireup(bldr builder, jt *jointab) error

	// SupplyVar finds the common root between from and to. If it's
	// the common root, it supplies the requested var to the rhs tree.
	// If the primitive already has the column in its list, it should
	// just supply it to the 'to' node. Otherwise, it should request
	// for it by calling SupplyCol on the 'from' sub-tree to request the
	// column, and then supply it to the 'to' node.
	SupplyVar(from, to int, col *sqlparser.ColName, varname string)

	// SupplyCol is meant to be used for the wire-up process. This function
	// changes the primitive to supply the requested column and returns
	// the resultColumn and column number of the result. SupplyCol
	// is different from PushSelect because it may reuse an existing
	// resultColumn, whereas PushSelect guarantees the addition of a new
	// result column and returns a distinct symbol for it.
	SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int)

	// SupplyWeightString must supply a weight_string expression of the
	// specified column.
	SupplyWeightString(colNumber int) (weightcolNumber int, err error)

	// PushLock pushes "FOR UPDATE", "LOCK IN SHARE MODE" down to all routes
	PushLock(lock string) error

	// Primitive returns the underlying primitive.
	// This function should only be called after Wireup is finished.
	Primitive() engine.Primitive
}

//-------------------------------------------------------------------------

// ContextVSchema defines the interface for this package to fetch
// info about tables.
type ContextVSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error)
	FindTablesOrVindex(tablename sqlparser.TableName) ([]*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	DefaultKeyspace() (*vindexes.Keyspace, error)
	TargetString() string
	Destination() key.Destination
	TabletType() topodatapb.TabletType
	TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error)
	AnyKeyspace() (*vindexes.Keyspace, error)
	FirstSortedKeyspace() (*vindexes.Keyspace, error)
}

//-------------------------------------------------------------------------

// builderCommon implements some common functionality of builders.
// Make sure to override in case behavior needs to be changed.
type builderCommon struct {
	order int
	input builder
}

func newBuilderCommon(input builder) builderCommon {
	return builderCommon{input: input}
}

func (bc *builderCommon) Order() int {
	return bc.order
}

func (bc *builderCommon) Reorder(order int) {
	bc.input.Reorder(order)
	bc.order = bc.input.Order() + 1
}

func (bc *builderCommon) First() builder {
	return bc.input.First()
}

func (bc *builderCommon) ResultColumns() []*resultColumn {
	return bc.input.ResultColumns()
}

func (bc *builderCommon) SetUpperLimit(count *sqlparser.SQLVal) {
	bc.input.SetUpperLimit(count)
}

func (bc *builderCommon) PushMisc(sel *sqlparser.Select) {
	bc.input.PushMisc(sel)
}

func (bc *builderCommon) Wireup(bldr builder, jt *jointab) error {
	return bc.input.Wireup(bldr, jt)
}

func (bc *builderCommon) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	bc.input.SupplyVar(from, to, col, varname)
}

func (bc *builderCommon) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	return bc.input.SupplyCol(col)
}

func (bc *builderCommon) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return bc.input.SupplyWeightString(colNumber)
}

//-------------------------------------------------------------------------

type truncater interface {
	SetTruncateColumnCount(int)
}

// resultsBuilder is a superset of builderCommon. It also handles
// resultsColumn functionality.
type resultsBuilder struct {
	builderCommon
	resultColumns []*resultColumn
	weightStrings map[*resultColumn]int
	truncater     truncater
}

func newResultsBuilder(input builder, truncater truncater) resultsBuilder {
	return resultsBuilder{
		builderCommon: newBuilderCommon(input),
		resultColumns: input.ResultColumns(),
		weightStrings: make(map[*resultColumn]int),
		truncater:     truncater,
	}
}

func (rsb *resultsBuilder) ResultColumns() []*resultColumn {
	return rsb.resultColumns
}

// SupplyCol is currently unreachable because the builders using resultsBuilder
// are currently above a join, which is the only builder that uses it for now.
// This can change if we start supporting correlated subqueries.
func (rsb *resultsBuilder) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range rsb.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}
	rc, colNumber = rsb.input.SupplyCol(col)
	if colNumber < len(rsb.resultColumns) {
		return rc, colNumber
	}
	// Add result columns from input until colNumber is reached.
	for colNumber >= len(rsb.resultColumns) {
		rsb.resultColumns = append(rsb.resultColumns, rsb.input.ResultColumns()[len(rsb.resultColumns)])
	}
	rsb.truncater.SetTruncateColumnCount(len(rsb.resultColumns))
	return rc, colNumber
}

func (rsb *resultsBuilder) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	rc := rsb.resultColumns[colNumber]
	if weightcolNumber, ok := rsb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	weightcolNumber, err = rsb.input.SupplyWeightString(colNumber)
	if err != nil {
		return 0, nil
	}
	rsb.weightStrings[rc] = weightcolNumber
	if weightcolNumber < len(rsb.resultColumns) {
		return weightcolNumber, nil
	}
	// Add result columns from input until weightcolNumber is reached.
	for weightcolNumber >= len(rsb.resultColumns) {
		rsb.resultColumns = append(rsb.resultColumns, rsb.input.ResultColumns()[len(rsb.resultColumns)])
	}
	rsb.truncater.SetTruncateColumnCount(len(rsb.resultColumns))
	return weightcolNumber, nil
}

//-------------------------------------------------------------------------

// Build builds a plan for a query based on the specified vschema.
// This method is only used from tests
func Build(query string, vschema ContextVSchema) (*engine.Plan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	result, err := sqlparser.RewriteAST(stmt)
	if err != nil {
		return nil, err
	}

	return BuildFromStmt(query, result.AST, vschema, result.BindVarNeeds)
}

// ErrPlanNotSupported is an error for plan building not supported
var ErrPlanNotSupported = errors.New("plan building not supported")

// BuildFromStmt builds a plan based on the AST provided.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema ContextVSchema, bindVarNeeds sqlparser.BindVarNeeds) (*engine.Plan, error) {
	instruction, err := createInstructionFor(query, stmt, vschema)
	if err != nil {
		return nil, err
	}
	plan := &engine.Plan{
		Type:         sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: instruction,
		BindVarNeeds: bindVarNeeds,
	}
	return plan, nil
}

func buildRoutePlan(stmt sqlparser.Statement, vschema ContextVSchema, f func(statement sqlparser.Statement, schema ContextVSchema) (engine.Primitive, error)) (engine.Primitive, error) {
	if vschema.Destination() != nil {
		return buildPlanForBypass(stmt, vschema)
	}
	return f(stmt, vschema)
}

func createInstructionFor(query string, stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return buildRoutePlan(stmt, vschema, buildSelectPlan)
	case *sqlparser.Insert:
		return buildRoutePlan(stmt, vschema, buildInsertPlan)
	case *sqlparser.Update:
		return buildRoutePlan(stmt, vschema, buildUpdatePlan)
	case *sqlparser.Delete:
		return buildRoutePlan(stmt, vschema, buildDeletePlan)
	case *sqlparser.Union:
		return buildRoutePlan(stmt, vschema, buildUnionPlan)
	case *sqlparser.DDL:
		if sqlparser.IsVschemaDDL(stmt) {
			return buildVSchemaDDLPlan(stmt, vschema)
		}
		return buildDDLPlan(query, stmt, vschema)
	case *sqlparser.Use:
		return buildUsePlan(stmt, vschema)
	case *sqlparser.Explain:
		if stmt.Type == sqlparser.VitessStr {
			innerInstruction, err := createInstructionFor(query, stmt.Statement, vschema)
			if err != nil {
				return nil, err
			}
			return buildExplainPlan(innerInstruction)
		}
		return buildOtherReadAndAdmin(query, vschema)
	case *sqlparser.OtherRead, *sqlparser.OtherAdmin:
		return buildOtherReadAndAdmin(query, vschema)
	case *sqlparser.Set:
		return buildSetPlan(stmt, vschema)
	case *sqlparser.DBDDL:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Database DDL %v", sqlparser.String(stmt))
	case *sqlparser.Show, *sqlparser.SetTransaction:
		return nil, ErrPlanNotSupported
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint, *sqlparser.SRollback, *sqlparser.Release:
		// Empty by design. Not executed by a plan
		return nil, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected statement type: %T", stmt)
}
