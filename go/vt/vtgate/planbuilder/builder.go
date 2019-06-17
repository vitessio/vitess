/*
Copyright 2017 Google Inc.

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
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
// It's the main entry point for this package.
func Build(query string, vschema ContextVSchema) (*engine.Plan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	return BuildFromStmt(query, stmt, vschema)
}

// BuildFromStmt builds a plan based on the AST provided.
// TODO(sougou): The query input is trusted as the source
// of the AST. Maybe this function just returns instructions
// and engine.Plan can be built by the caller.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema ContextVSchema) (*engine.Plan, error) {
	var err error
	plan := &engine.Plan{
		Original: query,
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan.Instructions, err = buildSelectPlan(stmt, vschema)
	case *sqlparser.Insert:
		plan.Instructions, err = buildInsertPlan(stmt, vschema)
	case *sqlparser.Update:
		plan.Instructions, err = buildUpdatePlan(stmt, vschema)
	case *sqlparser.Delete:
		plan.Instructions, err = buildDeletePlan(stmt, vschema)
	case *sqlparser.Union:
		plan.Instructions, err = buildUnionPlan(stmt, vschema)
	case *sqlparser.Set:
		return nil, errors.New("unsupported construct: set")
	case *sqlparser.Show:
		return nil, errors.New("unsupported construct: show")
	case *sqlparser.DDL:
		return nil, errors.New("unsupported construct: ddl")
	case *sqlparser.DBDDL:
		return nil, errors.New("unsupported construct: ddl on database")
	case *sqlparser.OtherRead:
		return nil, errors.New("unsupported construct: other read")
	case *sqlparser.OtherAdmin:
		return nil, errors.New("unsupported construct: other admin")
	case *sqlparser.Begin:
		return nil, errors.New("unsupported construct: begin")
	case *sqlparser.Commit:
		return nil, errors.New("unsupported construct: commit")
	case *sqlparser.Rollback:
		return nil, errors.New("unsupported construct: rollback")
	default:
		return nil, fmt.Errorf("BUG: unexpected statement type: %T", stmt)
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
