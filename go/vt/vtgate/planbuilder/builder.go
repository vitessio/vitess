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

// ContextVSchema defines the interface for this package to fetch
// info about tables.
type ContextVSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error)
	FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	DefaultKeyspace() (*vindexes.Keyspace, error)
	TargetString() string
	Destination() key.Destination
	TabletType() topodatapb.TabletType
	TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error)
	AnyKeyspace() (*vindexes.Keyspace, error)
	FirstSortedKeyspace() (*vindexes.Keyspace, error)
	SysVarSetEnabled() bool
}

//-------------------------------------------------------------------------

// builderCommon implements some common functionality of builders.
// Make sure to override in case behavior needs to be changed.
type builderCommon struct {
	order int
	input logicalPlan
}

func newBuilderCommon(input logicalPlan) builderCommon {
	return builderCommon{input: input}
}

func (bc *builderCommon) Order() int {
	return bc.order
}

func (bc *builderCommon) Reorder(order int) {
	bc.input.Reorder(order)
	bc.order = bc.input.Order() + 1
}

func (bc *builderCommon) ResultColumns() []*resultColumn {
	return bc.input.ResultColumns()
}

func (bc *builderCommon) Wireup(bldr logicalPlan, jt *jointab) error {
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

func (bc *builderCommon) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "builderCommon: wrong number of inputs")
	}
	bc.input = inputs[0]
	return nil
}

func (bc *builderCommon) Inputs() []logicalPlan {
	return []logicalPlan{bc.input}
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

func newResultsBuilder(input logicalPlan, truncater truncater) resultsBuilder {
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

// SupplyCol is currently unreachable because the plans using resultsBuilder
// are currently above a join, which is the only logical plan that uses it for now.
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
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema ContextVSchema, bindVarNeeds *sqlparser.BindVarNeeds) (*engine.Plan, error) {
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
		return buildRoutePlan(stmt, vschema, buildSelectPlan(query))
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
		if sqlparser.IsOnlineSchemaDDL(stmt, query) {
			return buildOnlineDDLPlan(query, stmt, vschema)
		}
		return buildDDLPlan(query, stmt, vschema)
	case *sqlparser.Use:
		return buildUsePlan(stmt, vschema)
	case *sqlparser.Explain:
		if stmt.Type == sqlparser.VitessType {
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
	case *sqlparser.Load:
		return buildLoadPlan(query, vschema)
	case *sqlparser.DBDDL:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Database DDL %v", sqlparser.String(stmt))
	case *sqlparser.SetTransaction:
		return nil, ErrPlanNotSupported
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint, *sqlparser.SRollback, *sqlparser.Release:
		// Empty by design. Not executed by a plan
		return nil, nil
	case *sqlparser.Show:
		return buildShowPlan(stmt, vschema)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected statement type: %T", stmt)
}

func buildLoadPlan(query string, vschema ContextVSchema) (engine.Primitive, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}

	destination := vschema.Destination()
	if destination == nil {
		if keyspace.Sharded {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: this construct is not supported on sharded keyspace")
		}
		destination = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             query,
		IsDML:             true,
		SingleShardOnly:   true,
	}, nil
}
