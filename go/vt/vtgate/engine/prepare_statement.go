/*
Copyright 2026 The Vitess Authors.

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

package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_ Primitive = (*PrepareStmt)(nil)
	_ Primitive = (*DeallocateStmt)(nil)
)

type (
	// PrepareStmt executes a SQL-level PREPARE statement: it plans the
	// statement text and registers it in the session under the given name.
	PrepareStmt struct {
		noTxNeeded
		noInputs

		// Name of the prepared statement.
		Name string
		// Query is the statement text to prepare. It is empty when the text
		// comes from the user defined variable named by UserDefinedVariable.
		Query string
		// UserDefinedVariable is the name of the user defined variable
		// holding the statement text.
		UserDefinedVariable string
		// FromPositionalParameter is set when the statement text is a
		// positional parameter (PREPARE ... FROM ?), which the grammar
		// accepts but cannot supply the statement text. Execution fails
		// after deallocating any statement previously registered under the
		// name, like any other PREPARE whose text cannot be resolved.
		FromPositionalParameter bool
	}

	// DeallocateStmt executes a SQL-level DEALLOCATE PREPARE statement: it
	// removes the named prepared statement from the session.
	DeallocateStmt struct {
		noTxNeeded
		noInputs

		// Name of the prepared statement.
		Name string
	}
)

func (p *PrepareStmt) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// MySQL deallocates any prepared statement already registered under this
	// name before preparing the new one, so a failed PREPARE leaves no
	// statement with this name behind.
	vcursor.Session().ClearPrepareData(p.Name)

	query, err := p.statementText(vcursor)
	if err != nil {
		return nil, err
	}

	plan, err := vcursor.PlanPrepareStatement(ctx, query)
	if err != nil {
		return nil, err
	}

	vcursor.Session().StorePrepareData(p.Name, &vtgatepb.PrepareData{
		PrepareStatement: plan.Original,
		ParamsCount:      int32(plan.ParamsCount),
	})
	return &sqltypes.Result{}, nil
}

func (p *PrepareStmt) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := p.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

func (p *PrepareStmt) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("GetFields is not supported for PREPARE")
}

// statementText returns the statement text to prepare, resolving it from the
// session when it comes from a user defined variable.
func (p *PrepareStmt) statementText(vcursor VCursor) (string, error) {
	if p.FromPositionalParameter {
		return "", vterrors.VT12001("PREPARE with a positional parameter as the statement text")
	}
	if p.UserDefinedVariable == "" {
		return p.Query, nil
	}
	bv := vcursor.Session().GetUDV(p.UserDefinedVariable)
	if bv == nil {
		return "", vterrors.VT03024(p.UserDefinedVariable)
	}
	val, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		return "", err
	}
	return val.ToString(), nil
}

func (p *PrepareStmt) description() PrimitiveDescription {
	other := map[string]any{
		"Name": p.Name,
	}
	switch {
	case p.FromPositionalParameter:
		other["PositionalParameter"] = true
	case p.UserDefinedVariable != "":
		other["UserDefinedVariable"] = p.UserDefinedVariable
	default:
		other["Query"] = p.Query
	}
	return PrimitiveDescription{
		OperatorType: "PREPARE",
		Other:        other,
	}
}

func (d *DeallocateStmt) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	session := vcursor.Session()
	if session.GetPrepareData(d.Name) == nil {
		return nil, vterrors.VT09011(d.Name, "DEALLOCATE PREPARE")
	}
	session.ClearPrepareData(d.Name)
	return &sqltypes.Result{}, nil
}

func (d *DeallocateStmt) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := d.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

func (d *DeallocateStmt) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("GetFields is not supported for DEALLOCATE PREPARE")
}

func (d *DeallocateStmt) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "DEALLOCATE",
		Other: map[string]any{
			"Name": d.Name,
		},
	}
}
