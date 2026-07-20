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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// prepareStmtVCursor fakes the session prepared-statement state and the
// planner used by the PrepareStmt and DeallocateStmt primitives.
type prepareStmtVCursor struct {
	noopVCursor

	udvs        map[string]*querypb.BindVariable
	prepareData map[string]*vtgatepb.PrepareData

	plannedQuery string
	plan         *Plan
	planErr      error
}

func (f *prepareStmtVCursor) Session() SessionActions {
	return f
}

func (f *prepareStmtVCursor) GetUDV(key string) *querypb.BindVariable {
	return f.udvs[key]
}

func (f *prepareStmtVCursor) StorePrepareData(name string, v *vtgatepb.PrepareData) {
	f.prepareData[name] = v
}

func (f *prepareStmtVCursor) GetPrepareData(name string) *vtgatepb.PrepareData {
	return f.prepareData[name]
}

func (f *prepareStmtVCursor) ClearPrepareData(name string) {
	delete(f.prepareData, name)
}

func (f *prepareStmtVCursor) PlanPrepareStatement(ctx context.Context, query string) (*Plan, error) {
	f.plannedQuery = query
	return f.plan, f.planErr
}

func TestPrepareStmtFromLiteral(t *testing.T) {
	vc := &prepareStmtVCursor{
		prepareData: map[string]*vtgatepb.PrepareData{},
		plan:        &Plan{Original: "select id from t where id = :v1", ParamsCount: 1},
	}
	prep := &PrepareStmt{Name: "p1", Query: "select id from t where id = ?"}

	qr, err := prep.TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)
	require.Equal(t, "select id from t where id = ?", vc.plannedQuery)
	require.Equal(t, &vtgatepb.PrepareData{
		PrepareStatement: "select id from t where id = :v1",
		ParamsCount:      1,
	}, vc.prepareData["p1"])
}

func TestPrepareStmtFromUserDefinedVariable(t *testing.T) {
	vc := &prepareStmtVCursor{
		udvs:        map[string]*querypb.BindVariable{"stmt_text": sqltypes.StringBindVariable("select 1")},
		prepareData: map[string]*vtgatepb.PrepareData{},
		plan:        &Plan{Original: "select 1", ParamsCount: 0},
	}
	prep := &PrepareStmt{Name: "p1", UserDefinedVariable: "stmt_text"}

	_, err := prep.TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	require.Equal(t, "select 1", vc.plannedQuery)
	require.Equal(t, "select 1", vc.prepareData["p1"].PrepareStatement)
}

func TestPrepareStmtFromUndefinedUserDefinedVariable(t *testing.T) {
	// A PREPARE that cannot resolve its statement text must fail and, per
	// MySQL semantics, deallocate the statement previously registered under
	// the same name.
	vc := &prepareStmtVCursor{
		prepareData: map[string]*vtgatepb.PrepareData{
			"p1": {PrepareStatement: "select 1"},
		},
	}
	prep := &PrepareStmt{Name: "p1", UserDefinedVariable: "stmt_text"}

	_, err := prep.TryExecute(t.Context(), vc, nil, false)
	require.ErrorContains(t, err, "'stmt_text' user defined variable does not exists")
	require.NotContains(t, vc.prepareData, "p1")
}

func TestPrepareStmtPlanningFailure(t *testing.T) {
	// A PREPARE whose statement text fails to plan must fail and, per MySQL
	// semantics, deallocate the statement previously registered under the
	// same name.
	vc := &prepareStmtVCursor{
		prepareData: map[string]*vtgatepb.PrepareData{
			"p1": {PrepareStatement: "select 1"},
		},
		planErr: sqltypes.ErrIncompatibleTypeCast,
	}
	prep := &PrepareStmt{Name: "p1", Query: "select invalid"}

	_, err := prep.TryExecute(t.Context(), vc, nil, false)
	require.ErrorIs(t, err, sqltypes.ErrIncompatibleTypeCast)
	require.NotContains(t, vc.prepareData, "p1")
}

func TestDeallocateStmt(t *testing.T) {
	vc := &prepareStmtVCursor{
		prepareData: map[string]*vtgatepb.PrepareData{
			"p1": {PrepareStatement: "select 1"},
		},
	}
	dealloc := &DeallocateStmt{Name: "p1"}

	qr, err := dealloc.TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)
	require.NotContains(t, vc.prepareData, "p1")
}

func TestDeallocateStmtUnknownStatement(t *testing.T) {
	vc := &prepareStmtVCursor{
		prepareData: map[string]*vtgatepb.PrepareData{},
	}
	dealloc := &DeallocateStmt{Name: "p1"}

	_, err := dealloc.TryExecute(t.Context(), vc, nil, false)
	require.ErrorContains(t, err, "Unknown prepared statement handler (p1) given to DEALLOCATE PREPARE")
}
