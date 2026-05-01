/*
Copyright 2025 The Vitess Authors.

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
	"bytes"
	"context"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Condition lists arguments that need to point to the same value for the plan to be used
type Condition struct {
	// these arguments need to have equal values for this condition to be true
	A, B string
}

// PlanSwitcher chooses between a baseline and an optimized plan based on conditions
type PlanSwitcher struct {
	noTxNeeded

	// Conditions lists the conditions that need to be true for the optimized plan to be used
	Conditions  []Condition
	Baseline    Primitive
	BaselineErr error
	Optimized   Primitive
}

func (s *PlanSwitcher) GetFields(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	branch, _ := s.pickBranch(vcursor, bindVars)
	if branch == nil {
		return nil, s.BaselineErr
	}
	return branch.GetFields(ctx, vcursor, bindVars)
}

func (s *PlanSwitcher) TryExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
) (*sqltypes.Result, error) {
	branch, optimized := s.pickBranch(vcursor, bindVars)
	if branch == nil {
		return nil, s.BaselineErr
	}
	if optimized {
		s.addOptimizedExecStats(vcursor)
	}
	return branch.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (s *PlanSwitcher) TryStreamExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
) error {
	branch, optimized := s.pickBranch(vcursor, bindVars)
	if branch == nil {
		return s.BaselineErr
	}
	if optimized {
		s.addOptimizedExecStats(vcursor)
	}
	return branch.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

// pickBranch selects the branch to execute for the supplied bindVars, records
// it on the vcursor so later consumers don't need to re-evaluate the
// conditions, and reports whether the optimized branch was chosen.
func (s *PlanSwitcher) pickBranch(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (Primitive, bool) {
	if s.metCondition(bindVars) {
		vcursor.SetExecutedPrimitive(s.Optimized)
		return s.Optimized, true
	}
	vcursor.SetExecutedPrimitive(s.Baseline)
	return s.Baseline, false
}

func (s *PlanSwitcher) Inputs() ([]Primitive, []map[string]any) {
	conds := slice.Map(s.Conditions, func(c Condition) string {
		return c.A + "=" + c.B
	})
	specMap := map[string]any{
		inputName:    "Optimized",
		"Conditions": strings.Join(conds, ","),
	}
	if s.BaselineErr != nil || s.Baseline == nil {
		return []Primitive{s.Optimized}, []map[string]any{specMap}
	}
	genMap := map[string]any{
		inputName: "Baseline",
	}
	return []Primitive{s.Baseline, s.Optimized}, []map[string]any{genMap, specMap}
}

func (s *PlanSwitcher) description() PrimitiveDescription {
	other := map[string]any{}
	if s.BaselineErr != nil {
		other["BaselineErr"] = s.BaselineErr.Error()
	}
	return PrimitiveDescription{
		OperatorType: "PlanSwitcher",
		Other:        other,
	}
}

func (s *PlanSwitcher) metCondition(bindVars map[string]*querypb.BindVariable) bool {
	for _, condition := range s.Conditions {
		aVal, ok := bindVars[condition.A]
		if !ok {
			return false
		}
		bVal, ok := bindVars[condition.B]
		if !ok {
			return false
		}
		if aVal.Type != bVal.Type || !bytes.Equal(aVal.Value, bVal.Value) {
			return false
		}
	}
	return true
}

func (s *PlanSwitcher) addOptimizedExecStats(vcursor VCursor) {
	planType := getPlanType(s.Optimized)
	vcursor.GetExecutionMetrics().optimizedQueryExec.Add(planType.String(), 1)
}

var _ Primitive = (*PlanSwitcher)(nil)
