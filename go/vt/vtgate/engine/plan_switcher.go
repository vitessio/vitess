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

func (s *PlanSwitcher) RouteType() string {
	return "PlanSwitcher"
}

func (s *PlanSwitcher) GetKeyspaceName() string {
	if s.Baseline != nil {
		return s.Baseline.GetKeyspaceName()
	}
	return s.Optimized.GetKeyspaceName()
}

func (s *PlanSwitcher) GetTableName() string {
	if s.Baseline != nil {
		return s.Baseline.GetKeyspaceName()
	}
	return s.Optimized.GetKeyspaceName()
}

func (s *PlanSwitcher) GetFields(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	if s.metCondition(bindVars) {
		return s.Optimized.GetFields(ctx, vcursor, bindVars)
	}
	if s.Baseline == nil {
		return nil, s.BaselineErr
	}
	return s.Baseline.GetFields(ctx, vcursor, bindVars)
}

func (s *PlanSwitcher) TryExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
) (*sqltypes.Result, error) {
	if s.metCondition(bindVars) {
		s.addOptimizedExecStats(vcursor)
		return s.Optimized.TryExecute(ctx, vcursor, bindVars, wantfields)
	}
	if s.Baseline == nil {
		return nil, s.BaselineErr
	}
	return s.Baseline.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (s *PlanSwitcher) TryStreamExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
) error {
	if s.metCondition(bindVars) {
		s.addOptimizedExecStats(vcursor)
		return s.Optimized.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
	}
	if s.Baseline == nil {
		return s.BaselineErr
	}
	return s.Baseline.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
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
		OperatorType: s.RouteType(),
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
