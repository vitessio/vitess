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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// SpecializedCondition lists arguments that need to point to the same value for the plan to be used
type SpecializedCondition struct {
	// these arguments need to have equal values for this condition to be true
	A, B *sqlparser.Argument
}

// Specialized is a plan that can be used when certain conditions are met.
// Otherwise, it falls back to a generic plan.
type Specialized struct {
	noTxNeeded

	// Conditions lists the conditions that need to be true for this plan to be used
	Conditions []SpecializedCondition
	Generic    Primitive

	Specific Primitive
}

func (s *Specialized) RouteType() string {
	return "Specialized"
}

func (s *Specialized) GetKeyspaceName() string {
	return s.Generic.GetKeyspaceName()
}

func (s *Specialized) GetTableName() string {
	return s.Generic.GetTableName()
}

func (s *Specialized) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if s.metCondition(bindVars) {
		return s.Specific.GetFields(ctx, vcursor, bindVars)
	}
	return s.Generic.GetFields(ctx, vcursor, bindVars)
}

func (s *Specialized) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if s.metCondition(bindVars) {
		return s.Specific.TryExecute(ctx, vcursor, bindVars, wantfields)
	}
	return s.Generic.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (s *Specialized) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if s.metCondition(bindVars) {
		return s.Specific.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
	}
	return s.Generic.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

func (s *Specialized) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{s.Generic, s.Specific}, []map[string]any{{inputName: "Generic"}, {inputName: "Specific"}}
}

func (s *Specialized) description() PrimitiveDescription {
	var conds []string
	for _, condition := range s.Conditions {
		conds = append(conds, condition.A.Name+"="+condition.B.Name)
	}

	return PrimitiveDescription{
		OperatorType: s.RouteType(),
		Other: map[string]interface{}{
			"Conditions": strings.Join(conds, ","),
		},
	}
}

func (s *Specialized) metCondition(bindVars map[string]*querypb.BindVariable) bool {
	for _, condition := range s.Conditions {
		aVal, ok := bindVars[condition.A.Name]
		if !ok {
			return false
		}
		bVal, ok := bindVars[condition.B.Name]
		if !ok {
			return false
		}
		if aVal.Type != bVal.Type || !bytes.Equal(aVal.Value, bVal.Value) {
			return false
		}
	}
	return true
}

var _ Primitive = (*Specialized)(nil)
