/*
Copyright 2023 The Vitess Authors.

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
	"math/rand"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Mirror represents the instructions to execute an authoritative source,
// and compare the results of that execution to those of one or more
// non-authoritative mirroring targets.
type (
	Mirror struct {
		Primitive Primitive
		Targets   []MirrorTarget
	}

	MirrorTarget interface {
		Primitive
		Accept() bool
	}

	PercentMirrorTarget struct {
		Percent   float32
		Primitive Primitive
	}
)

var (
	_ Primitive    = (*Mirror)(nil)
	_ Primitive    = (MirrorTarget)(nil)
	_ MirrorTarget = (*PercentMirrorTarget)(nil)
)

// NewMirror creates a Mirror.
func NewMirror(primitive Primitive, targets []MirrorTarget) *Mirror {
	return &Mirror{
		primitive,
		targets,
	}
}

func (m *Mirror) RouteType() string {
	return "Mirror"
}

func (m *Mirror) GetKeyspaceName() string {
	return m.Primitive.GetKeyspaceName()
}

func (m *Mirror) GetTableName() string {
	return m.Primitive.GetTableName()
}

func (m *Mirror) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return m.Primitive.GetFields(ctx, vcursor, bindVars)
}

func (m *Mirror) NeedsTransaction() bool {
	return m.Primitive.NeedsTransaction()
}

func (m *Mirror) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for _, target := range m.Targets {
		if !target.Accept() {
			continue
		}

		wg.Add(1)
		go func(target Primitive, vcursor VCursor) {
			defer wg.Done()
			_, _ = target.TryExecute(ctx, vcursor, bindVars, wantfields)
		}(target, vcursor.CloneForMirroring(ctx))
	}

	return m.Primitive.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (m *Mirror) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for _, target := range m.Targets {
		if !target.Accept() {
			continue
		}

		wg.Add(1)
		go func(target Primitive, vcursor VCursor) {
			defer wg.Done()
			_ = target.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(_ *sqltypes.Result) error {
				return nil
			})
		}(target, vcursor.CloneForMirroring(ctx))
	}

	return m.Primitive.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

// Inputs is a slice containing the inputs to this Primitive.
// The returned map has additional information about the inputs, that is used in the description.
func (m *Mirror) Inputs() ([]Primitive, []map[string]any) {
	inputs := make([]Primitive, 1+len(m.Targets))
	inputs[0] = m.Primitive
	for i, target := range m.Targets {
		inputs[i+1] = target
	}
	return inputs, nil
}

// description is the description, sans the inputs, of this Primitive.
// to get the plan description with all children, use PrimitiveToPlanDescription()
func (m *Mirror) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Mirror",
	}
}

func (m *PercentMirrorTarget) RouteType() string {
	return "PercentMirrorTarget"
}

func (m *PercentMirrorTarget) GetKeyspaceName() string {
	return m.Primitive.GetKeyspaceName()
}

func (m *PercentMirrorTarget) GetTableName() string {
	return m.Primitive.GetTableName()
}

func (m *PercentMirrorTarget) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return m.Primitive.GetFields(ctx, vcursor, bindVars)
}

func (m *PercentMirrorTarget) NeedsTransaction() bool {
	return m.Primitive.NeedsTransaction()
}

func (m *PercentMirrorTarget) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return m.Primitive.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (m *PercentMirrorTarget) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return m.Primitive.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

// Inputs is a slice containing the inputs to this Primitive.
// The returned map has additional information about the inputs, that is used in the description.
func (m *PercentMirrorTarget) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{m.Primitive}, nil
}

// description is the description, sans the inputs, of this Primitive.
// to get the plan description with all children, use PrimitiveToPlanDescription()
func (m *PercentMirrorTarget) description() PrimitiveDescription {
	other := map[string]any{
		"Percent": m.Percent,
	}

	return PrimitiveDescription{
		OperatorType: "MirrorTarget",
		Variant:      "Percent",
		Other:        other,
	}
}

func (m *PercentMirrorTarget) Accept() bool {
	return m.Percent > (rand.Float32() * 100.0)
}
