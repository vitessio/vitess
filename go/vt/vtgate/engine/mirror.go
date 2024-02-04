/*
Copyright 2024 The Vitess Authors.

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

type (
	// Mirror represents the instructions to execute an authoritative source,
	// and compare the results of that execution to those of one or more
	// non-authoritative mirroring targets.
	Mirror struct {
		Primitive Primitive
		Target    MirrorTarget
	}

	// MirrorTarget contains the Primitive for mirroring a query to the
	// non-authoritative target of the Mirror primitive.
	MirrorTarget interface {
		Primitive
		Accept() bool
	}

	// PercentMirrorTarget contains the Primitive to mirror to, an will
	// Accept() an execution based if a random dice-roll is less than Percent.
	PercentMirrorTarget struct {
		Percent   float32
		Primitive Primitive
	}
)

var (
	_ Primitive    = (*Mirror)(nil)
	_ Primitive    = (MirrorTarget)(nil)
	_ Primitive    = (*PercentMirrorTarget)(nil)
	_ MirrorTarget = (*PercentMirrorTarget)(nil)
)

// NewMirror creates a Mirror.
func NewMirror(primitive Primitive, target MirrorTarget) *Mirror {
	return &Mirror{primitive, target}
}

// NewPercentMirrorTarget creates a percentage-based Mirror target.
func NewPercentMirrorTarget(percent float32, primitive Primitive) *PercentMirrorTarget {
	return &PercentMirrorTarget{percent, primitive}
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
	if m.Target.Accept() {
		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func(target Primitive, vcursor VCursor) {
			defer wg.Done()
			_, _ = target.TryExecute(ctx, vcursor, bindVars, wantfields)
		}(m.Target, vcursor.CloneForMirroring(ctx))
	}

	return m.Primitive.TryExecute(ctx, vcursor, bindVars, wantfields)
}

// TODO(maxeng): does it make sense mirror stream executions? One of the goals
// of mirroring is to be able to compare the error codes returned by the main
// and mirrored queries. Can we do that here?
func (m *Mirror) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if m.Target.Accept() {
		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func(target Primitive, vcursor VCursor) {
			defer wg.Done()
			_ = target.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(_ *sqltypes.Result) error {
				return nil
			})
		}(m.Target, vcursor.CloneForMirroring(ctx))
	}

	return m.Primitive.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

// Inputs is a slice containing the inputs to this Primitive.
// The returned map has additional information about the inputs, that is used in the description.
func (m *Mirror) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{m.Primitive, m.Target}, nil
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
	return PrimitiveDescription{
		OperatorType: "MirrorTarget",
		Variant:      "Percent",
		Other: map[string]any{
			"Percent": m.Percent,
		},
	}
}

func (m *PercentMirrorTarget) Accept() bool {
	return m.Percent >= (rand.Float32() * 100.0)
}
