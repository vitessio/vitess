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
	"math/rand/v2"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// percentBasedMirror represents the instructions to execute an
	// authoritative primitive and, based on whether a die-roll exceeds a
	// percentage, to also execute a target Primitive.
	percentBasedMirror struct {
		percent   float32
		primitive Primitive
		target    Primitive
	}
)

const (
	// maxMirrorTargetDuration is the maximum time a mirror target query
	// may execute before being cancelled. This is a safety bound to
	// prevent leaked goroutines; it does not affect primary query latency.
	maxMirrorTargetDuration = 30 * time.Second
)

var _ Primitive = (*percentBasedMirror)(nil)

// NewPercentBasedMirror creates a Mirror.
func NewPercentBasedMirror(percentage float32, primitive Primitive, target Primitive) Primitive {
	return &percentBasedMirror{percent: percentage, primitive: primitive, target: target}
}

func (m *percentBasedMirror) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return m.primitive.GetFields(ctx, vcursor, bindVars)
}

func (m *percentBasedMirror) NeedsTransaction() bool {
	return m.primitive.NeedsTransaction()
}

func (m *percentBasedMirror) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if !m.percentAtLeastDieRoll() {
		return vcursor.ExecutePrimitive(ctx, m.primitive, bindVars, wantfields)
	}

	// Execute the primary query first so we know its exec time.
	sourceStartTime := time.Now()
	r, err := vcursor.ExecutePrimitive(ctx, m.primitive, bindVars, wantfields)
	sourceExecTime := time.Since(sourceStartTime)

	// Fire mirror query in the background. Use a detached context so it
	// is not cancelled when this function returns the primary result.
	// The cloned VCursor has an independent SafeSession and logStats,
	// so it is safe to use after the primary request completes.
	mirrorCtx, mirrorCancel := context.WithTimeout(context.Background(), maxMirrorTargetDuration)
	mirrorVCursor := vcursor.CloneForMirroring(mirrorCtx)
	go func() {
		defer mirrorCancel()
		targetStartTime := time.Now()
		_, targetErr := mirrorVCursor.ExecutePrimitive(mirrorCtx, m.target, bindVars, wantfields)
		mirrorVCursor.RecordMirrorStats(sourceExecTime, time.Since(targetStartTime), targetErr)
	}()

	return r, err
}

func (m *percentBasedMirror) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if !m.percentAtLeastDieRoll() {
		return vcursor.StreamExecutePrimitive(ctx, m.primitive, bindVars, wantfields, callback)
	}

	// Execute the primary stream first so we know its exec time.
	sourceStartTime := time.Now()
	err := vcursor.StreamExecutePrimitive(ctx, m.primitive, bindVars, wantfields, callback)
	sourceExecTime := time.Since(sourceStartTime)

	// Fire mirror stream in the background.
	mirrorCtx, mirrorCancel := context.WithTimeout(context.Background(), maxMirrorTargetDuration)
	mirrorVCursor := vcursor.CloneForMirroring(mirrorCtx)
	go func() {
		defer mirrorCancel()
		targetStartTime := time.Now()
		targetErr := mirrorVCursor.StreamExecutePrimitive(mirrorCtx, m.target, bindVars, wantfields, func(_ *sqltypes.Result) error {
			return nil
		})
		mirrorVCursor.RecordMirrorStats(sourceExecTime, time.Since(targetStartTime), targetErr)
	}()

	return err
}

// Inputs is a slice containing the inputs to this Primitive.
// The returned map has additional information about the inputs, that is used in the description.
func (m *percentBasedMirror) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{m.primitive, m.target}, nil
}

// description is the description, sans the inputs, of this Primitive.
// to get the plan description with all children, use PrimitiveToPlanDescription()
func (m *percentBasedMirror) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Mirror",
		Variant:      "PercentBased",
		Other: map[string]any{
			"Percent": m.percent,
		},
	}
}

func (m *percentBasedMirror) percentAtLeastDieRoll() bool {
	return m.percent >= (rand.Float32() * 100.0)
}
