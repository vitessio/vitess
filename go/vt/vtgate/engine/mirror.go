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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var errMirrorTargetQueryTookTooLong = vterrors.Errorf(vtrpc.Code_ABORTED, "Mirror target query took too long")

type (
	// percentBasedMirror represents the instructions to execute an
	// authoritative primitive and, based on whether a die-roll exceeds a
	// percentage, to also execute a target Primitive.
	percentBasedMirror struct {
		percent   float32
		primitive Primitive
		target    Primitive
	}

	mirrorResult struct {
		execTime time.Duration
		err      error
	}
)

const (
	// maxMirrorTargetLag limits how long a mirror target may continue
	// executing after the main primitive has finished.
	maxMirrorTargetLag = 100 * time.Millisecond
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

	mirrorCh := make(chan mirrorResult, 1)
	mirrorCtx, mirrorCtxCancel := context.WithCancel(ctx)
	defer mirrorCtxCancel()

	go func() {
		mirrorVCursor := vcursor.CloneForMirroring(mirrorCtx)
		targetStartTime := time.Now()
		_, targetErr := mirrorVCursor.ExecutePrimitive(mirrorCtx, m.target, bindVars, wantfields)
		mirrorCh <- mirrorResult{
			execTime: time.Since(targetStartTime),
			err:      targetErr,
		}
	}()

	var (
		sourceExecTime, targetExecTime time.Duration
		targetErr                      error
	)

	sourceStartTime := time.Now()
	r, err := vcursor.ExecutePrimitive(ctx, m.primitive, bindVars, wantfields)
	sourceExecTime = time.Since(sourceStartTime)

	// Cancel the mirror context if it continues executing too long.
	select {
	case r := <-mirrorCh:
		// Mirror target finished on time.
		targetExecTime = r.execTime
		targetErr = r.err
	case <-time.After(maxMirrorTargetLag):
		// Mirror target took too long.
		mirrorCtxCancel()
		targetExecTime = sourceExecTime + maxMirrorTargetLag
		targetErr = errMirrorTargetQueryTookTooLong
	}

	vcursor.RecordMirrorStats(sourceExecTime, targetExecTime, targetErr)

	return r, err
}

func (m *percentBasedMirror) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if !m.percentAtLeastDieRoll() {
		return vcursor.StreamExecutePrimitive(ctx, m.primitive, bindVars, wantfields, callback)
	}

	mirrorCh := make(chan mirrorResult, 1)
	mirrorCtx, mirrorCtxCancel := context.WithCancel(ctx)
	defer mirrorCtxCancel()

	go func() {
		mirrorVCursor := vcursor.CloneForMirroring(mirrorCtx)
		mirrorStartTime := time.Now()
		targetErr := mirrorVCursor.StreamExecutePrimitive(mirrorCtx, m.target, bindVars, wantfields, func(_ *sqltypes.Result) error {
			return nil
		})
		mirrorCh <- mirrorResult{
			execTime: time.Since(mirrorStartTime),
			err:      targetErr,
		}
	}()

	var (
		sourceExecTime, targetExecTime time.Duration
		targetErr                      error
	)

	sourceStartTime := time.Now()
	err := vcursor.StreamExecutePrimitive(ctx, m.primitive, bindVars, wantfields, callback)
	sourceExecTime = time.Since(sourceStartTime)

	// Cancel the mirror context if it continues executing too long.
	select {
	case r := <-mirrorCh:
		// Mirror target finished on time.
		targetExecTime = r.execTime
		targetErr = r.err
	case <-time.After(maxMirrorTargetLag):
		// Mirror target took too long.
		mirrorCtxCancel()
		targetExecTime = sourceExecTime + maxMirrorTargetLag
		targetErr = errMirrorTargetQueryTookTooLong
	}

	vcursor.RecordMirrorStats(sourceExecTime, targetExecTime, targetErr)

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
