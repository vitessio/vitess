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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestDiscardExecute(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fields := sqltypes.MakeTestFields("col1", "int64")
	inputResult := sqltypes.MakeTestResult(fields, "1", "2")
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	disc := &Discard{Sources: []Primitive{fp}}

	result, err := disc.TryExecute(t.Context(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)

	// The source is executed for its side effects, but its result is
	// discarded: DO reports a plain OK with no rows and no fields.
	assert.Empty(t, result.Rows)
	assert.Empty(t, result.Fields)
	assert.EqualValues(t, 0, result.RowsAffected)

	// The source primitive was actually executed.
	fp.ExpectLog(t, []string{`Execute  false`})
}

// TestDiscardExecuteAllSources verifies that every source (as produced for a DO
// list that mixes lock functions and ordinary expressions) is executed and its
// result discarded.
func TestDiscardExecuteAllSources(t *testing.T) {
	fp1 := &fakePrimitive{results: []*sqltypes.Result{{}}}
	fp2 := &fakePrimitive{results: []*sqltypes.Result{{}}}
	fp3 := &fakePrimitive{results: []*sqltypes.Result{{}}}

	disc := &Discard{Sources: []Primitive{fp1, fp2, fp3}}

	result, err := disc.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
	assert.Empty(t, result.Fields)

	fp1.ExpectLog(t, []string{`Execute  false`})
	fp2.ExpectLog(t, []string{`Execute  false`})
	fp3.ExpectLog(t, []string{`Execute  false`})
}

func TestDiscardExecuteError(t *testing.T) {
	// The second source fails; the error propagates and later sources
	// (if any) are not reached.
	ok := &fakePrimitive{results: []*sqltypes.Result{{}}}
	boom := &fakePrimitive{sendErr: errors.New("source failed")}
	disc := &Discard{Sources: []Primitive{ok, boom}}

	_, err := disc.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	assert.ErrorContains(t, err, "source failed")
}

func TestDiscardStreamExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields("col1", "int64")
	inputResult := sqltypes.MakeTestResult(fields, "1", "2")
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}
	disc := &Discard{Sources: []Primitive{fp}}

	var results []*sqltypes.Result
	err := disc.TryStreamExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false, func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	})
	require.NoError(t, err)

	// A single empty result is streamed back, with no rows and no fields.
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Rows)
	assert.Empty(t, results[0].Fields)
}

func TestDiscardStreamExecuteError(t *testing.T) {
	fp := &fakePrimitive{
		sendErr: errors.New("source failed"),
	}
	disc := &Discard{Sources: []Primitive{fp}}

	err := disc.TryStreamExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false, func(*sqltypes.Result) error {
		assert.Fail(t, "callback should not be invoked when a source fails")
		return nil
	})
	assert.ErrorContains(t, err, "source failed")
}

func TestDiscardGetFields(t *testing.T) {
	fp := &fakePrimitive{}
	disc := &Discard{Sources: []Primitive{fp}}

	// DO never returns column information to the client.
	result, err := disc.GetFields(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable))
	require.NoError(t, err)
	assert.Empty(t, result.Fields)
	assert.Empty(t, result.Rows)

	// GetFields must not touch the sources.
	fp.ExpectLog(t, nil)
}

func TestDiscardInputs(t *testing.T) {
	fp1 := &fakePrimitive{}
	fp2 := &fakePrimitive{}
	disc := &Discard{Sources: []Primitive{fp1, fp2}}

	inputs, _ := disc.Inputs()
	require.Len(t, inputs, 2)
	assert.Same(t, fp1, inputs[0])
	assert.Same(t, fp2, inputs[1])
}
