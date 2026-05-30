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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// recordingVCursor captures the value passed to SetExecutedPrimitive so a test
// can assert which PlanSwitcher branch was selected during execution.
type recordingVCursor struct {
	noopVCursor
	executed Primitive
}

func (r *recordingVCursor) SetExecutedPrimitive(p Primitive) { r.executed = p }
func (r *recordingVCursor) ExecutedPrimitive() Primitive     { return r.executed }

// TestPlanSwitcherRecordsExecutedBranch verifies that PlanSwitcher.pickBranch
// records the chosen branch on the vcursor and that GetRoutingIndexes, run on
// that branch, reports vindex usage for only the executed plan — not both.
func TestPlanSwitcherRecordsExecutedBranch(t *testing.T) {
	hash, _ := vindexes.CreateVindex("hash", "hash", nil)
	hash2, _ := vindexes.CreateVindex("hash", "hash2", nil)

	baseline := &Route{
		RoutingParameters: &RoutingParameters{
			Opcode:   Scatter,
			Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
			Vindex:   hash,
		},
	}
	optimized := &Route{
		RoutingParameters: &RoutingParameters{
			Opcode:   EqualUnique,
			Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
			Vindex:   hash2,
		},
	}
	ps := &PlanSwitcher{
		Conditions: []Condition{{A: "a", B: "b"}},
		Baseline:   baseline,
		Optimized:  optimized,
	}

	// Conditions met: pickBranch records the optimized branch.
	vc := &recordingVCursor{}
	met := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
		"b": sqltypes.Int64BindVariable(1),
	}
	branch, isOptimized := ps.pickBranch(vc, met)
	assert.Same(t, optimized, branch)
	assert.True(t, isOptimized)
	assert.Same(t, optimized, vc.ExecutedPrimitive())
	assert.Equal(t, [][3]string{{"ks", "hash2", "EqualUnique"}}, GetRoutingIndexes(vc.ExecutedPrimitive()))

	// Conditions not met: pickBranch records the baseline branch.
	vc = &recordingVCursor{}
	notMet := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
		"b": sqltypes.Int64BindVariable(2),
	}
	branch, isOptimized = ps.pickBranch(vc, notMet)
	assert.Same(t, baseline, branch)
	assert.False(t, isOptimized)
	assert.Same(t, baseline, vc.ExecutedPrimitive())
	assert.Equal(t, [][3]string{{"ks", "hash", "Scatter"}}, GetRoutingIndexes(vc.ExecutedPrimitive()))
}
