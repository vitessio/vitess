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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vthash"
)

func TestPlanKeyHashFieldBoundaries(t *testing.T) {
	longField := strings.Repeat("x", 1<<16)
	for _, tc := range []struct {
		name  string
		first PlanKey
		next  PlanKey
	}{
		{
			name:  "keyspace_destination",
			first: PlanKey{CurrentKeyspace: "ks0", Query: "select id from user"},
			next:  PlanKey{CurrentKeyspace: "ks", Destination: "0", Query: "select id from user"},
		},
		{
			name:  "destination_comment",
			first: PlanKey{Destination: "ab", SetVarComment: "c"},
			next:  PlanKey{Destination: "a", SetVarComment: "bc"},
		},
		{
			name:  "comment_query",
			first: PlanKey{SetVarComment: "ab", Query: "c"},
			next:  PlanKey{SetVarComment: "a", Query: "bc"},
		},
		{
			name:  "long_fields",
			first: PlanKey{SetVarComment: longField, Query: "q"},
			next:  PlanKey{Query: longField + "q"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotEqual(t, tc.first.Hash(), tc.next.Hash())
		})
	}
}

func TestPlanKeyHashAllocationFree(t *testing.T) {
	tests := []struct {
		name string
		key  PlanKey
	}{
		{name: "empty"},
		{
			name: "populated",
			key: PlanKey{
				CurrentKeyspace: "commerce",
				TabletType:      topodatapb.TabletType_PRIMARY,
				Destination:     "DestinationShard(-80)",
				Query:           "select id from customer where id = :id",
				SetVarComment:   "/*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES') */",
				Collation:       309,
			},
		},
	}
	const headerSize = 2*2 + 4*8
	for _, size := range []int{63, 64, 65, 95, 96, 97} {
		tests = append(tests, struct {
			name string
			key  PlanKey
		}{
			name: fmt.Sprintf("bytes_%d", size),
			key: PlanKey{
				TabletType: topodatapb.TabletType_REPLICA,
				Query:      strings.Repeat("q", size-headerSize),
				Collation:  45,
			},
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fields := []string{tc.key.CurrentKeyspace, tc.key.Destination, tc.key.SetVarComment, tc.key.Query}
			input := binary.LittleEndian.AppendUint16(nil, uint16(tc.key.Collation))
			input = binary.LittleEndian.AppendUint16(input, uint16(tc.key.TabletType))
			for _, field := range fields {
				input = binary.LittleEndian.AppendUint64(input, uint64(len(field)))
			}
			for _, field := range fields {
				input = append(input, field...)
			}
			hasher := vthash.New256()
			_, _ = hasher.Write(input)
			var want theine.HashKey256
			hasher.Sum(want[:0])

			var got theine.HashKey256
			allocs := testing.AllocsPerRun(1000, func() {
				got = tc.key.Hash()
			})
			assert.Equal(t, want, got)
			assert.Zero(t, allocs)
		})
	}
}

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
