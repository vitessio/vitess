/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func unsharded(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectUnsharded,
		solved:      solved,
		keyspace:    keyspace,
	}
}
func selectDBA(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectDBA,
		solved:      solved,
		keyspace:    keyspace,
	}
}

func selectScatter(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectScatter,
		solved:      solved,
		keyspace:    keyspace,
	}
}

func TestMergeJoins(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "apa", Sharded: false}
	ks2 := &vindexes.Keyspace{Name: "banan", Sharded: false}

	type testCase struct {
		l, r, expected joinTree
		predicates     []sqlparser.Expr
	}

	tests := []testCase{{
		l:        unsharded(1, ks),
		r:        unsharded(2, ks),
		expected: unsharded(1|2, ks),
	}, {
		l:        unsharded(1, ks),
		r:        unsharded(2, ks2),
		expected: nil,
	}, {
		l:        unsharded(2, ks),
		r:        unsharded(1, ks2),
		expected: nil,
	}, {
		l:        selectDBA(1, ks),
		r:        selectDBA(2, ks),
		expected: selectDBA(1|2, ks),
	}, {
		l:        selectDBA(1, ks),
		r:        selectDBA(2, ks2),
		expected: nil,
	}, {
		l:        selectDBA(2, ks),
		r:        selectDBA(1, ks2),
		expected: nil,
	}, {
		l:        unsharded(1, ks),
		r:        selectDBA(2, ks),
		expected: nil,
	}, {
		l: selectScatter(1, ks),
		r: selectScatter(2, ks),
		predicates: []sqlparser.Expr{
			equals(colName("t1", "id"), colName("t2", "id")),
		},
		expected: nil,
	}}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			result := tryMerge(tc.l, tc.r, tc.predicates, semantics.NewSemTable())
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClone(t *testing.T) {
	original := &routePlan{
		routeOpCode: engine.SelectEqualUnique,
		vindexPreds: []*vindexPlusPredicates{{
			covered: false,
		}},
	}

	clone := original.clone()

	clonedRP := clone.(*routePlan)
	clonedRP.routeOpCode = engine.SelectDBA
	assert.Equal(t, clonedRP.routeOpCode, engine.SelectDBA)
	assert.Equal(t, original.routeOpCode, engine.SelectEqualUnique)

	clonedRP.vindexPreds[0].covered = true
	assert.True(t, clonedRP.vindexPreds[0].covered)
	assert.False(t, original.vindexPreds[0].covered)
}
