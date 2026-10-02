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

package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestRouteContainsSpecialUnionInput(t *testing.T) {
	keyspace := &vindexes.Keyspace{Name: "main", Sharded: true}
	tests := []struct {
		name  string
		route *Route
		want  bool
	}{
		{name: "reference route", route: &Route{Routing: &AnyShardRouting{keyspace: keyspace}}, want: true},
		{name: "dual route", route: &Route{Routing: &DualRouting{}}, want: true},
		{name: "merged provenance", route: &Route{Routing: &ShardedRouting{keyspace: keyspace}, ContainsSpecialUnionInput: true}, want: true},
		{name: "sharded route", route: &Route{Routing: &ShardedRouting{keyspace: keyspace}}, want: false},
		{name: "nil route", route: nil, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, routeContainsSpecialUnionInput(test.route))
		})
	}

	merged := &Route{Routing: &ShardedRouting{keyspace: keyspace, RouteOpCode: engine.EqualUnique}}
	markSpecialUnionInput(merged, routeContainsSpecialUnionInput(&Route{Routing: &DualRouting{}}))
	assert.True(t, merged.ContainsSpecialUnionInput)
}

func TestIsSingleShardRouting(t *testing.T) {
	for _, test := range []struct {
		opcode engine.Opcode
		want   bool
	}{
		{opcode: engine.EqualUnique, want: true},
		{opcode: engine.Reference, want: true},
		{opcode: engine.Equal},
		{opcode: engine.IN},
		{opcode: engine.Scatter},
	} {
		t.Run(test.opcode.String(), func(t *testing.T) {
			assert.Equal(t, test.want, isSingleShardRouting(&ShardedRouting{RouteOpCode: test.opcode}))
		})
	}
}

func TestContainsSpecialInputAfterJoin(t *testing.T) {
	keyspace := &vindexes.Keyspace{Name: "main", Sharded: true}
	reference := &Route{Routing: &AnyShardRouting{keyspace: keyspace}}
	sharded := &Route{Routing: &ShardedRouting{keyspace: keyspace, RouteOpCode: engine.Scatter}}
	unionRoute := &Route{
		Routing:                   &ShardedRouting{keyspace: keyspace, RouteOpCode: engine.EqualUnique},
		ContainsSpecialUnionInput: true,
	}
	tests := []struct {
		name     string
		joinType sqlparser.JoinType
		lhs, rhs *Route
		want     bool
	}{
		{name: "inner join consumes reference input", joinType: sqlparser.NormalJoinType, lhs: reference, rhs: sharded, want: false},
		{name: "left join keeps preserved reference input", joinType: sqlparser.LeftJoinType, lhs: reference, rhs: sharded, want: true},
		{name: "left join consumes right-side reference input", joinType: sqlparser.LeftJoinType, lhs: sharded, rhs: reference, want: false},
		{name: "inner join consumes union reference input", joinType: sqlparser.NormalJoinType, lhs: unionRoute, rhs: sharded, want: false},
		{name: "left join preserves union reference input", joinType: sqlparser.LeftJoinType, lhs: unionRoute, rhs: sharded, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			routing := &ShardedRouting{keyspace: keyspace, RouteOpCode: engine.Scatter}
			assert.Equal(t, test.want, containsSpecialInputAfterJoin(test.joinType, test.lhs, test.rhs, routing))
		})
	}
}
