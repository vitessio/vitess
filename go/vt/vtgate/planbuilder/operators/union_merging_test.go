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

	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestHasShardedReferenceAlternate verifies that UNION ALL keeps a reference
// alternate separate from sharded routes, including single-shard routes.
func TestHasShardedReferenceAlternate(t *testing.T) {
	referenceKeyspace := &vindexes.Keyspace{Name: "reference"}
	shardedKeyspace := &vindexes.Keyspace{Name: "sharded", Sharded: true}

	makeRoute := func(routing Routing) *Route {
		return &Route{Routing: routing}
	}
	makeReferenceRoute := func(alternate *Route) *Route {
		return makeRoute(&AnyShardRouting{
			keyspace: referenceKeyspace,
			Alternates: map[*vindexes.Keyspace]*Route{
				shardedKeyspace: alternate,
			},
		})
	}
	scatter := makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.Scatter})
	singleShard := makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.EqualUnique})
	none := makeRoute(&NoneRouting{keyspace: shardedKeyspace})

	tests := []struct {
		name string
		lhs  *Route
		rhs  *Route
		want bool
	}{
		{
			name: "blocks alternate scatter on the left",
			lhs:  makeReferenceRoute(makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.Scatter})),
			rhs:  scatter,
			want: true,
		},
		{
			name: "blocks alternate scatter on the right",
			lhs:  scatter,
			rhs:  makeReferenceRoute(makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.Scatter})),
			want: true,
		},
		{
			name: "blocks single-shard alternate even when other route is single-shard",
			lhs:  makeReferenceRoute(makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.Scatter})),
			rhs:  singleShard,
			want: true,
		},
		{
			name: "blocks scatter even when alternate is single-shard",
			lhs:  makeReferenceRoute(makeRoute(&AnyShardRouting{keyspace: shardedKeyspace})),
			rhs:  scatter,
			want: true,
		},
		{
			name: "does not block an empty alternate",
			lhs:  makeReferenceRoute(makeRoute(&NoneRouting{keyspace: shardedKeyspace})),
			rhs:  scatter,
			want: false,
		},
		{
			name: "does not block an empty other route",
			lhs:  makeReferenceRoute(makeRoute(&AnyShardRouting{keyspace: shardedKeyspace})),
			rhs:  none,
			want: false,
		},
		{
			name: "blocks single-shard alternate and route",
			lhs:  makeReferenceRoute(makeRoute(&ShardedRouting{keyspace: shardedKeyspace, RouteOpCode: engine.EqualUnique})),
			rhs:  singleShard,
			want: true,
		},
		{
			name: "does not block without alternate",
			lhs:  makeRoute(&AnyShardRouting{keyspace: referenceKeyspace}),
			rhs:  scatter,
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, hasShardedReferenceAlternate(test.lhs, test.rhs))
		})
	}
}
