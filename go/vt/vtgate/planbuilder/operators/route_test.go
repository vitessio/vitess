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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestRouteCloneMergeFallback(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "ks", Sharded: true}
	route := &Route{
		unaryOperator: newUnaryOp(&Route{Routing: &NoneRouting{keyspace: ks}}),
		Routing:       &ShardedRouting{keyspace: ks, RouteOpCode: engine.Scatter},
		MergeFallback: &ShardedRouting{keyspace: ks, RouteOpCode: engine.EqualUnique},
	}

	clone := route.Clone(route.Inputs()).(*Route)
	require.NotSame(t, route.MergeFallback, clone.MergeFallback)

	// mutating the clone's fallback must not reach through to the original
	clone.MergeFallback.RouteOpCode = engine.Scatter
	assert.Equal(t, engine.EqualUnique, route.MergeFallback.RouteOpCode)
}
