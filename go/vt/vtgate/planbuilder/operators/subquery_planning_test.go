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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestRouteUsesSubqueryJoinArgument(t *testing.T) {
	subquery := &SubQuery{JoinColumns: []applyJoinColumn{{LHSExprs: []BindVarExpr{{Name: "r_col"}}}}}
	route := &Route{Routing: &ShardedRouting{Selected: &VindexOption{ValueExprs: []sqlparser.Expr{sqlparser.NewArgument("r_col")}}}}

	require.True(t, routeUsesSubqueryJoinArgument(route, subquery))

	route.Routing.(*ShardedRouting).Selected.ValueExprs = []sqlparser.Expr{sqlparser.NewIntLiteral("5")}
	require.False(t, routeUsesSubqueryJoinArgument(route, subquery))
}
