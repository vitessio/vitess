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
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestUnionSortSharedBackendCollation(t *testing.T) {
	sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}}
	for range 2 {
		sort.CollationQueries = append(sort.CollationQueries, &fakePrimitive{results: []*sqltypes.Result{unionSortMetadataResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), "big5_chinese_ci|2")}})
	}
	_, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
	require.NoError(t, err, "a shared backend collation must not require local value collation support")
}

func TestUnionSortTwoCollationError(t *testing.T) {
	sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}}
	for _, c := range []string{"utf8mb4_unicode_ci|0", "utf8mb4_bin|0"} {
		sort.CollationQueries = append(sort.CollationQueries, &fakePrimitive{results: []*sqltypes.Result{unionSortMetadataResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), c)}})
	}
	_, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, nil)
	require.Error(t, err)
	se := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	require.Equal(t, sqlerror.ERCantAggregate2Collations, se.Number())
}

func TestUnionSortFallbackCoercionRules(t *testing.T) {
	env := collations.MySQL8()
	for _, tc := range []struct {
		name, left, right, want string
		lc, rc, wc              collations.Coercibility
		ascii                   bool
		fail                    bool
	}{
		{name: "same charset left stronger", left: "latin1_german2_ci", right: "latin1_general_ci", want: "latin1_german2_ci", lc: collations.CoerceImplicit, rc: collations.CoerceCoercible, wc: collations.CoerceImplicit},
		{name: "same charset right stronger", left: "latin1_german2_ci", right: "latin1_general_ci", want: "latin1_general_ci", lc: collations.CoerceCoercible, rc: collations.CoerceImplicit, wc: collations.CoerceImplicit},
		{name: "same charset binary winner", left: "latin1_german2_ci", right: "latin1_bin", want: "latin1_bin", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, wc: collations.CoerceImplicit},
		{name: "same charset NONE", left: "latin1_german2_ci", right: "latin1_general_ci", want: "latin1_bin", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, wc: collations.CoerceNone},
		{name: "utf8mb4 superset", left: "utf8mb4_general_ci", right: "big5_chinese_ci", want: "utf8mb4_general_ci", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, wc: collations.CoerceImplicit},
		{name: "other unicode superset", left: "ucs2_general_ci", right: "big5_chinese_ci", want: "ucs2_general_ci", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, wc: collations.CoerceImplicit},
		{name: "ASCII repertoire", left: "big5_chinese_ci", right: "ascii_general_ci", want: "big5_chinese_ci", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, wc: collations.CoerceImplicit, ascii: true},
		{name: "coercible nonunicode conversion", left: "latin1_swedish_ci", right: "big5_chinese_ci", want: "latin1_swedish_ci", lc: collations.CoerceImplicit, rc: collations.CoerceCoercible, wc: collations.CoerceImplicit},
		{name: "incompatible implicit nonunicode", left: "latin1_swedish_ci", right: "big5_chinese_ci", lc: collations.CoerceImplicit, rc: collations.CoerceImplicit, fail: true},
		{name: "incompatible explicit", left: "latin1_german2_ci", right: "latin1_bin", lc: collations.CoerceExplicit, rc: collations.CoerceExplicit, fail: true},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tc.name, reverse), func(t *testing.T) {
				left, _ := env.LookupID(tc.left)
				right, _ := env.LookupID(tc.right)
				a := collations.TypedCollation{Collation: left, Coercibility: tc.lc, Repertoire: collations.RepertoireUnicode}
				b := collations.TypedCollation{Collation: right, Coercibility: tc.rc, Repertoire: collations.RepertoireUnicode}
				if tc.ascii {
					b.Repertoire = collations.RepertoireASCII
				}
				if reverse {
					a, b = b, a
				}
				got, err := mergeUnionSortCollations(env, a, b)
				if tc.fail {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				want, _ := env.LookupID(tc.want)
				require.Equal(t, want, got.Collation)
				require.Equal(t, tc.wc, got.Coercibility)
			})
		}
	}
}

type unionSortProbeBarrier struct {
	*fakePrimitive
	started chan<- struct{}
	release <-chan struct{}
}

func (p *unionSortProbeBarrier) TryExecute(ctx context.Context, vc VCursor, vars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	select {
	case p.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.fakePrimitive.TryExecute(ctx, vc, vars, wantfields)
}

func TestUnionSortIndependentProbesRunConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var closeOnce sync.Once
	t.Cleanup(func() { closeOnce.Do(func() { close(release) }) })
	sort := &MemorySort{OrderBy: evalengine.Comparison{{Col: 0}}}
	for range 2 {
		sort.CollationQueries = append(sort.CollationQueries, &unionSortProbeBarrier{fakePrimitive: &fakePrimitive{writeBindVar: "probe", results: []*sqltypes.Result{unionSortMetadataResult(sqltypes.MakeTestFields("collation|coercibility", "varchar|int64"), "utf8mb4_general_ci|2")}}, started: started, release: release})
	}
	vars := map[string]*querypb.BindVariable{"kept": sqltypes.Int64BindVariable(1)}
	finished := make(chan error, 1)
	go func() { _, err := sort.resolveUnionCollations(t.Context(), &noopVCursor{}, vars); finished <- err }()
	assert.Eventually(t, func() bool { return len(started) == 2 }, 30*time.Second, time.Millisecond, "one probe must not wait for another independent arm")
	closeOnce.Do(func() { close(release) })
	require.NoError(t, <-finished)
	require.Len(t, vars, 1, "each concurrent probe owns its bind map")
}

func TestMemorySortSupportsServerWeightKeyOrderingCases(t *testing.T) {
	for _, tc := range []struct{ collation, left, right string }{
		{"tis620_thai_ci", "a", "a\x00"},
		{"cp1250_czech_cs", "", "  "},
		{"gb18030_chinese_ci", "ss", "ı"},
	} {
		name := tc.collation
		t.Run(name, func(t *testing.T) {
			env := collations.MySQL8()
			id, _ := env.LookupID(name)
			input := NewConcatenate([]Primitive{
				unionSortTestRoute(t, "select value, weight_string(value) from t"),
				unionSortTestRoute(t, "select value, weight_string(value) from u"),
			}, nil)
			metadata := &unionSortMetadata{
				common: []collations.TypedCollation{{Collation: id}},
				arms:   make([][]collations.TypedCollation, len(input.Sources)),
				types:  []sqltypes.Type{sqltypes.VarChar},
				scales: []int32{0},
				same:   true,
			}
			sort := &MemorySort{Input: input, OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: 1, Type: evalengine.NewType(sqltypes.VarChar, id)}}}
			_, _, err := sort.unionSortInput(metadata, 1, env)
			require.NoError(t, err)
			t.Logf("MySQL UNION ordering case: %q / %q", tc.left, tc.right)
		})
	}
}

func TestUnionSortJSONDoesNotUseCappedPlainPath(t *testing.T) {
	env := collations.MySQL8()
	input := NewConcatenate([]Primitive{unionSortTestRoute(t, "select value from t")}, nil)
	sort := &MemorySort{Input: input, OrderBy: evalengine.Comparison{{Col: 0, Type: evalengine.NewType(sqltypes.TypeJSON, collations.CollationBinaryID), CollationEnv: env}}}
	got, layout, err := sort.unionSortInput(nil, 1, env)
	require.NoError(t, err)
	require.Nil(t, layout)
	require.Same(t, input, got, "JSON uses MySQL ordering that differs from evalengine")
}

func TestUnionSortProbeFreeTextDoesNotCapArms(t *testing.T) {
	env := collations.MySQL8()
	aggregate := unionSortTestRoute(t, "select max(name) from t")
	plain := unionSortTestRoute(t, "select name, weight_string(name) from t")
	input := NewConcatenate([]Primitive{aggregate, plain}, nil)
	sort := &MemorySort{Input: input, OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: 1, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown), CollationEnv: env}}}
	got, _, err := sort.unionSortInput(nil, 3, env)
	require.NoError(t, err)
	require.Same(t, input, got, "without a common server collation, even existing arm weight strings are not safe to cap")
}

func TestUnionSortProbeRoutePreservesSessionRouting(t *testing.T) {
	route := &Route{RoutingParameters: &RoutingParameters{Opcode: EqualUnique}}
	transactional := unionSortProbeRoute(route, true)
	require.Equal(t, EqualUnique, transactional.Opcode)
	require.Same(t, route.RoutingParameters, transactional.RoutingParameters)
	autocommit := unionSortProbeRoute(route, false)
	require.Equal(t, None, autocommit.Opcode)
	require.Nil(t, autocommit.TargetDestination)
	require.Equal(t, EqualUnique, route.Opcode, "the cached arm route must stay unchanged")
}

func TestUnionSortGuardedPaths(t *testing.T) {
	env := collations.MySQL8()
	order := evalengine.Comparison{{Col: 0, Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)}}
	for _, tc := range []struct {
		name  string
		query string
		count int
		cap   bool
	}{
		{name: "plain", query: "select id from t", count: 3, cap: true},
		{name: "limit", query: "select id from t limit 5", count: 3},
		{name: "distinct", query: "select distinct id from t", count: 3},
		{name: "group", query: "select id from t group by id", count: 3},
		{name: "aggregate", query: "select count(*) from t", count: 3},
		{name: "unbounded", query: "select id from t", count: math.MaxInt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := unionSortTestRoute(t, tc.query)
			got := unionSortPlainSource(source, order, tc.count, nil, env).(*Route)
			stmt := got.QueryStatement.(*sqlparser.Select)
			if tc.cap {
				require.NotNil(t, stmt.Limit)
				require.Equal(t, "3", sqlparser.String(stmt.Limit.Rowcount))
			} else if tc.count == math.MaxInt || tc.name == "limit" || tc.name == "distinct" || tc.name == "group" || tc.name == "aggregate" {
				require.Same(t, source, got)
			} else {
				require.Nil(t, stmt.Limit)
			}
		})
	}

	plain := NewConcatenate([]Primitive{unionSortTestRoute(t, "select id from t")}, nil)
	function := NewConcatenate([]Primitive{unionSortTestRoute(t, "select lower(name) from t")}, nil)
	require.True(t, unionSortRepeatableSources(plain))
	require.False(t, unionSortRepeatableSources(function))
	orderedRoute := unionSortTestRoute(t, "select name, weight_string(name) from t")
	orderedRoute.OrderBy = evalengine.Comparison{{Col: 0, WeightStringCol: 1}}
	require.Same(t, orderedRoute, unionSortPlainSource(orderedRoute, evalengine.Comparison{{Col: 0, WeightStringCol: 1, Type: evalengine.NewType(sqltypes.Unknown, collations.Unknown)}}, 3, nil, env), "do not overwrite an existing arm comparator")
}

func TestUnionSortNativePathForRepeatableExpressions(t *testing.T) {
	env := collations.MySQL8()
	id := env.LookupByName("utf8mb4_general_ci")
	input := NewConcatenate([]Primitive{unionSortTestRoute(t, "select name from t"), unionSortTestRoute(t, "select name from u")}, nil)
	metadata := &unionSortMetadata{common: []collations.TypedCollation{{Collation: id}}, arms: make([][]collations.TypedCollation, 2), types: []sqltypes.Type{sqltypes.VarChar}, scales: []int32{0}, same: true}
	sort := &MemorySort{Input: input, OrderBy: evalengine.Comparison{{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarChar, id), CollationEnv: env}}}
	got, _, err := sort.unionSortInput(metadata, 3, env)
	require.NoError(t, err)
	for _, source := range got.(*Concatenate).Sources {
		projection, ok := source.(*SimpleProjection)
		require.True(t, ok, "repeatable projections use the native key path")
		route := projection.Input.(*Route)
		require.NotContains(t, route.Query, "__union_values")
	}
}

func TestUnionSortRouteCapRespectsArmLimit(t *testing.T) {
	env := collations.MySQL8()
	order := evalengine.Comparison{{Col: 0, Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)}}
	for _, tc := range []struct {
		query      string
		outerLimit bool
		innerLimit bool
	}{
		{query: "select id from t", outerLimit: true},
		{query: "select id from t limit 5", innerLimit: true},
	} {
		route, _, _, err := unionSortRoute(unionSortTestRoute(t, tc.query), order, []collations.TypedCollation{{Collation: collations.CollationBinaryID}}, nil, 3, env)
		require.NoError(t, err)
		outer := route.QueryStatement.(*sqlparser.Select)
		require.Equal(t, tc.outerLimit, outer.Limit != nil)
		derived := outer.From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.DerivedTable).Select.(*sqlparser.Select)
		require.Equal(t, tc.innerLimit, derived.Limit != nil)
	}
}

func TestUnionKnownFloatFallsBackForNonSimpleArms(t *testing.T) {
	env := collations.MySQL8()
	floatType := evalengine.NewType(sqltypes.Float64, collations.CollationBinaryID)
	for _, tc := range []struct {
		name      string
		query     string
		filter    bool
		localSort bool
	}{
		{name: "local order and limit", query: "select value from t order by value limit 2", localSort: true},
		{name: "aggregate", query: "select sum(value) from t"},
		{name: "locally merged union", query: "select value from t union all select value from u"},
		{name: "filter wrapper", query: "select value from t", filter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := unionSortTestRoute(t, tc.query)
			if tc.localSort {
				route.OrderBy = evalengine.Comparison{{Col: 0, Type: floatType, CollationEnv: env}}
			}
			var source Primitive = route
			if tc.filter {
				source = &Filter{Input: route}
			}
			input := NewConcatenate([]Primitive{source}, nil)
			sort := &MemorySort{Input: input, OrderBy: evalengine.Comparison{{Col: 0, Type: floatType, CollationEnv: env}}}
			got, layout, err := sort.unionSortInput(nil, 1, env)
			require.NoError(t, err)
			require.Nil(t, layout)
			require.Same(t, input, got, "unsupported arm shapes keep the original execution path")
		})
	}
}
