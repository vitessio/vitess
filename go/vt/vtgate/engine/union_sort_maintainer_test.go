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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func TestUnionSortRejectsNonOrderingServerKeys(t *testing.T) {
	for _, name := range []string{"tis620_thai_ci", "cp1250_czech_cs", "gb18030_chinese_ci"} {
		t.Run(name, func(t *testing.T) {
			env := collations.MySQL8()
			id, _ := env.LookupID(name)
			_, err := unionSortPadWeight(env, id)
			require.ErrorContains(t, err, "non-order-preserving MySQL weight strings")
		})
	}
}
