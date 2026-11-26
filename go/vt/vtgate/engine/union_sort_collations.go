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

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type unionSortMetadata struct {
	common []collations.TypedCollation
	arms   [][]collations.TypedCollation
	types  []sqltypes.Type
	scales []int32
	same   bool
}

type unionSortLookupCacheContextKey struct{}

type unionSortLookupResult struct {
	ids  []sqltypes.Value
	dest []key.ShardDestination
}

type unionSortLookupCache struct {
	mu      sync.Mutex
	results map[*VindexLookup]unionSortLookupResult
}

func withUnionSortLookupCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, unionSortLookupCacheContextKey{}, &unionSortLookupCache{results: make(map[*VindexLookup]unionSortLookupResult)})
}

func unionSortLookupCacheFromContext(ctx context.Context) *unionSortLookupCache {
	cache, _ := ctx.Value(unionSortLookupCacheContextKey{}).(*unionSortLookupCache)
	return cache
}

func (cache *unionSortLookupCache) get(key *VindexLookup) (unionSortLookupResult, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	result, ok := cache.results[key]
	return result, ok
}

func (cache *unionSortLookupCache) set(key *VindexLookup, result unionSortLookupResult) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.results[key] = result
}

// resolveUnionCollations asks MySQL about expression collations rather than
// inferring them from result fields, which describe character_set_results.
// The zero-row scalar subqueries also preserve the coercibility of empty arms.
func (ms *MemorySort) resolveUnionCollations(ctx context.Context, vc VCursor, bindVars map[string]*querypb.BindVariable) (*unionSortMetadata, error) {
	if len(ms.CollationQueries) == 0 {
		if !ms.UnionCollationKnown {
			return nil, nil
		}
		input := ms.Input.(*Concatenate)
		metadata := &unionSortMetadata{common: make([]collations.TypedCollation, len(ms.OrderBy)), arms: make([][]collations.TypedCollation, len(input.Sources)), types: make([]sqltypes.Type, len(ms.OrderBy)), scales: make([]int32, len(ms.OrderBy)), same: true}
		for i, key := range ms.OrderBy {
			metadata.common[i] = collations.TypedCollation{Collation: key.Type.Collation(), Coercibility: collations.CoerceImplicit, Repertoire: collations.RepertoireUnicode}
			metadata.types[i] = key.Type.Type()
			metadata.scales[i] = key.Type.Scale()
		}
		return metadata, nil
	}
	resolved := make([]collations.TypedCollation, len(ms.OrderBy))
	arms := make([][]collations.TypedCollation, len(ms.CollationQueries))
	metadata := &unionSortMetadata{common: resolved, arms: arms, types: make([]sqltypes.Type, len(resolved)), scales: make([]int32, len(resolved)), same: true}
	nativeTypes := make([][]sqltypes.Type, len(ms.CollationQueries))
	typers := make([]evalengine.TypeAggregator, len(resolved))
	results := make([]*sqltypes.Result, len(ms.CollationQueries))
	execute := func(i int) error {
		var err error
		query := ms.CollationQueries[i]
		if route, ok := query.(*Route); ok {
			query = unionSortProbeRoute(route, vc.Session().InTransaction() || vc.Session().InReservedConn())
		}
		results[i], err = vc.ExecutePrimitive(ctx, query, copyBindVars(bindVars), true)
		return err
	}
	if vc.Session().InTransaction() || vc.Session().InReservedConn() {
		for i := range results {
			if err := execute(i); err != nil {
				return nil, vterrors.Wrapf(err, "resolving UNION sort collations")
			}
		}
	} else {
		var group errgroup.Group
		for i := range results {
			group.Go(func() error { return execute(i) })
		}
		if err := group.Wait(); err != nil {
			return nil, vterrors.Wrapf(err, "resolving UNION sort collations")
		}
	}
	env := vc.Environment().CollationEnv()
	for arm, result := range results {
		nativeTypes[arm] = make([]sqltypes.Type, len(resolved))
		if len(result.Rows) == 0 {
			return nil, vterrors.VT13001("UNION collation query returned no metadata")
		}
		for _, row := range result.Rows {
			if len(row) < 3*len(resolved) || len(row)%3 != 0 {
				return nil, vterrors.VT13001("UNION collation query returned an unexpected column count")
			}
			localCount := len(row)/3 - len(resolved)
			if arms[arm] == nil {
				arms[arm] = make([]collations.TypedCollation, localCount)
			}
			if len(arms[arm]) != localCount {
				return nil, vterrors.VT13001("UNION arm metadata has inconsistent ordering columns")
			}
			for i := 0; i < len(row)/3; i++ {
				name := row[3*i].ToString()
				id, _ := env.LookupID(name)
				if id == collations.Unknown {
					return nil, vterrors.VT12001("UNION ordering with unsupported collation " + name)
				}
				coercibility, err := row[3*i+1].ToUint64()
				if err != nil || coercibility > uint64(collations.CoerceIgnorable) {
					return nil, vterrors.VT13001("UNION collation query returned invalid coercibility")
				}
				current := collations.TypedCollation{Collation: id, Coercibility: collations.Coercibility(coercibility), Repertoire: collations.RepertoireUnicode}
				if cs, _ := env.ConnectionCharset(id); cs == "ascii" {
					current.Repertoire = collations.RepertoireASCII
				}
				var target *collations.TypedCollation
				if i < len(resolved) {
					field := result.Fields[3*i+2]
					nativeTypes[arm][i] = field.Type
					typ := evalengine.NewTypeEx(field.Type, collations.CollationBinaryID, true, int32(field.ColumnLength), int32(field.Decimals), nil)
					if err := typers[i].Add(typ, env); err != nil {
						return nil, err
					}
					if resolved[i].Valid() && resolved[i].Collation != current.Collation {
						metadata.same = false
					}
					target = &resolved[i]
				} else {
					target = &arms[arm][i-len(resolved)]
				}
				if !target.Valid() {
					*target = current
					continue
				}
				*target, err = mergeUnionSortCollations(env, *target, current)
				if err != nil {
					return nil, vterrors.Wrapf(vterrors.NewError(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantAggregate2Collations, err.Error()), "resolving UNION sort collation")
				}
			}
		}
	}
	for _, collation := range resolved {
		if collation.Coercibility == collations.CoerceNone {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantAggregateCollations, "Illegal mix of collations for operation 'UNION'")
		}
	}
	for i := range resolved {
		typ := typers[i].Type()
		metadata.types[i] = typ.Type()
		metadata.scales[i] = typ.Scale()
		for _, native := range nativeTypes {
			if native[i] != metadata.types[i] {
				metadata.same = false
			}
		}
	}
	return metadata, nil
}

func unionSortProbeRoute(route *Route, keepArmRouting bool) *Route {
	clone := *route
	if route.RoutingParameters == nil || keepArmRouting {
		return &clone
	}
	routing := *route.RoutingParameters
	// Outside transaction state, a metadata-only probe may use any shard.
	routing.Opcode = None
	routing.TargetDestination = nil
	clone.RoutingParameters = &routing
	return &clone
}

// Metadata aggregation does not compare or decode values. An unsupported arm
// collation can therefore participate in a common comparator whose keys are
// generated by MySQL. Use the normal merge for supported pairs.
func mergeUnionSortCollations(env *collations.Environment, left, right collations.TypedCollation) (collations.TypedCollation, error) {
	if colldata.Lookup(left.Collation) != nil && colldata.Lookup(right.Collation) != nil {
		merged, _, _, err := colldata.Merge(env, left, right, colldata.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
		return merged, err
	}
	leftCS, _ := env.ConnectionCharset(left.Collation)
	rightCS, _ := env.ConnectionCharset(right.Collation)
	if left.Collation == right.Collation {
		left.Coercibility = min(left.Coercibility, right.Coercibility)
		left.Repertoire |= right.Repertoire
		return left, nil
	}
	if left.Coercibility == collations.CoerceExplicit && right.Coercibility == collations.CoerceExplicit {
		return collations.TypedCollation{}, fmt.Errorf("Illegal mix of explicit collations (%d,%d)", left.Collation, right.Collation)
	}
	if leftCS == rightCS {
		switch {
		case left.Coercibility < right.Coercibility:
			left.Repertoire |= right.Repertoire
			return left, nil
		case right.Coercibility < left.Coercibility:
			right.Repertoire |= left.Repertoire
			return right, nil
		}
		binary, _ := env.LookupID(leftCS + "_bin")
		switch binary {
		case left.Collation:
			return left, nil
		case right.Collation:
			return right, nil
		}
		return collations.TypedCollation{Collation: binary, Coercibility: collations.CoerceNone, Repertoire: left.Repertoire | right.Repertoire}, nil
	}
	if leftCS == "binary" && left.Coercibility <= right.Coercibility || rightCS == "binary" && right.Coercibility > left.Coercibility {
		return left, nil
	}
	if rightCS == "binary" && right.Coercibility <= left.Coercibility || leftCS == "binary" && left.Coercibility > right.Coercibility {
		return right, nil
	}
	// These are the charset/coercibility rules used by colldata.Merge. No
	// conversion function is needed: result values arrive in the wire charset.
	canConvert := func(to, from collations.TypedCollation, toCS, fromCS string) bool {
		unicode := func(cs string) bool {
			switch cs {
			case "utf8", "utf8mb3", "utf8mb4", "ucs2", "utf16", "utf16le", "utf32":
				return true
			}
			return false
		}
		if to.Coercibility <= from.Coercibility {
			if toCS == "utf8mb4" || unicode(toCS) && (to.Coercibility < from.Coercibility || !unicode(fromCS)) {
				return true
			}
			if from.Repertoire == collations.RepertoireASCII && (to.Coercibility < from.Coercibility || to.Repertoire == collations.RepertoireUnicode) {
				return true
			}
		}
		return to.Coercibility < from.Coercibility && from.Coercibility > collations.CoerceImplicit
	}
	if canConvert(left, right, leftCS, rightCS) {
		return left, nil
	}
	if canConvert(right, left, rightCS, leftCS) {
		return right, nil
	}
	return collations.TypedCollation{}, fmt.Errorf("Illegal mix of collations (%d,%s) and (%d,%s)", left.Collation, left.Coercibility, right.Collation, right.Coercibility)
}
