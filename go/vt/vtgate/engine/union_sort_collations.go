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
	"slices"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// resolveUnionCollations asks MySQL about expression collations rather than
// inferring them from result fields, which describe character_set_results.
// The zero-row scalar subqueries also preserve the coercibility of empty arms.
func (ms *MemorySort) resolveUnionCollations(ctx context.Context, vc VCursor, bindVars map[string]*querypb.BindVariable) ([]collations.TypedCollation, error) {
	if len(ms.CollationQueries) == 0 {
		return nil, nil
	}
	resolved := make([]collations.TypedCollation, len(ms.OrderBy))
	env := vc.Environment().CollationEnv()
	for _, query := range ms.CollationQueries {
		// Request fields so an empty destination uses GetFields, which reuses
		// the transaction's existing shard before choosing an arbitrary one.
		result, err := vc.ExecutePrimitive(ctx, query, copyBindVars(bindVars), true)
		if err != nil {
			return nil, vterrors.Wrapf(err, "resolving UNION sort collations")
		}
		if len(result.Rows) == 0 {
			return nil, vterrors.VT13001("UNION collation query returned no metadata")
		}
		for _, row := range result.Rows {
			if len(row) != 2*len(resolved) {
				return nil, vterrors.VT13001("UNION collation query returned an unexpected column count")
			}
			for i := range resolved {
				name := row[2*i].ToString()
				id, found := env.LookupID(name)
				if !found || colldata.Lookup(id) == nil {
					return nil, vterrors.VT12001("UNION ordering with unsupported collation " + name)
				}
				coercibility, err := row[2*i+1].ToUint64()
				if err != nil || coercibility > uint64(collations.CoerceIgnorable) {
					return nil, vterrors.VT13001("UNION collation query returned invalid coercibility")
				}
				current := collations.TypedCollation{Collation: id, Coercibility: collations.Coercibility(coercibility), Repertoire: collations.RepertoireUnicode}
				if !resolved[i].Valid() {
					resolved[i] = current
					continue
				}
				resolved[i], _, _, err = colldata.Merge(env, resolved[i], current, colldata.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
				if err != nil {
					return nil, vterrors.Wrapf(vterrors.NewError(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantAggregateCollations, err.Error()), "resolving UNION sort collation")
				}
			}
		}
	}
	for _, collation := range resolved {
		if collation.Coercibility == collations.CoerceNone {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantAggregateCollations, "Illegal mix of collations for operation 'UNION'")
		}
	}
	return resolved, nil
}

type unionSortKey struct {
	column   int
	from, to colldata.Collation
}

// prepareUnionSort compares original UNION values under the common collation.
// Temporary keys are encoded in that collation's charset; original values and
// their wire encoding remain intact for the caller.
func prepareUnionSort(orderBy evalengine.Comparison, fields []*querypb.Field, resolved []collations.TypedCollation) (evalengine.Comparison, []*querypb.Field, []unionSortKey, error) {
	orderBy = slices.Clone(orderBy)
	fields = slices.Clone(fields)
	width := len(fields)
	var keys []unionSortKey
	for i := range resolved {
		order := &orderBy[i]
		if order.Col < 0 || order.Col >= width {
			return nil, nil, nil, vterrors.VT13001("UNION ordering column is outside the result")
		}
		field := fields[order.Col]
		order.Type = evalengine.NewTypeFromField(field)
		order.WeightStringCol = -1
		if !sqltypes.IsText(field.Type) {
			continue
		}
		from := colldata.Lookup(collations.ID(field.Charset))
		to := colldata.Lookup(resolved[i].Collation)
		if from == nil || to == nil {
			return nil, nil, nil, vterrors.VT12001("UNION ordering requires supported result and expression collations")
		}
		keys = append(keys, unionSortKey{column: order.Col, from: from, to: to})
		order.Col = len(fields)
		order.Type = evalengine.NewType(field.Type, resolved[i].Collation)
		fields = append(fields, &querypb.Field{Name: field.Name, Type: field.Type, Charset: uint32(resolved[i].Collation)})
	}
	return orderBy, fields, keys, nil
}

func appendUnionSortKeys(row sqltypes.Row, keys []unionSortKey) (sqltypes.Row, error) {
	if len(keys) == 0 {
		return row, nil
	}
	out := make(sqltypes.Row, len(row), len(row)+len(keys))
	copy(out, row)
	for _, key := range keys {
		value := row[key.column]
		if value.IsNull() {
			out = append(out, sqltypes.NULL)
			continue
		}
		data := value.Raw()
		if key.from.Charset().Name() != key.to.Charset().Name() && key.to.ID() != collations.CollationBinaryID {
			var err error
			data, err = charset.Convert(nil, key.to.Charset(), data, key.from.Charset())
			if err != nil {
				return nil, vterrors.Wrapf(vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error()), "converting UNION sort key")
			}
		}
		out = append(out, sqltypes.MakeTrusted(value.Type(), data))
	}
	return out, nil
}
