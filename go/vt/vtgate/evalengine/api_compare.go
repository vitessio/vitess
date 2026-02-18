/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// UnsupportedCollationError represents the error where the comparison using provided collation is unsupported on vitess
type UnsupportedCollationError struct {
	ID collations.ID
}

// Error function implements the error interface
func (err UnsupportedCollationError) Error() string {
	return fmt.Sprintf("cannot compare strings, collation is unknown or unsupported (collation ID: %d)", err.ID)
}

// UnsupportedCollationHashError is returned when we try to get the hash value and are missing the collation to use
var UnsupportedCollationHashError = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "text type with an unknown/unsupported collation cannot be hashed")

func compare(v1, v2 sqltypes.Value, collationEnv *collations.Environment, collationID collations.ID, values *EnumSetValues) (int, error) {
	v1t := v1.Type()

	// We have a fast path here for the case where both values are
	// the same type, and it's one of the basic types we can compare
	// directly. This is a common case for equality checks.
	if v1t == v2.Type() {
		switch {
		case sqltypes.IsText(v1t):
			if collationID == collations.CollationBinaryID {
				return bytes.Compare(v1.Raw(), v2.Raw()), nil
			}
			coll := colldata.Lookup(collationID)
			if coll == nil {
				return 0, UnsupportedCollationError{ID: collationID}
			}
			result := coll.Collate(v1.Raw(), v2.Raw(), false)
			switch {
			case result < 0:
				return -1, nil
			case result > 0:
				return 1, nil
			default:
				return 0, nil
			}
		case sqltypes.IsBinary(v1t), v1t == sqltypes.Date, v1t == sqltypes.Datetime, v1t == sqltypes.Timestamp:
			// We can't optimize for Time here, since Time is not sortable
			// based on the raw bytes. This is because of cases like
			// '24:00:00' and '101:00:00' which are both valid times and
			// order wrong based on the raw bytes.
			return bytes.Compare(v1.Raw(), v2.Raw()), nil
		case sqltypes.IsSigned(v1t):
			i1, err := v1.ToInt64()
			if err != nil {
				return 0, err
			}
			i2, err := v2.ToInt64()
			if err != nil {
				return 0, err
			}
			switch {
			case i1 < i2:
				return -1, nil
			case i1 > i2:
				return 1, nil
			default:
				return 0, nil
			}
		case sqltypes.IsUnsigned(v1t):
			u1, err := v1.ToUint64()
			if err != nil {
				return 0, err
			}
			u2, err := v2.ToUint64()
			if err != nil {
				return 0, err
			}
			switch {
			case u1 < u2:
				return -1, nil
			case u1 > u2:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}

	v1eval, err := valueToEval(v1, collations.TypedCollation{
		Collation:    collationID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireUnicode,
	}, values)
	if err != nil {
		return 0, err
	}

	v2eval, err := valueToEval(v2, collations.TypedCollation{
		Collation:    collationID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireUnicode,
	}, values)
	if err != nil {
		return 0, err
	}

	out, err := evalCompare(v1eval, v2eval, collationEnv)
	if err != nil {
		return 0, err
	}
	if out == 0 {
		return 0, nil
	}
	if out > 0 {
		return 1, nil
	}
	return -1, nil
}

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is
// numeric, then a numeric comparison is performed after
// necessary conversions. If none are numeric, then it's
// a simple binary comparison. Uncomparable values return an error.
func NullsafeCompare(v1, v2 sqltypes.Value, collationEnv *collations.Environment, collationID collations.ID, values *EnumSetValues) (int, error) {
	// Based on the categorization defined for the types,
	// we're going to allow comparison of the following:
	// Null, isNumber, IsBinary. This will exclude IsQuoted
	// types that are not Binary, and Expression.
	if v1.IsNull() {
		if v2.IsNull() {
			return 0, nil
		}
		return -1, nil
	}
	if v2.IsNull() {
		return 1, nil
	}
	return compare(v1, v2, collationEnv, collationID, values)
}

// OrderByParams specifies the parameters for ordering.
// This is used for merge-sorting scatter queries.
type (
	OrderByParams struct {
		Col int
		// WeightStringCol is the weight_string column that will be used for sorting.
		// It is set to -1 if such a column is not added to the query
		WeightStringCol int
		Desc            bool

		// Type for knowing if the collation is relevant
		Type Type

		CollationEnv *collations.Environment
	}

	Comparison []OrderByParams

	tinyWeighter struct {
		col   int
		apply func(v *sqltypes.Value)
	}
)

// String returns a string. Used for plan descriptions
func (obp *OrderByParams) String() string {
	val := strconv.Itoa(obp.Col)
	if obp.WeightStringCol != -1 && obp.WeightStringCol != obp.Col {
		val = fmt.Sprintf("(%s|%d)", val, obp.WeightStringCol)
	}
	if obp.Desc {
		val += " DESC"
	} else {
		val += " ASC"
	}

	if sqltypes.IsText(obp.Type.Type()) && obp.Type.Collation() != collations.Unknown {
		val += " COLLATE " + obp.CollationEnv.LookupName(obp.Type.Collation())
	}
	return val
}

func (obp *OrderByParams) Compare(r1, r2 []sqltypes.Value) int {
	v1 := r1[obp.Col]
	v2 := r2[obp.Col]
	cmp := v1.TinyWeightCmp(v2)

	if cmp == 0 {
		var err error
		cmp, err = NullsafeCompare(v1, v2, obp.CollationEnv, obp.Type.Collation(), obp.Type.values)
		if err != nil {
			_, isCollationErr := err.(UnsupportedCollationError)
			if !isCollationErr || obp.WeightStringCol == -1 {
				panic(err)
			}
			// in case of a comparison or collation error switch to using the weight string column for ordering
			obp.Col = obp.WeightStringCol
			obp.WeightStringCol = -1
			cmp, err = NullsafeCompare(r1[obp.Col], r2[obp.Col], obp.CollationEnv, obp.Type.Collation(), obp.Type.values)
			if err != nil {
				panic(err)
			}
		}
	}
	// change the result if descending ordering is required
	if obp.Desc {
		cmp = -cmp
	}
	return cmp
}

func (cmp Comparison) tinyWeighters(fields []*querypb.Field) []tinyWeighter {
	weights := make([]tinyWeighter, 0, len(cmp))
	for _, c := range cmp {
		if apply := TinyWeighter(fields[c.Col], c.Type.Collation()); apply != nil {
			weights = append(weights, tinyWeighter{c.Col, apply})
		}
	}
	return weights
}

func (cmp Comparison) ApplyTinyWeights(out *sqltypes.Result) {
	weights := cmp.tinyWeighters(out.Fields)
	if len(weights) == 0 {
		return
	}

	for _, row := range out.Rows {
		for _, w := range weights {
			w.apply(&row[w.col])
		}
	}
}

func (cmp Comparison) Compare(a, b sqltypes.Row) int {
	for _, c := range cmp {
		if cmp := c.Compare(a, b); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (cmp Comparison) Less(a, b sqltypes.Row) bool {
	for _, c := range cmp {
		if cmp := c.Compare(a, b); cmp != 0 {
			return cmp < 0
		}
	}
	return false
}

func (cmp Comparison) More(a, b sqltypes.Row) bool {
	for _, c := range cmp {
		if cmp := c.Compare(a, b); cmp != 0 {
			return cmp > 0
		}
	}
	return false
}

func PanicHandler(err *error) {
	if r := recover(); r != nil {
		badness, ok := r.(error)
		if !ok {
			panic(r)
		}

		*err = badness
	}
}

func (cmp Comparison) SortResult(out *sqltypes.Result) (err error) {
	defer PanicHandler(&err)
	cmp.ApplyTinyWeights(out)
	cmp.Sort(out.Rows)
	return
}

func (cmp Comparison) Sort(out []sqltypes.Row) {
	slices.SortFunc(out, func(a, b sqltypes.Row) int {
		return cmp.Compare(a, b)
	})
}

type Sorter struct {
	Compare Comparison
	Limit   int

	rows []sqltypes.Row
	heap bool
}

func (s *Sorter) Len() int {
	return len(s.rows)
}

func (s *Sorter) Push(row sqltypes.Row) {
	if len(s.rows) < s.Limit {
		s.rows = append(s.rows, row)
		return
	}
	if !s.heap {
		heapify(s.rows, s.Compare.More)
		s.heap = true
	}
	if s.Compare.Compare(s.rows[0], row) < 0 {
		return
	}
	s.rows[0] = row
	fix(s.rows, 0, s.Compare.More)
}

func (s *Sorter) Sorted() []sqltypes.Row {
	if !s.heap {
		s.Compare.Sort(s.rows)
		return s.rows
	}

	h := s.rows
	end := len(h)
	for end > 1 {
		end = end - 1
		h[end], h[0] = h[0], h[end]
		down(h[:end], 0, s.Compare.More)
	}
	return h
}

type mergeRow struct {
	row    sqltypes.Row
	source int
}

// Merger performs a k-way merge of sorted streams using a tournament loser tree.
// A loser tree requires only log₂(k) comparisons per ReplaceMin operation,
// compared to 2·log₂(k) for a binary heap, because each internal node stores
// the loser of its match and only one comparison is needed at each tree level.
type Merger struct {
	Compare Comparison

	tree  []int      // tree[0] = winner index, tree[1..cap-1] = loser indices
	keys  []mergeRow // keys[i] = current row from stream i; source < 0 means exhausted
	cap   int        // power-of-2 capacity of the tree
	count int        // number of active (non-exhausted) streams
}

func (m *Merger) Len() int {
	return m.count
}

// Push adds a row from a source stream. Must be called before Init.
func (m *Merger) Push(row sqltypes.Row, source int) {
	m.keys = append(m.keys, mergeRow{row, source})
	m.count++
}

// Init builds the loser tree from the pushed entries.
func (m *Merger) Init() {
	n := len(m.keys)
	if n == 0 {
		return
	}

	// Round up to next power of 2
	m.cap = 1 << bits.Len(uint(n-1))

	// Pad keys to capacity; padding entries are sentinel (source < 0)
	m.keys = slices.Grow(m.keys, m.cap-n)
	for i := n; i < m.cap; i++ {
		m.keys = append(m.keys, mergeRow{source: -1})
	}

	// Initialize all tree nodes as unset
	m.tree = make([]int, m.cap)
	for i := range m.tree {
		m.tree[i] = -1
	}

	// Insert each leaf from right to left to build the tree
	for i := m.cap - 1; i >= 0; i-- {
		m.replay(i)
	}
}

// Peek returns the current minimum element without removing it.
func (m *Merger) Peek() (sqltypes.Row, int) {
	w := m.tree[0]
	return m.keys[w].row, m.keys[w].source
}

// Pop removes and returns the current minimum element.
func (m *Merger) Pop() (sqltypes.Row, int) {
	w := m.tree[0]
	result := m.keys[w]
	m.keys[w].source = -1 // mark as exhausted
	m.count--
	if m.count > 0 {
		m.replay(w)
	}
	return result.row, result.source
}

// ReplaceMin replaces the current minimum with a new row and restores the
// tree. This performs exactly ⌈log₂(k)⌉ comparisons.
func (m *Merger) ReplaceMin(row sqltypes.Row, source int) {
	w := m.tree[0]
	m.keys[w] = mergeRow{row, source}
	m.replay(w)
}

// replay walks from leaf idx to the root, comparing the ascending value
// against the loser stored at each internal node and swapping when the
// ascending value loses.
func (m *Merger) replay(idx int) {
	winner := idx
	for t := (m.cap + idx) / 2; t > 0; t /= 2 {
		if m.tree[t] == -1 {
			// Uninitialized node during Init; store and stop.
			m.tree[t] = winner
			return
		}
		if m.less(m.tree[t], winner) {
			winner, m.tree[t] = m.tree[t], winner
		}
	}
	m.tree[0] = winner
}

// less returns true if stream a should come before stream b in the output.
// Exhausted streams (source < 0) are treated as infinity and never win.
func (m *Merger) less(a, b int) bool {
	if m.keys[a].source < 0 {
		return false
	}
	if m.keys[b].source < 0 {
		return true
	}
	return m.Compare.Less(m.keys[a].row, m.keys[b].row)
}

func heapify[T any](h []T, less func(a, b T) bool) {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, less)
	}
}

func fix[T any](h []T, i int, less func(a, b T) bool) {
	if !down(h, i, less) {
		up(h, i, less)
	}
}

func down[T any](h []T, i0 int, less func(a, b T) bool) bool {
	i := i0
	for {
		left, right := 2*i+1, 2*i+2
		if left >= len(h) || left < 0 { // `left < 0` in case of overflow
			break
		}

		// find the smallest child
		j := left
		if right < len(h) && less(h[right], h[left]) {
			j = right
		}

		if !less(h[j], h[i]) {
			break
		}

		h[i], h[j] = h[j], h[i]
		i = j
	}
	return i > i0
}

func up[T any](h []T, i int, less func(a, b T) bool) {
	for {
		parent := (i - 1) / 2
		if i == 0 || !less(h[i], h[parent]) {
			break
		}

		h[i], h[parent] = h[parent], h[i]
		i = parent
	}
}
