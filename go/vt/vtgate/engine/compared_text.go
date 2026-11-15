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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// comparedCollation returns the collation that vtgate compares, sorts, groups
// or hashes the values of the column col of rows with these fields in.
//
// The plan has the collation of the column, which is the one that MySQL
// compares it in. The field says which character set the text of a row is in.
// The two differ when a tablet without
// --queryserver-config-raw-result-charset sends a latin1_german2_ci column:
// it sends 'Müller' as utf8mb4, 4D C3 BC 6C 6C 65 72, and latin1_german2_ci
// takes those bytes for the latin1 text 'MÃ¼ller', which is not equal to
// 'Mueller', as 'Müller' is in that collation. They also differ when rows
// that are in latin1 are compared with a column of another character set in
// the collation of that one. The collation that this returns converts each
// text to the character set of the planned collation before it compares it,
// see colldata.ForText. The rows stay as they are.
//
// It returns the planned collation without --convert-result-charset, without
// fields, and when the text is in the character set of the planned collation.
// The plan is shared between executions, so the collation is found for each
// execution and is not stored in the plan.
func comparedCollation(vcursor VCursor, fields []*querypb.Field, col int, planned collations.ID) collations.ID {
	if !vcursor.ConvertResultCharset() || col < 0 || col >= len(fields) {
		return planned
	}
	field := fields[col]
	if !sqltypes.IsText(field.Type) {
		return planned
	}
	return colldata.ForText(planned, collations.ID(field.Charset))
}

// comparedType returns the planned type with the collation of
// comparedCollation.
func comparedType(vcursor VCursor, fields []*querypb.Field, col int, planned evalengine.Type) evalengine.Type {
	if !planned.Valid() {
		return planned
	}
	collation := comparedCollation(vcursor, fields, col, planned.Collation())
	if collation == planned.Collation() {
		return planned
	}
	return evalengine.NewTypeEx(planned.Type(), collation, planned.Nullable(), planned.Size(), planned.Scale(), planned.Values())
}

// comparedOrder returns the planned order with the collations of
// comparedCollation. It is the planned order itself when none changes.
func comparedOrder(vcursor VCursor, fields []*querypb.Field, planned evalengine.Comparison) evalengine.Comparison {
	var order evalengine.Comparison
	for i, param := range planned {
		typ := comparedType(vcursor, fields, param.Col, param.Type)
		if typ.Collation() == param.Type.Collation() {
			continue
		}
		if order == nil {
			order = make(evalengine.Comparison, len(planned))
			copy(order, planned)
		}
		order[i].Type = typ
	}
	if order == nil {
		return planned
	}
	return order
}

// comparedGroupBy returns the planned keys with the collations of
// comparedCollation. It is the planned keys themselves when none changes.
func comparedGroupBy(vcursor VCursor, fields []*querypb.Field, planned []*GroupByParams) []*GroupByParams {
	var keys []*GroupByParams
	for i, param := range planned {
		typ := comparedType(vcursor, fields, param.KeyCol, param.Type)
		if typ.Collation() == param.Type.Collation() {
			continue
		}
		if keys == nil {
			keys = make([]*GroupByParams, len(planned))
			copy(keys, planned)
		}
		key := *param
		key.Type = typ
		keys[i] = &key
	}
	if keys == nil {
		return planned
	}
	return keys
}

// comparedCheckCols returns the planned columns of a DISTINCT with the
// collations of comparedCollation. It is the planned columns themselves when
// none changes.
func comparedCheckCols(vcursor VCursor, fields []*querypb.Field, planned []CheckCol) []CheckCol {
	var cols []CheckCol
	for i, col := range planned {
		typ := comparedType(vcursor, fields, col.Col, col.Type)
		if typ.Collation() == col.Type.Collation() {
			continue
		}
		if cols == nil {
			cols = make([]CheckCol, len(planned))
			copy(cols, planned)
		}
		cols[i].Type = typ
	}
	if cols == nil {
		return planned
	}
	return cols
}

// streamComparedFields has the fields of the rows of one stream, which come
// with its first chunk, for comparedCollation. It is not safe for concurrent
// use.
type streamComparedFields struct {
	fields []*querypb.Field
}

// first returns true for the first chunk that has fields, and keeps them.
func (s *streamComparedFields) first(chunk *sqltypes.Result) bool {
	if s.fields != nil || len(chunk.Fields) == 0 {
		return false
	}
	s.fields = chunk.Fields
	return true
}

// rowFields gives an evaluation the fields of its rows, which say which
// character set the text of each column is in. The evaluation then converts
// the text of a column to the character set of the collation that it
// evaluates the column in, see evalengine.ExpressionEnv.FieldCharsets, as
// comparedCollation does for a comparison. Without --convert-result-charset,
// and without fields, the evaluation is the one it was.
func rowFields(vcursor VCursor, env *evalengine.ExpressionEnv, fields []*querypb.Field) {
	if !vcursor.ConvertResultCharset() || len(fields) == 0 {
		return
	}
	env.Fields = fields
	env.FieldCharsets = true
}
