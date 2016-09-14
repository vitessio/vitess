// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import querypb "github.com/youtube/vitess/go/vt/proto/query"

// Result represents a query result.
type Result struct {
	Fields       []*querypb.Field `json:"fields"`
	RowsAffected uint64           `json:"rows_affected"`
	InsertID     uint64           `json:"insert_id"`
	Rows         [][]Value        `json:"rows"`
}

// ResultStream is an interface for receiving Result. It is used for
// RPC interfaces.
type ResultStream interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() (*Result, error)
}

// Repair fixes the type info in the rows
// to conform to the supplied field types.
func (result *Result) Repair(fields []*querypb.Field) {
	// Usage of j is intentional.
	for j, f := range fields {
		for _, r := range result.Rows {
			if r[j].typ != Null {
				r[j].typ = f.Type
			}
		}
	}
}

// Copy creates a deep copy of Result.
func (result *Result) Copy() *Result {
	out := &Result{
		InsertID:     result.InsertID,
		RowsAffected: result.RowsAffected,
	}
	if result.Fields != nil {
		fieldsp := make([]*querypb.Field, len(result.Fields))
		fields := make([]querypb.Field, len(result.Fields))
		for i, f := range result.Fields {
			fields[i] = *f
			fieldsp[i] = &fields[i]
		}
		out.Fields = fieldsp
	}
	if result.Rows != nil {
		rows := make([][]Value, len(result.Rows))
		for i, r := range result.Rows {
			rows[i] = make([]Value, len(r))
			totalLen := 0
			for _, c := range r {
				totalLen += len(c.val)
			}
			arena := make([]byte, 0, totalLen)
			for j, c := range r {
				start := len(arena)
				arena = append(arena, c.val...)
				rows[i][j] = MakeTrusted(c.typ, arena[start:start+len(c.val)])
			}
		}
		out.Rows = rows
	}
	return out
}

// MakeRowTrusted converts a *querypb.Row to []Value based on the types
// in fields. It does not sanity check the values against the type.
// Every place this function is called, a comment is needed that explains
// why it's justified.
func MakeRowTrusted(fields []*querypb.Field, row *querypb.Row) []Value {
	sqlRow := make([]Value, len(row.Lengths))
	var offset int64
	for i, length := range row.Lengths {
		if length < 0 {
			continue
		}
		sqlRow[i] = MakeTrusted(fields[i].Type, row.Values[offset:offset+length])
		offset += length
	}
	return sqlRow
}

// StripFieldNames will remove the field names from the Fields.
// It does not change the original fields, but creates new ones if it needs to.
func (result *Result) StripFieldNames() {
	if len(result.Fields) == 0 {
		return
	}
	newFields := make([]*querypb.Field, len(result.Fields))
	newFieldsArray := make([]querypb.Field, len(result.Fields))
	for i, f := range result.Fields {
		newFields[i] = &newFieldsArray[i]
		newFieldsArray[i].Type = f.Type
	}
	result.Fields = newFields
}
