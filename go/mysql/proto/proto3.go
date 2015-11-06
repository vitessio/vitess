// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/sqltypes"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
)

// This file contains the proto3 conversion functions for the structures
// defined here.

// FieldsToProto3 converts an internal []Field to the proto3 version
func FieldsToProto3(f []Field) ([]*pbq.Field, error) {
	if len(f) == 0 {
		return nil, nil
	}

	result := make([]*pbq.Field, len(f))
	for i, f := range f {
		vitessType, err := sqltypes.MySQLToType(f.Type, f.Flags)
		if err != nil {
			return nil, err
		}
		result[i] = &pbq.Field{
			Name: f.Name,
			Type: vitessType,
		}
	}
	return result, nil
}

// Proto3ToFields converts a proto3 []Fields to an internal data structure.
func Proto3ToFields(f []*pbq.Field) []Field {
	if len(f) == 0 {
		return nil
	}
	result := make([]Field, len(f))
	for i, f := range f {
		result[i].Name = f.Name
		result[i].Type, result[i].Flags = sqltypes.TypeToMySQL(f.Type)
	}
	return result
}

// RowsToProto3 converts an internal [][]sqltypes.Value to the proto3 version
func RowsToProto3(rows [][]sqltypes.Value) []*pbq.Row {
	if len(rows) == 0 {
		return nil
	}

	result := make([]*pbq.Row, len(rows))
	for i, r := range rows {
		row := &pbq.Row{}
		result[i] = row
		row.Lengths = make([]int64, 0, len(r))
		total := 0
		for _, c := range r {
			if c.IsNull() {
				row.Lengths = append(row.Lengths, -1)
				continue
			}
			length := len(c.Raw())
			row.Lengths = append(row.Lengths, int64(length))
			total += length
		}
		row.Values = make([]byte, 0, total)
		for _, c := range r {
			if c.IsNull() {
				continue
			}
			row.Values = append(row.Values, c.Raw()...)
		}
	}
	return result
}

// Proto3ToRows converts a proto3 []Row to an internal data structure.
func Proto3ToRows(rows []*pbq.Row) [][]sqltypes.Value {
	if len(rows) == 0 {
		return [][]sqltypes.Value{}
	}

	result := make([][]sqltypes.Value, len(rows))
	for i, r := range rows {
		index := 0
		result[i] = make([]sqltypes.Value, len(r.Lengths))
		for j, l := range r.Lengths {
			if l <= -1 {
				result[i][j] = sqltypes.NULL
			} else {
				end := index + int(l)
				if end > len(r.Values) {
					result[i][j] = sqltypes.NULL
					index = len(r.Values)
				} else {
					result[i][j] = sqltypes.MakeString(r.Values[index:end])
					index = end
				}
			}
		}
	}
	return result
}

// QueryResultToProto3 converts an internal QueryResult to the proto3 version
func QueryResultToProto3(qr *QueryResult) (*pbq.QueryResult, error) {
	if qr == nil {
		return nil, nil
	}
	fields, err := FieldsToProto3(qr.Fields)
	if err != nil {
		return nil, err
	}
	return &pbq.QueryResult{
		Fields:       fields,
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		Rows:         RowsToProto3(qr.Rows),
	}, nil
}

// Proto3ToQueryResult converts a proto3 QueryResult to an internal data structure.
func Proto3ToQueryResult(qr *pbq.QueryResult) *QueryResult {
	if qr == nil {
		return nil
	}
	return &QueryResult{
		Fields:       Proto3ToFields(qr.Fields),
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		Rows:         Proto3ToRows(qr.Rows),
	}
}

// Proto3ToQueryResults converts an array os proto3 QueryResult to an
// internal data structure.
func Proto3ToQueryResults(qr []*pbq.QueryResult) []QueryResult {
	if len(qr) == 0 {
		return nil
	}
	result := make([]QueryResult, len(qr))
	for i, q := range qr {
		result[i] = *Proto3ToQueryResult(q)
	}
	return result
}
