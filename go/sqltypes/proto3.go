// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import pbq "github.com/youtube/vitess/go/vt/proto/query"

// This file contains the proto3 conversion functions for the structures
// defined here.

// RowsToProto3 converts [][]Value to proto3.
func RowsToProto3(rows [][]Value) []*pbq.Row {
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

// Proto3ToRows converts a proto3 rows to [][]Value.
func Proto3ToRows(fields []*pbq.Field, rows []*pbq.Row) [][]Value {
	if len(rows) == 0 {
		return [][]Value{}
	}

	result := make([][]Value, len(rows))
	for i, r := range rows {
		index := 0
		result[i] = make([]Value, len(r.Lengths))
		for j, l := range r.Lengths {
			if l < 0 {
				result[i][j] = NULL
			} else {
				end := index + int(l)
				result[i][j] = MakeValue(fields[j].Type, r.Values[index:end])
				index = end
			}
		}
	}
	return result
}

// ResultToProto3 converts Result to proto3.
func ResultToProto3(qr *Result) *pbq.QueryResult {
	if qr == nil {
		return nil
	}
	return &pbq.QueryResult{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertID,
		Rows:         RowsToProto3(qr.Rows),
	}
}

// Proto3ToResult converts a proto3 Result to an internal data structure.
func Proto3ToResult(qr *pbq.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertID:     qr.InsertId,
		Rows:         Proto3ToRows(qr.Fields, qr.Rows),
	}
}

// Proto3ToResults converts proto3 results to []Result.
func Proto3ToResults(qr []*pbq.QueryResult) []Result {
	if len(qr) == 0 {
		return nil
	}
	result := make([]Result, len(qr))
	for i, q := range qr {
		result[i] = *Proto3ToResult(q)
	}
	return result
}
