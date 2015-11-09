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
			if l < 0 {
				result[i][j] = sqltypes.NULL
			} else {
				end := index + int(l)
				result[i][j] = sqltypes.MakeString(r.Values[index:end])
				index = end
			}
		}
	}
	return result
}

// QueryResultToProto3 converts an internal QueryResult to the proto3 version
func QueryResultToProto3(qr *QueryResult) *pbq.QueryResult {
	if qr == nil {
		return nil
	}
	return &pbq.QueryResult{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		Rows:         RowsToProto3(qr.Rows),
	}
}

// Proto3ToQueryResult converts a proto3 QueryResult to an internal data structure.
func Proto3ToQueryResult(qr *pbq.QueryResult) *QueryResult {
	if qr == nil {
		return nil
	}
	return &QueryResult{
		Fields:       qr.Fields,
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
