// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import querypb "github.com/youtube/vitess/go/vt/proto/query"

// This file contains the proto3 conversion functions for the structures
// defined here.

// RowsToProto3 converts [][]Value to proto3.
func RowsToProto3(rows [][]Value) []*querypb.Row {
	if len(rows) == 0 {
		return nil
	}

	result := make([]*querypb.Row, len(rows))
	for i, r := range rows {
		row := &querypb.Row{}
		result[i] = row
		row.Lengths = make([]int64, 0, len(r))
		total := 0
		for _, c := range r {
			if c.IsNull() {
				row.Lengths = append(row.Lengths, -1)
				continue
			}
			length := c.Len()
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

// proto3ToRows converts a proto3 rows to [][]Value. The function is private
// because it uses the trusted API.
func proto3ToRows(fields []*querypb.Field, rows []*querypb.Row) [][]Value {
	if len(rows) == 0 {
		// TODO(sougou): This is needed for backward compatibility.
		// Remove when it's not needed any more.
		return [][]Value{}
	}

	result := make([][]Value, len(rows))
	for i, r := range rows {
		result[i] = MakeRowTrusted(fields, r)
	}
	return result
}

// ResultToProto3 converts Result to proto3.
func ResultToProto3(qr *Result) *querypb.QueryResult {
	if qr == nil {
		return nil
	}
	return &querypb.QueryResult{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertID,
		Rows:         RowsToProto3(qr.Rows),
	}
}

// Proto3ToResult converts a proto3 Result to an internal data structure. This function
// should be used only if the field info is populated in qr.
func Proto3ToResult(qr *querypb.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertID:     qr.InsertId,
		Rows:         proto3ToRows(qr.Fields, qr.Rows),
	}
}

// CustomProto3ToResult converts a proto3 Result to an internal data structure. This function
// takes a separate fields input because not all QueryResults contain the field info.
// In particular, only the first packet of streaming queries contain the field info.
func CustomProto3ToResult(fields []*querypb.Field, qr *querypb.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertID:     qr.InsertId,
		Rows:         proto3ToRows(fields, qr.Rows),
	}
}

// ResultsToProto3 converts []Result to proto3.
func ResultsToProto3(qr []Result) []*querypb.QueryResult {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*querypb.QueryResult, len(qr))
	for i, q := range qr {
		result[i] = ResultToProto3(&q)
	}
	return result
}

// Proto3ToResults converts proto3 results to []Result.
func Proto3ToResults(qr []*querypb.QueryResult) []Result {
	if len(qr) == 0 {
		return nil
	}
	result := make([]Result, len(qr))
	for i, q := range qr {
		result[i] = *Proto3ToResult(q)
	}
	return result
}
