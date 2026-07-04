/*
Copyright 2019 The Vitess Authors.

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

package sqltypes

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file contains the proto3 conversion functions for the structures
// defined here.

// RowToProto3 converts []Value to proto3.
func RowToProto3(row []Value) *querypb.Row {
	result := &querypb.Row{}
	_ = RowToProto3Inplace(row, result)
	return result
}

// RowToProto3Inplace converts []Value to proto3 and stores the conversion in the provided Row
func RowToProto3Inplace(row []Value, result *querypb.Row) int {
	if result.Lengths == nil {
		result.Lengths = make([]int64, 0, len(row))
	} else {
		result.Lengths = result.Lengths[:0]
	}
	total := 0
	for _, c := range row {
		if c.IsNull() {
			result.Lengths = append(result.Lengths, -1)
			continue
		}
		length := c.Len()
		result.Lengths = append(result.Lengths, int64(length))
		total += length
	}
	if result.Values == nil {
		result.Values = make([]byte, 0, total)
	} else {
		result.Values = result.Values[:0]
	}
	for _, c := range row {
		if c.IsNull() {
			continue
		}
		result.Values = append(result.Values, c.Raw()...)
	}
	return total
}

// RowsToProto3 converts [][]Value to proto3.
func RowsToProto3(rows [][]Value) []*querypb.Row {
	if len(rows) == 0 {
		return nil
	}

	result := make([]*querypb.Row, len(rows))
	for i, r := range rows {
		result[i] = RowToProto3(r)
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

// queryResultPool holds QueryResult messages for ResultToProto3Pooled.
var queryResultPool = sync.Pool{
	New: func() any { return &querypb.QueryResult{} },
}

// ResultToProto3Pooled converts qr like ResultToProto3, drawing the
// QueryResult and its Row messages from message pools. It is meant for
// transient conversions such as marshaling a streamed result into a gRPC
// response: once the message has been sent, the caller must release it with
// ReleaseProto3Result and must not retain any reference to it or its rows.
//
// Unlike ResultToProto3 it never uses the cached proto3 rows, so the
// returned message always owns its rows; the Fields remain shared with qr.
func ResultToProto3Pooled(qr *Result) *querypb.QueryResult {
	if qr == nil {
		return nil
	}
	res := queryResultPool.Get().(*querypb.QueryResult)
	res.Fields = qr.Fields
	res.RowsAffected = qr.RowsAffected
	res.InsertId = qr.InsertID
	res.InsertIdChanged = qr.InsertIDChanged
	res.Info = qr.Info
	res.SessionStateChanges = qr.SessionStateChanges
	if cap(res.Rows) < len(qr.Rows) {
		res.Rows = make([]*querypb.Row, 0, len(qr.Rows))
	}
	for _, row := range qr.Rows {
		r := querypb.RowFromVTPool()
		RowToProto3Inplace(row, r)
		res.Rows = append(res.Rows, r)
	}
	return res
}

// ReleaseProto3Result returns a QueryResult obtained from
// ResultToProto3Pooled, along with its rows, to their pools. The Field
// messages are shared with the Result the message was converted from, so
// only the references are dropped; the fields themselves are not reset.
func ReleaseProto3Result(res *querypb.QueryResult) {
	if res == nil {
		return
	}
	for _, r := range res.Rows {
		r.ReturnToVTPool()
	}
	rows := res.Rows[:0]
	res.Reset()
	res.Rows = rows
	queryResultPool.Put(res)
}

func ResultToProto3(qr *Result) *querypb.QueryResult {
	if qr == nil {
		return nil
	}
	// This read is susceptible to TOCTOU if proto3Rows is populated
	// concurrently, but the worst case is a redundant RowsToProto3 call.
	// In the consolidation path this can't happen today (the leader sets
	// the cached value before releasing the RWMutex), but the fallback is
	// harmless.
	rows := qr.proto3Rows
	if rows == nil {
		rows = RowsToProto3(qr.Rows)
	}
	return &querypb.QueryResult{
		Fields:              qr.Fields,
		RowsAffected:        qr.RowsAffected,
		InsertId:            qr.InsertID,
		InsertIdChanged:     qr.InsertIDChanged,
		Rows:                rows,
		Info:                qr.Info,
		SessionStateChanges: qr.SessionStateChanges,
	}
}

// Proto3ToResult converts a proto3 Result to an internal data structure. This function
// should be used only if the field info is populated in qr.
func Proto3ToResult(qr *querypb.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:              qr.Fields,
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertId,
		InsertIDChanged:     qr.InsertIdChanged,
		Rows:                proto3ToRows(qr.Fields, qr.Rows),
		Info:                qr.Info,
		SessionStateChanges: qr.SessionStateChanges,
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
		Fields:              fields,
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertId,
		InsertIDChanged:     qr.InsertIdChanged,
		Rows:                proto3ToRows(fields, qr.Rows),
		Info:                qr.Info,
		SessionStateChanges: qr.SessionStateChanges,
	}
}

// ResultsToProto3 converts []Result to proto3.
func ResultsToProto3(qr []*Result) []*querypb.QueryResult {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*querypb.QueryResult, len(qr))
	for i, q := range qr {
		result[i] = ResultToProto3(q)
	}
	return result
}

// Proto3ToResults converts proto3 results to []Result.
func Proto3ToResults(qr []*querypb.QueryResult) []*Result {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*Result, len(qr))
	for i, q := range qr {
		result[i] = Proto3ToResult(q)
	}
	return result
}

// QueryResponseToProto3 converts QueryResponse to proto3.
func QueryResponseToProto3(qr QueryResponse) *querypb.ResultWithError {
	return &querypb.ResultWithError{
		Result: ResultToProto3(qr.QueryResult),
		Error:  vterrors.ToVTRPC(qr.QueryError),
	}
}

// QueryResponsesToProto3 converts []QueryResponse to proto3.
func QueryResponsesToProto3(qr []QueryResponse) []*querypb.ResultWithError {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*querypb.ResultWithError, len(qr))
	for i, q := range qr {
		result[i] = QueryResponseToProto3(q)
	}
	return result
}

// Proto3ToQueryReponses converts proto3 queryResponse to []QueryResponse.
func Proto3ToQueryReponses(qr []*querypb.ResultWithError) []QueryResponse {
	if len(qr) == 0 {
		return nil
	}
	result := make([]QueryResponse, len(qr))
	for i, q := range qr {
		result[i] = QueryResponse{
			QueryResult: Proto3ToResult(q.Result),
			QueryError:  vterrors.FromVTRPC(q.Error),
		}
	}
	return result
}

// Proto3ResultsEqual compares two arrays of proto3 Result.
// reflect.DeepEqual shouldn't be used because of the protos.
func Proto3ResultsEqual(r1, r2 []*querypb.QueryResult) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !proto.Equal(r, r2[i]) {
			return false
		}
	}
	return true
}

// Proto3QueryResponsesEqual compares two arrays of proto3 QueryResponse.
// reflect.DeepEqual shouldn't be used because of the protos.
func Proto3QueryResponsesEqual(r1, r2 []*querypb.ResultWithError) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !proto.Equal(r, r2[i]) {
			return false
		}
	}
	return true
}

// Proto3ValuesEqual compares two arrays of proto3 Value.
func Proto3ValuesEqual(v1, v2 []*querypb.Value) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i, v := range v1 {
		if !proto.Equal(v, v2[i]) {
			return false
		}
	}
	return true
}
