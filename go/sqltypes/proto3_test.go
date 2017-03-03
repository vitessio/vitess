// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
)

func TestResult(t *testing.T) {
	fields := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	sqlResult := &Result{
		Fields:       fields,
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{{
			testVal(VarChar, "aa"),
			testVal(Int64, "1"),
			testVal(Float64, "2"),
		}, {
			MakeTrusted(VarChar, []byte("bb")),
			NULL,
			NULL,
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard0",
				Position:  "position0",
			},
		},
	}
	p3Result := &querypb.QueryResult{
		Fields:       fields,
		InsertId:     1,
		RowsAffected: 2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}, {
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard0",
				Position:  "position0",
			},
		},
	}
	p3converted := ResultToProto3(sqlResult)
	if !reflect.DeepEqual(p3converted, p3Result) {
		t.Errorf("P3:\n%v, want\n%v", p3converted, p3Result)
	}

	reverse := Proto3ToResult(p3Result)
	if !reflect.DeepEqual(reverse, sqlResult) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResult)
	}

	// Test custom fields.
	fields[1].Type = VarBinary
	sqlResult.Rows[0][1] = testVal(VarBinary, "1")
	reverse = CustomProto3ToResult(fields, p3Result)
	if !reflect.DeepEqual(reverse, sqlResult) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResult)
	}
}

func TestResults(t *testing.T) {
	fields1 := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	fields2 := []*querypb.Field{{
		Name: "col11",
		Type: VarChar,
	}, {
		Name: "col12",
		Type: Int64,
	}, {
		Name: "col13",
		Type: Float64,
	}}
	sqlResults := []Result{{
		Fields:       fields1,
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{{
			testVal(VarChar, "aa"),
			testVal(Int64, "1"),
			testVal(Float64, "2"),
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard0",
				Position:  "position0",
			},
		},
	}, {
		Fields:       fields2,
		InsertID:     3,
		RowsAffected: 4,
		Rows: [][]Value{{
			testVal(VarChar, "bb"),
			testVal(Int64, "3"),
			testVal(Float64, "4"),
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard1",
				Position:  "position1",
			},
		},
	}}
	p3Results := []*querypb.QueryResult{{
		Fields:       fields1,
		InsertId:     1,
		RowsAffected: 2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard0",
				Position:  "position0",
			},
		},
	}, {
		Fields:       fields2,
		InsertId:     3,
		RowsAffected: 4,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("bb34"),
		}},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "shard1",
				Position:  "position1",
			},
		},
	}}
	p3converted := ResultsToProto3(sqlResults)
	if !reflect.DeepEqual(p3converted, p3Results) {
		t.Errorf("P3:\n%v, want\n%v", p3converted, p3Results)
	}

	reverse := Proto3ToResults(p3Results)
	if !reflect.DeepEqual(reverse, sqlResults) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResults)
	}
}

func TestQueryReponses(t *testing.T) {
	fields1 := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	fields2 := []*querypb.Field{{
		Name: "col11",
		Type: VarChar,
	}, {
		Name: "col12",
		Type: Int64,
	}, {
		Name: "col13",
		Type: Float64,
	}}

	queryResponses := []QueryResponse{
		{
			QueryResult: &Result{
				Fields:       fields1,
				InsertID:     1,
				RowsAffected: 2,
				Rows: [][]Value{{
					testVal(VarChar, "aa"),
					testVal(Int64, "1"),
					testVal(Float64, "2"),
				}},
				Extras: &querypb.ResultExtras{
					EventToken: &querypb.EventToken{
						Timestamp: 123,
						Shard:     "shard0",
						Position:  "position0",
					},
				},
			},
			QueryError: nil,
		}, {
			QueryResult: &Result{
				Fields:       fields2,
				InsertID:     3,
				RowsAffected: 4,
				Rows: [][]Value{{
					testVal(VarChar, "bb"),
					testVal(Int64, "3"),
					testVal(Float64, "4"),
				}},
				Extras: &querypb.ResultExtras{
					EventToken: &querypb.EventToken{
						Timestamp: 123,
						Shard:     "shard1",
						Position:  "position1",
					},
				},
			},
			QueryError: nil,
		}, {
			QueryResult: nil,
			QueryError:  vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "deadline exceeded"),
		},
	}

	p3ResultWithError := []*querypb.ResultWithError{
		{
			Error: nil,
			Result: &querypb.QueryResult{
				Fields:       fields1,
				InsertId:     1,
				RowsAffected: 2,
				Rows: []*querypb.Row{{
					Lengths: []int64{2, 1, 1},
					Values:  []byte("aa12"),
				}},
				Extras: &querypb.ResultExtras{
					EventToken: &querypb.EventToken{
						Timestamp: 123,
						Shard:     "shard0",
						Position:  "position0",
					},
				},
			},
		}, {
			Error: nil,
			Result: &querypb.QueryResult{
				Fields:       fields2,
				InsertId:     3,
				RowsAffected: 4,
				Rows: []*querypb.Row{{
					Lengths: []int64{2, 1, 1},
					Values:  []byte("bb34"),
				}},
				Extras: &querypb.ResultExtras{
					EventToken: &querypb.EventToken{
						Timestamp: 123,
						Shard:     "shard1",
						Position:  "position1",
					},
				},
			},
		}, {
			Error: &vtrpcpb.RPCError{
				LegacyCode: vtrpcpb.LegacyErrorCode_DEADLINE_EXCEEDED_LEGACY,
				Message:    "deadline exceeded",
				Code:       vtrpcpb.Code_DEADLINE_EXCEEDED,
			},
			Result: nil,
		},
	}
	p3converted := QueryResponsesToProto3(queryResponses)
	if !reflect.DeepEqual(p3converted, p3ResultWithError) {
		t.Errorf("P3:\n%v, want\n%v", p3converted, p3ResultWithError)
	}

	reverse := Proto3ToQueryReponses(p3ResultWithError)
	if !reflect.DeepEqual(reverse, queryResponses) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, queryResponses)
	}
}
