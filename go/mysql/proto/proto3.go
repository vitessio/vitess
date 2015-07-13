// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/sqltypes"

	pbb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbq "github.com/youtube/vitess/go/vt/proto/query"
)

// This file contains the proto3 conversion functions for the structures
// defined here.

// CharsetToProto converts a Charset to a proto3
func CharsetToProto(c *Charset) *pbb.Charset {
	if c == nil {
		return nil
	}
	return &pbb.Charset{
		Client: int32(c.Client),
		Conn:   int32(c.Conn),
		Server: int32(c.Server),
	}
}

// ProtoToCharset converts a proto to a Charset
func ProtoToCharset(c *pbb.Charset) *Charset {
	if c == nil {
		return nil
	}
	return &Charset{
		Client: int(c.Client),
		Conn:   int(c.Conn),
		Server: int(c.Server),
	}
}

// FieldsToProto3 converts an internal []Field to the proto3 version
func FieldsToProto3(f []Field) []*pbq.Field {
	if len(f) == 0 {
		return nil
	}

	result := make([]*pbq.Field, len(f))
	for i, f := range f {
		result[i] = &pbq.Field{
			Name:  f.Name,
			Type:  pbq.Field_Type(f.Type),
			Flags: int64(f.Flags),
		}
	}
	return result
}

// Proto3ToFields converts a proto3 []Fields to an internal data structure.
func Proto3ToFields(f []*pbq.Field) []Field {
	if len(f) == 0 {
		return nil
	}
	result := make([]Field, len(f))
	for i, f := range f {
		result[i].Name = f.Name
		result[i].Type = int64(f.Type)
		result[i].Flags = int64(f.Flags)
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
		result[i] = &pbq.Row{
			Values: make([][]byte, len(r)),
		}
		for j, c := range r {
			result[i].Values[j] = c.Raw()
		}
	}
	return result
}

// Proto3ToRows converts a proto3 []Row to an internal data structure.
func Proto3ToRows(rows []*pbq.Row) [][]sqltypes.Value {
	if len(rows) == 0 {
		return nil
	}

	result := make([][]sqltypes.Value, len(rows))
	for i, r := range rows {
		result[i] = make([]sqltypes.Value, len(r.Values))
		for j, c := range r.Values {
			if c == nil {
				result[i][j] = sqltypes.NULL
			} else {
				result[i][j] = sqltypes.MakeString(c)
			}
		}
	}
	return result
}

// QueryResultToProto3 converts an internal QueryResult to the proto3 version
func QueryResultToProto3(qr *QueryResult) *pbq.QueryResult {
	return &pbq.QueryResult{
		Fields:       FieldsToProto3(qr.Fields),
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		Rows:         RowsToProto3(qr.Rows),
	}
}

// Proto3ToQueryResult converts a proto3 QueryResult to an internal data structure.
func Proto3ToQueryResult(qr *pbq.QueryResult) *QueryResult {
	return &QueryResult{
		Fields:       Proto3ToFields(qr.Fields),
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
		Rows:         Proto3ToRows(qr.Rows),
	}
}
