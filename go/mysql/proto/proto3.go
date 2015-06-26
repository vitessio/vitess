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

// QueryResultToProto3 converts an internal QueryResult to the proto3 version
func QueryResultToProto3(qr *QueryResult) *pbq.QueryResult {
	result := &pbq.QueryResult{
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
	}

	if len(qr.Fields) > 0 {
		result.Fields = make([]*pbq.Field, len(qr.Fields))
		for i, f := range qr.Fields {
			result.Fields[i] = &pbq.Field{
				Name:  f.Name,
				Type:  pbq.Field_Type(f.Type),
				Flags: int64(f.Flags),
			}
		}
	}

	if len(qr.Rows) > 0 {
		result.Rows = make([]*pbq.Row, len(qr.Rows))
		for i, r := range qr.Rows {
			result.Rows[i] = &pbq.Row{
				Values: make([][]byte, len(r)),
			}
			for j, c := range r {
				result.Rows[i].Values[j] = c.Raw()
			}

		}
	}

	return result
}

// Proto3ToQueryResult converts a proto3 QueryResult to an internal data structure.
func Proto3ToQueryResult(qr *pbq.QueryResult) *QueryResult {
	result := &QueryResult{
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertId,
	}

	if len(qr.Fields) > 0 {
		result.Fields = make([]Field, len(qr.Fields))
		for i, f := range qr.Fields {
			result.Fields[i].Name = f.Name
			result.Fields[i].Type = int64(f.Type)
			result.Fields[i].Flags = int64(f.Flags)
		}
	}

	if len(qr.Rows) > 0 {
		result.Rows = make([][]sqltypes.Value, len(qr.Rows))
		for i, r := range qr.Rows {
			result.Rows[i] = make([]sqltypes.Value, len(r.Values))
			for j, c := range r.Values {
				if c == nil {
					result.Rows[i][j] = sqltypes.NULL
				} else {
					result.Rows[i][j] = sqltypes.MakeString(c)
				}
			}
		}
	}

	return result
}
