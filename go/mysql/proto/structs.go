// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// QueryResult is the structure returned by the mysql library.
// When transmitted over the wire, the Rows all come back as strings
// and lose their original sqltypes. use Fields.Type to convert
// them back if needed, using the following functions.
type QueryResult struct {
	Fields       []*querypb.Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
	Err          *RPCError
}

// RPCError is the structure that is returned by each RPC call, which contains
// the error information for that call.
type RPCError struct {
	Code    int64
	Message string
}

//go:generate bsongen -file $GOFILE -type RPCError -o rpcerror_bson.go

// Convert takes a type and a value, and returns the type:
// - nil for NULL value
// - uint64 for unsigned BIGINT values
// - int64 for all other integer values (signed and unsigned)
// - float64 for floating point values that fit in a float
// - []byte for everything else
func Convert(field *querypb.Field, val sqltypes.Value) (interface{}, error) {
	if field.Type == sqltypes.Null {
		return nil, nil
	} else if sqltypes.IsSigned(field.Type) {
		return strconv.ParseInt(val.String(), 0, 64)
	} else if sqltypes.IsUnsigned(field.Type) {
		return strconv.ParseUint(val.String(), 0, 64)
	} else if sqltypes.IsFloat(field.Type) {
		return strconv.ParseFloat(val.String(), 64)
	}
	return val.Raw(), nil
}
