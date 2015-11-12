// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// File has been manually edited. Do not regenerate.

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// BSONField is a temporary struct for backward compatibility.
type BSONField struct {
	Name  string
	Type  int64
	Flags int64
}

// MarshalBson bson-encodes QueryResult.
func (queryResult *QueryResult) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	// []*query.Field
	{
		bson.EncodePrefix(buf, bson.Array, "Fields")
		lenWriter := bson.NewLenWriter(buf)
		var f BSONField
		for _i, _v1 := range queryResult.Fields {
			// *query.Field
			// This part was manually changed.
			f.Name = _v1.Name
			f.Type, f.Flags = sqltypes.TypeToMySQL(_v1.Type)
			bson.EncodeOptionalPrefix(buf, bson.Object, bson.Itoa(_i))
			bson.MarshalToBuffer(buf, &f)
		}
		lenWriter.Close()
	}
	bson.EncodeUint64(buf, "RowsAffected", queryResult.RowsAffected)
	bson.EncodeUint64(buf, "InsertId", queryResult.InsertId)
	// [][]sqltypes.Value
	{
		bson.EncodePrefix(buf, bson.Array, "Rows")
		lenWriter := bson.NewLenWriter(buf)
		for _i, _v2 := range queryResult.Rows {
			// []sqltypes.Value
			{
				bson.EncodePrefix(buf, bson.Array, bson.Itoa(_i))
				lenWriter := bson.NewLenWriter(buf)
				for _i, _v3 := range _v2 {
					_v3.MarshalBson(buf, bson.Itoa(_i))
				}
				lenWriter.Close()
			}
		}
		lenWriter.Close()
	}
	// *RPCError
	if queryResult.Err == nil {
		bson.EncodePrefix(buf, bson.Null, "Err")
	} else {
		(*queryResult.Err).MarshalBson(buf, "Err")
	}

	lenWriter.Close()
}

// UnmarshalBson bson-decodes into QueryResult.
func (queryResult *QueryResult) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for QueryResult", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		switch bson.ReadCString(buf) {
		case "Fields":
			// []*query.Field
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for queryResult.Fields", kind))
				}
				bson.Next(buf, 4)
				queryResult.Fields = make([]*querypb.Field, 0, 8)
				var f BSONField
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v1 *querypb.Field
					// *query.Field
					_v1 = new(querypb.Field)
					bson.UnmarshalFromBuffer(buf, &f)
					_v1.Name = f.Name
					_v1.Type = sqltypes.MySQLToType(f.Type, f.Flags)
					queryResult.Fields = append(queryResult.Fields, _v1)
				}
			}
		case "RowsAffected":
			queryResult.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			queryResult.InsertId = bson.DecodeUint64(buf, kind)
		case "Rows":
			// [][]sqltypes.Value
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for queryResult.Rows", kind))
				}
				bson.Next(buf, 4)
				queryResult.Rows = make([][]sqltypes.Value, 0, 8)
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v2 []sqltypes.Value
					// []sqltypes.Value
					if kind != bson.Null {
						if kind != bson.Array {
							panic(bson.NewBsonError("unexpected kind %v for _v2", kind))
						}
						bson.Next(buf, 4)
						_v2 = make([]sqltypes.Value, 0, 8)
						for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
							bson.SkipIndex(buf)
							var _v3 sqltypes.Value
							_v3.UnmarshalBson(buf, kind)
							_v2 = append(_v2, _v3)
						}
					}
					queryResult.Rows = append(queryResult.Rows, _v2)
				}
			}
		case "Err":
			// *RPCError
			if kind != bson.Null {
				queryResult.Err = new(RPCError)
				(*queryResult.Err).UnmarshalBson(buf, kind)
			}
		default:
			bson.Skip(buf, kind)
		}
	}
}
