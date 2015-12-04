// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

// File has been manually edited. Do not regenerate.

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// BSONField is a temporary struct for backward compatibility.
type BSONField struct {
	Name  string
	Type  int64
	Flags int64
}

// MarshalBson bson-encodes Result.
func (result *Result) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	// []*query.Field
	{
		bson.EncodePrefix(buf, bson.Array, "Fields")
		lenWriter := bson.NewLenWriter(buf)
		var f BSONField
		for _i, _v1 := range result.Fields {
			// *query.Field
			// This part was manually changed.
			f.Name = _v1.Name
			f.Type, f.Flags = TypeToMySQL(_v1.Type)
			bson.EncodeOptionalPrefix(buf, bson.Object, bson.Itoa(_i))
			bson.MarshalToBuffer(buf, &f)
		}
		lenWriter.Close()
	}
	bson.EncodeUint64(buf, "RowsAffected", result.RowsAffected)
	bson.EncodeUint64(buf, "InsertId", result.InsertID)
	// [][]Value
	{
		bson.EncodePrefix(buf, bson.Array, "Rows")
		lenWriter := bson.NewLenWriter(buf)
		for _i, _v2 := range result.Rows {
			// []Value
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

	lenWriter.Close()
}

// UnmarshalBson bson-decodes into Result.
func (result *Result) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for Result", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		switch bson.ReadCString(buf) {
		case "Fields":
			// []*query.Field
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for result.Fields", kind))
				}
				bson.Next(buf, 4)
				result.Fields = make([]*querypb.Field, 0, 8)
				var f BSONField
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v1 *querypb.Field
					// *query.Field
					_v1 = new(querypb.Field)
					bson.UnmarshalFromBuffer(buf, &f)
					_v1.Name = f.Name
					_v1.Type = MySQLToType(f.Type, f.Flags)
					result.Fields = append(result.Fields, _v1)
				}
			}
		case "RowsAffected":
			result.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			result.InsertID = bson.DecodeUint64(buf, kind)
		case "Rows":
			// [][]Value
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for result.Rows", kind))
				}
				bson.Next(buf, 4)
				result.Rows = make([][]Value, 0, 8)
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v2 []Value
					// []Value
					if kind != bson.Null {
						if kind != bson.Array {
							panic(bson.NewBsonError("unexpected kind %v for _v2", kind))
						}
						bson.Next(buf, 4)
						_v2 = make([]Value, 0, 8)
						for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
							bson.SkipIndex(buf)
							var _v3 Value
							_v3.UnmarshalBson(buf, kind)
							_v2 = append(_v2, _v3)
						}
					}
					result.Rows = append(result.Rows, _v2)
				}
			}
		default:
			bson.Skip(buf, kind)
		}
	}
}
