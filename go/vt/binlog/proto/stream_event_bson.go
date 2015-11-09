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
	"github.com/youtube/vitess/go/vt/proto/query"
)

// BSONField is a temporary struct for backward compatibility.
type BSONField struct {
	Name  string
	Type  int64
	Flags int64
}

// MarshalBson bson-encodes StreamEvent.
func (streamEvent *StreamEvent) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Category", streamEvent.Category)
	bson.EncodeString(buf, "TableName", streamEvent.TableName)
	// []*query.Field
	{
		bson.EncodePrefix(buf, bson.Array, "PrimaryKeyFields")
		lenWriter := bson.NewLenWriter(buf)
		var f BSONField
		for _i, _v1 := range streamEvent.PrimaryKeyFields {
			// *query.Field
			// This part was manually changed.
			f.Name = _v1.Name
			f.Type, f.Flags = sqltypes.TypeToMySQL(_v1.Type)
			bson.EncodeOptionalPrefix(buf, bson.Object, bson.Itoa(_i))
			bson.MarshalToBuffer(buf, &f)
		}
		lenWriter.Close()
	}
	// [][]sqltypes.Value
	{
		bson.EncodePrefix(buf, bson.Array, "PrimaryKeyValues")
		lenWriter := bson.NewLenWriter(buf)
		for _i, _v2 := range streamEvent.PrimaryKeyValues {
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
	bson.EncodeString(buf, "Sql", streamEvent.Sql)
	bson.EncodeInt64(buf, "Timestamp", streamEvent.Timestamp)
	bson.EncodeString(buf, "TransactionID", streamEvent.TransactionID)

	lenWriter.Close()
}

// UnmarshalBson bson-decodes into StreamEvent.
func (streamEvent *StreamEvent) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for StreamEvent", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		switch bson.ReadCString(buf) {
		case "Category":
			streamEvent.Category = bson.DecodeString(buf, kind)
		case "TableName":
			streamEvent.TableName = bson.DecodeString(buf, kind)
		case "PrimaryKeyFields":
			// []*query.Field
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for streamEvent.PrimaryKeyFields", kind))
				}
				bson.Next(buf, 4)
				streamEvent.PrimaryKeyFields = make([]*query.Field, 0, 8)
				var f BSONField
				for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
					bson.SkipIndex(buf)
					var _v1 *query.Field
					// *query.Field
					_v1 = new(query.Field)
					bson.UnmarshalFromBuffer(buf, &f)
					_v1.Name = f.Name
					_v1.Type = sqltypes.MySQLToType(f.Type, f.Flags)
					streamEvent.PrimaryKeyFields = append(streamEvent.PrimaryKeyFields, _v1)
				}
			}
		case "PrimaryKeyValues":
			// [][]sqltypes.Value
			if kind != bson.Null {
				if kind != bson.Array {
					panic(bson.NewBsonError("unexpected kind %v for streamEvent.PrimaryKeyValues", kind))
				}
				bson.Next(buf, 4)
				streamEvent.PrimaryKeyValues = make([][]sqltypes.Value, 0, 8)
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
					streamEvent.PrimaryKeyValues = append(streamEvent.PrimaryKeyValues, _v2)
				}
			}
		case "Sql":
			streamEvent.Sql = bson.DecodeString(buf, kind)
		case "Timestamp":
			streamEvent.Timestamp = bson.DecodeInt64(buf, kind)
		case "TransactionID":
			streamEvent.TransactionID = bson.DecodeString(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
	}
}
