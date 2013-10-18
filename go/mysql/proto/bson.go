// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	"github.com/youtube/vitess/go/sqltypes"
)

func MarshalFieldBson(field Field, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Name", field.Name)
	bson.EncodeInt64(buf, "Type", field.Type)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func UnmarshalFieldBson(field *Field, buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Name":
			field.Name = bson.DecodeString(buf, kind)
		case "Type":
			field.Type = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (qr *QueryResult) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	encodeFieldsBson(qr.Fields, "Fields", buf)
	bson.EncodeInt64(buf, "RowsAffected", int64(qr.RowsAffected))
	bson.EncodeInt64(buf, "InsertId", int64(qr.InsertId))
	encodeRowsBson(qr.Rows, "Rows", buf)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeFieldsBson(fields []Field, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range fields {
		MarshalFieldBson(v, bson.Itoa(i), buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeRowsBson(rows [][]sqltypes.Value, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range rows {
		encodeRowBson(v, bson.Itoa(i), buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeRowBson(row []sqltypes.Value, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range row {
		if v.IsNull() {
			bson.EncodePrefix(buf, bson.Null, bson.Itoa(i))
		} else {
			bson.EncodeBinary(buf, bson.Itoa(i), v.Raw())
		}
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (qr *QueryResult) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Fields":
			qr.Fields = decodeFieldsBson(buf, kind)
		case "RowsAffected":
			qr.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			qr.InsertId = bson.DecodeUint64(buf, kind)
		case "Rows":
			qr.Rows = decodeRowsBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func decodeFieldsBson(buf *bytes.Buffer, kind byte) []Field {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Fields", kind))
	}

	bson.Next(buf, 4)
	fields := make([]Field, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for Query.Field", kind))
		}
		bson.ExpectIndex(buf, i)
		var field Field
		UnmarshalFieldBson(&field, buf)
		fields = append(fields, field)
		kind = bson.NextByte(buf)
	}
	return fields
}

func decodeRowsBson(buf *bytes.Buffer, kind byte) [][]sqltypes.Value {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Rows", kind))
	}

	bson.Next(buf, 4)
	rows := make([][]sqltypes.Value, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		bson.ExpectIndex(buf, i)
		rows = append(rows, decodeRowBson(buf, kind))
		kind = bson.NextByte(buf)
	}
	return rows
}

func decodeRowBson(buf *bytes.Buffer, kind byte) []sqltypes.Value {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Row", kind))
	}

	bson.Next(buf, 4)
	row := make([]sqltypes.Value, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		bson.ExpectIndex(buf, i)
		if kind != bson.Null {
			row = append(row, sqltypes.MakeString(bson.DecodeBytes(buf, kind)))
		} else {
			row = append(row, sqltypes.Value{})
		}
		kind = bson.NextByte(buf)
	}
	return row
}
