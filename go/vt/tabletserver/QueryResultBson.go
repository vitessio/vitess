// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"code.google.com/p/vitess/go/mysql"
)

type QueryResult mysql.QueryResult

// StreamQueryResult is used to return values for streaming queries.
// Exactly one of Fields and Row will be nil:
// - Fields will be set on the first returned value (and Row nil)
// - Row will be set for each database value (and Fields nil)
type StreamQueryResult struct {
	Fields []mysql.Field
	Row    []interface{}
}

func MarshalFieldBson(self mysql.Field, buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "Name")
	bson.EncodeString(buf, self.Name)

	bson.EncodePrefix(buf, bson.Long, "Type")
	bson.EncodeUint64(buf, uint64(self.Type))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func UnmarshalFieldBson(self *mysql.Field, buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Name":
			self.Name = bson.DecodeString(buf, kind)
		case "Type":
			self.Type = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (self *QueryResult) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Array, "Fields")
	encodeFieldsBson(self.Fields, buf)

	bson.EncodePrefix(buf, bson.Long, "RowsAffected")
	bson.EncodeUint64(buf, uint64(self.RowsAffected))

	bson.EncodePrefix(buf, bson.Long, "InsertId")
	bson.EncodeUint64(buf, uint64(self.InsertId))

	bson.EncodePrefix(buf, bson.Array, "Rows")
	encodeRowsBson(self.Rows, buf)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (sqr *StreamQueryResult) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Array, "Fields")
	encodeFieldsBson(sqr.Fields, buf)

	bson.EncodePrefix(buf, bson.Array, "Row")
	encodeRowBson(sqr.Row, buf)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeFieldsBson(fields []mysql.Field, buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range fields {
		bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
		MarshalFieldBson(v, buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeRowsBson(rows [][]interface{}, buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range rows {
		bson.EncodePrefix(buf, bson.Array, bson.Itoa(i))
		encodeRowBson(v, buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeRowBson(row []interface{}, buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range row {
		if v == nil {
			bson.EncodePrefix(buf, bson.Null, bson.Itoa(i))
		} else {
			bson.EncodePrefix(buf, bson.Binary, bson.Itoa(i))
			switch vv := v.(type) {
			case string:
				bson.EncodeString(buf, vv)
			case []byte:
				bson.EncodeBinary(buf, vv)
			default:
				panic(bson.NewBsonError("Unrecognized type %T", v))
			}
		}
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *QueryResult) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Fields":
			self.Fields = decodeFieldsBson(buf, kind)
		case "RowsAffected":
			self.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			self.InsertId = bson.DecodeUint64(buf, kind)
		case "Rows":
			self.Rows = decodeRowsBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (sqr *StreamQueryResult) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Fields":
			sqr.Fields = decodeFieldsBson(buf, kind)
		case "Row":
			sqr.Row = decodeRowBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func decodeFieldsBson(buf *bytes.Buffer, kind byte) []mysql.Field {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Fields", kind))
	}

	bson.Next(buf, 4)
	fields := make([]mysql.Field, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for Query.Field", kind))
		}
		bson.ExpectIndex(buf, i)
		var field mysql.Field
		UnmarshalFieldBson(&field, buf)
		fields = append(fields, field)
		kind = bson.NextByte(buf)
	}
	return fields
}

func decodeRowsBson(buf *bytes.Buffer, kind byte) [][]interface{} {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Rows", kind))
	}

	bson.Next(buf, 4)
	rows := make([][]interface{}, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		bson.ExpectIndex(buf, i)
		rows = append(rows, decodeRowBson(buf, kind))
		kind = bson.NextByte(buf)
	}
	return rows
}

func decodeRowBson(buf *bytes.Buffer, kind byte) []interface{} {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.Row", kind))
	}

	bson.Next(buf, 4)
	row := make([]interface{}, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		bson.ExpectIndex(buf, i)
		if kind != bson.Null {
			row = append(row, bson.DecodeString(buf, kind))
		} else {
			row = append(row, nil)
		}
		kind = bson.NextByte(buf)
	}
	return row
}
