// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

// StreamEvent represents one event for the update stream.
type StreamEvent struct {
	// Category can be "DML", "DDL", "ERR" or "POS"
	Category string

	// DML
	TableName  string
	PKColNames []string
	PKValues   [][]interface{}

	// DDL or ERR
	Sql string

	// Timestamp is set for DML, DDL or ERR
	Timestamp int64

	// POS
	GroupId int64
}

func (ste *StreamEvent) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	bson.EncodeString(buf, "Category", ste.Category)
	bson.EncodeString(buf, "TableName", ste.TableName)
	bson.EncodeStringArray(buf, "PKColNames", ste.PKColNames)
	MarshalPKValuesBson(buf, "PKValues", ste.PKValues)
	bson.EncodeString(buf, "Sql", ste.Sql)
	bson.EncodeInt64(buf, "Timestamp", ste.Timestamp)
	bson.EncodeInt64(buf, "GroupId", ste.GroupId)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func MarshalPKValuesBson(buf *bytes2.ChunkedWriter, key string, pkValues [][]interface{}) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, row := range pkValues {
		MarshalPKRowBson(buf, bson.Itoa(i), row)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func MarshalPKRowBson(buf *bytes2.ChunkedWriter, key string, pkRow []interface{}) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range pkRow {
		bson.EncodeField(buf, bson.Itoa(i), v)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (ste *StreamEvent) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Category":
			ste.Category = bson.DecodeString(buf, kind)
		case "TableName":
			ste.TableName = bson.DecodeString(buf, kind)
		case "PKColNames":
			ste.PKColNames = bson.DecodeStringArray(buf, kind)
		case "PKValues":
			ste.PKValues = UnmarshalPKValuesBson(buf, kind)
		case "Sql":
			ste.Sql = bson.DecodeString(buf, kind)
		case "Timestamp":
			ste.Timestamp = bson.DecodeInt64(buf, kind)
		case "GroupId":
			ste.GroupId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func UnmarshalPKValuesBson(buf *bytes.Buffer, kind byte) [][]interface{} {
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
		rows = append(rows, UnmarshalPKRowBson(buf, kind))
		kind = bson.NextByte(buf)
	}
	return rows
}

func UnmarshalPKRowBson(buf *bytes.Buffer, kind byte) []interface{} {
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
		var val interface{}
		switch kind {
		case bson.Binary, bson.String:
			val = bson.DecodeString(buf, kind)
		case bson.Long:
			val = bson.DecodeInt64(buf, kind)
		case bson.Ulong:
			val = bson.DecodeUint64(buf, kind)
		}
		row = append(row, val)
		kind = bson.NextByte(buf)
	}
	return row
}
