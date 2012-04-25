/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"code.google.com/p/vitess/go/mysql"
)

type QueryResult mysql.QueryResult

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
	self.encodeFieldsBson(buf)

	bson.EncodePrefix(buf, bson.Long, "RowsAffected")
	bson.EncodeUint64(buf, uint64(self.RowsAffected))

	bson.EncodePrefix(buf, bson.Long, "InsertId")
	bson.EncodeUint64(buf, uint64(self.InsertId))

	bson.EncodePrefix(buf, bson.Array, "Rows")
	self.encodeRowsBson(buf)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *QueryResult) encodeFieldsBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range self.Fields {
		bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
		MarshalFieldBson(v, buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *QueryResult) encodeRowsBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range self.Rows {
		bson.EncodePrefix(buf, bson.Array, bson.Itoa(i))
		self.encodeRowBson(v, buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *QueryResult) encodeRowBson(row []interface{}, buf *bytes2.ChunkedWriter) {
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
			self.Fields = self.decodeFieldsBson(buf, kind)
		case "RowsAffected":
			self.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			self.InsertId = bson.DecodeUint64(buf, kind)
		case "Rows":
			self.Rows = self.decodeRowsBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (self *QueryResult) decodeFieldsBson(buf *bytes.Buffer, kind byte) []mysql.Field {
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

func (self *QueryResult) decodeRowsBson(buf *bytes.Buffer, kind byte) [][]interface{} {
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
		rows = append(rows, self.decodeRowBson(buf, kind))
		kind = bson.NextByte(buf)
	}
	return rows
}

func (self *QueryResult) decodeRowBson(buf *bytes.Buffer, kind byte) []interface{} {
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
