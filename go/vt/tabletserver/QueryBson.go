// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"fmt"
)

type Query struct {
	Sql           string
	BindVariables map[string]interface{}
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

func (self *Query) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "Sql")
	bson.EncodeString(buf, self.Sql)

	bson.EncodePrefix(buf, bson.Object, "BindVariables")
	self.encodeBindVariablesBson(buf)

	bson.EncodePrefix(buf, bson.Long, "TransactionId")
	bson.EncodeUint64(buf, uint64(self.TransactionId))

	bson.EncodePrefix(buf, bson.Long, "ConnectionId")
	bson.EncodeUint64(buf, uint64(self.ConnectionId))

	bson.EncodePrefix(buf, bson.Long, "SessionId")
	bson.EncodeUint64(buf, uint64(self.SessionId))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *Query) encodeBindVariablesBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for k, v := range self.BindVariables {
		bson.EncodeField(buf, k, v)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *Query) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			self.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			self.decodeBindVariablesBson(buf, kind)
		case "TransactionId":
			self.TransactionId = bson.DecodeInt64(buf, kind)
		case "ConnectionId":
			self.ConnectionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			self.SessionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (self *Query) decodeBindVariablesBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.Object:
		if err := bson.UnmarshalFromBuffer(buf, &self.BindVariables); err != nil {
			panic(err)
		}
	case bson.Null:
		// no op
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.BindVariables", kind))
	}
}

// String prints a readable version of Query, and also truncates
// data if it's too long
func (self *Query) String() string {
	buf := bytes2.NewChunkedWriter(1024)
	fmt.Fprintf(buf, "Sql: %#v, BindVars: {", self.Sql)
	for k, v := range self.BindVariables {
		switch val := v.(type) {
		case []byte:
			fmt.Fprintf(buf, "%s: %#v, ", k, slimit(string(val)))
		case string:
			fmt.Fprintf(buf, "%s: %#v, ", k, slimit(val))
		default:
			fmt.Fprintf(buf, "%s: %v, ", k, v)
		}
	}
	fmt.Fprintf(buf, "}")
	return string(buf.Bytes())
}

func slimit(s string) string {
	l := len(s)
	if l > 256 {
		l = 256
	}
	return s[:l]
}
