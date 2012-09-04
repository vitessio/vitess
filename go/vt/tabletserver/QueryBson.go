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

func (query *Query) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "Sql")
	bson.EncodeString(buf, query.Sql)

	bson.EncodePrefix(buf, bson.Object, "BindVariables")
	query.encodeBindVariablesBson(buf)

	bson.EncodePrefix(buf, bson.Long, "TransactionId")
	bson.EncodeUint64(buf, uint64(query.TransactionId))

	bson.EncodePrefix(buf, bson.Long, "ConnectionId")
	bson.EncodeUint64(buf, uint64(query.ConnectionId))

	bson.EncodePrefix(buf, bson.Long, "SessionId")
	bson.EncodeUint64(buf, uint64(query.SessionId))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (query *Query) encodeBindVariablesBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	for k, v := range query.BindVariables {
		bson.EncodeField(buf, k, v)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (query *Query) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			query.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			query.decodeBindVariablesBson(buf, kind)
		case "TransactionId":
			query.TransactionId = bson.DecodeInt64(buf, kind)
		case "ConnectionId":
			query.ConnectionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			query.SessionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (query *Query) decodeBindVariablesBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.Object:
		if err := bson.UnmarshalFromBuffer(buf, &query.BindVariables); err != nil {
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
func (query *Query) String() string {
	buf := bytes2.NewChunkedWriter(1024)
	fmt.Fprintf(buf, "Sql: %#v, BindVars: {", query.Sql)
	for k, v := range query.BindVariables {
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
