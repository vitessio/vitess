// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
)

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
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.BindVariables", kind))
	}
	bson.Next(buf, 4)
	query.BindVariables = make(map[string]interface{})
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		key := bson.ReadCString(buf)
		switch kind {
		case bson.Number:
			ui64 := bson.Pack.Uint64(buf.Next(8))
			query.BindVariables[key] = math.Float64frombits(ui64)
		case bson.String:
			l := int(bson.Pack.Uint32(buf.Next(4)))
			query.BindVariables[key] = buf.Next(l - 1)
			buf.ReadByte()
		case bson.Binary:
			l := int(bson.Pack.Uint32(buf.Next(4)))
			buf.ReadByte()
			query.BindVariables[key] = buf.Next(l)
		case bson.Int:
			query.BindVariables[key] = int32(bson.Pack.Uint32(buf.Next(4)))
		case bson.Long:
			query.BindVariables[key] = int64(bson.Pack.Uint64(buf.Next(8)))
		case bson.Ulong:
			query.BindVariables[key] = bson.Pack.Uint64(buf.Next(8))
		case bson.Datetime:
			i64 := int64(bson.Pack.Uint64(buf.Next(8)))
			// micro->nano->UTC
			query.BindVariables[key] = time.Unix(0, i64*1e6).UTC()
		case bson.Null:
			query.BindVariables[key] = nil
		default:
			panic(bson.NewBsonError("don't know how to handle kind %v yet", kind))
		}
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

func (session *Session) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Long, "TransactionId")
	bson.EncodeUint64(buf, uint64(session.TransactionId))

	bson.EncodePrefix(buf, bson.Long, "ConnectionId")
	bson.EncodeUint64(buf, uint64(session.ConnectionId))

	bson.EncodePrefix(buf, bson.Long, "SessionId")
	bson.EncodeUint64(buf, uint64(session.SessionId))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (session *Session) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "TransactionId":
			session.TransactionId = bson.DecodeInt64(buf, kind)
		case "ConnectionId":
			session.ConnectionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			session.SessionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}
