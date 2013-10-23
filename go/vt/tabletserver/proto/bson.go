// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

func (query *Query) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", query.Sql)
	EncodeBindVariablesBson(buf, "BindVariables", query.BindVariables)
	bson.EncodeInt64(buf, "TransactionId", query.TransactionId)
	bson.EncodeInt64(buf, "ConnectionId", query.ConnectionId)
	bson.EncodeInt64(buf, "SessionId", query.SessionId)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func EncodeBindVariablesBson(buf *bytes2.ChunkedWriter, key string, bindVars map[string]interface{}) {
	bson.EncodePrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)
	for k, v := range bindVars {
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
			query.BindVariables = DecodeBindVariablesBson(buf, kind)
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

func DecodeBindVariablesBson(buf *bytes.Buffer, kind byte) (bindVars map[string]interface{}) {
	switch kind {
	case bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Query.BindVariables", kind))
	}
	bson.Next(buf, 4)
	bindVars = make(map[string]interface{})
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		key := bson.ReadCString(buf)
		switch kind {
		case bson.Number:
			ui64 := bson.Pack.Uint64(buf.Next(8))
			bindVars[key] = math.Float64frombits(ui64)
		case bson.String:
			l := int(bson.Pack.Uint32(buf.Next(4)))
			bindVars[key] = buf.Next(l - 1)
			buf.ReadByte()
		case bson.Binary:
			l := int(bson.Pack.Uint32(buf.Next(4)))
			buf.ReadByte()
			bindVars[key] = buf.Next(l)
		case bson.Int:
			bindVars[key] = int32(bson.Pack.Uint32(buf.Next(4)))
		case bson.Long:
			bindVars[key] = int64(bson.Pack.Uint64(buf.Next(8)))
		case bson.Ulong:
			bindVars[key] = bson.Pack.Uint64(buf.Next(8))
		case bson.Datetime:
			i64 := int64(bson.Pack.Uint64(buf.Next(8)))
			// micro->nano->UTC
			bindVars[key] = time.Unix(0, i64*1e6).UTC()
		case bson.Null:
			bindVars[key] = nil
		default:
			panic(bson.NewBsonError("don't know how to handle kind %v yet", kind))
		}
	}
	return
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

	bson.EncodeInt64(buf, "TransactionId", session.TransactionId)
	bson.EncodeInt64(buf, "ConnectionId", session.ConnectionId)
	bson.EncodeInt64(buf, "SessionId", session.SessionId)

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

func (bdq *BoundQuery) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", bdq.Sql)
	EncodeBindVariablesBson(buf, "BindVariables", bdq.BindVariables)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (bdq *BoundQuery) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			bdq.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			bdq.BindVariables = DecodeBindVariablesBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func EncodeQueriesBson(queries []BoundQuery, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range queries {
		bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
		v.MarshalBson(buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func DecodeQueriesBson(buf *bytes.Buffer, kind byte) (queries []BoundQuery) {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Queries", kind))
	}

	bson.Next(buf, 4)
	queries = make([]BoundQuery, 0, 8)
	kind = bson.NextByte(buf)
	var bdq BoundQuery
	for i := 0; kind != bson.EOO; i++ {
		bson.ExpectIndex(buf, i)
		bdq.UnmarshalBson(buf)
		queries = append(queries, bdq)
		kind = bson.NextByte(buf)
	}
	return queries
}
