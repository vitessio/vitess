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
	mproto "github.com/youtube/vitess/go/mysql/proto"
)

func (query *Query) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", query.Sql)
	EncodeBindVariablesBson(buf, "BindVariables", query.BindVariables)
	bson.EncodeInt64(buf, "TransactionId", query.TransactionId)
	bson.EncodeInt64(buf, "SessionId", query.SessionId)

	lenWriter.Close()
}

func EncodeBindVariablesBson(buf *bytes2.ChunkedWriter, key string, bindVars map[string]interface{}) {
	bson.EncodePrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)
	for k, v := range bindVars {
		bson.EncodeField(buf, k, v)
	}
	lenWriter.Close()
}

func (query *Query) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			query.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			query.BindVariables = DecodeBindVariablesBson(buf, kind)
		case "TransactionId":
			query.TransactionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			query.SessionId = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
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

func (session *Session) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeInt64(buf, "TransactionId", session.TransactionId)
	bson.EncodeInt64(buf, "SessionId", session.SessionId)

	lenWriter.Close()
}

func (session *Session) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "TransactionId":
			session.TransactionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			session.SessionId = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func (bdq *BoundQuery) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", bdq.Sql)
	EncodeBindVariablesBson(buf, "BindVariables", bdq.BindVariables)

	lenWriter.Close()
}

func (bdq *BoundQuery) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			bdq.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			bdq.BindVariables = DecodeBindVariablesBson(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func EncodeQueriesBson(queries []BoundQuery, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range queries {
		v.MarshalBson(buf, bson.Itoa(i))
	}
	lenWriter.Close()
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
	for kind != bson.EOO {
		bson.SkipIndex(buf)
		bdq.UnmarshalBson(buf, kind)
		queries = append(queries, bdq)
		kind = bson.NextByte(buf)
	}
	return queries
}

func (ql *QueryList) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	EncodeQueriesBson(ql.Queries, "Queries", buf)
	bson.EncodeInt64(buf, "TransactionId", ql.TransactionId)
	bson.EncodeInt64(buf, "SessionId", ql.SessionId)

	lenWriter.Close()
}

func (ql *QueryList) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Queries":
			ql.Queries = DecodeQueriesBson(buf, kind)
		case "TransactionId":
			ql.TransactionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			ql.SessionId = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func (qrl *QueryResultList) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)
	EncodeResultsBson(qrl.List, "List", buf)
	lenWriter.Close()
}

func EncodeResultsBson(results []mproto.QueryResult, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range results {
		v.MarshalBson(buf, bson.Itoa(i))
	}
	lenWriter.Close()
}

func (qrl *QueryResultList) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "List":
			qrl.List = DecodeResultsBson(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func DecodeResultsBson(buf *bytes.Buffer, kind byte) (results []mproto.QueryResult) {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for Queries", kind))
	}

	bson.Next(buf, 4)
	results = make([]mproto.QueryResult, 0, 8)
	kind = bson.NextByte(buf)
	var result mproto.QueryResult
	for kind != bson.EOO {
		bson.SkipIndex(buf)
		result.UnmarshalBson(buf, kind)
		results = append(results, result)
		kind = bson.NextByte(buf)
	}
	return results
}
