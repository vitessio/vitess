// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	vproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// VTGate defines the interface for the rpc service.
type VTGate interface {
	GetSessionId(sessionParams *SessionParams, session *Session) error
	ExecuteShard(context *rpcproto.Context, query *QueryShard, reply *mproto.QueryResult) error
	ExecuteBatchShard(context *rpcproto.Context, batchQuery *BatchQueryShard, reply *tproto.QueryResultList) error
	StreamExecuteShard(context *rpcproto.Context, query *QueryShard, sendReply func(interface{}) error) error
	Begin(context *rpcproto.Context, session *Session, noOutput *string) error
	Commit(context *rpcproto.Context, session *Session, noOutput *string) error
	Rollback(context *rpcproto.Context, session *Session, noOutput *string) error
	CloseSession(context *rpcproto.Context, session *Session, noOutput *string) error
}

type SessionParams struct {
	TabletType topo.TabletType
}

func (spm *SessionParams) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "TabletType", string(spm.TabletType))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (spm *SessionParams) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "TabletType":
			spm.TabletType = topo.TabletType(bson.DecodeString(buf, kind))
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type Session struct {
	SessionId int64
}

func (session *Session) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

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
		case "SessionId":
			session.SessionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type QueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	Keyspace      string
	Shards        []string
}

func (qrs *QueryShard) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", qrs.Sql)
	vproto.EncodeBindVariablesBson(buf, "BindVariables", qrs.BindVariables)
	bson.EncodeInt64(buf, "SessionId", qrs.SessionId)
	bson.EncodeString(buf, "Keyspace", qrs.Keyspace)
	bson.EncodeStringArray(buf, "Shards", qrs.Shards)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (qrs *QueryShard) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Sql":
			qrs.Sql = bson.DecodeString(buf, kind)
		case "BindVariables":
			qrs.BindVariables = vproto.DecodeBindVariablesBson(buf, kind)
		case "SessionId":
			qrs.SessionId = bson.DecodeInt64(buf, kind)
		case "Keyspace":
			qrs.Keyspace = bson.DecodeString(buf, kind)
		case "Shards":
			qrs.Shards = bson.DecodeStringArray(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type BoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

func (bdq *BoundQuery) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", bdq.Sql)
	vproto.EncodeBindVariablesBson(buf, "BindVariables", bdq.BindVariables)

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
			bdq.BindVariables = vproto.DecodeBindVariablesBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type BatchQueryShard struct {
	Queries   []BoundQuery
	SessionId int64
	Keyspace  string
	Shards    []string
}

func (bqs *BatchQueryShard) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	encodeQueriesBson(bqs.Queries, "Queries", buf)
	bson.EncodeInt64(buf, "SessionId", bqs.SessionId)
	bson.EncodeString(buf, "Keyspace", bqs.Keyspace)
	bson.EncodeStringArray(buf, "Shards", bqs.Shards)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeQueriesBson(queries []BoundQuery, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range queries {
		bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
		v.MarshalBson(buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (bqs *BatchQueryShard) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Queries":
			bqs.Queries = decodeQueriesBson(buf, kind)
		case "SessionId":
			bqs.SessionId = bson.DecodeInt64(buf, kind)
		case "Keyspace":
			bqs.Keyspace = bson.DecodeString(buf, kind)
		case "Shards":
			bqs.Shards = bson.DecodeStringArray(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func decodeQueriesBson(buf *bytes.Buffer, kind byte) (queries []BoundQuery) {
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

// RegisterAuthenticated registers the server.
func RegisterAuthenticated(vtgate VTGate) {
	rpcwrap.RegisterAuthenticated(vtgate)
}
