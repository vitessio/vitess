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
	"github.com/youtube/vitess/go/vt/topo"
)

// VTGate defines the interface for the rpc service.
type VTGate interface {
	ExecuteShard(context *rpcproto.Context, query *QueryShard, reply *mproto.QueryResult) error
	ExecuteBatchShard(context *rpcproto.Context, batchQuery *BatchQueryShard, reply *tproto.QueryResultList) error
	StreamExecuteShard(context *rpcproto.Context, query *QueryShard, sendReply func(interface{}) error) error
	Begin(context *rpcproto.Context, inSession, outSession *Session) error
	Commit(context *rpcproto.Context, inSession, outSession *Session) error
	Rollback(context *rpcproto.Context, inSession, outSession *Session) error
}

type Session struct {
	InTransaction bool
	ShardSessions []*ShardSession
}

type ShardSession struct {
	Keyspace      string
	Shard         string
	TabletType    topo.TabletType
	TransactionId int64
}

func (session *Session) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeBool(buf, "InTransaction", session.InTransaction)
	encodeShardSessionsBson(session.ShardSessions, "ShardSessions", buf)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeShardSessionsBson(shardSessions []*ShardSession, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range shardSessions {
		bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
		v.MarshalBson(buf)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (shardSession *ShardSession) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Keyspace", shardSession.Keyspace)
	bson.EncodeString(buf, "Shard", shardSession.Shard)
	bson.EncodeString(buf, "TabletType", string(shardSession.TabletType))
	bson.EncodeInt64(buf, "TransactionId", shardSession.TransactionId)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (session *Session) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "InTransaction":
			session.InTransaction = bson.DecodeBool(buf, kind)
		case "ShardSessions":
			session.ShardSessions = decodeShardSessionsBson(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func decodeShardSessionsBson(buf *bytes.Buffer, kind byte) []*ShardSession {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for ShardSessions", kind))
	}

	bson.Next(buf, 4)
	shardSessions := make([]*ShardSession, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for ShardSession", kind))
		}
		bson.ExpectIndex(buf, i)
		shardSession := new(ShardSession)
		shardSession.UnmarshalBson(buf)
		shardSessions = append(shardSessions, shardSession)
		kind = bson.NextByte(buf)
	}
	return shardSessions
}

func (shardSession *ShardSession) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Keyspace":
			shardSession.Keyspace = bson.DecodeString(buf, kind)
		case "Shard":
			shardSession.Shard = bson.DecodeString(buf, kind)
		case "TabletType":
			shardSession.TabletType = topo.TabletType(bson.DecodeString(buf, kind))
		case "TransactionId":
			shardSession.TransactionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type QueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	Shards        []string
	Sessn         *Session
}

func (qrs *QueryShard) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Sql", qrs.Sql)
	tproto.EncodeBindVariablesBson(buf, "BindVariables", qrs.BindVariables)
	bson.EncodeString(buf, "Keyspace", qrs.Keyspace)
	bson.EncodeStringArray(buf, "Shards", qrs.Shards)

	if qrs.Sessn != nil {
		bson.EncodePrefix(buf, bson.Object, "Sessn")
		qrs.Sessn.MarshalBson(buf)
	}

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
			qrs.BindVariables = tproto.DecodeBindVariablesBson(buf, kind)
		case "Keyspace":
			qrs.Keyspace = bson.DecodeString(buf, kind)
		case "Shards":
			qrs.Shards = bson.DecodeStringArray(buf, kind)
		case "Sessn":
			qrs.Sessn = new(Session)
			qrs.Sessn.UnmarshalBson(buf)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type QueryResult struct {
	mproto.QueryResult
	Sessn *Session
}

func (qr *QueryResult) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	mproto.EncodeFieldsBson(qr.Fields, "Fields", buf)
	bson.EncodeInt64(buf, "RowsAffected", int64(qr.RowsAffected))
	bson.EncodeInt64(buf, "InsertId", int64(qr.InsertId))
	mproto.EncodeRowsBson(qr.Rows, "Rows", buf)

	if qr.Sessn != nil {
		bson.EncodePrefix(buf, bson.Object, "Sessn")
		qr.Sessn.MarshalBson(buf)
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
			qr.Fields = mproto.DecodeFieldsBson(buf, kind)
		case "RowsAffected":
			qr.RowsAffected = bson.DecodeUint64(buf, kind)
		case "InsertId":
			qr.InsertId = bson.DecodeUint64(buf, kind)
		case "Rows":
			qr.Rows = mproto.DecodeRowsBson(buf, kind)
		case "Sessn":
			qr.Sessn = new(Session)
			qr.Sessn.UnmarshalBson(buf)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type BatchQueryShard struct {
	Queries  []tproto.BoundQuery
	Keyspace string
	Shards   []string
	Sessn    *Session
}

func (bqs *BatchQueryShard) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	tproto.EncodeQueriesBson(bqs.Queries, "Queries", buf)
	bson.EncodeString(buf, "Keyspace", bqs.Keyspace)
	bson.EncodeStringArray(buf, "Shards", bqs.Shards)

	if bqs.Sessn != nil {
		bson.EncodePrefix(buf, bson.Object, "Sessn")
		bqs.Sessn.MarshalBson(buf)
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
			bqs.Queries = tproto.DecodeQueriesBson(buf, kind)
		case "Keyspace":
			bqs.Keyspace = bson.DecodeString(buf, kind)
		case "Shards":
			bqs.Shards = bson.DecodeStringArray(buf, kind)
		case "Sessn":
			bqs.Sessn = new(Session)
			bqs.Sessn.UnmarshalBson(buf)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type QueryResultList struct {
	tproto.QueryResultList
	Sessn *Session
}

func (qrl *QueryResultList) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	tproto.EncodeResultsBson(qrl.List, "List", buf)

	if qrl.Sessn != nil {
		bson.EncodePrefix(buf, bson.Object, "Sessn")
		qrl.Sessn.MarshalBson(buf)
	}

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (qrl *QueryResultList) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "List":
			qrl.List = tproto.DecodeResultsBson(buf, kind)
		case "Sessn":
			qrl.Sessn = new(Session)
			qrl.Sessn.UnmarshalBson(buf)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

// RegisterAuthenticated registers the server.
func RegisterAuthenticated(vtgate VTGate) {
	rpcwrap.RegisterAuthenticated(vtgate)
}
