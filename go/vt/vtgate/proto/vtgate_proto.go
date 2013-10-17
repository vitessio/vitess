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
	vproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// VTGate defines the interface for the rpc service.
type VTGate interface {
	GetSessionId(sessionParams *SessionParams, session *Session) error
	ExecuteShard(context *rpcproto.Context, query *QueryShard, reply *mproto.QueryResult) error
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

	bson.EncodePrefix(buf, bson.Binary, "TabletType")
	bson.EncodeString(buf, string(spm.TabletType))

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

	bson.EncodePrefix(buf, bson.Binary, "Sql")
	bson.EncodeString(buf, qrs.Sql)

	bson.EncodePrefix(buf, bson.Object, "BindVariables")
	vproto.EncodeBindVariablesBson(buf, qrs.BindVariables)

	bson.EncodePrefix(buf, bson.Long, "SessionId")
	bson.EncodeUint64(buf, uint64(qrs.SessionId))

	bson.EncodePrefix(buf, bson.Binary, "Keyspace")
	bson.EncodeString(buf, qrs.Keyspace)

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

// RegisterAuthenticated registers the server.
func RegisterAuthenticated(vtgate VTGate) {
	rpcwrap.RegisterAuthenticated(vtgate)
}
