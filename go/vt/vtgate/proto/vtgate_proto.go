// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"fmt"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// Session represents the session state. It keeps track of
// the shards on which transactions are in progress, along
// with the corresponding tranaction ids.
type Session struct {
	InTransaction bool
	ShardSessions []*ShardSession
}

// ShardSession represents the session state for a shard.
type ShardSession struct {
	Keyspace      string
	Shard         string
	TabletType    topo.TabletType
	TransactionId int64
}

// MarshalBson marshals Session into buf.
func (session *Session) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeBool(buf, "InTransaction", session.InTransaction)
	encodeShardSessionsBson(session.ShardSessions, "ShardSessions", buf)

	lenWriter.Close()
}

func (session *Session) String() string {
	return fmt.Sprintf("InTransaction: %v, ShardSession: %+v", session.InTransaction, session.ShardSessions)
}

func encodeShardSessionsBson(shardSessions []*ShardSession, key string, buf *bytes2.ChunkedWriter) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range shardSessions {
		v.MarshalBson(buf, bson.Itoa(i))
	}
	lenWriter.Close()
}

// MarshalBson marshals ShardSession into buf.
func (shardSession *ShardSession) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Keyspace", shardSession.Keyspace)
	bson.EncodeString(buf, "Shard", shardSession.Shard)
	bson.EncodeString(buf, "TabletType", string(shardSession.TabletType))
	bson.EncodeInt64(buf, "TransactionId", shardSession.TransactionId)

	lenWriter.Close()
}

// UnmarshalBson unmarshals Session from buf.
func (session *Session) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		keyName := bson.ReadCString(buf)
		switch keyName {
		case "InTransaction":
			session.InTransaction = bson.DecodeBool(buf, kind)
		case "ShardSessions":
			session.ShardSessions = decodeShardSessionsBson(buf, kind)
		default:
			bson.Skip(buf, kind)
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
	for kind != bson.EOO {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for ShardSession", kind))
		}
		bson.SkipIndex(buf)
		shardSession := new(ShardSession)
		shardSession.UnmarshalBson(buf, kind)
		shardSessions = append(shardSessions, shardSession)
		kind = bson.NextByte(buf)
	}
	return shardSessions
}

// UnmarshalBson unmarshals ShardSession from buf.
func (shardSession *ShardSession) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		keyName := bson.ReadCString(buf)
		switch keyName {
		case "Keyspace":
			shardSession.Keyspace = bson.DecodeString(buf, kind)
		case "Shard":
			shardSession.Shard = bson.DecodeString(buf, kind)
		case "TabletType":
			shardSession.TabletType = topo.TabletType(bson.DecodeString(buf, kind))
		case "TransactionId":
			shardSession.TransactionId = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

// QueryShard represents a query request for the
// specified list of shards.
type QueryShard struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	Shards        []string
	TabletType    topo.TabletType
	Session       *Session
}

// QueryResult is mproto.QueryResult+Session (for now).
type QueryResult struct {
	Fields       []mproto.Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
	Session      *Session
	Error        string
}

func PopulateQueryResult(in *mproto.QueryResult, out *QueryResult) {
	out.Fields = in.Fields
	out.RowsAffected = in.RowsAffected
	out.InsertId = in.InsertId
	out.Rows = in.Rows
}

// BatchQueryShard represents a batch query request
// for the specified shards.
type BatchQueryShard struct {
	Queries    []tproto.BoundQuery
	Keyspace   string
	Shards     []string
	TabletType topo.TabletType
	Session    *Session
}

// QueryResultList is mproto.QueryResultList+Session
type QueryResultList struct {
	List    []mproto.QueryResult
	Session *Session
	Error   string
}

type StreamQueryKeyRange struct {
	Sql           string
	BindVariables map[string]interface{}
	Keyspace      string
	KeyRange      string
	TabletType    topo.TabletType
	Session       *Session
}
