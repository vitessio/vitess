// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// Barcacle defines the interface for tbe rpc service.
type Barnacle interface {
	GetSessionId(sessionParams *SessionParams, session *Session) error
	Execute(context *rpcproto.Context, query *Query, reply *mproto.QueryResult) error
	StreamExecute(context *rpcproto.Context, query *Query, sendReply func(interface{}) error) error
	Begin(context *rpcproto.Context, session *Session, noOutput *string) error
	Commit(context *rpcproto.Context, session *Session, noOutput *string) error
	Rollback(context *rpcproto.Context, session *Session, noOutput *string) error
	CloseSession(context *rpcproto.Context, session *Session, noOutput *string) error
}

type SessionParams struct {
	TabletType topo.TabletType
}

type Session struct {
	SessionId int64
}

type Query struct {
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	Keyspace      string
	Shards        []string
}

// RegisterAuthenticated registers the server.
func RegisterAuthenticated(barnacle Barnacle) {
	rpcwrap.RegisterAuthenticated(barnacle)
}
