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
	GetSessionId(sessionParams *SessionParams, sessionInfo *SessionInfo) error
	Execute(context *rpcproto.Context, query *Query, reply *mproto.QueryResult) error
}

type SessionParams struct {
	TabletType topo.TabletType
}

type SessionInfo struct {
	SessionId int64
}

type Query struct {
	Sql           string
	BindVariables map[string]interface{}
	TransactionId int64
	SessionId     int64
	Keyspace      string
	Shards        []string
}

type Session struct {
	TransactionId int64
	SessionId     int64
}

type TransactionInfo struct {
	TransactionId int64
}

// RegisterAuthenticated registers the server.
func RegisterAuthenticated(barnacle Barnacle) {
	rpcwrap.RegisterAuthenticated(barnacle)
}
