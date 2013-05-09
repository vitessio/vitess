// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/rpcwrap"
	rpcproto "code.google.com/p/vitess/go/rpcwrap/proto"
)

// defines the RPC services
// the service name to use is 'SqlQuery'
type SqlQuery interface {
	GetSessionId(sessionParams *SessionParams, sessionInfo *SessionInfo) error

	// FIXME(sugu) Note the client will support both returning an
	// int64 or a structure. Using the structure will be rolled
	// out after the client is rolled out.
	Begin(context *rpcproto.Context, session *Session, txInfo *TransactionInfo) error
	Commit(context *rpcproto.Context, session *Session, noOutput *string) error
	Rollback(context *rpcproto.Context, session *Session, noOutput *string) error

	CreateReserved(session *Session, connectionInfo *ConnectionInfo) error
	CloseReserved(session *Session, noOutput *string) error

	Execute(context *rpcproto.Context, query *Query, reply *mproto.QueryResult) error
	StreamExecute(context *rpcproto.Context, query *Query, sendReply func(reply interface{}) error) error
	ExecuteBatch(context *rpcproto.Context, queryList *QueryList, reply *QueryResultList) error
}

// helper method to register the server (does interface checking)
func RegisterAuthenticated(sqlQuery SqlQuery) {
	rpcwrap.RegisterAuthenticated(sqlQuery)
}
