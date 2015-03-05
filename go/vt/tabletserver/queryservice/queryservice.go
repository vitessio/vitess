// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

// QueryService is the interface implemented by the tablet's query service.
type QueryService interface {
	// establish a session to survive restart
	GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error

	// Transaction management
	Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) error
	Commit(ctx context.Context, session *proto.Session) error
	Rollback(ctx context.Context, session *proto.Session) error

	// Query execution
	Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) error
	StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) error
	ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error

	// Map reduce helper
	SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error
}
