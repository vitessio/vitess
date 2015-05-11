// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	"fmt"

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

	// Helper for RPC panic handling: call this in a defer statement
	// at the beginning of each RPC handling method.
	HandlePanic(*error)
}

// ErrorQueryService is an implementation of QueryService that returns a
// configurable error for some of its methods.
type ErrorQueryService struct {
	GetSessionIdError error
}

// GetSessionId is part of QueryService interface
func (e *ErrorQueryService) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return e.GetSessionIdError
}

// Begin is part of QueryService interface
func (e *ErrorQueryService) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Commit is part of QueryService interface
func (e *ErrorQueryService) Commit(ctx context.Context, session *proto.Session) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Rollback is part of QueryService interface
func (e *ErrorQueryService) Rollback(ctx context.Context, session *proto.Session) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Execute is part of QueryService interface
func (e *ErrorQueryService) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamExecute is part of QueryService interface
func (e *ErrorQueryService) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// ExecuteBatch is part of QueryService interface
func (e *ErrorQueryService) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// SplitQuery is part of QueryService interface
func (e *ErrorQueryService) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// HandlePanic is part of QueryService interface
func (e *ErrorQueryService) HandlePanic(*error) {
}
