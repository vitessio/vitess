// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package queryservice contains the interface for the service definition
// of the Query Service.
package queryservice

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

// QueryService is the interface implemented by the tablet's query service.
type QueryService interface {
	// GetSessionId establishes a session to guarantee the current
	// query service state doesn't change.
	// This is begin deprecated, replaced by the Target structure.
	GetSessionId(keyspace, shard string) (int64, error)

	// Transaction management

	// Begin returns the transaction id to use for further operations
	Begin(ctx context.Context, target *querypb.Target, sessionID int64) (int64, error)

	// Commit commits the current transaction
	Commit(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error

	// Rollback aborts the current transaction
	Rollback(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error

	// Query execution

	Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID, transactionID int64) (*sqltypes.Result, error)
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID int64, sendReply func(*sqltypes.Result) error) error
	ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, sessionID int64, asTransaction bool, transactionID int64) ([]sqltypes.Result, error)

	// SplitQuery is a map reduce helper function
	SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64, sessionID int64) ([]querytypes.QuerySplit, error)

	// StreamHealthRegister registers a listener for StreamHealth
	StreamHealthRegister(chan<- *querypb.StreamHealthResponse) (int, error)

	// StreamHealthUnregister unregisters a listener for StreamHealth
	StreamHealthUnregister(int) error

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
func (e *ErrorQueryService) GetSessionId(keyspace, shard string) (int64, error) {
	return 0, e.GetSessionIdError
}

// Begin is part of QueryService interface
func (e *ErrorQueryService) Begin(ctx context.Context, target *querypb.Target, sessionID int64) (int64, error) {
	return 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// Commit is part of QueryService interface
func (e *ErrorQueryService) Commit(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Rollback is part of QueryService interface
func (e *ErrorQueryService) Rollback(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Execute is part of QueryService interface
func (e *ErrorQueryService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID, transactionID int64) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamExecute is part of QueryService interface
func (e *ErrorQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID int64, sendReply func(*sqltypes.Result) error) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// ExecuteBatch is part of QueryService interface
func (e *ErrorQueryService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, sessionID int64, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// SplitQuery is part of QueryService interface
func (e *ErrorQueryService) SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64, sessionID int64) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamHealthRegister is part of QueryService interface
func (e *ErrorQueryService) StreamHealthRegister(chan<- *querypb.StreamHealthResponse) (int, error) {
	return 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamHealthUnregister is part of QueryService interface
func (e *ErrorQueryService) StreamHealthUnregister(int) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// HandlePanic is part of QueryService interface
func (e *ErrorQueryService) HandlePanic(*error) {
}

// make sure ErrorQueryService implements QueryService
var _ QueryService = &ErrorQueryService{}
