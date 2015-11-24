// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"errors"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// QueryClient provides a convenient wrapper for TabletServer's query service.
// It's not thread safe, but you can create multiple clients that point to the
// same server.
type QueryClient struct {
	ctx           context.Context
	target        querypb.Target
	server        *tabletserver.TabletServer
	transactionID int64
}

// NewClient creates a new client for Server.
func NewClient() *QueryClient {
	return &QueryClient{
		ctx: callerid.NewContext(
			context.Background(),
			&vtrpcpb.CallerID{},
			&querypb.VTGateCallerID{Username: "dev"},
		),
		target: Target,
		server: Server,
	}
}

// Begin begins a transaction.
func (client *QueryClient) Begin() error {
	if client.transactionID != 0 {
		return errors.New("already in transaction")
	}
	var txinfo proto.TransactionInfo
	err := client.server.Begin(client.ctx, &client.target, &proto.Session{}, &txinfo)
	if err != nil {
		return err
	}
	client.transactionID = txinfo.TransactionId
	return nil
}

// Commit commits the current transaction.
func (client *QueryClient) Commit() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Commit(client.ctx, &client.target, &proto.Session{TransactionId: client.transactionID})
}

// Rollback rolls back the current transaction.
func (client *QueryClient) Rollback() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Rollback(client.ctx, &client.target, &proto.Session{TransactionId: client.transactionID})
}

// Execute executes a query.
func (client *QueryClient) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	var qr = &sqltypes.Result{}
	err := client.server.Execute(
		client.ctx,
		&client.target,
		&proto.Query{
			Sql:           query,
			BindVariables: bindvars,
			TransactionId: client.transactionID,
		},
		qr,
	)
	return qr, err
}

// StreamExecute executes a query & streams the results.
func (client *QueryClient) StreamExecute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	err := client.server.StreamExecute(
		client.ctx,
		&client.target,
		&proto.Query{
			Sql:           query,
			BindVariables: bindvars,
			TransactionId: client.transactionID,
		},
		func(res *sqltypes.Result) error {
			if result.Fields == nil {
				result.Fields = res.Fields
			}
			result.Rows = append(result.Rows, res.Rows...)
			result.RowsAffected += uint64(len(res.Rows))
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Stream streams the resutls of a query.
func (client *QueryClient) Stream(query string, bindvars map[string]interface{}, sendFunc func(*sqltypes.Result) error) error {
	return client.server.StreamExecute(
		client.ctx,
		&client.target,
		&proto.Query{
			Sql:           query,
			BindVariables: bindvars,
			TransactionId: client.transactionID,
		},
		sendFunc,
	)
}

// ExecuteBatch executes a batch of queries.
func (client *QueryClient) ExecuteBatch(queries []proto.BoundQuery, asTransaction bool) (*proto.QueryResultList, error) {
	var qr = &proto.QueryResultList{}
	err := client.server.ExecuteBatch(
		client.ctx,
		&client.target,
		&proto.QueryList{
			Queries:       queries,
			AsTransaction: asTransaction,
			TransactionId: client.transactionID,
		},
		qr,
	)
	return qr, err
}
