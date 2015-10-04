// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"errors"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

// QueryClient provides a convenient wrapper for TabletServer's query service.
// It's not thread safe, but you can create multiple clients that point to the
// same server.
type QueryClient struct {
	target        query.Target
	server        *tabletserver.TabletServer
	transactionID int64
}

// NewDefaultClient creates a new client for the default server.
func NewDefaultClient() *QueryClient {
	return &QueryClient{
		target: Target,
		server: DefaultServer,
	}
}

// NewClient creates a new client for the specified server.
func NewClient(server *tabletserver.TabletServer) *QueryClient {
	return &QueryClient{
		target: Target,
		server: server,
	}
}

// Begin begins a transaction.
func (client *QueryClient) Begin() error {
	if client.transactionID != 0 {
		return errors.New("already in transaction")
	}
	var txinfo proto.TransactionInfo
	err := client.server.Begin(context.Background(), &client.target, &proto.Session{}, &txinfo)
	if err != nil {
		return err
	}
	client.transactionID = txinfo.TransactionId
	return nil
}

// Commit commits the current transaction.
func (client *QueryClient) Commit() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Commit(context.Background(), &client.target, &proto.Session{TransactionId: client.transactionID})
}

// Rollback rolls back the current transaction.
func (client *QueryClient) Rollback() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Rollback(context.Background(), &client.target, &proto.Session{TransactionId: client.transactionID})
}

// Execute executes a query.
func (client *QueryClient) Execute(query string, bindvars map[string]interface{}) (*mproto.QueryResult, error) {
	var qr = &mproto.QueryResult{}
	err := client.server.Execute(
		context.Background(),
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

// ExecuteBatch executes a batch of queries.
func (client *QueryClient) ExecuteBatch(queries []proto.BoundQuery, asTransaction bool) (*proto.QueryResultList, error) {
	var qr = &proto.QueryResultList{}
	err := client.server.ExecuteBatch(
		context.Background(),
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
