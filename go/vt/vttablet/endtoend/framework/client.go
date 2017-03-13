// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"errors"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	transactionID, err := client.server.Begin(client.ctx, &client.target)
	if err != nil {
		return err
	}
	client.transactionID = transactionID
	return nil
}

// Commit commits the current transaction.
func (client *QueryClient) Commit() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Commit(client.ctx, &client.target, client.transactionID)
}

// Rollback rolls back the current transaction.
func (client *QueryClient) Rollback() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Rollback(client.ctx, &client.target, client.transactionID)
}

// Prepare executes a prepare on the current transaction.
func (client *QueryClient) Prepare(dtid string) error {
	defer func() { client.transactionID = 0 }()
	return client.server.Prepare(client.ctx, &client.target, client.transactionID, dtid)
}

// CommitPrepared commits a prepared transaction.
func (client *QueryClient) CommitPrepared(dtid string) error {
	return client.server.CommitPrepared(client.ctx, &client.target, dtid)
}

// RollbackPrepared rollsback a prepared transaction.
func (client *QueryClient) RollbackPrepared(dtid string, originalID int64) error {
	return client.server.RollbackPrepared(client.ctx, &client.target, dtid, originalID)
}

// CreateTransaction issues a CreateTransaction to TabletServer.
func (client *QueryClient) CreateTransaction(dtid string, participants []*querypb.Target) error {
	return client.server.CreateTransaction(client.ctx, &client.target, dtid, participants)
}

// StartCommit issues a StartCommit to TabletServer for the current transaction.
func (client *QueryClient) StartCommit(dtid string) error {
	defer func() { client.transactionID = 0 }()
	return client.server.StartCommit(client.ctx, &client.target, client.transactionID, dtid)
}

// SetRollback issues a SetRollback to TabletServer.
func (client *QueryClient) SetRollback(dtid string, transactionID int64) error {
	return client.server.SetRollback(client.ctx, &client.target, dtid, client.transactionID)
}

// ConcludeTransaction issues a ConcludeTransaction to TabletServer.
func (client *QueryClient) ConcludeTransaction(dtid string) error {
	return client.server.ConcludeTransaction(client.ctx, &client.target, dtid)
}

// ReadTransaction returns the transaction metadata.
func (client *QueryClient) ReadTransaction(dtid string) (*querypb.TransactionMetadata, error) {
	return client.server.ReadTransaction(client.ctx, &client.target, dtid)
}

// SetServingType is for testing transitions.
// It currently supports only master->replica and back.
func (client *QueryClient) SetServingType(tabletType topodatapb.TabletType) error {
	_, err := client.server.SetServingType(tabletType, true, nil)
	return err
}

// Execute executes a query.
func (client *QueryClient) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	return client.ExecuteWithOptions(query, bindvars, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
}

// ExecuteWithOptions executes a query using 'options'.
func (client *QueryClient) ExecuteWithOptions(query string, bindvars map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return client.server.Execute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		client.transactionID,
		options,
	)
}

// StreamExecute executes a query & returns the results.
func (client *QueryClient) StreamExecute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	return client.StreamExecuteWithOptions(query, bindvars, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
}

// StreamExecuteWithOptions executes a query & returns the results using 'options'.
func (client *QueryClient) StreamExecuteWithOptions(query string, bindvars map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	err := client.server.StreamExecute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		options,
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

// Stream streams the results of a query.
func (client *QueryClient) Stream(query string, bindvars map[string]interface{}, sendFunc func(*sqltypes.Result) error) error {
	return client.server.StreamExecute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
		sendFunc,
	)
}

// ExecuteBatch executes a batch of queries.
func (client *QueryClient) ExecuteBatch(queries []querytypes.BoundQuery, asTransaction bool) ([]sqltypes.Result, error) {
	return client.server.ExecuteBatch(
		client.ctx,
		&client.target,
		queries,
		asTransaction,
		client.transactionID,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
	)
}

// MessageStream streams messages from the message table.
func (client *QueryClient) MessageStream(name string, callback func(*sqltypes.Result) error) (err error) {
	return client.server.MessageStream(client.ctx, &client.target, name, callback)
}

// MessageAck acks messages
func (client *QueryClient) MessageAck(name string, ids []string) (int64, error) {
	bids := make([]*querypb.Value, 0, len(ids))
	for _, id := range ids {
		bids = append(bids, &querypb.Value{
			Type:  sqltypes.VarChar,
			Value: []byte(id),
		})
	}
	return client.server.MessageAck(client.ctx, &client.target, name, bids)
}
