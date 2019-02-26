/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"errors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// NewClientWithContext creates a new client for Server with the provided context.
func NewClientWithContext(ctx context.Context) *QueryClient {
	return &QueryClient{
		ctx:    ctx,
		target: Target,
		server: Server,
	}
}

// Begin begins a transaction.
func (client *QueryClient) Begin(clientFoundRows bool) error {
	if client.transactionID != 0 {
		return errors.New("already in transaction")
	}
	var options *querypb.ExecuteOptions
	if clientFoundRows {
		options = &querypb.ExecuteOptions{ClientFoundRows: clientFoundRows}
	}
	transactionID, err := client.server.Begin(client.ctx, &client.target, options)
	if err != nil {
		return err
	}
	client.transactionID = transactionID
	return nil
}

// BeginWithOptions is like Begin, but you can pass in your custom ExecuteOptions.
func (client *QueryClient) BeginWithOptions(options *querypb.ExecuteOptions) error {
	if client.transactionID != 0 {
		return errors.New("already in transaction")
	}
	transactionID, err := client.server.Begin(client.ctx, &client.target, options)
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
func (client *QueryClient) Execute(query string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return client.ExecuteWithOptions(query, bindvars, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
}

// BeginExecute performs a BeginExecute.
func (client *QueryClient) BeginExecute(query string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if client.transactionID != 0 {
		return nil, errors.New("already in transaction")
	}
	qr, transactionID, err := client.server.BeginExecute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
	)
	if err != nil {
		return nil, err
	}
	client.transactionID = transactionID
	return qr, nil
}

// ExecuteWithOptions executes a query using 'options'.
func (client *QueryClient) ExecuteWithOptions(query string, bindvars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
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
func (client *QueryClient) StreamExecute(query string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return client.StreamExecuteWithOptions(query, bindvars, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
}

// StreamExecuteWithOptions executes a query & returns the results using 'options'.
func (client *QueryClient) StreamExecuteWithOptions(query string, bindvars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	err := client.server.StreamExecute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		0,
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
func (client *QueryClient) Stream(query string, bindvars map[string]*querypb.BindVariable, sendFunc func(*sqltypes.Result) error) error {
	return client.server.StreamExecute(
		client.ctx,
		&client.target,
		query,
		bindvars,
		0,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
		sendFunc,
	)
}

// ExecuteBatch executes a batch of queries.
func (client *QueryClient) ExecuteBatch(queries []*querypb.BoundQuery, asTransaction bool) ([]sqltypes.Result, error) {
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
