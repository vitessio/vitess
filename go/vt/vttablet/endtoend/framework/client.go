/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

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
	ctx                 context.Context
	target              *querypb.Target
	server              *tabletserver.TabletServer
	transactionID       int64
	reservedID          int64
	sessionStateChanges string
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

// NewClientWithTabletType creates a new client for Server with the provided tablet type.
func NewClientWithTabletType(tabletType topodatapb.TabletType) *QueryClient {
	targetCopy := proto.Clone(Target).(*querypb.Target)
	targetCopy.TabletType = tabletType
	return &QueryClient{
		ctx: callerid.NewContext(
			context.Background(),
			&vtrpcpb.CallerID{},
			&querypb.VTGateCallerID{Username: "dev"},
		),
		target: targetCopy,
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
	state, err := client.server.Begin(client.ctx, client.target, options)
	if err != nil {
		return err
	}
	client.transactionID = state.TransactionID
	client.sessionStateChanges = state.SessionStateChanges
	return nil
}

// Commit commits the current transaction.
func (client *QueryClient) Commit() error {
	defer func() { client.transactionID = 0 }()
	rID, err := client.server.Commit(client.ctx, client.target, client.transactionID)
	client.reservedID = rID
	if err != nil {
		return err
	}
	return nil
}

// Rollback rolls back the current transaction.
func (client *QueryClient) Rollback() error {
	defer func() { client.transactionID = 0 }()
	rID, err := client.server.Rollback(client.ctx, client.target, client.transactionID)
	client.reservedID = rID
	if err != nil {
		return err
	}
	return nil
}

// Prepare executes a prepare on the current transaction.
func (client *QueryClient) Prepare(dtid string) error {
	defer func() { client.transactionID = 0 }()
	return client.server.Prepare(client.ctx, client.target, client.transactionID, dtid)
}

// CommitPrepared commits a prepared transaction.
func (client *QueryClient) CommitPrepared(dtid string) error {
	return client.server.CommitPrepared(client.ctx, client.target, dtid)
}

// RollbackPrepared rollsback a prepared transaction.
func (client *QueryClient) RollbackPrepared(dtid string, originalID int64) error {
	return client.server.RollbackPrepared(client.ctx, client.target, dtid, originalID)
}

// CreateTransaction issues a CreateTransaction to TabletServer.
func (client *QueryClient) CreateTransaction(dtid string, participants []*querypb.Target) error {
	return client.server.CreateTransaction(client.ctx, client.target, dtid, participants)
}

// StartCommit issues a StartCommit to TabletServer for the current transaction.
func (client *QueryClient) StartCommit(dtid string) error {
	defer func() { client.transactionID = 0 }()
	return client.server.StartCommit(client.ctx, client.target, client.transactionID, dtid)
}

// SetRollback issues a SetRollback to TabletServer.
func (client *QueryClient) SetRollback(dtid string, transactionID int64) error {
	return client.server.SetRollback(client.ctx, client.target, dtid, client.transactionID)
}

// ConcludeTransaction issues a ConcludeTransaction to TabletServer.
func (client *QueryClient) ConcludeTransaction(dtid string) error {
	return client.server.ConcludeTransaction(client.ctx, client.target, dtid)
}

// ReadTransaction returns the transaction metadata.
func (client *QueryClient) ReadTransaction(dtid string) (*querypb.TransactionMetadata, error) {
	return client.server.ReadTransaction(client.ctx, client.target, dtid)
}

// SetServingType is for testing transitions.
// It currently supports only primary->replica and back.
func (client *QueryClient) SetServingType(tabletType topodatapb.TabletType) error {
	err := client.server.SetServingType(tabletType, time.Time{}, true /* serving */, "" /* reason */)
	// Wait for TwoPC transition, if necessary
	client.server.TwoPCEngineWait()
	return err
}

// Execute executes a query.
func (client *QueryClient) Execute(query string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return client.ExecuteWithOptions(query, bindvars, &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	})
}

// BeginExecute performs a BeginExecute.
func (client *QueryClient) BeginExecute(query string, bindvars map[string]*querypb.BindVariable, preQueries []string) (*sqltypes.Result, error) {
	if client.transactionID != 0 {
		return nil, errors.New("already in transaction")
	}
	state, qr, err := client.server.BeginExecute(
		client.ctx,
		client.target,
		preQueries,
		query,
		bindvars,
		client.reservedID,
		&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
	)
	client.transactionID = state.TransactionID
	client.sessionStateChanges = state.SessionStateChanges
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// ExecuteWithOptions executes a query using 'options'.
func (client *QueryClient) ExecuteWithOptions(query string, bindvars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return client.server.Execute(
		client.ctx,
		client.target,
		query,
		bindvars,
		client.transactionID,
		client.reservedID,
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
	err := client.server.StreamExecute(client.ctx,
		client.target,
		query,
		bindvars,
		client.transactionID,
		client.reservedID,
		options,
		func(res *sqltypes.Result) error {
			if result.Fields == nil {
				result.Fields = res.Fields
			}
			result.Rows = append(result.Rows, res.Rows...)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// StreamBeginExecuteWithOptions starts a tx and executes a query using 'options', returning the results .
func (client *QueryClient) StreamBeginExecuteWithOptions(query string, preQueries []string, bindvars map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	state, err := client.server.BeginStreamExecute(
		client.ctx,
		client.target,
		preQueries,
		query,
		bindvars,
		client.reservedID,
		options,
		func(res *sqltypes.Result) error {
			if result.Fields == nil {
				result.Fields = res.Fields
			}
			result.Rows = append(result.Rows, res.Rows...)
			return nil
		},
	)
	client.transactionID = state.TransactionID
	client.sessionStateChanges = state.SessionStateChanges
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Stream streams the results of a query.
func (client *QueryClient) Stream(query string, bindvars map[string]*querypb.BindVariable, sendFunc func(*sqltypes.Result) error) error {
	return client.server.StreamExecute(client.ctx, client.target, query, bindvars, 0, 0, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL}, sendFunc)
}

// MessageStream streams messages from the message table.
func (client *QueryClient) MessageStream(name string, callback func(*sqltypes.Result) error) (err error) {
	return client.server.MessageStream(client.ctx, client.target, name, callback)
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
	return client.server.MessageAck(client.ctx, client.target, name, bids)
}

// ReserveExecute performs a ReserveExecute.
func (client *QueryClient) ReserveExecute(query string, preQueries []string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if client.reservedID != 0 {
		return nil, errors.New("already reserved a connection")
	}
	state, qr, err := client.server.ReserveExecute(client.ctx, client.target, preQueries, query, bindvars, client.transactionID, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
	client.reservedID = state.ReservedID
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// ReserveBeginExecute performs a ReserveBeginExecute.
func (client *QueryClient) ReserveBeginExecute(query string, preQueries []string, postBeginQueries []string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if client.reservedID != 0 {
		return nil, errors.New("already reserved a connection")
	}
	if client.transactionID != 0 {
		return nil, errors.New("already in transaction")
	}
	state, qr, err := client.server.ReserveBeginExecute(client.ctx, client.target, preQueries, postBeginQueries, query, bindvars, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
	client.transactionID = state.TransactionID
	client.reservedID = state.ReservedID
	client.sessionStateChanges = state.SessionStateChanges
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// Release performs a Release.
func (client *QueryClient) Release() error {
	err := client.server.Release(client.ctx, client.target, client.transactionID, client.reservedID)
	client.reservedID = 0
	client.transactionID = 0
	if err != nil {
		return err
	}
	return nil
}

// TransactionID returns transactionID
func (client *QueryClient) TransactionID() int64 {
	return client.transactionID
}

// ReservedID returns reservedID
func (client *QueryClient) ReservedID() int64 {
	return client.reservedID
}

// SetTransactionID does what it says
func (client *QueryClient) SetTransactionID(id int64) {
	client.transactionID = id
}

// SetReservedID does what it says
func (client *QueryClient) SetReservedID(id int64) {
	client.reservedID = id
}

// StreamHealth receives the health response
func (client *QueryClient) StreamHealth(sendFunc func(*querypb.StreamHealthResponse) error) error {
	return client.server.StreamHealth(client.ctx, sendFunc)
}
