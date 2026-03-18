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

package grpctabletconn

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const protocolName = "grpc"

var (
	cert string
	key  string
	ca   string
	crl  string
	name string

	connectionsPerTablet = 10
)

func registerFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &cert, "tablet-grpc-cert", cert, "the cert to use to connect")
	utils.SetFlagStringVar(fs, &key, "tablet-grpc-key", key, "the key to use to connect")
	utils.SetFlagStringVar(fs, &ca, "tablet-grpc-ca", ca, "the server ca to use to validate servers when connecting")
	utils.SetFlagStringVar(fs, &crl, "tablet-grpc-crl", crl, "the server crl to use to validate server certificates when connecting")
	utils.SetFlagStringVar(fs, &name, "tablet-grpc-server-name", name, "the server name to use to validate server certificate")
	fs.IntVar(&connectionsPerTablet, "tablet-grpc-connections", connectionsPerTablet, "number of gRPC connections to open per tablet from vtgate; increasing this can reduce head-of-line blocking on a single HTTP/2 connection")
}

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
	for _, cmd := range []string{
		"vtbench",
		"vtctl",
		"vtctld",
		"vtgate",
		"vttablet",
	} {
		servenv.OnParseFor(cmd, registerFlags)
	}
}

type (
	// connEntry holds a single gRPC connection and its query client.
	connEntry struct {
		cc *grpc.ClientConn
		c  queryservicepb.QueryClient
	}

	// gRPCQueryClient implements a gRPC implementation for QueryService.
	// It holds one or more underlying gRPC connections and distributes
	// RPCs across them via round-robin.
	gRPCQueryClient struct {
		// tablet is set at construction time, and never changed
		tablet *topodatapb.Tablet

		// mu protects conns
		mu    sync.RWMutex
		conns []connEntry

		// next is the round-robin counter, accessed atomically
		next atomic.Uint64
	}
)

var _ queryservice.QueryService = (*gRPCQueryClient)(nil)

// pick returns the next QueryClient via round-robin.
// The caller must hold at least an RLock on conn.mu.
func (conn *gRPCQueryClient) pick() (queryservicepb.QueryClient, error) {
	if len(conn.conns) == 0 {
		return nil, tabletconn.ConnClosed
	}
	idx := conn.next.Add(1) % uint64(len(conn.conns))
	return conn.conns[idx].c, nil
}

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(ctx context.Context, tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	// create the RPC client
	addr := ""
	if grpcPort, ok := tablet.PortMap["grpc"]; ok {
		addr = netutil.JoinHostPort(tablet.Hostname, grpcPort)
	} else {
		addr = tablet.Hostname
	}
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, err
	}

	n := max(connectionsPerTablet, 1)

	conns := make([]connEntry, 0, n)
	for range n {
		cc, err := grpcclient.DialContext(ctx, addr, failFast, opt)
		if err != nil {
			for _, entry := range conns {
				entry.cc.Close()
			}
			return nil, err
		}
		conns = append(conns, connEntry{
			cc: cc,
			c:  queryservicepb.NewQueryClient(cc),
		})
	}

	return &gRPCQueryClient{
		tablet: tablet,
		conns:  conns,
	}, nil
}

// Execute sends the query to VTTablet.
func (conn *gRPCQueryClient) Execute(ctx context.Context, _ queryservice.Session, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return nil, err
	}

	req := &querypb.ExecuteRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		TransactionId: transactionID,
		Options:       options,
		ReservedId:    reservedID,
	}
	er, err := c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

// StreamExecute executes the query and streams results back through callback.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	// All streaming clients should follow the code pattern below.
	// The first part of the function starts the stream while holding
	// a lock on conn.mu. The second part receives the data and calls
	// callback.
	// A new cancelable context is needed because there's currently
	// no direct API to end a stream from the client side. If callback
	// returns an error, we return from the function. The deferred
	// cancel will then cause the stream to be terminated.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.Query_StreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &querypb.StreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query: &querypb.BoundQuery{
				Sql:           query,
				BindVariables: bindVars,
			},
			Options:       options,
			TransactionId: transactionID,
			ReservedId:    reservedID,
		}
		stream, err := c.StreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// Begin starts a transaction.
func (conn *gRPCQueryClient) Begin(ctx context.Context, _ queryservice.Session, target *querypb.Target, options *querypb.ExecuteOptions) (state queryservice.TransactionState, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return state, err
	}

	req := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
	}
	br, err := c.Begin(ctx, req)
	if err != nil {
		return state, tabletconn.ErrorFromGRPC(err)
	}
	state.TransactionID = br.TransactionId
	state.TabletAlias = br.TabletAlias
	state.SessionStateChanges = br.SessionStateChanges
	return state, nil
}

// Commit commits the ongoing transaction.
func (conn *gRPCQueryClient) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return 0, err
	}

	req := &querypb.CommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := c.Commit(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return resp.ReservedId, nil
}

// Rollback rolls back the ongoing transaction.
func (conn *gRPCQueryClient) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return 0, err
	}

	req := &querypb.RollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := c.Rollback(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return resp.ReservedId, nil
}

// Prepare executes a Prepare on the ongoing transaction.
func (conn *gRPCQueryClient) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.PrepareRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err = c.Prepare(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// CommitPrepared commits the prepared transaction.
func (conn *gRPCQueryClient) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.CommitPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err = c.CommitPrepared(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// RollbackPrepared rolls back the prepared transaction.
func (conn *gRPCQueryClient) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.RollbackPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     originalID,
		Dtid:              dtid,
	}
	_, err = c.RollbackPrepared(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (conn *gRPCQueryClient) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.CreateTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
		Participants:      participants,
	}
	_, err = c.CreateTransaction(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (conn *gRPCQueryClient) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (querypb.StartCommitState, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		// This can be marked as fail as no other process will try to commit this transaction.
		return querypb.StartCommitState_Fail, err
	}

	req := &querypb.StartCommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	resp, err := c.StartCommit(ctx, req)
	err = tabletconn.ErrorFromGRPC(err)
	if resp != nil {
		return resp.State, err
	}
	return querypb.StartCommitState_Unknown, err
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (conn *gRPCQueryClient) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.SetRollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err = c.SetRollback(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (conn *gRPCQueryClient) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.ConcludeTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err = c.ConcludeTransaction(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// ReadTransaction returns the metadata for the specified dtid.
func (conn *gRPCQueryClient) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (*querypb.TransactionMetadata, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return nil, err
	}

	req := &querypb.ReadTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	response, err := c.ReadTransaction(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(err)
	}
	return response.Metadata, nil
}

// UnresolvedTransactions returns all unresolved distributed transactions.
func (conn *gRPCQueryClient) UnresolvedTransactions(ctx context.Context, target *querypb.Target, abandonAgeSeconds int64) ([]*querypb.TransactionMetadata, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return nil, err
	}

	req := &querypb.UnresolvedTransactionsRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		AbandonAge:        abandonAgeSeconds,
	}
	response, err := c.UnresolvedTransactions(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(err)
	}
	return response.Transactions, nil
}

// BeginExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (state queryservice.TransactionState, result *sqltypes.Result, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return state, nil, err
	}

	req := &querypb.BeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		PreQueries:        preQueries,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		ReservedId: reservedID,
		Options:    options,
	}
	reply, err := c.BeginExecute(ctx, req)
	if err != nil {
		return state, nil, tabletconn.ErrorFromGRPC(err)
	}
	state.TransactionID = reply.TransactionId
	state.TabletAlias = conn.tablet.Alias
	state.SessionStateChanges = reply.SessionStateChanges
	if reply.Error != nil {
		return state, nil, tabletconn.ErrorFromVTRPC(reply.Error)
	}
	return state, sqltypes.Proto3ToResult(reply.Result), nil
}

// BeginStreamExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginStreamExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (state queryservice.TransactionState, err error) {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.conns) == 0 {
		return state, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_BeginStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &querypb.BeginStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			PreQueries:        preQueries,
			Query: &querypb.BoundQuery{
				Sql:           query,
				BindVariables: bindVars,
			},
			ReservedId: reservedID,
			Options:    options,
		}
		stream, err := c.BeginStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return state, err
	}
	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if state.TransactionID == 0 && ser.GetTransactionId() != 0 {
			state.TransactionID = ser.GetTransactionId()
		}
		if state.TabletAlias == nil && ser.GetTabletAlias() != nil {
			state.TabletAlias = ser.GetTabletAlias()
		}
		if state.SessionStateChanges == "" && ser.GetSessionStateChanges() != "" {
			state.SessionStateChanges = ser.GetSessionStateChanges()
		}

		if err != nil {
			return state, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return state, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return state, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == io.EOF {
				return state, nil
			}
			return state, err
		}
	}
}

// MessageStream streams messages.
func (conn *gRPCQueryClient) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.Query_MessageStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &querypb.MessageStreamRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Name:              name,
		}
		stream, err := c.MessageStream(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	var fields []*querypb.Field
	for {
		msr, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if fields == nil {
			fields = msr.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, msr.Result)); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// MessageAck acks messages.
func (conn *gRPCQueryClient) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return 0, err
	}
	req := &querypb.MessageAckRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Name:              name,
		Ids:               ids,
	}
	reply, err := c.MessageAck(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return int64(reply.Result.RowsAffected), nil
}

// StreamHealth starts a streaming RPC for VTTablet health status updates.
// It is pinned to the first connection to keep health traffic separate
// from query traffic distributed across the remaining connections.
func (conn *gRPCQueryClient) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.Query_StreamHealthClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if len(conn.conns) == 0 {
			return nil, tabletconn.ConnClosed
		}

		stream, err := conn.conns[0].c.StreamHealth(ctx, &querypb.StreamHealthRequest{})
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		shr, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if err := callback(shr); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// VStream starts a VReplication stream.
func (conn *gRPCQueryClient) VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := func() (queryservicepb.Query_VStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &binlogdatapb.VStreamRequest{
			Target:            request.Target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Position:          request.Position,
			Filter:            request.Filter,
			TableLastPKs:      request.TableLastPKs,
			Options:           request.Options,
		}
		stream, err := c.VStream(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := send(r.Events); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// VStreamRows streams rows of a query from the specified starting point.
func (conn *gRPCQueryClient) VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := func() (queryservicepb.Query_VStreamRowsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &binlogdatapb.VStreamRowsRequest{
			Target:            request.Target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             request.Query,
			Lastpk:            request.Lastpk,
			Options:           request.Options,
		}
		stream, err := c.VStreamRows(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	r := binlogdatapb.VStreamRowsResponseFromVTPool()
	defer r.ReturnToVTPool()
	for {
		err := stream.RecvMsg(r)
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := send(r); err != nil {
			return err
		}
		r.ResetVT()
	}
}

// VStreamTables streams rows of a query from the specified starting point.
func (conn *gRPCQueryClient) VStreamTables(ctx context.Context, request *binlogdatapb.VStreamTablesRequest, send func(*binlogdatapb.VStreamTablesResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := func() (queryservicepb.Query_VStreamTablesClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &binlogdatapb.VStreamTablesRequest{
			Target:            request.Target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		}
		stream, err := c.VStreamTables(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	r := binlogdatapb.VStreamTablesResponseFromVTPool()
	defer r.ReturnToVTPool()
	for {
		err := stream.RecvMsg(r)
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := send(r); err != nil {
			return err
		}
		r.ResetVT()
	}
}

// VStreamResults streams rows of a query from the specified starting point.
func (conn *gRPCQueryClient) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := func() (queryservicepb.Query_VStreamResultsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &binlogdatapb.VStreamResultsRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             query,
		}
		stream, err := c.VStreamResults(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := send(r); err != nil {
			return err
		}
	}
}

// HandlePanic is a no-op.
func (conn *gRPCQueryClient) HandlePanic(err *error) {
}

// ReserveBeginExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveBeginExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (state queryservice.ReservedTransactionState, result *sqltypes.Result, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return state, nil, err
	}

	req := &querypb.ReserveBeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
		PreQueries:        preQueries,
		PostBeginQueries:  postBeginQueries,
		Query: &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		},
	}
	reply, err := c.ReserveBeginExecute(ctx, req)
	if err != nil {
		return state, nil, tabletconn.ErrorFromGRPC(err)
	}
	state.ReservedID = reply.ReservedId
	state.TransactionID = reply.TransactionId
	state.TabletAlias = conn.tablet.Alias
	state.SessionStateChanges = reply.SessionStateChanges
	if reply.Error != nil {
		return state, nil, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return state, sqltypes.Proto3ToResult(reply.Result), nil
}

// ReserveBeginStreamExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveBeginStreamExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (state queryservice.ReservedTransactionState, err error) {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.conns) == 0 {
		return state, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_ReserveBeginStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &querypb.ReserveBeginStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Options:           options,
			PreQueries:        preQueries,
			PostBeginQueries:  postBeginQueries,
			Query: &querypb.BoundQuery{
				Sql:           sql,
				BindVariables: bindVariables,
			},
		}
		stream, err := c.ReserveBeginStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return state, tabletconn.ErrorFromGRPC(err)
	}

	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if state.TransactionID == 0 && ser.GetTransactionId() != 0 {
			state.TransactionID = ser.GetTransactionId()
		}
		if state.ReservedID == 0 && ser.GetReservedId() != 0 {
			state.ReservedID = ser.GetReservedId()
		}
		if state.TabletAlias == nil && ser.GetTabletAlias() != nil {
			state.TabletAlias = ser.GetTabletAlias()
		}
		if state.SessionStateChanges == "" && ser.GetSessionStateChanges() != "" {
			state.SessionStateChanges = ser.GetSessionStateChanges()
		}

		if err != nil {
			return state, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return state, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return state, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == io.EOF {
				return state, nil
			}
			return state, err
		}
	}
}

// ReserveExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (state queryservice.ReservedState, result *sqltypes.Result, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return state, nil, err
	}

	req := &querypb.ReserveExecuteRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		Query: &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		},
		TransactionId: transactionID,
		Options:       options,
		PreQueries:    preQueries,
	}
	reply, err := c.ReserveExecute(ctx, req)
	if err != nil {
		return state, nil, tabletconn.ErrorFromGRPC(err)
	}
	state.ReservedID = reply.ReservedId
	state.TabletAlias = reply.TabletAlias
	if reply.Error != nil {
		return state, nil, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return state, sqltypes.Proto3ToResult(reply.Result), nil
}

// ReserveStreamExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveStreamExecute(ctx context.Context, _ queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (state queryservice.ReservedState, err error) {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.conns) == 0 {
		return state, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_ReserveStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		req := &querypb.ReserveStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Options:           options,
			PreQueries:        preQueries,
			Query: &querypb.BoundQuery{
				Sql:           sql,
				BindVariables: bindVariables,
			},
			TransactionId: transactionID,
		}
		stream, err := c.ReserveStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return state, tabletconn.ErrorFromGRPC(err)
	}

	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if state.ReservedID == 0 && ser.GetReservedId() != 0 {
			state.ReservedID = ser.GetReservedId()
		}
		if state.TabletAlias == nil && ser.GetTabletAlias() != nil {
			state.TabletAlias = ser.GetTabletAlias()
		}

		if err != nil {
			return state, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return state, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return state, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == io.EOF {
				return state, nil
			}
			return state, err
		}
	}
}

func (conn *gRPCQueryClient) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	c, err := conn.pick()
	if err != nil {
		return err
	}

	req := &querypb.ReleaseRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		TransactionId:     transactionID,
		ReservedId:        reservedID,
	}
	_, err = c.Release(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
}

// GetSchema implements the queryservice interface
func (conn *gRPCQueryClient) GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.conns) == 0 {
		return tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_GetSchemaClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		c, err := conn.pick()
		if err != nil {
			return nil, err
		}

		stream, err := c.GetSchema(ctx, &querypb.GetSchemaRequest{
			Target:     target,
			TableType:  tableType,
			TableNames: tableNames,
		})
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		shr, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if err := callback(shr); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// Close closes underlying gRPC channel.
func (conn *gRPCQueryClient) Close(ctx context.Context) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.conns == nil {
		return nil
	}

	var firstErr error
	for _, entry := range conn.conns {
		if err := entry.cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	conn.conns = nil
	return firstErr
}

// Tablet returns the rpc end point.
func (conn *gRPCQueryClient) Tablet() *topodatapb.Tablet {
	return conn.tablet
}
