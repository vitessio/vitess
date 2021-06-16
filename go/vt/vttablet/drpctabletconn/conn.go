package drpctabletconn

import (
	"context"
	"io"
	"net"
	"sync"

	"storj.io/drpc"
	"storj.io/drpc/drpcconn"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/drpctabletconn/drpcpool"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
)

func init() {
	tabletconn.RegisterDialer("drpc", DialTablet)
}

type dRPCQueryClient struct {
	// tablet is set at construction time, and never changed
	tablet *topodatapb.Tablet

	// mu protects the next fields
	mu sync.RWMutex
	cc drpc.Conn
	c  queryservicepb.DRPCQueryClient
}

func (conn *dRPCQueryClient) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, nil, tabletconn.ErrorFromDRPC(err)
	}
	// For backwards compatibility, we don't require tablet alias to be present in the response
	// TODO(deepthi): After 7.0 change this
	//	return br.TransactionId, br.TabletAlias, nil
	// also assert that br.TabletAlias == conn.tablet.Alias
	return br.TransactionId, conn.tablet.Alias, nil
}

func (conn *dRPCQueryClient) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.CommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := conn.c.Commit(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromDRPC(err)
	}
	return resp.ReservedId, nil
}

func (conn *dRPCQueryClient) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.RollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromDRPC(err)
	}
	return resp.ReservedId, nil
}

func (conn *dRPCQueryClient) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.PrepareRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.Prepare(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CommitPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err := conn.c.CommitPrepared(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.RollbackPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     originalID,
		Dtid:              dtid,
	}
	_, err := conn.c.RollbackPrepared(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CreateTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
		Participants:      participants,
	}
	_, err := conn.c.CreateTransaction(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.StartCommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.StartCommit(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.SetRollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.SetRollback(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.ConcludeTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err := conn.c.ConcludeTransaction(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (*querypb.TransactionMetadata, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ReadTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	response, err := conn.c.ReadTransaction(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromDRPC(err)
	}
	return response.Metadata, nil
}

func (conn *dRPCQueryClient) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
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
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromDRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

func (conn *dRPCQueryClient) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
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

	stream, err := func() (queryservicepb.DRPCQuery_StreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
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
		}
		stream, err := conn.c.StreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
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
			return tabletconn.ErrorFromDRPC(err)
		}
		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (conn *dRPCQueryClient) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ExecuteBatchRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           queries,
		AsTransaction:     asTransaction,
		TransactionId:     transactionID,
		Options:           options,
	}
	ebr, err := conn.c.ExecuteBatch(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromDRPC(err)
	}
	return sqltypes.Proto3ToResults(ebr.Results), nil
}

func (conn *dRPCQueryClient) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, nil, tabletconn.ConnClosed
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
	reply, err := conn.c.BeginExecute(ctx, req)
	if err != nil {
		return nil, 0, nil, tabletconn.ErrorFromDRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}
	return sqltypes.Proto3ToResult(reply.Result), reply.TransactionId, conn.tablet.Alias, nil
}

func (conn *dRPCQueryClient) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.BeginExecuteBatchRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           queries,
		AsTransaction:     asTransaction,
		Options:           options,
	}

	reply, err := conn.c.BeginExecuteBatch(ctx, req)
	if err != nil {
		return nil, 0, nil, tabletconn.ErrorFromDRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}
	return sqltypes.Proto3ToResults(reply.Results), reply.TransactionId, conn.tablet.Alias, nil
}

func (conn *dRPCQueryClient) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.DRPCQuery_MessageStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.MessageStreamRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Name:              name,
		}
		stream, err := conn.c.MessageStream(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
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
			return tabletconn.ErrorFromDRPC(err)
		}
		if fields == nil {
			fields = msr.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, msr.Result)); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (conn *dRPCQueryClient) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}
	req := &querypb.MessageAckRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Name:              name,
		Ids:               ids,
	}
	reply, err := conn.c.MessageAck(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromDRPC(err)
	}
	return int64(reply.Result.RowsAffected), nil
}

func (conn *dRPCQueryClient) VStream(ctx context.Context, target *querypb.Target, startPos string, tableLastPKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	stream, err := func() (queryservicepb.DRPCQuery_VStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &binlogdatapb.VStreamRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Position:          startPos,
			Filter:            filter,
			TableLastPKs:      tableLastPKs,
		}
		stream, err := conn.c.VStream(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromDRPC(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := send(r.Events); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (conn *dRPCQueryClient) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	stream, err := func() (queryservicepb.DRPCQuery_VStreamRowsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}
		req := &binlogdatapb.VStreamRowsRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             query,
			Lastpk:            lastpk,
		}
		stream, err := conn.c.VStreamRows(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromDRPC(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := send(r); err != nil {
			return err
		}
	}
}

func (conn *dRPCQueryClient) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	stream, err := func() (queryservicepb.DRPCQuery_VStreamResultsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &binlogdatapb.VStreamResultsRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             query,
		}
		stream, err := conn.c.VStreamResults(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromDRPC(err)
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

func (conn *dRPCQueryClient) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.DRPCQuery_StreamHealthClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		stream, err := conn.c.StreamHealth(ctx, &querypb.StreamHealthRequest{})
		if err != nil {
			return nil, tabletconn.ErrorFromDRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		shr, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromDRPC(err)
		}
		if err := callback(shr); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (conn *dRPCQueryClient) HandlePanic(err *error) {
}

func (conn *dRPCQueryClient) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.ReserveBeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
		PreQueries:        preQueries,
		Query: &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		},
	}
	reply, err := conn.c.ReserveBeginExecute(ctx, req)
	if err != nil {
		return nil, 0, 0, nil, tabletconn.ErrorFromDRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, reply.ReservedId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return sqltypes.Proto3ToResult(reply.Result), reply.TransactionId, reply.ReservedId, conn.tablet.Alias, nil
}

func (conn *dRPCQueryClient) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, nil, tabletconn.ConnClosed
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
	reply, err := conn.c.ReserveExecute(ctx, req)
	if err != nil {
		return nil, 0, nil, tabletconn.ErrorFromDRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.ReservedId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return sqltypes.Proto3ToResult(reply.Result), reply.ReservedId, conn.tablet.Alias, nil
}

func (conn *dRPCQueryClient) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.ReleaseRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		TransactionId:     transactionID,
		ReservedId:        reservedID,
	}
	_, err := conn.c.Release(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromDRPC(err)
	}
	return nil
}

func (conn *dRPCQueryClient) Close(_ context.Context) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.cc == nil {
		return nil
	}

	cc := conn.cc
	conn.cc = nil
	return cc.Close()
}

func DialTablet(tablet *topodatapb.Tablet, failFast bool) (queryservice.QueryService, error) {
	cc := drpcpool.OpenConnectionPool(func(ctx context.Context) (drpc.Conn, error) {
		return dialTablet1(tablet, failFast)
	})

	c := queryservicepb.NewDRPCQueryClient(cc)
	return &dRPCQueryClient{
		tablet: tablet,
		cc:     cc,
		c:      c,
	}, nil
}

func dialTablet1(tablet *topodatapb.Tablet, failFast bool) (drpc.Conn, error) {
	var addr string
	if drpcPort, ok := tablet.PortMap["drpc"]; ok {
		addr = netutil.JoinHostPort(tablet.Hostname, drpcPort)
	} else {
		addr = tablet.Hostname
	}

	rawconn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	// TODO: tls
	return drpcconn.New(rawconn), nil
}
