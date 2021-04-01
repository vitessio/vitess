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

// Package fakerpcvtgateconn provides a fake implementation of
// vtgateconn.Impl that doesn't do any RPC, but uses a local
// map to return results.
package fakerpcvtgateconn

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL           string
	BindVariables map[string]*querypb.BindVariable
	Session       *vtgatepb.Session
}

type queryResponse struct {
	execQuery *queryExecute
	reply     *sqltypes.Result
	err       error
}

// FakeVTGateConn provides a fake implementation of vtgateconn.Impl
type FakeVTGateConn struct {
	execMap map[string]*queryResponse
}

// RegisterFakeVTGateConnDialer registers the proper dialer for this fake,
// and returns the underlying instance that will be returned by the dialer,
// and the protocol to use to get this fake.
func RegisterFakeVTGateConnDialer() (*FakeVTGateConn, string) {
	protocol := "fake"
	impl := &FakeVTGateConn{
		execMap: make(map[string]*queryResponse),
	}
	vtgateconn.RegisterDialer(protocol, func(ctx context.Context, address string) (vtgateconn.Impl, error) {
		return impl, nil
	})
	return impl, protocol
}

// AddQuery adds a query and expected result.
func (conn *FakeVTGateConn) AddQuery(
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	session *vtgatepb.Session,
	expectedResult *sqltypes.Result) {
	conn.execMap[sql] = &queryResponse{
		execQuery: &queryExecute{
			SQL:           sql,
			BindVariables: bindVariables,
			Session:       session,
		},
		reply: expectedResult,
	}
}

// Execute please see vtgateconn.Impl.Execute
func (conn *FakeVTGateConn) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	response, ok := conn.execMap[sql]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVars,
		Session:       session,
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, nil, fmt.Errorf(
			"Execute: %+v, want %+v", query, response.execQuery)
	}
	reply := *response.reply
	s := newSession(true, "test_keyspace", []string{}, topodatapb.TabletType_MASTER)
	return s, &reply, nil
}

// ExecuteBatch please see vtgateconn.Impl.ExecuteBatch
func (conn *FakeVTGateConn) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVarsList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	panic("not implemented")
}

// StreamExecute please see vtgateconn.Impl.StreamExecute
func (conn *FakeVTGateConn) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error) {
	response, ok := conn.execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVars,
		Session:       session,
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, fmt.Errorf("StreamExecute: %+v, want %+v", sql, response.execQuery)
	}
	if response.err != nil {
		return nil, response.err
	}
	var resultChan chan *sqltypes.Result
	defer close(resultChan)
	if response.reply != nil {
		// create a result channel big enough to buffer all of
		// the responses so we don't need to fork a go routine.
		resultChan = make(chan *sqltypes.Result, len(response.reply.Rows)+1)
		result := &sqltypes.Result{}
		result.Fields = response.reply.Fields
		resultChan <- result
		for _, row := range response.reply.Rows {
			result := &sqltypes.Result{}
			result.Rows = [][]sqltypes.Value{row}
			resultChan <- result
		}
	} else {
		resultChan = make(chan *sqltypes.Result)
	}
	return &streamExecuteAdapter{resultChan}, nil
}

type streamExecuteAdapter struct {
	c chan *sqltypes.Result
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	r, ok := <-a.c
	if !ok {
		return nil, io.EOF
	}
	return r, nil
}

// ResolveTransaction please see vtgateconn.Impl.ResolveTransaction
func (conn *FakeVTGateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	return nil
}

// VStream streams binlog events.
func (conn *FakeVTGateConn) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid,
	filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags) (vtgateconn.VStreamReader, error) {

	return nil, fmt.Errorf("NYI")
}

// Close please see vtgateconn.Impl.Close
func (conn *FakeVTGateConn) Close() {
}

func newSession(
	inTransaction bool,
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType) *vtgatepb.Session {
	shardSessions := make([]*vtgatepb.Session_ShardSession, len(shards))
	for _, shard := range shards {
		shardSessions = append(shardSessions, &vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      shard,
				TabletType: tabletType,
			},
			TransactionId: rand.Int63(),
		})
	}
	return &vtgatepb.Session{
		InTransaction: inTransaction,
		ShardSessions: shardSessions,
	}
}

// Make sure FakeVTGateConn implements vtgateconn.Impl
var _ (vtgateconn.Impl) = (*FakeVTGateConn)(nil)
