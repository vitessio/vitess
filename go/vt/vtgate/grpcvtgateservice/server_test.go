/*
Copyright 2024 The Vitess Authors.

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

package grpcvtgateservice

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/onsi/gomega/gleak/goroutine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

type mockVtgateService struct{}

func (m *mockVtgateService) Execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVtgateService) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	//TODO implement me
	panic("implement me")
}

// StreamExecute in mockVtgateService calls the callback from two different go routines.
func (m *mockVtgateService) StreamExecute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error) {
	resOne := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col", "int64|int64"), "1|1", "2|1")
	resTwo := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col", "int64|int64"), "1|1", "2|2")
	var errOne, errTwo error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		errOne = callback(resOne)
		wg.Done()
	}()
	go func() {
		errTwo = callback(resTwo)
		wg.Done()
	}()
	wg.Wait()
	if errOne != nil {
		return session, errOne
	}
	return session, errTwo
}

func (m *mockVtgateService) Prepare(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, []*querypb.Field, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVtgateService) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVtgateService) ResolveTransaction(ctx context.Context, dtid string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVtgateService) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVtgateService) HandlePanic(err *error) {}

var _ vtgateservice.VTGateService = (*mockVtgateService)(nil)

type mockStreamExecuteServer struct {
	mu          sync.Mutex
	goRoutineId uint64
}

// Send in mockStreamExecuteServer stores the go routine ID that it is called from.
// If Send is called from 2 different go-routines, then it throws an error.
func (m *mockStreamExecuteServer) Send(response *vtgatepb.StreamExecuteResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	gr := goroutine.Current()
	if m.goRoutineId == 0 {
		m.goRoutineId = gr.ID
	}
	if gr.ID != m.goRoutineId {
		return fmt.Errorf("two go routines are calling Send - %v and %v", gr.ID, m.goRoutineId)
	}
	return nil
}

func (m *mockStreamExecuteServer) SetHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockStreamExecuteServer) SendHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockStreamExecuteServer) SetTrailer(md metadata.MD) {
	//TODO implement me
	panic("implement me")
}

func (m *mockStreamExecuteServer) Context() context.Context {
	return context.Background()
}

func (m *mockStreamExecuteServer) SendMsg(msg any) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockStreamExecuteServer) RecvMsg(msg any) error {
	//TODO implement me
	panic("implement me")
}

var _ vtgateservicepb.Vitess_StreamExecuteServer = (*mockStreamExecuteServer)(nil)

// TestVTGateStreamExecuteConcurrency tests that calling StreamExecute with a mock executor that calls
// Send from 2 different go routines is safe.
func TestVTGateStreamExecuteConcurrency(t *testing.T) {
	testcases := []struct {
		name        string
		sendSession bool
	}{
		{
			name:        "send session",
			sendSession: true,
		},
		{
			name:        "dont send session",
			sendSession: false,
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			oldVal := sendSessionInStreaming
			defer func() {
				sendSessionInStreaming = oldVal
			}()
			sendSessionInStreaming = tt.sendSession
			vtg := &VTGate{
				UnimplementedVitessServer: vtgateservicepb.UnimplementedVitessServer{},
				server:                    &mockVtgateService{},
			}
			err := vtg.StreamExecute(&vtgatepb.StreamExecuteRequest{
				CallerId: &vtrpcpb.CallerID{},
				Query:    &querypb.BoundQuery{},
			}, &mockStreamExecuteServer{
				mu:          sync.Mutex{},
				goRoutineId: 0,
			})
			require.NoError(t, err)
		})
	}

}
