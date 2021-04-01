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

package grpcvtgateconn

// This is agnostic of grpc and was in a separate package 'vtgateconntest'.
// This has been moved here for better readability. If we introduce
// protocols other than grpc in the future, this will have to be
// moved back to its own package for reusability.

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"context"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
	t        *testing.T
	panics   bool
	hasError bool

	errorWait chan struct{}
}

const (
	expectedErrMatch string       = "test vtgate error"
	expectedCode     vtrpcpb.Code = vtrpcpb.Code_INVALID_ARGUMENT
)

var errTestVtGateError = vterrors.New(expectedCode, expectedErrMatch)

func newContext() context.Context {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, nil)
	return ctx
}

func (f *fakeVTGateService) checkCallerID(ctx context.Context, name string) {
	ef := callerid.EffectiveCallerIDFromContext(ctx)
	if ef == nil {
		f.t.Errorf("no effective caller id for %v", name)
	} else {
		if !proto.Equal(ef, testCallerID) {
			f.t.Errorf("invalid effective caller id for %v: got %v expected %v", name, ef, testCallerID)
		}
	}
}

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL           string
	BindVariables map[string]*querypb.BindVariable
	Session       *vtgatepb.Session
}

func (q *queryExecute) equal(q2 *queryExecute) bool {
	return q.SQL == q2.SQL &&
		sqltypes.BindVariablesEqual(q.BindVariables, q2.BindVariables) &&
		proto.Equal(q.Session, q2.Session)
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	if f.hasError {
		return session, nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "Execute")
	execCase, ok := execMap[sql]
	if !ok {
		return session, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Session:       session,
	}
	if !query.equal(execCase.execQuery) {
		f.t.Errorf("Execute:\n%+v, want\n%+v", query, execCase.execQuery)
		return session, nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return session, execCase.result, nil
}

// ExecuteBatch is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if f.hasError {
		return session, nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatch")
	execCase, ok := execMap[sqlList[0]]
	if !ok {
		return session, nil, fmt.Errorf("no match for: %s", sqlList[0])
	}
	query := &queryExecute{
		SQL:           sqlList[0],
		BindVariables: bindVariablesList[0],
		Session:       session,
	}
	if !query.equal(execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return session, nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return session, []sqltypes.QueryResponse{{
		QueryResult: execCase.result,
		QueryError:  nil,
	}}, nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	f.checkCallerID(ctx, "StreamExecute")
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Session:       session,
	}
	if !query.equal(execCase.execQuery) {
		f.t.Errorf("StreamExecute:\n%+v, want\n%+v", query, execCase.execQuery)
		return nil
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := callback(result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := callback(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveTransaction is part of the VTGateService interface
func (f *fakeVTGateService) ResolveTransaction(ctx context.Context, dtid string) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ResolveTransaction")
	if dtid != dtid2 {
		return errors.New("ResolveTransaction: dtid mismatch")
	}
	return nil
}

func (f *fakeVTGateService) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	panic("unimplemented")
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) vtgateservice.VTGateService {
	return &fakeVTGateService{
		t:         t,
		panics:    false,
		errorWait: make(chan struct{}),
	}
}

// RegisterTestDialProtocol registers a vtgateconn implementation under the "test" protocol
func RegisterTestDialProtocol(impl vtgateconn.Impl) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string) (vtgateconn.Impl, error) {
		return impl, nil
	})
}

// HandlePanic is part of the VTGateService interface
func (f *fakeVTGateService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		// gRPC 0.13 chokes when you return a streaming error that contains newlines.
		*err = fmt.Errorf("uncaught panic: %v, %s", x,
			strings.Replace(string(tb.Stack(4)), "\n", ";", -1))
	}
}

// RunTests runs all the tests
func RunTests(t *testing.T, impl vtgateconn.Impl, fakeServer vtgateservice.VTGateService) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string) (vtgateconn.Impl, error) {
		return impl, nil
	})
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "")
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}
	session := conn.Session("connection_ks@rdonly", testExecuteOptions)

	fs := fakeServer.(*fakeVTGateService)

	testExecute(t, session)
	testStreamExecute(t, session)
	testExecuteBatch(t, session)

	// force a panic at every call, then test that works
	fs.panics = true
	testExecutePanic(t, session)
	testExecuteBatchPanic(t, session)
	testStreamExecutePanic(t, session)
	fs.panics = false
}

// RunErrorTests runs all the tests that expect errors
func RunErrorTests(t *testing.T, fakeServer vtgateservice.VTGateService) {
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "")
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}
	session := conn.Session("connection_ks@rdonly", testExecuteOptions)

	fs := fakeServer.(*fakeVTGateService)

	// return an error for every call, make sure they're handled properly
	fs.hasError = true
	testExecuteError(t, session, fs)
	testExecuteBatchError(t, session, fs)
	testStreamExecuteError(t, session, fs)
	fs.hasError = false
}

func expectPanic(t *testing.T, err error) {
	expected1 := "test forced panic"
	expected2 := "uncaught panic"
	if err == nil || !strings.Contains(err.Error(), expected1) || !strings.Contains(err.Error(), expected2) {
		t.Fatalf("Expected a panic error with '%v' or '%v' but got: %v", expected1, expected2, err)
	}
}

// Verifies the returned error has the properties that we expect.
func verifyError(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}
	// verify error code
	code := vterrors.Code(err)
	if code != expectedCode {
		t.Errorf("Unexpected error code from %s: got %v, wanted %v", method, code, expectedCode)
	}
	verifyErrorString(t, err, method)
}

func verifyErrorString(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}

	if !strings.Contains(err.Error(), expectedErrMatch) {
		t.Errorf("Unexpected error from %s: got %v, wanted err containing: %v", method, err, errTestVtGateError.Error())
	}
}

func testExecute(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := session.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	require.NoError(t, err)
	if !qr.Equal(execCase.result) {
		t.Errorf("Unexpected result from Execute: got\n%#v want\n%#v", qr, execCase.result)
	}

	_, err = session.Execute(ctx, "none", nil)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteError(t *testing.T, session *vtgateconn.VTGateSession, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := session.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	verifyError(t, err, "Execute")
}

func testExecutePanic(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := session.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	expectPanic(t, err)
}

func testExecuteBatch(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := session.ExecuteBatch(ctx, []string{execCase.execQuery.SQL}, []map[string]*querypb.BindVariable{execCase.execQuery.BindVariables})
	require.NoError(t, err)
	if !qr[0].QueryResult.Equal(execCase.result) {
		t.Errorf("Unexpected result from Execute: got\n%#v want\n%#v", qr, execCase.result)
	}

	_, err = session.ExecuteBatch(ctx, []string{"none"}, nil)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteBatchError(t *testing.T, session *vtgateconn.VTGateSession, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := session.ExecuteBatch(ctx, []string{execCase.execQuery.SQL}, []map[string]*querypb.BindVariable{execCase.execQuery.BindVariables})
	verifyError(t, err, "ExecuteBatch")
}

func testExecuteBatchPanic(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := session.ExecuteBatch(ctx, []string{execCase.execQuery.SQL}, []map[string]*querypb.BindVariable{execCase.execQuery.BindVariables})
	expectPanic(t, err)
}

func testStreamExecute(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := session.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	if err != nil {
		t.Fatal(err)
	}
	var qr sqltypes.Result
	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.result
	wantResult.RowsAffected = 0
	wantResult.InsertID = 0
	if !qr.Equal(&wantResult) {
		t.Errorf("Unexpected result from StreamExecute: got %+v want %+v", qr, wantResult)
	}

	stream, err = session.StreamExecute(ctx, "none", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testStreamExecuteError(t *testing.T, session *vtgateconn.VTGateSession, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := session.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecute failed: cannot read result1: %v", err)
	}

	if !qr.Equal(&streamResultFields) {
		t.Errorf("Unexpected result from StreamExecute: got %#v want %#v", qr, &streamResultFields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("StreamExecute channel wasn't closed")
	}
	verifyError(t, err, "StreamExecute")
}

func testStreamExecutePanic(t *testing.T, session *vtgateconn.VTGateSession) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := session.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

var testCallerID = &vtrpcpb.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var testExecuteOptions = &querypb.ExecuteOptions{
	IncludedFields: querypb.ExecuteOptions_TYPE_ONLY,
}

var execMap = map[string]struct {
	execQuery  *queryExecute
	result     *sqltypes.Result
	outSession *vtgatepb.Session
	err        error
}{
	"request1": {
		execQuery: &queryExecute{
			SQL: "request1",
			BindVariables: map[string]*querypb.BindVariable{
				"bind1": sqltypes.Int64BindVariable(0),
			},
			Session: &vtgatepb.Session{
				TargetString: "connection_ks@rdonly",
				Options:      testExecuteOptions,
				Autocommit:   true,
			},
		},
		result: &result1,
	},
	"errorRequst": {
		execQuery: &queryExecute{
			SQL: "errorRequst",
			BindVariables: map[string]*querypb.BindVariable{
				"bind1": sqltypes.Int64BindVariable(0),
			},
			Session: &vtgatepb.Session{
				TargetString: "connection_ks@rdonly",
				Options:      testExecuteOptions,
			},
		},
	},
}

var result1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int16,
		},
		{
			Name: "field2",
			Type: sqltypes.Int32,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.TestValue(sqltypes.Int16, "1"),
			sqltypes.NULL,
		},
		{
			sqltypes.TestValue(sqltypes.Int16, "2"),
			sqltypes.NewInt32(3),
		},
	},
}

// streamResultFields is only the fields, sent as the first packet
var streamResultFields = sqltypes.Result{
	Fields: result1.Fields,
	Rows:   [][]sqltypes.Value{},
}

var dtid2 = "aa"
