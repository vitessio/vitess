// Copyright 2016, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package debugger

import (
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	clouddebuggerpb "google.golang.org/genproto/googleapis/devtools/clouddebugger/v2"
)

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var _ = io.EOF
var _ = ptypes.MarshalAny
var _ status.Status

type mockDebugger2Server struct {
	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockDebugger2Server) SetBreakpoint(_ context.Context, req *clouddebuggerpb.SetBreakpointRequest) (*clouddebuggerpb.SetBreakpointResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.SetBreakpointResponse), nil
}

func (s *mockDebugger2Server) GetBreakpoint(_ context.Context, req *clouddebuggerpb.GetBreakpointRequest) (*clouddebuggerpb.GetBreakpointResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.GetBreakpointResponse), nil
}

func (s *mockDebugger2Server) DeleteBreakpoint(_ context.Context, req *clouddebuggerpb.DeleteBreakpointRequest) (*google_protobuf.Empty, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*google_protobuf.Empty), nil
}

func (s *mockDebugger2Server) ListBreakpoints(_ context.Context, req *clouddebuggerpb.ListBreakpointsRequest) (*clouddebuggerpb.ListBreakpointsResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.ListBreakpointsResponse), nil
}

func (s *mockDebugger2Server) ListDebuggees(_ context.Context, req *clouddebuggerpb.ListDebuggeesRequest) (*clouddebuggerpb.ListDebuggeesResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.ListDebuggeesResponse), nil
}

type mockController2Server struct {
	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockController2Server) RegisterDebuggee(_ context.Context, req *clouddebuggerpb.RegisterDebuggeeRequest) (*clouddebuggerpb.RegisterDebuggeeResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.RegisterDebuggeeResponse), nil
}

func (s *mockController2Server) ListActiveBreakpoints(_ context.Context, req *clouddebuggerpb.ListActiveBreakpointsRequest) (*clouddebuggerpb.ListActiveBreakpointsResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.ListActiveBreakpointsResponse), nil
}

func (s *mockController2Server) UpdateActiveBreakpoint(_ context.Context, req *clouddebuggerpb.UpdateActiveBreakpointRequest) (*clouddebuggerpb.UpdateActiveBreakpointResponse, error) {
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouddebuggerpb.UpdateActiveBreakpointResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockDebugger2   mockDebugger2Server
	mockController2 mockController2Server
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	clouddebuggerpb.RegisterDebugger2Server(serv, &mockDebugger2)
	clouddebuggerpb.RegisterController2Server(serv, &mockController2)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatal(err)
	}
	go serv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	clientOpt = option.WithGRPCConn(conn)

	os.Exit(m.Run())
}

func TestDebugger2SetBreakpoint(t *testing.T) {
	var expectedResponse *clouddebuggerpb.SetBreakpointResponse = &clouddebuggerpb.SetBreakpointResponse{}

	mockDebugger2.err = nil
	mockDebugger2.reqs = nil

	mockDebugger2.resps = append(mockDebugger2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var breakpoint *clouddebuggerpb.Breakpoint = &clouddebuggerpb.Breakpoint{}
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.SetBreakpointRequest{
		DebuggeeId:    debuggeeId,
		Breakpoint:    breakpoint,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetBreakpoint(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDebugger2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDebugger2SetBreakpointError(t *testing.T) {
	errCode := codes.Internal
	mockDebugger2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var breakpoint *clouddebuggerpb.Breakpoint = &clouddebuggerpb.Breakpoint{}
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.SetBreakpointRequest{
		DebuggeeId:    debuggeeId,
		Breakpoint:    breakpoint,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetBreakpoint(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDebugger2GetBreakpoint(t *testing.T) {
	var expectedResponse *clouddebuggerpb.GetBreakpointResponse = &clouddebuggerpb.GetBreakpointResponse{}

	mockDebugger2.err = nil
	mockDebugger2.reqs = nil

	mockDebugger2.resps = append(mockDebugger2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var breakpointId string = "breakpointId498424873"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.GetBreakpointRequest{
		DebuggeeId:    debuggeeId,
		BreakpointId:  breakpointId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetBreakpoint(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDebugger2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDebugger2GetBreakpointError(t *testing.T) {
	errCode := codes.Internal
	mockDebugger2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var breakpointId string = "breakpointId498424873"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.GetBreakpointRequest{
		DebuggeeId:    debuggeeId,
		BreakpointId:  breakpointId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetBreakpoint(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDebugger2DeleteBreakpoint(t *testing.T) {
	var expectedResponse *google_protobuf.Empty = &google_protobuf.Empty{}

	mockDebugger2.err = nil
	mockDebugger2.reqs = nil

	mockDebugger2.resps = append(mockDebugger2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var breakpointId string = "breakpointId498424873"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.DeleteBreakpointRequest{
		DebuggeeId:    debuggeeId,
		BreakpointId:  breakpointId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteBreakpoint(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDebugger2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDebugger2DeleteBreakpointError(t *testing.T) {
	errCode := codes.Internal
	mockDebugger2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var breakpointId string = "breakpointId498424873"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.DeleteBreakpointRequest{
		DebuggeeId:    debuggeeId,
		BreakpointId:  breakpointId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteBreakpoint(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDebugger2ListBreakpoints(t *testing.T) {
	var nextWaitToken string = "nextWaitToken1006864251"
	var expectedResponse = &clouddebuggerpb.ListBreakpointsResponse{
		NextWaitToken: nextWaitToken,
	}

	mockDebugger2.err = nil
	mockDebugger2.reqs = nil

	mockDebugger2.resps = append(mockDebugger2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.ListBreakpointsRequest{
		DebuggeeId:    debuggeeId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListBreakpoints(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDebugger2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDebugger2ListBreakpointsError(t *testing.T) {
	errCode := codes.Internal
	mockDebugger2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.ListBreakpointsRequest{
		DebuggeeId:    debuggeeId,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListBreakpoints(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDebugger2ListDebuggees(t *testing.T) {
	var expectedResponse *clouddebuggerpb.ListDebuggeesResponse = &clouddebuggerpb.ListDebuggeesResponse{}

	mockDebugger2.err = nil
	mockDebugger2.reqs = nil

	mockDebugger2.resps = append(mockDebugger2.resps[:0], expectedResponse)

	var project string = "project-309310695"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.ListDebuggeesRequest{
		Project:       project,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDebuggees(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDebugger2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDebugger2ListDebuggeesError(t *testing.T) {
	errCode := codes.Internal
	mockDebugger2.err = grpc.Errorf(errCode, "test error")

	var project string = "project-309310695"
	var clientVersion string = "clientVersion-1506231196"
	var request = &clouddebuggerpb.ListDebuggeesRequest{
		Project:       project,
		ClientVersion: clientVersion,
	}

	c, err := NewDebugger2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDebuggees(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestController2RegisterDebuggee(t *testing.T) {
	var expectedResponse *clouddebuggerpb.RegisterDebuggeeResponse = &clouddebuggerpb.RegisterDebuggeeResponse{}

	mockController2.err = nil
	mockController2.reqs = nil

	mockController2.resps = append(mockController2.resps[:0], expectedResponse)

	var debuggee *clouddebuggerpb.Debuggee = &clouddebuggerpb.Debuggee{}
	var request = &clouddebuggerpb.RegisterDebuggeeRequest{
		Debuggee: debuggee,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RegisterDebuggee(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockController2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestController2RegisterDebuggeeError(t *testing.T) {
	errCode := codes.Internal
	mockController2.err = grpc.Errorf(errCode, "test error")

	var debuggee *clouddebuggerpb.Debuggee = &clouddebuggerpb.Debuggee{}
	var request = &clouddebuggerpb.RegisterDebuggeeRequest{
		Debuggee: debuggee,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RegisterDebuggee(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestController2ListActiveBreakpoints(t *testing.T) {
	var nextWaitToken string = "nextWaitToken1006864251"
	var waitExpired bool = false
	var expectedResponse = &clouddebuggerpb.ListActiveBreakpointsResponse{
		NextWaitToken: nextWaitToken,
		WaitExpired:   waitExpired,
	}

	mockController2.err = nil
	mockController2.reqs = nil

	mockController2.resps = append(mockController2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var request = &clouddebuggerpb.ListActiveBreakpointsRequest{
		DebuggeeId: debuggeeId,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListActiveBreakpoints(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockController2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestController2ListActiveBreakpointsError(t *testing.T) {
	errCode := codes.Internal
	mockController2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var request = &clouddebuggerpb.ListActiveBreakpointsRequest{
		DebuggeeId: debuggeeId,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListActiveBreakpoints(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestController2UpdateActiveBreakpoint(t *testing.T) {
	var expectedResponse *clouddebuggerpb.UpdateActiveBreakpointResponse = &clouddebuggerpb.UpdateActiveBreakpointResponse{}

	mockController2.err = nil
	mockController2.reqs = nil

	mockController2.resps = append(mockController2.resps[:0], expectedResponse)

	var debuggeeId string = "debuggeeId-997255898"
	var breakpoint *clouddebuggerpb.Breakpoint = &clouddebuggerpb.Breakpoint{}
	var request = &clouddebuggerpb.UpdateActiveBreakpointRequest{
		DebuggeeId: debuggeeId,
		Breakpoint: breakpoint,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateActiveBreakpoint(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockController2.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestController2UpdateActiveBreakpointError(t *testing.T) {
	errCode := codes.Internal
	mockController2.err = grpc.Errorf(errCode, "test error")

	var debuggeeId string = "debuggeeId-997255898"
	var breakpoint *clouddebuggerpb.Breakpoint = &clouddebuggerpb.Breakpoint{}
	var request = &clouddebuggerpb.UpdateActiveBreakpointRequest{
		DebuggeeId: debuggeeId,
		Breakpoint: breakpoint,
	}

	c, err := NewController2Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateActiveBreakpoint(context.Background(), request)

	if c := grpc.Code(err); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
