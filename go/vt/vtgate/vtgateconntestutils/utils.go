// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateconntestutils provides util methods to ease of
// writing unit tests for components that rely on vtgate server
package vtgateconntestutils

import (
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/vtgate/gorpcvtgateservice"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconntest"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
)

// VtGateServerParam stores information that best describe VtGate server
type VtGateServerParam struct {
	listener net.Listener
}

// Addr returns vtgate address
func (param *VtGateServerParam) Addr() string {
	return param.listener.Addr().String()
}

// StartFakeVtGateServer starts a fake vtgate server for testing
func StartFakeVtGateServer(t *testing.T) (vtgateservice.VTGateService, *VtGateServerParam) {
	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	// fake service
	service := vtgateconntest.CreateFakeServer(t)
	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	server.Register(gorpcvtgateservice.New(service))
	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server, false)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)
	return service, &VtGateServerParam{listener}
}

// StopFakeVtGateServer stop a runnning fake vtgate server
func StopFakeVtGateServer(service vtgateservice.VTGateService, param *VtGateServerParam) {
	param.listener.Close()
}
