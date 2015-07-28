// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtctlclient contains the go rpc version of the vtctl client protocol
package gorpcvtctlclient

import (
	"fmt"
	"time"

	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/gorpcproto"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"
)

type goRPCVtctlClient struct {
	rpcClient *rpc.Client
}

func goRPCVtctlClientFactory(addr string, dialTimeout time.Duration) (vtctlclient.VtctlClient, error) {
	// create the RPC client
	rpcClient, err := bsonrpc.DialHTTP("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("RPC error for %v: %v", addr, err)
	}

	return &goRPCVtctlClient{rpcClient}, nil
}

// ExecuteVtctlCommand is part of the VtctlClient interface.
// Note the bson rpc version doesn't honor timeouts in the context
// (but the server side will honor the actionTimeout)
func (client *goRPCVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, vtctlclient.ErrFunc, error) {
	req := &gorpcproto.ExecuteVtctlCommandArgs{
		Args:          args,
		ActionTimeout: actionTimeout,
		LockTimeout:   lockTimeout,
	}
	sr := make(chan *logutil.LoggerEvent, 10)
	c := client.rpcClient.StreamGo("VtctlServer.ExecuteVtctlCommand", req, sr)
	return sr, func() error { return c.Error }, nil
}

// Close is part of the VtctlClient interface
func (client *goRPCVtctlClient) Close() {
	client.rpcClient.Close()
}

func init() {
	vtctlclient.RegisterFactory("gorpc", goRPCVtctlClientFactory)
}
