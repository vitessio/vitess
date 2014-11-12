// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcmysqlctlclient contains the go rpc version of the mysqlctl
// client protocol.
package gorpcmysqlctlclient

import (
	"fmt"
	"time"

	"code.google.com/p/go.net/context"

	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/mysqlctl/mysqlctlclient"
)

type goRpcMysqlctlClient struct {
	rpcClient *rpc.Client
}

func goRpcMysqlctlClientFactory(network, addr string, dialTimeout time.Duration) (mysqlctlclient.MysqlctlClient, error) {
	// create the RPC client
	rpcClient, err := bsonrpc.DialHTTP(network, addr, dialTimeout, nil)
	if err != nil {
		return nil, fmt.Errorf("RPC error for %v: %v", addr, err)
	}

	return &goRpcMysqlctlClient{rpcClient}, nil
}

// Start is part of the MysqlctlClient interface.
func (c *goRpcMysqlctlClient) Start(mysqlWaitTime time.Duration) error {
	return c.rpcClient.Call(context.TODO(), "MysqlctlServer.Start", &mysqlWaitTime, nil)
}

// Shutdown is part of the MysqlctlClient interface.
func (c *goRpcMysqlctlClient) Shutdown(waitForMysqld bool, mysqlWaitTime time.Duration) error {
	if !waitForMysqld {
		mysqlWaitTime = 0
	}
	return c.rpcClient.Call(context.TODO(), "MysqlctlServer.Shutdown", &mysqlWaitTime, nil)
}

// Close is part of the MysqlctlClient interface.
func (client *goRpcMysqlctlClient) Close() {
	client.rpcClient.Close()
}

func init() {
	mysqlctlclient.RegisterMysqlctlClientFactory("gorpc", goRpcMysqlctlClientFactory)
}
