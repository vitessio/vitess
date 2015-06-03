// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcmysqlctlclient contains the go rpc version of the mysqlctl
// client protocol.
package gorpcmysqlctlclient

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/mysqlctl/mysqlctlclient"
	"github.com/youtube/vitess/go/vt/rpc"
)

type goRPCMysqlctlClient struct {
	rpcClient *rpcplus.Client
}

func goRPCMysqlctlClientFactory(network, addr string, dialTimeout time.Duration) (mysqlctlclient.MysqlctlClient, error) {
	// create the RPC client
	rpcClient, err := bsonrpc.DialHTTP(network, addr, dialTimeout, nil)
	if err != nil {
		return nil, fmt.Errorf("RPC error for %v: %v", addr, err)
	}

	return &goRPCMysqlctlClient{rpcClient}, nil
}

// Start is part of the MysqlctlClient interface.
func (c *goRPCMysqlctlClient) Start(mysqlWaitTime time.Duration) error {
	return c.rpcClient.Call(context.TODO(), "MysqlctlServer.Start", &mysqlWaitTime, &rpc.Unused{})
}

// Shutdown is part of the MysqlctlClient interface.
func (c *goRPCMysqlctlClient) Shutdown(waitForMysqld bool, mysqlWaitTime time.Duration) error {
	if !waitForMysqld {
		mysqlWaitTime = 0
	}
	return c.rpcClient.Call(context.TODO(), "MysqlctlServer.Shutdown", &mysqlWaitTime, &rpc.Unused{})
}

// RunMysqlUpgrade is part of the MysqlctlClient interface.
func (c *goRPCMysqlctlClient) RunMysqlUpgrade() error {
	return c.rpcClient.Call(context.TODO(), "MysqlctlServer.RunMysqlUpgrade", &rpc.Unused{}, &rpc.Unused{})
}

// Close is part of the MysqlctlClient interface.
func (c *goRPCMysqlctlClient) Close() {
	c.rpcClient.Close()
}

func init() {
	mysqlctlclient.RegisterFactory("gorpc", goRPCMysqlctlClientFactory)
}
