// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcmysqlctlclient contains the gRPC1 version of the mysqlctl
// client protocol.
package grpcmysqlctlclient

import (
	"net"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/mysqlctl/mysqlctlclient"

	mysqlctlpb "github.com/youtube/vitess/go/vt/proto/mysqlctl"
)

type client struct {
	cc *grpc.ClientConn
	c  mysqlctlpb.MysqlCtlClient
}

func factory(network, addr string, dialTimeout time.Duration) (mysqlctlclient.MysqlctlClient, error) {
	// create the RPC client
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(dialTimeout), grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout(network, addr, timeout)
	}))
	if err != nil {
		return nil, err
	}
	c := mysqlctlpb.NewMysqlCtlClient(cc)

	return &client{
		cc: cc,
		c:  c,
	}, nil
}

// Start is part of the MysqlctlClient interface.
func (c *client) Start(ctx context.Context, mysqldArgs ...string) error {
	_, err := c.c.Start(ctx, &mysqlctlpb.StartRequest{
		MysqldArgs: mysqldArgs,
	})
	return err
}

// Shutdown is part of the MysqlctlClient interface.
func (c *client) Shutdown(ctx context.Context, waitForMysqld bool) error {
	_, err := c.c.Shutdown(ctx, &mysqlctlpb.ShutdownRequest{
		WaitForMysqld: waitForMysqld,
	})
	return err
}

// RunMysqlUpgrade is part of the MysqlctlClient interface.
func (c *client) RunMysqlUpgrade(ctx context.Context) error {
	_, err := c.c.RunMysqlUpgrade(ctx, &mysqlctlpb.RunMysqlUpgradeRequest{})
	return err
}

// ReinitConfig is part of the MysqlctlClient interface.
func (c *client) ReinitConfig(ctx context.Context) error {
	_, err := c.c.ReinitConfig(ctx, &mysqlctlpb.ReinitConfigRequest{})
	return err
}

// Close is part of the MysqlctlClient interface.
func (c *client) Close() {
	c.cc.Close()
}

func init() {
	mysqlctlclient.RegisterFactory("grpc", factory)
}
