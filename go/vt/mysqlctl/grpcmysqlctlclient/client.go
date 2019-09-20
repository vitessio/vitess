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

// Package grpcmysqlctlclient contains the gRPC1 version of the mysqlctl
// client protocol.
package grpcmysqlctlclient

import (
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/mysqlctl/mysqlctlclient"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
)

type client struct {
	cc *grpc.ClientConn
	c  mysqlctlpb.MysqlCtlClient
}

func factory(network, addr string) (mysqlctlclient.MysqlctlClient, error) {
	// create the RPC client
	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), grpc.WithInsecure(), grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
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
	return c.withRetry(ctx, func() error {
		_, err := c.c.Start(ctx, &mysqlctlpb.StartRequest{
			MysqldArgs: mysqldArgs,
		})
		return err
	})
}

// Shutdown is part of the MysqlctlClient interface.
func (c *client) Shutdown(ctx context.Context, waitForMysqld bool) error {
	return c.withRetry(ctx, func() error {
		_, err := c.c.Shutdown(ctx, &mysqlctlpb.ShutdownRequest{
			WaitForMysqld: waitForMysqld,
		})
		return err
	})
}

// RunMysqlUpgrade is part of the MysqlctlClient interface.
func (c *client) RunMysqlUpgrade(ctx context.Context) error {
	return c.withRetry(ctx, func() error {
		_, err := c.c.RunMysqlUpgrade(ctx, &mysqlctlpb.RunMysqlUpgradeRequest{})
		return err
	})
}

// ReinitConfig is part of the MysqlctlClient interface.
func (c *client) ReinitConfig(ctx context.Context) error {
	return c.withRetry(ctx, func() error {
		_, err := c.c.ReinitConfig(ctx, &mysqlctlpb.ReinitConfigRequest{})
		return err
	})
}

// RefreshConfig is part of the MysqlctlClient interface.
func (c *client) RefreshConfig(ctx context.Context) error {
	return c.withRetry(ctx, func() error {
		_, err := c.c.RefreshConfig(ctx, &mysqlctlpb.RefreshConfigRequest{})
		return err
	})
}

// Close is part of the MysqlctlClient interface.
func (c *client) Close() {
	c.cc.Close()
}

// withRetry is needed because grpc doesn't handle some transient errors
// correctly (like EAGAIN) when sockets are used.
func (c *client) withRetry(ctx context.Context, f func() error) error {
	var lastError error
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%v: %v", ctx.Err(), lastError)
		default:
		}
		if err := f(); err != nil {
			if st, ok := status.FromError(err); ok {
				code := st.Code()
				if code == codes.Unavailable {
					lastError = err
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
			return err
		}
		return nil
	}
}

func init() {
	mysqlctlclient.RegisterFactory("grpc", factory)
}
