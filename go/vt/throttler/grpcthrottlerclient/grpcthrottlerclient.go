// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcthrottlerclient contains the gRPC version of the throttler client protocol.
package grpcthrottlerclient

import (
	"flag"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
	"github.com/youtube/vitess/go/vt/proto/throttlerservice"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/throttler/throttlerclient"
	"github.com/youtube/vitess/go/vt/vterrors"
	"google.golang.org/grpc"
)

var (
	cert = flag.String("throttler_client_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("throttler_client_grpc_key", "", "the key to use to connect")
	ca   = flag.String("throttler_client_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("throttler_client_grpc_server_name", "", "the server name to use to validate server certificate")
)

type client struct {
	conn       *grpc.ClientConn
	gRPCClient throttlerservice.ThrottlerClient
}

func factory(addr string) (throttlerclient.Client, error) {
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(addr, opt)
	if err != nil {
		return nil, err
	}
	gRPCClient := throttlerservice.NewThrottlerClient(conn)

	return &client{conn, gRPCClient}, nil
}

// MaxRates is part of the throttlerclient.Client interface and returns the
// current max rate for each throttler of the process.
func (c *client) MaxRates(ctx context.Context) (map[string]int64, error) {
	response, err := c.gRPCClient.MaxRates(ctx, &throttlerdata.MaxRatesRequest{})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Rates, nil
}

// SetMaxRate is part of the throttlerclient.Client interface and sets the rate
// on all throttlers of the server.
func (c *client) SetMaxRate(ctx context.Context, rate int64) ([]string, error) {
	request := &throttlerdata.SetMaxRateRequest{
		Rate: rate,
	}

	response, err := c.gRPCClient.SetMaxRate(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Names, nil
}

// GetConfiguration is part of the throttlerclient.Client interface.
func (c *client) GetConfiguration(ctx context.Context, throttlerName string) (map[string]*throttlerdata.Configuration, error) {
	response, err := c.gRPCClient.GetConfiguration(ctx, &throttlerdata.GetConfigurationRequest{
		ThrottlerName: throttlerName,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Configurations, nil
}

// UpdateConfiguration is part of the throttlerclient.Client interface.
func (c *client) UpdateConfiguration(ctx context.Context, throttlerName string, configuration *throttlerdata.Configuration, copyZeroValues bool) ([]string, error) {
	response, err := c.gRPCClient.UpdateConfiguration(ctx, &throttlerdata.UpdateConfigurationRequest{
		ThrottlerName:  throttlerName,
		Configuration:  configuration,
		CopyZeroValues: copyZeroValues,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Names, nil
}

// ResetConfiguration is part of the throttlerclient.Client interface.
func (c *client) ResetConfiguration(ctx context.Context, throttlerName string) ([]string, error) {
	response, err := c.gRPCClient.ResetConfiguration(ctx, &throttlerdata.ResetConfigurationRequest{
		ThrottlerName: throttlerName,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Names, nil
}

// Close is part of the throttlerclient.Client interface.
func (c *client) Close() {
	c.conn.Close()
}

func init() {
	throttlerclient.RegisterFactory("grpc", factory)
}
