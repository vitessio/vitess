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

// Package grpcthrottlerclient contains the gRPC version of the throttler client protocol.
package grpcthrottlerclient

import (
	"flag"

	"context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/throttler/throttlerclient"
	"vitess.io/vitess/go/vt/vterrors"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	throttlerservicepb "vitess.io/vitess/go/vt/proto/throttlerservice"
)

var (
	cert = flag.String("throttler_client_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("throttler_client_grpc_key", "", "the key to use to connect")
	ca   = flag.String("throttler_client_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("throttler_client_grpc_server_name", "", "the server name to use to validate server certificate")
)

type client struct {
	conn       *grpc.ClientConn
	gRPCClient throttlerservicepb.ThrottlerClient
}

func factory(addr string) (throttlerclient.Client, error) {
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	conn, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}
	gRPCClient := throttlerservicepb.NewThrottlerClient(conn)

	return &client{conn, gRPCClient}, nil
}

// MaxRates is part of the throttlerclient.Client interface and returns the
// current max rate for each throttler of the process.
func (c *client) MaxRates(ctx context.Context) (map[string]int64, error) {
	response, err := c.gRPCClient.MaxRates(ctx, &throttlerdatapb.MaxRatesRequest{})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Rates, nil
}

// SetMaxRate is part of the throttlerclient.Client interface and sets the rate
// on all throttlers of the server.
func (c *client) SetMaxRate(ctx context.Context, rate int64) ([]string, error) {
	request := &throttlerdatapb.SetMaxRateRequest{
		Rate: rate,
	}

	response, err := c.gRPCClient.SetMaxRate(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Names, nil
}

// GetConfiguration is part of the throttlerclient.Client interface.
func (c *client) GetConfiguration(ctx context.Context, throttlerName string) (map[string]*throttlerdatapb.Configuration, error) {
	response, err := c.gRPCClient.GetConfiguration(ctx, &throttlerdatapb.GetConfigurationRequest{
		ThrottlerName: throttlerName,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Configurations, nil
}

// UpdateConfiguration is part of the throttlerclient.Client interface.
func (c *client) UpdateConfiguration(ctx context.Context, throttlerName string, configuration *throttlerdatapb.Configuration, copyZeroValues bool) ([]string, error) {
	response, err := c.gRPCClient.UpdateConfiguration(ctx, &throttlerdatapb.UpdateConfigurationRequest{
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
	response, err := c.gRPCClient.ResetConfiguration(ctx, &throttlerdatapb.ResetConfigurationRequest{
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
