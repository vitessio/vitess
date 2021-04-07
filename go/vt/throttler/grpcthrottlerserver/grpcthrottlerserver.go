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

// Package grpcthrottlerserver contains the gRPC implementation of the server
// side of the throttler service.
package grpcthrottlerserver

import (
	"context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/throttler"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	throttlerservicepb "vitess.io/vitess/go/vt/proto/throttlerservice"
)

// Server is the gRPC server implementation of the Throttler service.
type Server struct {
	manager throttler.Manager
}

// NewServer creates a new RPC server for a given throttler manager.
func NewServer(m throttler.Manager) *Server {
	return &Server{m}
}

// MaxRates implements the gRPC server interface. It returns the current max
// rate for each throttler of the process.
func (s *Server) MaxRates(_ context.Context, request *throttlerdatapb.MaxRatesRequest) (_ *throttlerdatapb.MaxRatesResponse, err error) {
	defer servenv.HandlePanic("throttler", &err)

	rates := s.manager.MaxRates()
	return &throttlerdatapb.MaxRatesResponse{
		Rates: rates,
	}, nil
}

// SetMaxRate implements the gRPC server interface. It sets the rate on all
// throttlers controlled by the manager.
func (s *Server) SetMaxRate(_ context.Context, request *throttlerdatapb.SetMaxRateRequest) (_ *throttlerdatapb.SetMaxRateResponse, err error) {
	defer servenv.HandlePanic("throttler", &err)

	names := s.manager.SetMaxRate(request.Rate)
	return &throttlerdatapb.SetMaxRateResponse{
		Names: names,
	}, nil
}

// GetConfiguration implements the gRPC server interface.
func (s *Server) GetConfiguration(_ context.Context, request *throttlerdatapb.GetConfigurationRequest) (_ *throttlerdatapb.GetConfigurationResponse, err error) {
	defer servenv.HandlePanic("throttler", &err)

	configurations, err := s.manager.GetConfiguration(request.ThrottlerName)
	if err != nil {
		return nil, err
	}
	return &throttlerdatapb.GetConfigurationResponse{
		Configurations: configurations,
	}, nil
}

// UpdateConfiguration implements the gRPC server interface.
func (s *Server) UpdateConfiguration(_ context.Context, request *throttlerdatapb.UpdateConfigurationRequest) (_ *throttlerdatapb.UpdateConfigurationResponse, err error) {
	defer servenv.HandlePanic("throttler", &err)

	names, err := s.manager.UpdateConfiguration(request.ThrottlerName, request.Configuration, request.CopyZeroValues)
	if err != nil {
		return nil, err
	}
	return &throttlerdatapb.UpdateConfigurationResponse{
		Names: names,
	}, nil
}

// ResetConfiguration implements the gRPC server interface.
func (s *Server) ResetConfiguration(_ context.Context, request *throttlerdatapb.ResetConfigurationRequest) (_ *throttlerdatapb.ResetConfigurationResponse, err error) {
	defer servenv.HandlePanic("throttler", &err)

	names, err := s.manager.ResetConfiguration(request.ThrottlerName)
	if err != nil {
		return nil, err
	}
	return &throttlerdatapb.ResetConfigurationResponse{
		Names: names,
	}, nil
}

// RegisterServer registers a new throttler server instance with the gRPC server.
func RegisterServer(s *grpc.Server, m throttler.Manager) {
	throttlerservicepb.RegisterThrottlerServer(s, NewServer(m))
}

func init() {
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("throttler") {
			RegisterServer(servenv.GRPCServer, throttler.GlobalManager)
		}
	})
}
