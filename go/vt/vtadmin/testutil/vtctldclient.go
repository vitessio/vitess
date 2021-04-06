/*
Copyright 2021 The Vitess Authors.

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

package testutil

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// VtctldClient provides a partial mock implementation of the
// vtctldclient.VtctldClient interface for use in testing.
type VtctldClient struct {
	vtctldclient.VtctldClient

	FindAllShardsInKeyspaceResults map[string]struct {
		Response *vtctldatapb.FindAllShardsInKeyspaceResponse
		Error    error
	}
	GetKeyspacesResults struct {
		Keyspaces []*vtctldatapb.Keyspace
		Error     error
	}
	GetSchemaResults map[string]struct {
		Response *vtctldatapb.GetSchemaResponse
		Error    error
	}
	GetVSchemaResults map[string]struct {
		Response *vtctldatapb.GetVSchemaResponse
		Error    error
	}
	GetWorkflowsResults map[string]struct {
		Response *vtctldatapb.GetWorkflowsResponse
		Error    error
	}
}

// Compile-time type assertion to make sure we haven't overriden a method
// incorrectly.
var _ vtctldclient.VtctldClient = (*VtctldClient)(nil)

// FindAllShardsInKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) FindAllShardsInKeyspace(ctx context.Context, req *vtctldatapb.FindAllShardsInKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.FindAllShardsInKeyspaceResponse, error) {
	if fake.FindAllShardsInKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: FindAllShardsInKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.FindAllShardsInKeyspaceResults[req.Keyspace]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for keyspace %s", assert.AnError, req.Keyspace)
}

// GetKeyspaces is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest, opts ...grpc.CallOption) (*vtctldatapb.GetKeyspacesResponse, error) {
	if fake.GetKeyspacesResults.Error != nil {
		return nil, fake.GetKeyspacesResults.Error
	}

	return &vtctldatapb.GetKeyspacesResponse{
		Keyspaces: fake.GetKeyspacesResults.Keyspaces,
	}, nil
}

// GetSchema is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetSchema(ctx context.Context, req *vtctldatapb.GetSchemaRequest, opts ...grpc.CallOption) (*vtctldatapb.GetSchemaResponse, error) {
	if fake.GetSchemaResults == nil {
		return nil, fmt.Errorf("%w: GetSchemaResults not set on fake vtctldclient", assert.AnError)
	}

	if req.TabletAlias == nil {
		return nil, fmt.Errorf("%w: req.TabletAlias == nil", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)

	if result, ok := fake.GetSchemaResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for tablet alias %s", assert.AnError, key)
}

// GetVSchema is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetVSchema(ctx context.Context, req *vtctldatapb.GetVSchemaRequest, opts ...grpc.CallOption) (*vtctldatapb.GetVSchemaResponse, error) {
	if fake.GetVSchemaResults == nil {
		return nil, fmt.Errorf("%w: GetVSchemaResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.GetVSchemaResults[req.Keyspace]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for keyspace %s", assert.AnError, req.Keyspace)
}

// GetWorkflows is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest, opts ...grpc.CallOption) (*vtctldatapb.GetWorkflowsResponse, error) {
	if fake.GetWorkflowsResults == nil {
		return nil, fmt.Errorf("%w: GetWorkflowsResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.GetWorkflowsResults[req.Keyspace]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for keyspace %s", assert.AnError, req.Keyspace)
}
