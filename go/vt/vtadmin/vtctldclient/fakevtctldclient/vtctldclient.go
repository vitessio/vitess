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

package fakevtctldclient

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// VtctldClient provides a partial mock implementation of the
// vtctldclient.VtctldClient interface for use in testing.
type VtctldClient struct {
	vtctldclient.VtctldClient

	CreateKeyspaceShouldErr bool
	CreateShardShouldErr    bool
	DeleteKeyspaceShouldErr bool
	// Keyed by _sorted_ <ks/shard> list string joined by commas.
	DeleteShardsResults map[string]error
	// Keyed by _sorted_ TabletAlias list string joined by commas.
	DeleteTabletsResults          map[string]error
	EmergencyReparentShardResults map[string]struct {
		Response *vtctldatapb.EmergencyReparentShardResponse
		Error    error
	}
	FindAllShardsInKeyspaceResults map[string]struct {
		Response *vtctldatapb.FindAllShardsInKeyspaceResponse
		Error    error
	}
	GetBackupsResults map[string]struct {
		Response *vtctldatapb.GetBackupsResponse
		Error    error
	}
	GetCellInfoNamesResults *struct {
		Response *vtctldatapb.GetCellInfoNamesResponse
		Error    error
	}
	GetCellInfoResults map[string]struct {
		Response *vtctldatapb.GetCellInfoResponse
		Error    error
	}
	GetCellsAliasesResults *struct {
		Response *vtctldatapb.GetCellsAliasesResponse
		Error    error
	}
	GetKeyspaceResults map[string]struct {
		Response *vtctldatapb.GetKeyspaceResponse
		Error    error
	}
	GetKeyspacesResults *struct {
		Keyspaces []*vtctldatapb.Keyspace
		Error     error
	}
	GetSchemaResults map[string]struct {
		Response *vtctldatapb.GetSchemaResponse
		Error    error
	}
	GetSrvVSchemaResults map[string]struct {
		Response *vtctldatapb.GetSrvVSchemaResponse
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
	PingTabletResults           map[string]error
	PlannedReparentShardResults map[string]struct {
		Response *vtctldatapb.PlannedReparentShardResponse
		Error    error
	}
	RefreshStateResults         map[string]error
	ReloadSchemaKeyspaceResults map[string]struct {
		Response *vtctldatapb.ReloadSchemaKeyspaceResponse
		Error    error
	}
	ReloadSchemaResults map[string]struct {
		Response *vtctldatapb.ReloadSchemaResponse
		Error    error
	}
	ReloadSchemaShardResults map[string]struct {
		Response *vtctldatapb.ReloadSchemaShardResponse
		Error    error
	}
	ReparentTabletResults map[string]struct {
		Response *vtctldatapb.ReparentTabletResponse
		Error    error
	}
	RunHealthCheckResults            map[string]error
	SetWritableResults               map[string]error
	ShardReplicationPositionsResults map[string]struct {
		Response *vtctldatapb.ShardReplicationPositionsResponse
		Error    error
	}
	StartReplicationResults           map[string]error
	StopReplicationResults            map[string]error
	TabletExternallyReparentedResults map[string]struct {
		Response *vtctldatapb.TabletExternallyReparentedResponse
		Error    error
	}
	ValidateKeyspaceResults map[string]struct {
		Response *vtctldatapb.ValidateKeyspaceResponse
		Error    error
	}
	ValidateSchemaKeyspaceResults map[string]struct {
		Response *vtctldatapb.ValidateSchemaKeyspaceResponse
		Error    error
	}
	ValidateVersionKeyspaceResults map[string]struct {
		Response *vtctldatapb.ValidateVersionKeyspaceResponse
		Error    error
	}
}

// Compile-time type assertion to make sure we haven't overriden a method
// incorrectly.
var _ vtctldclient.VtctldClient = (*VtctldClient)(nil)

// Close is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) Close() error { return nil }

// CreateKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) CreateKeyspace(ctx context.Context, req *vtctldatapb.CreateKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.CreateKeyspaceResponse, error) {
	if fake.CreateKeyspaceShouldErr {
		return nil, fmt.Errorf("%w: CreateKeyspace error", assert.AnError)
	}

	ks := &topodatapb.Keyspace{
		ServedFroms:  req.ServedFroms,
		KeyspaceType: req.Type,
		BaseKeyspace: req.BaseKeyspace,
		SnapshotTime: req.SnapshotTime,
	}

	return &vtctldatapb.CreateKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     req.Name,
			Keyspace: ks,
		},
	}, nil
}

// CreateShard is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) CreateShard(cxt context.Context, req *vtctldatapb.CreateShardRequest, opts ...grpc.CallOption) (*vtctldatapb.CreateShardResponse, error) {
	if fake.CreateShardShouldErr {
		return nil, fmt.Errorf("%w: CreateShard error", assert.AnError)
	}

	return &vtctldatapb.CreateShardResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     req.Keyspace,
			Keyspace: &topodatapb.Keyspace{},
		},
		Shard: &vtctldatapb.Shard{
			Keyspace: req.Keyspace,
			Name:     req.ShardName,
			Shard:    &topodatapb.Shard{},
		},
	}, nil
}

// DeleteKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) DeleteKeyspace(ctx context.Context, req *vtctldatapb.DeleteKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.DeleteKeyspaceResponse, error) {
	if fake.DeleteKeyspaceShouldErr {
		return nil, fmt.Errorf("%w: DeleteKeyspace error", assert.AnError)
	}

	return &vtctldatapb.DeleteKeyspaceResponse{}, nil
}

// DeleteShards is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) DeleteShards(ctx context.Context, req *vtctldatapb.DeleteShardsRequest, opts ...grpc.CallOption) (*vtctldatapb.DeleteShardsResponse, error) {
	if fake.DeleteShardsResults == nil {
		return nil, fmt.Errorf("%w: DeleteShardsResults not set on fake vtctldclient", assert.AnError)
	}

	shards := make([]string, len(req.Shards))
	for i, shard := range req.Shards {
		shards[i] = fmt.Sprintf("%s/%s", shard.Keyspace, shard.Name)
	}
	sort.Strings(shards)
	key := strings.Join(shards, ",")

	if err, ok := fake.DeleteShardsResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.DeleteShardsResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// DeleteTablets is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) DeleteTablets(ctx context.Context, req *vtctldatapb.DeleteTabletsRequest, opts ...grpc.CallOption) (*vtctldatapb.DeleteTabletsResponse, error) {
	if fake.DeleteTabletsResults == nil {
		return nil, fmt.Errorf("%w: DeleteTabletsResults not set on fake vtctldclient", assert.AnError)
	}

	aliases := topoproto.TabletAliasList(req.TabletAliases)
	sort.Sort(aliases)
	key := strings.Join(aliases.ToStringSlice(), ",")

	if err, ok := fake.DeleteTabletsResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.DeleteTabletsResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// EmergencyReparentShard is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) EmergencyReparentShard(ctx context.Context, req *vtctldatapb.EmergencyReparentShardRequest, opts ...grpc.CallOption) (*vtctldatapb.EmergencyReparentShardResponse, error) {
	if fake.EmergencyReparentShardResults == nil {
		return nil, fmt.Errorf("%w: EmergencyReparentShardResults not set on fake vtctldclient", assert.AnError)
	}

	key := fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)
	if result, ok := fake.EmergencyReparentShardResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

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

// GetBackups is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetBackups(ctx context.Context, req *vtctldatapb.GetBackupsRequest, opts ...grpc.CallOption) (*vtctldatapb.GetBackupsResponse, error) {
	if fake.GetBackupsResults == nil {
		return nil, fmt.Errorf("%w: GetBackupsResults not set on fake vtctldclient", assert.AnError)
	}

	key := fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)
	if result, ok := fake.GetBackupsResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// GetCellInfo is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetCellInfo(ctx context.Context, req *vtctldatapb.GetCellInfoRequest, opts ...grpc.CallOption) (*vtctldatapb.GetCellInfoResponse, error) {
	if fake.GetCellInfoResults == nil {
		return nil, fmt.Errorf("%w: GetCellInfoResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.GetCellInfoResults[req.Cell]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: GetCellInfoResults no result set for %s", assert.AnError, req.Cell)
}

// GetCellInfoNames is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetCellInfoNames(ctx context.Context, req *vtctldatapb.GetCellInfoNamesRequest, opts ...grpc.CallOption) (*vtctldatapb.GetCellInfoNamesResponse, error) {
	if fake.GetCellInfoNamesResults == nil {
		return nil, fmt.Errorf("%w: GetCellInfoNamesResults not set on fake vtctldclient", assert.AnError)
	}

	return fake.GetCellInfoNamesResults.Response, fake.GetCellInfoNamesResults.Error
}

// GetCellsAliases is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetCellsAliases(ctx context.Context, req *vtctldatapb.GetCellsAliasesRequest, opts ...grpc.CallOption) (*vtctldatapb.GetCellsAliasesResponse, error) {
	if fake.GetCellsAliasesResults == nil {
		return nil, fmt.Errorf("%w: GetCellsAliasesResults not set on fake vtctldclient", assert.AnError)
	}

	return fake.GetCellsAliasesResults.Response, fake.GetCellsAliasesResults.Error
}

// GetKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.GetKeyspaceResponse, error) {
	if fake.GetKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: GetKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.GetKeyspaceResults[req.Keyspace]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for keyspace %s", assert.AnError, req.Keyspace)
}

// GetKeyspaces is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetKeyspaces(ctx context.Context, req *vtctldatapb.GetKeyspacesRequest, opts ...grpc.CallOption) (*vtctldatapb.GetKeyspacesResponse, error) {
	if fake.GetKeyspacesResults == nil {
		return nil, fmt.Errorf("%w: GetKeyspacesResults not set on fake vtctldclient", assert.AnError)
	}

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

// GetSrvVSchema is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetSrvVSchema(ctx context.Context, req *vtctldatapb.GetSrvVSchemaRequest, opts ...grpc.CallOption) (*vtctldatapb.GetSrvVSchemaResponse, error) {
	if fake.GetSrvVSchemaResults == nil {
		return nil, fmt.Errorf("%w: GetSrvVSchemaResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.GetSrvVSchemaResults[req.Cell]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for key %s", assert.AnError, req.Cell)
}

// GetSrvVSchemas is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetSrvVSchemas(ctx context.Context, req *vtctldatapb.GetSrvVSchemasRequest, opts ...grpc.CallOption) (resp *vtctldatapb.GetSrvVSchemasResponse, err error) {
	resp = &vtctldatapb.GetSrvVSchemasResponse{
		SrvVSchemas: map[string]*vschemapb.SrvVSchema{},
	}

	cells := req.Cells
	if len(req.Cells) == 0 {
		for cell := range fake.GetSrvVSchemaResults {
			cells = append(cells, cell)
		}
	}

	for _, cell := range cells {
		r, err := fake.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: cell}, opts...)
		if err != nil {
			return nil, err
		}

		resp.SrvVSchemas[cell] = r.SrvVSchema
	}

	return resp, nil
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

// PingTablet is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) PingTablet(ctx context.Context, req *vtctldatapb.PingTabletRequest, opts ...grpc.CallOption) (*vtctldatapb.PingTabletResponse, error) {
	if fake.PingTabletResults == nil {
		return nil, fmt.Errorf("%w: PingTabletResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.PingTabletResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.PingTabletResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// PlannedReparentShard is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) PlannedReparentShard(ctx context.Context, req *vtctldatapb.PlannedReparentShardRequest, opts ...grpc.CallOption) (*vtctldatapb.PlannedReparentShardResponse, error) {
	if fake.PlannedReparentShardResults == nil {
		return nil, fmt.Errorf("%w: PlannedReparentShardResults not set on fake vtctldclient", assert.AnError)
	}

	key := fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)
	if result, ok := fake.PlannedReparentShardResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// RefreshState is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) RefreshState(ctx context.Context, req *vtctldatapb.RefreshStateRequest, opts ...grpc.CallOption) (*vtctldatapb.RefreshStateResponse, error) {
	if fake.RefreshStateResults == nil {
		return nil, fmt.Errorf("%w: RefreshStateResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.RefreshStateResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.RefreshStateResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ReloadSchema is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ReloadSchema(ctx context.Context, req *vtctldatapb.ReloadSchemaRequest, opts ...grpc.CallOption) (*vtctldatapb.ReloadSchemaResponse, error) {
	if fake.ReloadSchemaResults == nil {
		return nil, fmt.Errorf("%w: ReloadSchemaResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if result, ok := fake.ReloadSchemaResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ReloadSchemaKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ReloadSchemaKeyspace(ctx context.Context, req *vtctldatapb.ReloadSchemaKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.ReloadSchemaKeyspaceResponse, error) {
	if fake.ReloadSchemaKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: ReloadSchemaKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	if result, ok := fake.ReloadSchemaKeyspaceResults[req.Keyspace]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, req.Keyspace)
}

// ReloadSchemaShard is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ReloadSchemaShard(ctx context.Context, req *vtctldatapb.ReloadSchemaShardRequest, opts ...grpc.CallOption) (*vtctldatapb.ReloadSchemaShardResponse, error) {
	if fake.ReloadSchemaShardResults == nil {
		return nil, fmt.Errorf("%w: ReloadSchemaShardResults not set on fake vtctldclient", assert.AnError)
	}

	key := fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)
	if result, ok := fake.ReloadSchemaShardResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ReparentTablet is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ReparentTablet(ctx context.Context, req *vtctldatapb.ReparentTabletRequest, opts ...grpc.CallOption) (*vtctldatapb.ReparentTabletResponse, error) {
	if fake.ReparentTabletResults == nil {
		return nil, fmt.Errorf("%w: ReparentTabletResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.Tablet)
	if result, ok := fake.ReparentTabletResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// RunHealthCheck is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) RunHealthCheck(ctx context.Context, req *vtctldatapb.RunHealthCheckRequest, opts ...grpc.CallOption) (*vtctldatapb.RunHealthCheckResponse, error) {
	if fake.RunHealthCheckResults == nil {
		return nil, fmt.Errorf("%w: RunHealthCheckResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.RunHealthCheckResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.RunHealthCheckResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// SetWritable is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) SetWritable(ctx context.Context, req *vtctldatapb.SetWritableRequest, opts ...grpc.CallOption) (*vtctldatapb.SetWritableResponse, error) {
	if fake.SetWritableResults == nil {
		return nil, fmt.Errorf("%w: SetWritableResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.SetWritableResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.SetWritableResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ShardReplicationPositions is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ShardReplicationPositions(ctx context.Context, req *vtctldatapb.ShardReplicationPositionsRequest, opts ...grpc.CallOption) (*vtctldatapb.ShardReplicationPositionsResponse, error) {
	if fake.ShardReplicationPositionsResults == nil {
		return nil, fmt.Errorf("%w: ShardReplicationPositionsResults not set on fake vtctldclient", assert.AnError)
	}

	key := fmt.Sprintf("%s/%s", req.Keyspace, req.Shard)
	if result, ok := fake.ShardReplicationPositionsResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// StartReplication is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) StartReplication(ctx context.Context, req *vtctldatapb.StartReplicationRequest, opts ...grpc.CallOption) (*vtctldatapb.StartReplicationResponse, error) {
	if fake.StartReplicationResults == nil {
		return nil, fmt.Errorf("%w: StartReplicationResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.StartReplicationResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.StartReplicationResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// StopReplication is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) StopReplication(ctx context.Context, req *vtctldatapb.StopReplicationRequest, opts ...grpc.CallOption) (*vtctldatapb.StopReplicationResponse, error) {
	if fake.StopReplicationResults == nil {
		return nil, fmt.Errorf("%w: StopReplicationResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)
	if err, ok := fake.StopReplicationResults[key]; ok {
		if err != nil {
			return nil, err
		}

		return &vtctldatapb.StopReplicationResponse{}, nil
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// TabletExternallyReparented is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) TabletExternallyReparented(ctx context.Context, req *vtctldatapb.TabletExternallyReparentedRequest, opts ...grpc.CallOption) (*vtctldatapb.TabletExternallyReparentedResponse, error) {
	if fake.TabletExternallyReparentedResults == nil {
		return nil, fmt.Errorf("%w: TabletExternallyReparentedResults not set on fake vtctldclient", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.Tablet)
	if result, ok := fake.TabletExternallyReparentedResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ValidateKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ValidateKeyspace(ctx context.Context, req *vtctldatapb.ValidateKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.ValidateKeyspaceResponse, error) {
	if fake.ValidateKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: ValidateKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	key := req.Keyspace
	if result, ok := fake.ValidateKeyspaceResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ValidateSchemaKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ValidateSchemaKeyspace(ctx context.Context, req *vtctldatapb.ValidateSchemaKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error) {
	if fake.ValidateSchemaKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: ValidateSchemaKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	key := req.Keyspace
	if result, ok := fake.ValidateSchemaKeyspaceResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}

// ValidateVersionKeyspace is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) ValidateVersionKeyspace(ctx context.Context, req *vtctldatapb.ValidateVersionKeyspaceRequest, opts ...grpc.CallOption) (*vtctldatapb.ValidateVersionKeyspaceResponse, error) {
	if fake.ValidateVersionKeyspaceResults == nil {
		return nil, fmt.Errorf("%w: ValidateVersionKeyspaceResults not set on fake vtctldclient", assert.AnError)
	}

	key := req.Keyspace
	if result, ok := fake.ValidateVersionKeyspaceResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for %s", assert.AnError, key)
}
