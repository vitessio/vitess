/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin2

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// vtAdminAPI defines exactly the subset of the vtadmin API that the
// server-rendered UI consumes. Declaring it here keeps vtadmin2 decoupled
// from the full generated VTAdminServer interface; vtadmin.API satisfies it.
type (
	vtAdminAPI interface {
		ApplySchema(ctx context.Context, req *vtadminpb.ApplySchemaRequest) (*vtctldatapb.ApplySchemaResponse, error)
		ConcludeTransaction(ctx context.Context, req *vtadminpb.ConcludeTransactionRequest) (*vtctldatapb.ConcludeTransactionResponse, error)
		CreateKeyspace(ctx context.Context, req *vtadminpb.CreateKeyspaceRequest) (*vtadminpb.CreateKeyspaceResponse, error)
		CreateShard(ctx context.Context, req *vtadminpb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error)
		DeleteShards(ctx context.Context, req *vtadminpb.DeleteShardsRequest) (*vtctldatapb.DeleteShardsResponse, error)
		DeleteTablet(ctx context.Context, req *vtadminpb.DeleteTabletRequest) (*vtadminpb.DeleteTabletResponse, error)
		EmergencyFailoverShard(ctx context.Context, req *vtadminpb.EmergencyFailoverShardRequest) (*vtadminpb.EmergencyFailoverShardResponse, error)
		GetBackups(ctx context.Context, req *vtadminpb.GetBackupsRequest) (*vtadminpb.GetBackupsResponse, error)
		GetCellInfos(ctx context.Context, req *vtadminpb.GetCellInfosRequest) (*vtadminpb.GetCellInfosResponse, error)
		GetCellsAliases(ctx context.Context, req *vtadminpb.GetCellsAliasesRequest) (*vtadminpb.GetCellsAliasesResponse, error)
		GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error)
		GetFullStatus(ctx context.Context, req *vtadminpb.GetFullStatusRequest) (*vtctldatapb.GetFullStatusResponse, error)
		GetGates(ctx context.Context, req *vtadminpb.GetGatesRequest) (*vtadminpb.GetGatesResponse, error)
		GetKeyspace(ctx context.Context, req *vtadminpb.GetKeyspaceRequest) (*vtadminpb.Keyspace, error)
		GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error)
		GetSchema(ctx context.Context, req *vtadminpb.GetSchemaRequest) (*vtadminpb.Schema, error)
		GetSchemaMigrations(ctx context.Context, req *vtadminpb.GetSchemaMigrationsRequest) (*vtadminpb.GetSchemaMigrationsResponse, error)
		GetSchemas(ctx context.Context, req *vtadminpb.GetSchemasRequest) (*vtadminpb.GetSchemasResponse, error)
		GetShardReplicationPositions(ctx context.Context, req *vtadminpb.GetShardReplicationPositionsRequest) (*vtadminpb.GetShardReplicationPositionsResponse, error)
		GetSrvKeyspaces(ctx context.Context, req *vtadminpb.GetSrvKeyspacesRequest) (*vtadminpb.GetSrvKeyspacesResponse, error)
		GetSrvVSchemas(ctx context.Context, req *vtadminpb.GetSrvVSchemasRequest) (*vtadminpb.GetSrvVSchemasResponse, error)
		GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error)
		GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error)
		GetTopologyPath(ctx context.Context, req *vtadminpb.GetTopologyPathRequest) (*vtctldatapb.GetTopologyPathResponse, error)
		GetTransactionInfo(ctx context.Context, req *vtadminpb.GetTransactionInfoRequest) (*vtctldatapb.GetTransactionInfoResponse, error)
		GetUnresolvedTransactions(ctx context.Context, req *vtadminpb.GetUnresolvedTransactionsRequest) (*vtctldatapb.GetUnresolvedTransactionsResponse, error)
		GetVSchema(ctx context.Context, req *vtadminpb.GetVSchemaRequest) (*vtadminpb.VSchema, error)
		GetVSchemas(ctx context.Context, req *vtadminpb.GetVSchemasRequest) (*vtadminpb.GetVSchemasResponse, error)
		GetVtctlds(ctx context.Context, req *vtadminpb.GetVtctldsRequest) (*vtadminpb.GetVtctldsResponse, error)
		GetWorkflow(ctx context.Context, req *vtadminpb.GetWorkflowRequest) (*vtadminpb.Workflow, error)
		GetWorkflows(ctx context.Context, req *vtadminpb.GetWorkflowsRequest) (*vtadminpb.GetWorkflowsResponse, error)
		GetWorkflowStatus(ctx context.Context, req *vtadminpb.GetWorkflowStatusRequest) (*vtctldatapb.WorkflowStatusResponse, error)
		MaterializeCreate(ctx context.Context, req *vtadminpb.MaterializeCreateRequest) (*vtctldatapb.MaterializeCreateResponse, error)
		MoveTablesComplete(ctx context.Context, req *vtadminpb.MoveTablesCompleteRequest) (*vtctldatapb.MoveTablesCompleteResponse, error)
		MoveTablesCreate(ctx context.Context, req *vtadminpb.MoveTablesCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error)
		PingTablet(ctx context.Context, req *vtadminpb.PingTabletRequest) (*vtadminpb.PingTabletResponse, error)
		PlannedFailoverShard(ctx context.Context, req *vtadminpb.PlannedFailoverShardRequest) (*vtadminpb.PlannedFailoverShardResponse, error)
		RebuildKeyspaceGraph(ctx context.Context, req *vtadminpb.RebuildKeyspaceGraphRequest) (*vtadminpb.RebuildKeyspaceGraphResponse, error)
		RefreshState(ctx context.Context, req *vtadminpb.RefreshStateRequest) (*vtadminpb.RefreshStateResponse, error)
		RefreshTabletReplicationSource(ctx context.Context, req *vtadminpb.RefreshTabletReplicationSourceRequest) (*vtadminpb.RefreshTabletReplicationSourceResponse, error)
		ReloadSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) (*vtadminpb.ReloadSchemasResponse, error)
		ReloadSchemaShard(ctx context.Context, req *vtadminpb.ReloadSchemaShardRequest) (*vtadminpb.ReloadSchemaShardResponse, error)
		RemoveKeyspaceCell(ctx context.Context, req *vtadminpb.RemoveKeyspaceCellRequest) (*vtadminpb.RemoveKeyspaceCellResponse, error)
		ReshardCreate(ctx context.Context, req *vtadminpb.ReshardCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error)
		RunHealthCheck(ctx context.Context, req *vtadminpb.RunHealthCheckRequest) (*vtadminpb.RunHealthCheckResponse, error)
		SetReadOnly(ctx context.Context, req *vtadminpb.SetReadOnlyRequest) (*vtadminpb.SetReadOnlyResponse, error)
		SetReadWrite(ctx context.Context, req *vtadminpb.SetReadWriteRequest) (*vtadminpb.SetReadWriteResponse, error)
		StartReplication(ctx context.Context, req *vtadminpb.StartReplicationRequest) (*vtadminpb.StartReplicationResponse, error)
		StartWorkflow(ctx context.Context, req *vtadminpb.StartWorkflowRequest) (*vtctldatapb.WorkflowUpdateResponse, error)
		StopReplication(ctx context.Context, req *vtadminpb.StopReplicationRequest) (*vtadminpb.StopReplicationResponse, error)
		StopWorkflow(ctx context.Context, req *vtadminpb.StopWorkflowRequest) (*vtctldatapb.WorkflowUpdateResponse, error)
		TabletExternallyPromoted(ctx context.Context, req *vtadminpb.TabletExternallyPromotedRequest) (*vtadminpb.TabletExternallyPromotedResponse, error)
		ValidateKeyspace(ctx context.Context, req *vtadminpb.ValidateKeyspaceRequest) (*vtctldatapb.ValidateKeyspaceResponse, error)
		ValidateSchemaKeyspace(ctx context.Context, req *vtadminpb.ValidateSchemaKeyspaceRequest) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error)
		ValidateShard(ctx context.Context, req *vtadminpb.ValidateShardRequest) (*vtctldatapb.ValidateShardResponse, error)
		ValidateVersionKeyspace(ctx context.Context, req *vtadminpb.ValidateVersionKeyspaceRequest) (*vtctldatapb.ValidateVersionKeyspaceResponse, error)
		ValidateVersionShard(ctx context.Context, req *vtadminpb.ValidateVersionShardRequest) (*vtctldatapb.ValidateVersionShardResponse, error)
		VDiffCreate(ctx context.Context, req *vtadminpb.VDiffCreateRequest) (*vtctldatapb.VDiffCreateResponse, error)
		VDiffShow(ctx context.Context, req *vtadminpb.VDiffShowRequest) (*vtadminpb.VDiffShowResponse, error)
		VExplain(ctx context.Context, req *vtadminpb.VExplainRequest) (*vtadminpb.VExplainResponse, error)
		VTExplain(ctx context.Context, req *vtadminpb.VTExplainRequest) (*vtadminpb.VTExplainResponse, error)
		WorkflowDelete(ctx context.Context, req *vtadminpb.WorkflowDeleteRequest) (*vtctldatapb.WorkflowDeleteResponse, error)
		WorkflowSwitchTraffic(ctx context.Context, req *vtadminpb.WorkflowSwitchTrafficRequest) (*vtctldatapb.WorkflowSwitchTrafficResponse, error)
	}
)
