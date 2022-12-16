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

package grpctmserver

import (
	"time"

	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"context"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
)

// server is the gRPC implementation of the RPC server
type server struct {
	tabletmanagerservicepb.UnimplementedTabletManagerServer
	// implementation of the tm to call
	tm tabletmanager.RPCTM
}

func (s *server) Ping(ctx context.Context, request *tabletmanagerdatapb.PingRequest) (response *tabletmanagerdatapb.PingResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "Ping", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PingResponse{
		Payload: s.tm.Ping(ctx, request.Payload),
	}
	return response, nil
}

func (s *server) Sleep(ctx context.Context, request *tabletmanagerdatapb.SleepRequest) (response *tabletmanagerdatapb.SleepResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "Sleep", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SleepResponse{}
	s.tm.Sleep(ctx, time.Duration(request.Duration))
	return response, nil
}

func (s *server) ExecuteHook(ctx context.Context, request *tabletmanagerdatapb.ExecuteHookRequest) (response *tabletmanagerdatapb.ExecuteHookResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ExecuteHook", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteHookResponse{}
	hr := s.tm.ExecuteHook(ctx, &hook.Hook{
		Name:       request.Name,
		Parameters: request.Parameters,
		ExtraEnv:   request.ExtraEnv,
	})
	response.ExitStatus = int64(hr.ExitStatus)
	response.Stdout = hr.Stdout
	response.Stderr = hr.Stderr
	return response, nil
}

func (s *server) GetSchema(ctx context.Context, request *tabletmanagerdatapb.GetSchemaRequest) (response *tabletmanagerdatapb.GetSchemaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "GetSchema", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetSchemaResponse{}
	sd, err := s.tm.GetSchema(ctx, request)
	if err == nil {
		response.SchemaDefinition = sd
	}
	return response, err
}

func (s *server) GetPermissions(ctx context.Context, request *tabletmanagerdatapb.GetPermissionsRequest) (response *tabletmanagerdatapb.GetPermissionsResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "GetPermissions", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetPermissionsResponse{}
	p, err := s.tm.GetPermissions(ctx)
	if err == nil {
		response.Permissions = p
	}
	return response, err
}

//
// Various read-write methods
//

func (s *server) SetReadOnly(ctx context.Context, request *tabletmanagerdatapb.SetReadOnlyRequest) (response *tabletmanagerdatapb.SetReadOnlyResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "SetReadOnly", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetReadOnlyResponse{}
	return response, s.tm.SetReadOnly(ctx, true)
}

func (s *server) SetReadWrite(ctx context.Context, request *tabletmanagerdatapb.SetReadWriteRequest) (response *tabletmanagerdatapb.SetReadWriteResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "SetReadWrite", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetReadWriteResponse{}
	return response, s.tm.SetReadOnly(ctx, false)
}

func (s *server) ChangeType(ctx context.Context, request *tabletmanagerdatapb.ChangeTypeRequest) (response *tabletmanagerdatapb.ChangeTypeResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ChangeType", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ChangeTypeResponse{}
	return response, s.tm.ChangeType(ctx, request.TabletType, request.GetSemiSync())
}

func (s *server) RefreshState(ctx context.Context, request *tabletmanagerdatapb.RefreshStateRequest) (response *tabletmanagerdatapb.RefreshStateResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "RefreshState", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.RefreshStateResponse{}
	return response, s.tm.RefreshState(ctx)
}

func (s *server) RunHealthCheck(ctx context.Context, request *tabletmanagerdatapb.RunHealthCheckRequest) (response *tabletmanagerdatapb.RunHealthCheckResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "RunHealthCheck", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.RunHealthCheckResponse{}
	s.tm.RunHealthCheck(ctx)
	return response, nil
}

func (s *server) ReloadSchema(ctx context.Context, request *tabletmanagerdatapb.ReloadSchemaRequest) (response *tabletmanagerdatapb.ReloadSchemaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ReloadSchema", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ReloadSchemaResponse{}
	return response, s.tm.ReloadSchema(ctx, request.WaitPosition)
}

func (s *server) PreflightSchema(ctx context.Context, request *tabletmanagerdatapb.PreflightSchemaRequest) (response *tabletmanagerdatapb.PreflightSchemaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "PreflightSchema", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PreflightSchemaResponse{}
	results, err := s.tm.PreflightSchema(ctx, request.Changes)
	if err == nil {
		response.ChangeResults = results
	}
	return response, err
}

func (s *server) ApplySchema(ctx context.Context, request *tabletmanagerdatapb.ApplySchemaRequest) (response *tabletmanagerdatapb.ApplySchemaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ApplySchema", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ApplySchemaResponse{}
	scr, err := s.tm.ApplySchema(ctx, &tmutils.SchemaChange{
		SQL:              request.Sql,
		Force:            request.Force,
		AllowReplication: request.AllowReplication,
		BeforeSchema:     request.BeforeSchema,
		AfterSchema:      request.AfterSchema,
		SQLMode:          request.SqlMode,
	})
	if err == nil {
		response.BeforeSchema = scr.BeforeSchema
		response.AfterSchema = scr.AfterSchema
	}
	return response, err
}

func (s *server) LockTables(ctx context.Context, req *tabletmanagerdatapb.LockTablesRequest) (*tabletmanagerdatapb.LockTablesResponse, error) {
	err := s.tm.LockTables(ctx)
	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.LockTablesResponse{}, nil
}

func (s *server) UnlockTables(ctx context.Context, req *tabletmanagerdatapb.UnlockTablesRequest) (*tabletmanagerdatapb.UnlockTablesResponse, error) {
	err := s.tm.UnlockTables(ctx)
	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.UnlockTablesResponse{}, nil
}

func (s *server) ExecuteQuery(ctx context.Context, request *tabletmanagerdatapb.ExecuteQueryRequest) (response *tabletmanagerdatapb.ExecuteQueryResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ExecuteQuery", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)

	// Attach the callerID as the EffectiveCallerID.
	if request.CallerId != nil {
		ctx = callerid.NewContext(ctx, request.CallerId, &querypb.VTGateCallerID{Username: request.CallerId.Principal})
	}
	response = &tabletmanagerdatapb.ExecuteQueryResponse{}
	qr, err := s.tm.ExecuteQuery(ctx, request)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

func (s *server) ExecuteFetchAsDba(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (response *tabletmanagerdatapb.ExecuteFetchAsDbaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ExecuteFetchAsDba", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsDbaResponse{}
	qr, err := s.tm.ExecuteFetchAsDba(ctx, request)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

func (s *server) ExecuteFetchAsAllPrivs(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (response *tabletmanagerdatapb.ExecuteFetchAsAllPrivsResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ExecuteFetchAsAllPrivs", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsAllPrivsResponse{}
	qr, err := s.tm.ExecuteFetchAsAllPrivs(ctx, request)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

func (s *server) ExecuteFetchAsApp(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (response *tabletmanagerdatapb.ExecuteFetchAsAppResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ExecuteFetchAsApp", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsAppResponse{}
	qr, err := s.tm.ExecuteFetchAsApp(ctx, request)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

//
// Replication related methods
//

func (s *server) ReplicationStatus(ctx context.Context, request *tabletmanagerdatapb.ReplicationStatusRequest) (response *tabletmanagerdatapb.ReplicationStatusResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ReplicationStatus", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ReplicationStatusResponse{}
	status, err := s.tm.ReplicationStatus(ctx)
	if err == nil {
		response.Status = status
	}
	return response, err
}

func (s *server) FullStatus(ctx context.Context, request *tabletmanagerdatapb.FullStatusRequest) (response *tabletmanagerdatapb.FullStatusResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "FullStatus", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.FullStatusResponse{}
	status, err := s.tm.FullStatus(ctx)
	if err == nil {
		response.Status = status
	}
	return response, err
}

func (s *server) PrimaryStatus(ctx context.Context, request *tabletmanagerdatapb.PrimaryStatusRequest) (response *tabletmanagerdatapb.PrimaryStatusResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "PrimaryStatus", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PrimaryStatusResponse{}
	status, err := s.tm.PrimaryStatus(ctx)
	if err == nil {
		response.Status = status
	}
	return response, err
}

func (s *server) PrimaryPosition(ctx context.Context, request *tabletmanagerdatapb.PrimaryPositionRequest) (response *tabletmanagerdatapb.PrimaryPositionResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "PrimaryPosition", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PrimaryPositionResponse{}
	position, err := s.tm.PrimaryPosition(ctx)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) WaitForPosition(ctx context.Context, request *tabletmanagerdatapb.WaitForPositionRequest) (response *tabletmanagerdatapb.WaitForPositionResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "WaitForPosition", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.WaitForPositionResponse{}
	return response, s.tm.WaitForPosition(ctx, request.Position)
}

func (s *server) StopReplication(ctx context.Context, request *tabletmanagerdatapb.StopReplicationRequest) (response *tabletmanagerdatapb.StopReplicationResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "StopReplication", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopReplicationResponse{}
	return response, s.tm.StopReplication(ctx)
}

func (s *server) StopReplicationMinimum(ctx context.Context, request *tabletmanagerdatapb.StopReplicationMinimumRequest) (response *tabletmanagerdatapb.StopReplicationMinimumResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "StopReplicationMinimum", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopReplicationMinimumResponse{}
	position, err := s.tm.StopReplicationMinimum(ctx, request.Position, time.Duration(request.WaitTimeout))
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) StartReplication(ctx context.Context, request *tabletmanagerdatapb.StartReplicationRequest) (response *tabletmanagerdatapb.StartReplicationResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "StartReplication", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StartReplicationResponse{}
	return response, s.tm.StartReplication(ctx, request.GetSemiSync())
}

func (s *server) StartReplicationUntilAfter(ctx context.Context, request *tabletmanagerdatapb.StartReplicationUntilAfterRequest) (response *tabletmanagerdatapb.StartReplicationUntilAfterResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "StartReplication", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StartReplicationUntilAfterResponse{}
	return response, s.tm.StartReplicationUntilAfter(ctx, request.Position, time.Duration(request.WaitTimeout))
}

func (s *server) GetReplicas(ctx context.Context, request *tabletmanagerdatapb.GetReplicasRequest) (response *tabletmanagerdatapb.GetReplicasResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "GetReplicas", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetReplicasResponse{}
	addrs, err := s.tm.GetReplicas(ctx)
	if err == nil {
		response.Addrs = addrs
	}
	return response, err
}

func (s *server) VExec(ctx context.Context, request *tabletmanagerdatapb.VExecRequest) (response *tabletmanagerdatapb.VExecResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "VExec", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.VExecResponse{}
	response.Result, err = s.tm.VExec(ctx, request.Query, request.Workflow, request.Keyspace)
	return response, err
}

func (s *server) VReplicationExec(ctx context.Context, request *tabletmanagerdatapb.VReplicationExecRequest) (response *tabletmanagerdatapb.VReplicationExecResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "VReplicationExec", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.VReplicationExecResponse{}
	response.Result, err = s.tm.VReplicationExec(ctx, request.Query)
	return response, err
}

func (s *server) VReplicationWaitForPos(ctx context.Context, request *tabletmanagerdatapb.VReplicationWaitForPosRequest) (response *tabletmanagerdatapb.VReplicationWaitForPosResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "VReplicationWaitForPos", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	err = s.tm.VReplicationWaitForPos(ctx, int(request.Id), request.Position)
	return &tabletmanagerdatapb.VReplicationWaitForPosResponse{}, err
}

func (s *server) VDiff(ctx context.Context, request *tabletmanagerdatapb.VDiffRequest) (response *tabletmanagerdatapb.VDiffResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "VDiff", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response, err = s.tm.VDiff(ctx, request)
	return response, err
}

//
// Reparenting related functions
//

func (s *server) ResetReplication(ctx context.Context, request *tabletmanagerdatapb.ResetReplicationRequest) (response *tabletmanagerdatapb.ResetReplicationResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ResetReplication", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ResetReplicationResponse{}
	return response, s.tm.ResetReplication(ctx)
}

func (s *server) InitPrimary(ctx context.Context, request *tabletmanagerdatapb.InitPrimaryRequest) (response *tabletmanagerdatapb.InitPrimaryResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "InitPrimary", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.InitPrimaryResponse{}
	position, err := s.tm.InitPrimary(ctx, request.GetSemiSync())
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) PopulateReparentJournal(ctx context.Context, request *tabletmanagerdatapb.PopulateReparentJournalRequest) (response *tabletmanagerdatapb.PopulateReparentJournalResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "PopulateReparentJournal", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PopulateReparentJournalResponse{}
	return response, s.tm.PopulateReparentJournal(ctx, request.TimeCreatedNs, request.ActionName, request.PrimaryAlias, request.ReplicationPosition)
}

func (s *server) InitReplica(ctx context.Context, request *tabletmanagerdatapb.InitReplicaRequest) (response *tabletmanagerdatapb.InitReplicaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "InitReplica", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.InitReplicaResponse{}
	return response, s.tm.InitReplica(ctx, request.Parent, request.ReplicationPosition, request.TimeCreatedNs, request.GetSemiSync())
}

func (s *server) DemotePrimary(ctx context.Context, request *tabletmanagerdatapb.DemotePrimaryRequest) (response *tabletmanagerdatapb.DemotePrimaryResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "DemotePrimary", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.DemotePrimaryResponse{}
	status, err := s.tm.DemotePrimary(ctx)
	if err == nil {
		response.PrimaryStatus = status
	}
	return response, err
}

func (s *server) UndoDemotePrimary(ctx context.Context, request *tabletmanagerdatapb.UndoDemotePrimaryRequest) (response *tabletmanagerdatapb.UndoDemotePrimaryResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "UndoDemotePrimary", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.UndoDemotePrimaryResponse{}
	err = s.tm.UndoDemotePrimary(ctx, request.GetSemiSync())
	return response, err
}

func (s *server) ReplicaWasPromoted(ctx context.Context, request *tabletmanagerdatapb.ReplicaWasPromotedRequest) (response *tabletmanagerdatapb.ReplicaWasPromotedResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ReplicaWasPromoted", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ReplicaWasPromotedResponse{}
	return response, s.tm.ReplicaWasPromoted(ctx)
}

func (s *server) ResetReplicationParameters(ctx context.Context, request *tabletmanagerdatapb.ResetReplicationParametersRequest) (response *tabletmanagerdatapb.ResetReplicationParametersResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ResetReplicationParameters", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ResetReplicationParametersResponse{}
	return response, s.tm.ResetReplicationParameters(ctx)
}

func (s *server) SetReplicationSource(ctx context.Context, request *tabletmanagerdatapb.SetReplicationSourceRequest) (response *tabletmanagerdatapb.SetReplicationSourceResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "SetReplicationSource", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetReplicationSourceResponse{}
	return response, s.tm.SetReplicationSource(ctx, request.Parent, request.TimeCreatedNs, request.WaitPosition, request.ForceStartReplication, request.GetSemiSync())
}

func (s *server) ReplicaWasRestarted(ctx context.Context, request *tabletmanagerdatapb.ReplicaWasRestartedRequest) (response *tabletmanagerdatapb.ReplicaWasRestartedResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "ReplicaWasRestarted", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ReplicaWasRestartedResponse{}
	return response, s.tm.ReplicaWasRestarted(ctx, request.Parent)
}

func (s *server) StopReplicationAndGetStatus(ctx context.Context, request *tabletmanagerdatapb.StopReplicationAndGetStatusRequest) (response *tabletmanagerdatapb.StopReplicationAndGetStatusResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "StopReplicationAndGetStatus", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopReplicationAndGetStatusResponse{}
	statusResponse, err := s.tm.StopReplicationAndGetStatus(ctx, request.StopReplicationMode)
	if err == nil {
		response.Status = statusResponse.Status
	}
	return response, err
}

func (s *server) PromoteReplica(ctx context.Context, request *tabletmanagerdatapb.PromoteReplicaRequest) (response *tabletmanagerdatapb.PromoteReplicaResponse, err error) {
	defer s.tm.HandleRPCPanic(ctx, "PromoteReplica", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PromoteReplicaResponse{}
	position, err := s.tm.PromoteReplica(ctx, request.GetSemiSync())
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) Backup(request *tabletmanagerdatapb.BackupRequest, stream tabletmanagerservicepb.TabletManager_BackupServer) (err error) {
	ctx := stream.Context()
	defer s.tm.HandleRPCPanic(ctx, "Backup", request, nil, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)

	// create a logger, send the result back to the caller
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		// If the client disconnects, we will just fail
		// to send the log events, but won't interrupt
		// the backup.
		stream.Send(&tabletmanagerdatapb.BackupResponse{
			Event: e,
		})
	})

	return s.tm.Backup(ctx, logger, request)
}

func (s *server) RestoreFromBackup(request *tabletmanagerdatapb.RestoreFromBackupRequest, stream tabletmanagerservicepb.TabletManager_RestoreFromBackupServer) (err error) {
	ctx := stream.Context()
	defer s.tm.HandleRPCPanic(ctx, "RestoreFromBackup", request, nil, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)

	// create a logger, send the result back to the caller
	logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
		// If the client disconnects, we will just fail
		// to send the log events, but won't interrupt
		// the backup.
		stream.Send(&tabletmanagerdatapb.RestoreFromBackupResponse{
			Event: e,
		})
	})

	return s.tm.RestoreFromBackup(ctx, logger, request)
}

// registration glue

func init() {
	tabletmanager.RegisterTabletManagers = append(tabletmanager.RegisterTabletManagers, func(tm *tabletmanager.TabletManager) {
		if servenv.GRPCCheckServiceMap("tabletmanager") {
			tabletmanagerservicepb.RegisterTabletManagerServer(servenv.GRPCServer, &server{tm: tm})
		}
	})
}

// RegisterForTest will register the RPC, to be used by test instances only
func RegisterForTest(s *grpc.Server, tm tabletmanager.RPCTM) {
	tabletmanagerservicepb.RegisterTabletManagerServer(s, &server{tm: tm})
}
