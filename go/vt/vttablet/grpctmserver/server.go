/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"
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
	// implementation of the agent to call
	agent tabletmanager.RPCAgent
}

func (s *server) Ping(ctx context.Context, request *tabletmanagerdatapb.PingRequest) (response *tabletmanagerdatapb.PingResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "Ping", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PingResponse{
		Payload: s.agent.Ping(ctx, request.Payload),
	}
	return response, nil
}

func (s *server) Sleep(ctx context.Context, request *tabletmanagerdatapb.SleepRequest) (response *tabletmanagerdatapb.SleepResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "Sleep", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SleepResponse{}
	s.agent.Sleep(ctx, time.Duration(request.Duration))
	return response, nil
}

func (s *server) ExecuteHook(ctx context.Context, request *tabletmanagerdatapb.ExecuteHookRequest) (response *tabletmanagerdatapb.ExecuteHookResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ExecuteHook", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteHookResponse{}
	hr := s.agent.ExecuteHook(ctx, &hook.Hook{
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
	defer s.agent.HandleRPCPanic(ctx, "GetSchema", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetSchemaResponse{}
	sd, err := s.agent.GetSchema(ctx, request.Tables, request.ExcludeTables, request.IncludeViews)
	if err == nil {
		response.SchemaDefinition = sd
	}
	return response, err
}

func (s *server) GetPermissions(ctx context.Context, request *tabletmanagerdatapb.GetPermissionsRequest) (response *tabletmanagerdatapb.GetPermissionsResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "GetPermissions", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetPermissionsResponse{}
	p, err := s.agent.GetPermissions(ctx)
	if err == nil {
		response.Permissions = p
	}
	return response, err
}

//
// Various read-write methods
//

func (s *server) SetReadOnly(ctx context.Context, request *tabletmanagerdatapb.SetReadOnlyRequest) (response *tabletmanagerdatapb.SetReadOnlyResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SetReadOnly", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetReadOnlyResponse{}
	return response, s.agent.SetReadOnly(ctx, true)
}

func (s *server) SetReadWrite(ctx context.Context, request *tabletmanagerdatapb.SetReadWriteRequest) (response *tabletmanagerdatapb.SetReadWriteResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SetReadWrite", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetReadWriteResponse{}
	return response, s.agent.SetReadOnly(ctx, false)
}

func (s *server) ChangeType(ctx context.Context, request *tabletmanagerdatapb.ChangeTypeRequest) (response *tabletmanagerdatapb.ChangeTypeResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ChangeType", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ChangeTypeResponse{}
	return response, s.agent.ChangeType(ctx, request.TabletType)
}

func (s *server) RefreshState(ctx context.Context, request *tabletmanagerdatapb.RefreshStateRequest) (response *tabletmanagerdatapb.RefreshStateResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "RefreshState", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.RefreshStateResponse{}
	return response, s.agent.RefreshState(ctx)
}

func (s *server) RunHealthCheck(ctx context.Context, request *tabletmanagerdatapb.RunHealthCheckRequest) (response *tabletmanagerdatapb.RunHealthCheckResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "RunHealthCheck", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.RunHealthCheckResponse{}
	s.agent.RunHealthCheck(ctx)
	return response, nil
}

func (s *server) IgnoreHealthError(ctx context.Context, request *tabletmanagerdatapb.IgnoreHealthErrorRequest) (response *tabletmanagerdatapb.IgnoreHealthErrorResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "IgnoreHealthError", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.IgnoreHealthErrorResponse{}
	return response, s.agent.IgnoreHealthError(ctx, request.Pattern)
}

func (s *server) ReloadSchema(ctx context.Context, request *tabletmanagerdatapb.ReloadSchemaRequest) (response *tabletmanagerdatapb.ReloadSchemaResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ReloadSchema", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ReloadSchemaResponse{}
	return response, s.agent.ReloadSchema(ctx, request.WaitPosition)
}

func (s *server) PreflightSchema(ctx context.Context, request *tabletmanagerdatapb.PreflightSchemaRequest) (response *tabletmanagerdatapb.PreflightSchemaResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "PreflightSchema", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PreflightSchemaResponse{}
	results, err := s.agent.PreflightSchema(ctx, request.Changes)
	if err == nil {
		response.ChangeResults = results
	}
	return response, err
}

func (s *server) ApplySchema(ctx context.Context, request *tabletmanagerdatapb.ApplySchemaRequest) (response *tabletmanagerdatapb.ApplySchemaResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ApplySchema", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ApplySchemaResponse{}
	scr, err := s.agent.ApplySchema(ctx, &tmutils.SchemaChange{
		SQL:              request.Sql,
		Force:            request.Force,
		AllowReplication: request.AllowReplication,
		BeforeSchema:     request.BeforeSchema,
		AfterSchema:      request.AfterSchema,
	})
	if err == nil {
		response.BeforeSchema = scr.BeforeSchema
		response.AfterSchema = scr.AfterSchema
	}
	return response, err
}

func (s *server) LockTables(ctx context.Context, req *tabletmanagerdatapb.LockTablesRequest) (*tabletmanagerdatapb.LockTablesResponse, error) {
	err := s.agent.LockTables(ctx)
	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.LockTablesResponse{}, nil
}

func (s *server) UnlockTables(ctx context.Context, req *tabletmanagerdatapb.UnlockTablesRequest) (*tabletmanagerdatapb.UnlockTablesResponse, error) {
	err := s.agent.UnlockTables(ctx)
	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.UnlockTablesResponse{}, nil
}

func (s *server) ExecuteFetchAsDba(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (response *tabletmanagerdatapb.ExecuteFetchAsDbaResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ExecuteFetchAsDba", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsDbaResponse{}
	qr, err := s.agent.ExecuteFetchAsDba(ctx, request.Query, request.DbName, int(request.MaxRows), request.DisableBinlogs, request.ReloadSchema)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

func (s *server) ExecuteFetchAsAllPrivs(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (response *tabletmanagerdatapb.ExecuteFetchAsAllPrivsResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ExecuteFetchAsAllPrivs", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsAllPrivsResponse{}
	qr, err := s.agent.ExecuteFetchAsAllPrivs(ctx, request.Query, request.DbName, int(request.MaxRows), request.ReloadSchema)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

func (s *server) ExecuteFetchAsApp(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (response *tabletmanagerdatapb.ExecuteFetchAsAppResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ExecuteFetchAsApp", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ExecuteFetchAsAppResponse{}
	qr, err := s.agent.ExecuteFetchAsApp(ctx, request.Query, int(request.MaxRows))
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	response.Result = qr
	return response, nil
}

//
// Replication related methods
//

func (s *server) SlaveStatus(ctx context.Context, request *tabletmanagerdatapb.SlaveStatusRequest) (response *tabletmanagerdatapb.SlaveStatusResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SlaveStatus", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SlaveStatusResponse{}
	status, err := s.agent.SlaveStatus(ctx)
	if err == nil {
		response.Status = status
	}
	return response, err
}

func (s *server) MasterPosition(ctx context.Context, request *tabletmanagerdatapb.MasterPositionRequest) (response *tabletmanagerdatapb.MasterPositionResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "MasterPosition", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.MasterPositionResponse{}
	position, err := s.agent.MasterPosition(ctx)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) StopSlave(ctx context.Context, request *tabletmanagerdatapb.StopSlaveRequest) (response *tabletmanagerdatapb.StopSlaveResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "StopSlave", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopSlaveResponse{}
	return response, s.agent.StopSlave(ctx)
}

func (s *server) StopSlaveMinimum(ctx context.Context, request *tabletmanagerdatapb.StopSlaveMinimumRequest) (response *tabletmanagerdatapb.StopSlaveMinimumResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "StopSlaveMinimum", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopSlaveMinimumResponse{}
	position, err := s.agent.StopSlaveMinimum(ctx, request.Position, time.Duration(request.WaitTimeout))
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) StartSlave(ctx context.Context, request *tabletmanagerdatapb.StartSlaveRequest) (response *tabletmanagerdatapb.StartSlaveResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "StartSlave", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StartSlaveResponse{}
	return response, s.agent.StartSlave(ctx)
}

func (s *server) StartSlaveUntilAfter(ctx context.Context, request *tabletmanagerdatapb.StartSlaveUntilAfterRequest) (response *tabletmanagerdatapb.StartSlaveUntilAfterResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "StartSlave", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StartSlaveUntilAfterResponse{}
	return response, s.agent.StartSlaveUntilAfter(ctx, request.Position, time.Duration(request.WaitTimeout))
}

func (s *server) TabletExternallyReparented(ctx context.Context, request *tabletmanagerdatapb.TabletExternallyReparentedRequest) (response *tabletmanagerdatapb.TabletExternallyReparentedResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "TabletExternallyReparented", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.TabletExternallyReparentedResponse{}
	return response, s.agent.TabletExternallyReparented(ctx, request.ExternalId)
}

func (s *server) TabletExternallyElected(ctx context.Context, request *tabletmanagerdatapb.TabletExternallyElectedRequest) (*tabletmanagerdatapb.TabletExternallyElectedResponse, error) {
	return &tabletmanagerdatapb.TabletExternallyElectedResponse{}, nil
}

func (s *server) GetSlaves(ctx context.Context, request *tabletmanagerdatapb.GetSlavesRequest) (response *tabletmanagerdatapb.GetSlavesResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "GetSlaves", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.GetSlavesResponse{}
	addrs, err := s.agent.GetSlaves(ctx)
	if err == nil {
		response.Addrs = addrs
	}
	return response, err
}

func (s *server) VReplicationExec(ctx context.Context, request *tabletmanagerdatapb.VReplicationExecRequest) (response *tabletmanagerdatapb.VReplicationExecResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "VReplicationExec", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.VReplicationExecResponse{}
	response.Result, err = s.agent.VReplicationExec(ctx, request.Query)
	return response, err
}

func (s *server) VReplicationWaitForPos(ctx context.Context, request *tabletmanagerdatapb.VReplicationWaitForPosRequest) (response *tabletmanagerdatapb.VReplicationWaitForPosResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "VReplicationWaitForPos", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	err = s.agent.VReplicationWaitForPos(ctx, int(request.Id), request.Position)
	return &tabletmanagerdatapb.VReplicationWaitForPosResponse{}, err
}

//
// Reparenting related functions
//

func (s *server) ResetReplication(ctx context.Context, request *tabletmanagerdatapb.ResetReplicationRequest) (response *tabletmanagerdatapb.ResetReplicationResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "ResetReplication", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.ResetReplicationResponse{}
	return response, s.agent.ResetReplication(ctx)
}

func (s *server) InitMaster(ctx context.Context, request *tabletmanagerdatapb.InitMasterRequest) (response *tabletmanagerdatapb.InitMasterResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "InitMaster", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.InitMasterResponse{}
	position, err := s.agent.InitMaster(ctx)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) PopulateReparentJournal(ctx context.Context, request *tabletmanagerdatapb.PopulateReparentJournalRequest) (response *tabletmanagerdatapb.PopulateReparentJournalResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "PopulateReparentJournal", request, response, false /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PopulateReparentJournalResponse{}
	return response, s.agent.PopulateReparentJournal(ctx, request.TimeCreatedNs, request.ActionName, request.MasterAlias, request.ReplicationPosition)
}

func (s *server) InitSlave(ctx context.Context, request *tabletmanagerdatapb.InitSlaveRequest) (response *tabletmanagerdatapb.InitSlaveResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "InitSlave", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.InitSlaveResponse{}
	return response, s.agent.InitSlave(ctx, request.Parent, request.ReplicationPosition, request.TimeCreatedNs)
}

func (s *server) DemoteMaster(ctx context.Context, request *tabletmanagerdatapb.DemoteMasterRequest) (response *tabletmanagerdatapb.DemoteMasterResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "DemoteMaster", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.DemoteMasterResponse{}
	position, err := s.agent.DemoteMaster(ctx)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) UndoDemoteMaster(ctx context.Context, request *tabletmanagerdatapb.UndoDemoteMasterRequest) (response *tabletmanagerdatapb.UndoDemoteMasterResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "UndoDemoteMaster", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.UndoDemoteMasterResponse{}
	err = s.agent.UndoDemoteMaster(ctx)
	return response, err
}

func (s *server) PromoteSlaveWhenCaughtUp(ctx context.Context, request *tabletmanagerdatapb.PromoteSlaveWhenCaughtUpRequest) (response *tabletmanagerdatapb.PromoteSlaveWhenCaughtUpResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "PromoteSlaveWhenCaughtUp", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PromoteSlaveWhenCaughtUpResponse{}
	position, err := s.agent.PromoteSlaveWhenCaughtUp(ctx, request.Position)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) SlaveWasPromoted(ctx context.Context, request *tabletmanagerdatapb.SlaveWasPromotedRequest) (response *tabletmanagerdatapb.SlaveWasPromotedResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SlaveWasPromoted", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SlaveWasPromotedResponse{}
	return response, s.agent.SlaveWasPromoted(ctx)
}

func (s *server) SetMaster(ctx context.Context, request *tabletmanagerdatapb.SetMasterRequest) (response *tabletmanagerdatapb.SetMasterResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SetMaster", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SetMasterResponse{}
	return response, s.agent.SetMaster(ctx, request.Parent, request.TimeCreatedNs, request.ForceStartSlave)
}

func (s *server) SlaveWasRestarted(ctx context.Context, request *tabletmanagerdatapb.SlaveWasRestartedRequest) (response *tabletmanagerdatapb.SlaveWasRestartedResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "SlaveWasRestarted", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.SlaveWasRestartedResponse{}
	return response, s.agent.SlaveWasRestarted(ctx, request.Parent)
}

func (s *server) StopReplicationAndGetStatus(ctx context.Context, request *tabletmanagerdatapb.StopReplicationAndGetStatusRequest) (response *tabletmanagerdatapb.StopReplicationAndGetStatusResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "StopReplicationAndGetStatus", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.StopReplicationAndGetStatusResponse{}
	status, err := s.agent.StopReplicationAndGetStatus(ctx)
	if err == nil {
		response.Status = status
	}
	return response, err
}

func (s *server) PromoteSlave(ctx context.Context, request *tabletmanagerdatapb.PromoteSlaveRequest) (response *tabletmanagerdatapb.PromoteSlaveResponse, err error) {
	defer s.agent.HandleRPCPanic(ctx, "PromoteSlave", request, response, true /*verbose*/, &err)
	ctx = callinfo.GRPCCallInfo(ctx)
	response = &tabletmanagerdatapb.PromoteSlaveResponse{}
	position, err := s.agent.PromoteSlave(ctx)
	if err == nil {
		response.Position = position
	}
	return response, err
}

func (s *server) Backup(request *tabletmanagerdatapb.BackupRequest, stream tabletmanagerservicepb.TabletManager_BackupServer) (err error) {
	ctx := stream.Context()
	defer s.agent.HandleRPCPanic(ctx, "Backup", request, nil, true /*verbose*/, &err)
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

	return s.agent.Backup(ctx, int(request.Concurrency), logger, bool(request.AllowMaster))
}

func (s *server) RestoreFromBackup(request *tabletmanagerdatapb.RestoreFromBackupRequest, stream tabletmanagerservicepb.TabletManager_RestoreFromBackupServer) (err error) {
	ctx := stream.Context()
	defer s.agent.HandleRPCPanic(ctx, "RestoreFromBackup", request, nil, true /*verbose*/, &err)
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

	return s.agent.RestoreFromBackup(ctx, logger)
}

// registration glue

func init() {
	tabletmanager.RegisterQueryServices = append(tabletmanager.RegisterQueryServices, func(agent *tabletmanager.ActionAgent) {
		if servenv.GRPCCheckServiceMap("tabletmanager") {
			tabletmanagerservicepb.RegisterTabletManagerServer(servenv.GRPCServer, &server{agent})
		}
	})
}

// RegisterForTest will register the RPC, to be used by test instances only
func RegisterForTest(s *grpc.Server, agent *tabletmanager.ActionAgent) {
	tabletmanagerservicepb.RegisterTabletManagerServer(s, &server{agent})
}
