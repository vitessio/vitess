// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/vterrors"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
)

// server is the gRPC implementation of the RPC server
type server struct {
	// implementation of the agent to call
	agent tabletmanager.RPCAgent
}

func (s *server) Ping(ctx context.Context, request *tabletmanagerdatapb.PingRequest) (*tabletmanagerdatapb.PingResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.PingResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionPing, request, response, func() error {
		response.Payload = s.agent.Ping(ctx, request.Payload)
		return nil
	})
}

func (s *server) Sleep(ctx context.Context, request *tabletmanagerdatapb.SleepRequest) (*tabletmanagerdatapb.SleepResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SleepResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSleep, request, response, true, func() error {
		s.agent.Sleep(ctx, time.Duration(request.Duration))
		return nil
	})
}

func (s *server) ExecuteHook(ctx context.Context, request *tabletmanagerdatapb.ExecuteHookRequest) (*tabletmanagerdatapb.ExecuteHookResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ExecuteHookResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionExecuteHook, request, response, true, func() error {
		hr := s.agent.ExecuteHook(ctx, &hook.Hook{
			Name:       request.Name,
			Parameters: request.Parameters,
			ExtraEnv:   request.ExtraEnv,
		})
		response.ExitStatus = int64(hr.ExitStatus)
		response.Stdout = hr.Stdout
		response.Stderr = hr.Stderr
		return nil
	})
}

func (s *server) GetSchema(ctx context.Context, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.GetSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.GetSchemaResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionGetSchema, request, response, func() error {
		sd, err := s.agent.GetSchema(ctx, request.Tables, request.ExcludeTables, request.IncludeViews)
		if err == nil {
			response.SchemaDefinition = sd
		}
		return err
	})
}

func (s *server) GetPermissions(ctx context.Context, request *tabletmanagerdatapb.GetPermissionsRequest) (*tabletmanagerdatapb.GetPermissionsResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.GetPermissionsResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionGetPermissions, request, response, func() error {
		p, err := s.agent.GetPermissions(ctx)
		if err == nil {
			response.Permissions = p
		}
		return err
	})
}

//
// Various read-write methods
//

func (s *server) SetReadOnly(ctx context.Context, request *tabletmanagerdatapb.SetReadOnlyRequest) (*tabletmanagerdatapb.SetReadOnlyResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SetReadOnlyResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSetReadOnly, request, response, true, func() error {
		return s.agent.SetReadOnly(ctx, true)
	})
}

func (s *server) SetReadWrite(ctx context.Context, request *tabletmanagerdatapb.SetReadWriteRequest) (*tabletmanagerdatapb.SetReadWriteResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SetReadWriteResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSetReadWrite, request, response, true, func() error {
		return s.agent.SetReadOnly(ctx, false)
	})
}

func (s *server) ChangeType(ctx context.Context, request *tabletmanagerdatapb.ChangeTypeRequest) (*tabletmanagerdatapb.ChangeTypeResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ChangeTypeResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionChangeType, request, response, true, func() error {
		return s.agent.ChangeType(ctx, request.TabletType)
	})
}

func (s *server) RefreshState(ctx context.Context, request *tabletmanagerdatapb.RefreshStateRequest) (*tabletmanagerdatapb.RefreshStateResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.RefreshStateResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionRefreshState, request, response, true, func() error {
		s.agent.RefreshState(ctx)
		return nil
	})
}

func (s *server) RunHealthCheck(ctx context.Context, request *tabletmanagerdatapb.RunHealthCheckRequest) (*tabletmanagerdatapb.RunHealthCheckResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.RunHealthCheckResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionRunHealthCheck, request, response, func() error {
		s.agent.RunHealthCheck(ctx)
		return nil
	})
}

func (s *server) IgnoreHealthError(ctx context.Context, request *tabletmanagerdatapb.IgnoreHealthErrorRequest) (*tabletmanagerdatapb.IgnoreHealthErrorResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.IgnoreHealthErrorResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionIgnoreHealthError, request, response, func() error {
		return s.agent.IgnoreHealthError(ctx, request.Pattern)
	})
}

func (s *server) ReloadSchema(ctx context.Context, request *tabletmanagerdatapb.ReloadSchemaRequest) (*tabletmanagerdatapb.ReloadSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ReloadSchemaResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionReloadSchema, request, response, func() error {
		return s.agent.ReloadSchema(ctx, request.WaitPosition)
	})
}

func (s *server) PreflightSchema(ctx context.Context, request *tabletmanagerdatapb.PreflightSchemaRequest) (*tabletmanagerdatapb.PreflightSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.PreflightSchemaResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionPreflightSchema, request, response, true, func() error {
		results, err := s.agent.PreflightSchema(ctx, request.Changes)
		if err == nil {
			response.ChangeResults = results
		}
		return err
	})
}

func (s *server) ApplySchema(ctx context.Context, request *tabletmanagerdatapb.ApplySchemaRequest) (*tabletmanagerdatapb.ApplySchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ApplySchemaResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionApplySchema, request, response, true, func() error {
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
		return err
	})
}

func (s *server) ExecuteFetchAsDba(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*tabletmanagerdatapb.ExecuteFetchAsDbaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ExecuteFetchAsDbaResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionExecuteFetchAsDba, request, response, func() error {
		qr, err := s.agent.ExecuteFetchAsDba(ctx, request.Query, request.DbName, int(request.MaxRows), request.DisableBinlogs, request.ReloadSchema)
		if err != nil {
			return vterrors.ToGRPCError(err)
		}
		response.Result = qr
		return nil
	})
}

func (s *server) ExecuteFetchAsApp(ctx context.Context, request *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*tabletmanagerdatapb.ExecuteFetchAsAppResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ExecuteFetchAsAppResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionExecuteFetchAsApp, request, response, func() error {
		qr, err := s.agent.ExecuteFetchAsApp(ctx, request.Query, int(request.MaxRows))
		if err != nil {
			return vterrors.ToGRPCError(err)
		}
		response.Result = qr
		return nil
	})
}

//
// Replication related methods
//

func (s *server) SlaveStatus(ctx context.Context, request *tabletmanagerdatapb.SlaveStatusRequest) (*tabletmanagerdatapb.SlaveStatusResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SlaveStatusResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionSlaveStatus, request, response, func() error {
		status, err := s.agent.SlaveStatus(ctx)
		if err == nil {
			response.Status = status
		}
		return err
	})
}

func (s *server) MasterPosition(ctx context.Context, request *tabletmanagerdatapb.MasterPositionRequest) (*tabletmanagerdatapb.MasterPositionResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.MasterPositionResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionMasterPosition, request, response, func() error {
		position, err := s.agent.MasterPosition(ctx)
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) StopSlave(ctx context.Context, request *tabletmanagerdatapb.StopSlaveRequest) (*tabletmanagerdatapb.StopSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StopSlaveResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionStopSlave, request, response, true, func() error {
		return s.agent.StopSlave(ctx)
	})
}

func (s *server) StopSlaveMinimum(ctx context.Context, request *tabletmanagerdatapb.StopSlaveMinimumRequest) (*tabletmanagerdatapb.StopSlaveMinimumResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StopSlaveMinimumResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionStopSlaveMinimum, request, response, true, func() error {
		position, err := s.agent.StopSlaveMinimum(ctx, request.Position, time.Duration(request.WaitTimeout))
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) StartSlave(ctx context.Context, request *tabletmanagerdatapb.StartSlaveRequest) (*tabletmanagerdatapb.StartSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StartSlaveResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionStartSlave, request, response, true, func() error {
		return s.agent.StartSlave(ctx)
	})
}

func (s *server) TabletExternallyReparented(ctx context.Context, request *tabletmanagerdatapb.TabletExternallyReparentedRequest) (*tabletmanagerdatapb.TabletExternallyReparentedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.TabletExternallyReparentedResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionExternallyReparented, request, response, false, func() error {
		return s.agent.TabletExternallyReparented(ctx, request.ExternalId)
	})
}

func (s *server) TabletExternallyElected(ctx context.Context, request *tabletmanagerdatapb.TabletExternallyElectedRequest) (*tabletmanagerdatapb.TabletExternallyElectedResponse, error) {
	return &tabletmanagerdatapb.TabletExternallyElectedResponse{}, nil
}

func (s *server) GetSlaves(ctx context.Context, request *tabletmanagerdatapb.GetSlavesRequest) (*tabletmanagerdatapb.GetSlavesResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.GetSlavesResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionGetSlaves, request, response, func() error {
		addrs, err := s.agent.GetSlaves(ctx)
		if err == nil {
			response.Addrs = addrs
		}
		return err
	})
}

func (s *server) WaitBlpPosition(ctx context.Context, request *tabletmanagerdatapb.WaitBlpPositionRequest) (*tabletmanagerdatapb.WaitBlpPositionResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.WaitBlpPositionResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionWaitBLPPosition, request, response, true, func() error {
		return s.agent.WaitBlpPosition(ctx, request.BlpPosition, time.Duration(request.WaitTimeout))
	})
}

func (s *server) StopBlp(ctx context.Context, request *tabletmanagerdatapb.StopBlpRequest) (*tabletmanagerdatapb.StopBlpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StopBlpResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionStopBLP, request, response, true, func() error {
		positions, err := s.agent.StopBlp(ctx)
		if err == nil {
			response.BlpPositions = positions
		}
		return err
	})
}

func (s *server) StartBlp(ctx context.Context, request *tabletmanagerdatapb.StartBlpRequest) (*tabletmanagerdatapb.StartBlpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StartBlpResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionStartBLP, request, response, true, func() error {
		return s.agent.StartBlp(ctx)
	})
}

func (s *server) RunBlpUntil(ctx context.Context, request *tabletmanagerdatapb.RunBlpUntilRequest) (*tabletmanagerdatapb.RunBlpUntilResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.RunBlpUntilResponse{}
	return response, s.agent.RPCWrapLock(ctx, tabletmanager.TabletActionRunBLPUntil, request, response, true, func() error {
		position, err := s.agent.RunBlpUntil(ctx, request.BlpPositions, time.Duration(request.WaitTimeout))
		if err == nil {
			response.Position = position
		}
		return err
	})
}

//
// Reparenting related functions
//

func (s *server) ResetReplication(ctx context.Context, request *tabletmanagerdatapb.ResetReplicationRequest) (*tabletmanagerdatapb.ResetReplicationResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.ResetReplicationResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionResetReplication, request, response, true, func() error {
		return s.agent.ResetReplication(ctx)
	})
}

func (s *server) InitMaster(ctx context.Context, request *tabletmanagerdatapb.InitMasterRequest) (*tabletmanagerdatapb.InitMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.InitMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionInitMaster, request, response, true, func() error {
		position, err := s.agent.InitMaster(ctx)
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) PopulateReparentJournal(ctx context.Context, request *tabletmanagerdatapb.PopulateReparentJournalRequest) (*tabletmanagerdatapb.PopulateReparentJournalResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.PopulateReparentJournalResponse{}
	return response, s.agent.RPCWrap(ctx, tabletmanager.TabletActionPopulateReparentJournal, request, response, func() error {
		return s.agent.PopulateReparentJournal(ctx, request.TimeCreatedNs, request.ActionName, request.MasterAlias, request.ReplicationPosition)
	})
}

func (s *server) InitSlave(ctx context.Context, request *tabletmanagerdatapb.InitSlaveRequest) (*tabletmanagerdatapb.InitSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.InitSlaveResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionInitSlave, request, response, true, func() error {
		return s.agent.InitSlave(ctx, request.Parent, request.ReplicationPosition, request.TimeCreatedNs)
	})
}

func (s *server) DemoteMaster(ctx context.Context, request *tabletmanagerdatapb.DemoteMasterRequest) (*tabletmanagerdatapb.DemoteMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.DemoteMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionDemoteMaster, request, response, true, func() error {
		position, err := s.agent.DemoteMaster(ctx)
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) PromoteSlaveWhenCaughtUp(ctx context.Context, request *tabletmanagerdatapb.PromoteSlaveWhenCaughtUpRequest) (*tabletmanagerdatapb.PromoteSlaveWhenCaughtUpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.PromoteSlaveWhenCaughtUpResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionPromoteSlaveWhenCaughtUp, request, response, true, func() error {
		position, err := s.agent.PromoteSlaveWhenCaughtUp(ctx, request.Position)
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) SlaveWasPromoted(ctx context.Context, request *tabletmanagerdatapb.SlaveWasPromotedRequest) (*tabletmanagerdatapb.SlaveWasPromotedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SlaveWasPromotedResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSlaveWasPromoted, request, response, true, func() error {
		return s.agent.SlaveWasPromoted(ctx)
	})
}

func (s *server) SetMaster(ctx context.Context, request *tabletmanagerdatapb.SetMasterRequest) (*tabletmanagerdatapb.SetMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SetMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSetMaster, request, response, true, func() error {
		return s.agent.SetMaster(ctx, request.Parent, request.TimeCreatedNs, request.ForceStartSlave)
	})
}

func (s *server) SlaveWasRestarted(ctx context.Context, request *tabletmanagerdatapb.SlaveWasRestartedRequest) (*tabletmanagerdatapb.SlaveWasRestartedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.SlaveWasRestartedResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionSlaveWasRestarted, request, response, true, func() error {
		return s.agent.SlaveWasRestarted(ctx, request.Parent)
	})
}

func (s *server) StopReplicationAndGetStatus(ctx context.Context, request *tabletmanagerdatapb.StopReplicationAndGetStatusRequest) (*tabletmanagerdatapb.StopReplicationAndGetStatusResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.StopReplicationAndGetStatusResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionStopReplicationAndGetStatus, request, response, true, func() error {
		status, err := s.agent.StopReplicationAndGetStatus(ctx)
		if err == nil {
			response.Status = status
		}
		return err
	})
}

func (s *server) PromoteSlave(ctx context.Context, request *tabletmanagerdatapb.PromoteSlaveRequest) (*tabletmanagerdatapb.PromoteSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &tabletmanagerdatapb.PromoteSlaveResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionPromoteSlave, request, response, true, func() error {
		position, err := s.agent.PromoteSlave(ctx)
		if err == nil {
			response.Position = position
		}
		return err
	})
}

func (s *server) Backup(request *tabletmanagerdatapb.BackupRequest, stream tabletmanagerservicepb.TabletManager_BackupServer) error {
	ctx := callinfo.GRPCCallInfo(stream.Context())
	return s.agent.RPCWrapLockAction(ctx, tabletmanager.TabletActionBackup, request, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewCallbackLogger(func(e *logutilpb.Event) {
			// If the client disconnects, we will just fail
			// to send the log events, but won't interrupt
			// the backup.
			stream.Send(&tabletmanagerdatapb.BackupResponse{
				Event: e,
			})
		})

		return s.agent.Backup(ctx, int(request.Concurrency), logger)
	})
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
