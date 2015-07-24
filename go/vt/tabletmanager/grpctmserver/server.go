// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	pbs "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
)

// server is the gRPC implementation of the RPC server
type server struct {
	// implementation of the agent to call
	agent tabletmanager.RPCAgent
}

func (s *server) Ping(ctx context.Context, request *pb.PingRequest) (*pb.PingResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.PingResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionPing, request, response, func() error {
		response.Payload = s.agent.Ping(ctx, request.Payload)
		return nil
	})
}

func (s *server) Sleep(ctx context.Context, request *pb.SleepRequest) (*pb.SleepResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SleepResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSleep, request, response, true, func() error {
		s.agent.Sleep(ctx, time.Duration(request.Duration))
		return nil
	})
}

func (s *server) ExecuteHook(ctx context.Context, request *pb.ExecuteHookRequest) (*pb.ExecuteHookResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ExecuteHookResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionExecuteHook, request, response, true, func() error {
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

func (s *server) GetSchema(ctx context.Context, request *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.GetSchemaResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionGetSchema, request, response, func() error {
		sd, err := s.agent.GetSchema(ctx, request.Tables, request.ExcludeTables, request.IncludeViews)
		if err == nil {
			response.SchemaDefinition = myproto.SchemaDefinitionToProto(sd)
		}
		return err
	})
}

func (s *server) GetPermissions(ctx context.Context, request *pb.GetPermissionsRequest) (*pb.GetPermissionsResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.GetPermissionsResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionGetPermissions, request, response, func() error {
		p, err := s.agent.GetPermissions(ctx)
		if err == nil {
			response.Permissions = myproto.PermissionsToProto(p)
		}
		return err
	})
}

//
// Various read-write methods
//

func (s *server) SetReadOnly(ctx context.Context, request *pb.SetReadOnlyRequest) (*pb.SetReadOnlyResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SetReadOnlyResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSetReadOnly, request, response, true, func() error {
		return s.agent.SetReadOnly(ctx, true)
	})
}

func (s *server) SetReadWrite(ctx context.Context, request *pb.SetReadWriteRequest) (*pb.SetReadWriteResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SetReadWriteResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSetReadWrite, request, response, true, func() error {
		return s.agent.SetReadOnly(ctx, false)
	})
}

func (s *server) ChangeType(ctx context.Context, request *pb.ChangeTypeRequest) (*pb.ChangeTypeResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ChangeTypeResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionChangeType, request, response, true, func() error {
		return s.agent.ChangeType(ctx, topo.ProtoToTabletType(request.TabletType))
	})
}

func (s *server) Scrap(ctx context.Context, request *pb.ScrapRequest) (*pb.ScrapResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ScrapResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionScrap, request, response, true, func() error {
		return s.agent.Scrap(ctx)
	})
}

func (s *server) RefreshState(ctx context.Context, request *pb.RefreshStateRequest) (*pb.RefreshStateResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.RefreshStateResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionRefreshState, request, response, true, func() error {
		s.agent.RefreshState(ctx)
		return nil
	})
}

func (s *server) RunHealthCheck(ctx context.Context, request *pb.RunHealthCheckRequest) (*pb.RunHealthCheckResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.RunHealthCheckResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionRunHealthCheck, request, response, func() error {
		s.agent.RunHealthCheck(ctx, topo.ProtoToTabletType(request.TabletType))
		return nil
	})
}

func (s *server) ReloadSchema(ctx context.Context, request *pb.ReloadSchemaRequest) (*pb.ReloadSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ReloadSchemaResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionReloadSchema, request, response, true, func() error {
		s.agent.ReloadSchema(ctx)
		return nil
	})
}

func (s *server) PreflightSchema(ctx context.Context, request *pb.PreflightSchemaRequest) (*pb.PreflightSchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.PreflightSchemaResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionPreflightSchema, request, response, true, func() error {
		scr, err := s.agent.PreflightSchema(ctx, request.Change)
		if err == nil {
			response.BeforeSchema = myproto.SchemaDefinitionToProto(scr.BeforeSchema)
			response.AfterSchema = myproto.SchemaDefinitionToProto(scr.AfterSchema)
		}
		return err
	})
}

func (s *server) ApplySchema(ctx context.Context, request *pb.ApplySchemaRequest) (*pb.ApplySchemaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ApplySchemaResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionApplySchema, request, response, true, func() error {
		scr, err := s.agent.ApplySchema(ctx, &myproto.SchemaChange{
			Sql:              request.Sql,
			Force:            request.Force,
			AllowReplication: request.AllowReplication,
			BeforeSchema:     myproto.ProtoToSchemaDefinition(request.BeforeSchema),
			AfterSchema:      myproto.ProtoToSchemaDefinition(request.AfterSchema),
		})
		if err == nil {
			response.BeforeSchema = myproto.SchemaDefinitionToProto(scr.BeforeSchema)
			response.AfterSchema = myproto.SchemaDefinitionToProto(scr.AfterSchema)
		}
		return err
	})
}

func (s *server) ExecuteFetchAsDba(ctx context.Context, request *pb.ExecuteFetchAsDbaRequest) (*pb.ExecuteFetchAsDbaResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ExecuteFetchAsDbaResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionExecuteFetchAsDba, request, response, func() error {
		qr, err := s.agent.ExecuteFetchAsDba(ctx, request.Query, request.DbName, int(request.MaxRows), request.WantFields, request.DisableBinlogs, request.ReloadSchema)
		if err == nil {
			response.Result = mproto.QueryResultToProto3(qr)
		}
		return err
	})
}

func (s *server) ExecuteFetchAsApp(ctx context.Context, request *pb.ExecuteFetchAsAppRequest) (*pb.ExecuteFetchAsAppResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ExecuteFetchAsAppResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionExecuteFetchAsApp, request, response, func() error {
		qr, err := s.agent.ExecuteFetchAsApp(ctx, request.Query, int(request.MaxRows), request.WantFields)
		if err == nil {
			response.Result = mproto.QueryResultToProto3(qr)
		}
		return err
	})
}

//
// Replication related methods
//

func (s *server) SlaveStatus(ctx context.Context, request *pb.SlaveStatusRequest) (*pb.SlaveStatusResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SlaveStatusResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionSlaveStatus, request, response, func() error {
		status, err := s.agent.SlaveStatus(ctx)
		if err == nil {
			response.Status = myproto.ReplicationStatusToProto(status)
		}
		return err
	})
}

func (s *server) MasterPosition(ctx context.Context, request *pb.MasterPositionRequest) (*pb.MasterPositionResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.MasterPositionResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionMasterPosition, request, response, func() error {
		position, err := s.agent.MasterPosition(ctx)
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) StopSlave(ctx context.Context, request *pb.StopSlaveRequest) (*pb.StopSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StopSlaveResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionStopSlave, request, response, true, func() error {
		return s.agent.StopSlave(ctx)
	})
}

func (s *server) StopSlaveMinimum(ctx context.Context, request *pb.StopSlaveMinimumRequest) (*pb.StopSlaveMinimumResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StopSlaveMinimumResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionStopSlaveMinimum, request, response, true, func() error {
		position, err := myproto.DecodeReplicationPosition(request.Position)
		if err != nil {
			return err
		}
		position, err = s.agent.StopSlaveMinimum(ctx, position, time.Duration(request.WaitTimeout))
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) StartSlave(ctx context.Context, request *pb.StartSlaveRequest) (*pb.StartSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StartSlaveResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionStartSlave, request, response, true, func() error {
		return s.agent.StartSlave(ctx)
	})
}

func (s *server) TabletExternallyReparented(ctx context.Context, request *pb.TabletExternallyReparentedRequest) (*pb.TabletExternallyReparentedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.TabletExternallyReparentedResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionExternallyReparented, request, response, false, func() error {
		return s.agent.TabletExternallyReparented(ctx, request.ExternalId)
	})
}

func (s *server) TabletExternallyElected(ctx context.Context, request *pb.TabletExternallyElectedRequest) (*pb.TabletExternallyElectedResponse, error) {
	return &pb.TabletExternallyElectedResponse{}, nil
}

func (s *server) GetSlaves(ctx context.Context, request *pb.GetSlavesRequest) (*pb.GetSlavesResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.GetSlavesResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionGetSlaves, request, response, func() error {
		addrs, err := s.agent.GetSlaves(ctx)
		if err == nil {
			response.Addrs = addrs
		}
		return err
	})
}

func (s *server) WaitBlpPosition(ctx context.Context, request *pb.WaitBlpPositionRequest) (*pb.WaitBlpPositionResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.WaitBlpPositionResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionWaitBLPPosition, request, response, true, func() error {
		return s.agent.WaitBlpPosition(ctx, blproto.ProtoToBlpPosition(request.BlpPosition), time.Duration(request.WaitTimeout))
	})
}

func (s *server) StopBlp(ctx context.Context, request *pb.StopBlpRequest) (*pb.StopBlpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StopBlpResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionStopBLP, request, response, true, func() error {
		positions, err := s.agent.StopBlp(ctx)
		if err == nil {
			response.BlpPositions = blproto.BlpPositionListToProto(positions)
		}
		return err
	})
}

func (s *server) StartBlp(ctx context.Context, request *pb.StartBlpRequest) (*pb.StartBlpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StartBlpResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionStartBLP, request, response, true, func() error {
		return s.agent.StartBlp(ctx)
	})
}

func (s *server) RunBlpUntil(ctx context.Context, request *pb.RunBlpUntilRequest) (*pb.RunBlpUntilResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.RunBlpUntilResponse{}
	return response, s.agent.RPCWrapLock(ctx, actionnode.TabletActionRunBLPUntil, request, response, true, func() error {
		position, err := s.agent.RunBlpUntil(ctx, blproto.ProtoToBlpPositionList(request.BlpPositions), time.Duration(request.WaitTimeout))
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(*position)
		}
		return err
	})
}

//
// Reparenting related functions
//

func (s *server) ResetReplication(ctx context.Context, request *pb.ResetReplicationRequest) (*pb.ResetReplicationResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.ResetReplicationResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionResetReplication, request, response, true, func() error {
		return s.agent.ResetReplication(ctx)
	})
}

func (s *server) InitMaster(ctx context.Context, request *pb.InitMasterRequest) (*pb.InitMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.InitMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionInitMaster, request, response, true, func() error {
		position, err := s.agent.InitMaster(ctx)
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) PopulateReparentJournal(ctx context.Context, request *pb.PopulateReparentJournalRequest) (*pb.PopulateReparentJournalResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.PopulateReparentJournalResponse{}
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionPopulateReparentJournal, request, response, func() error {
		position, err := myproto.DecodeReplicationPosition(request.ReplicationPosition)
		if err != nil {
			return err
		}
		return s.agent.PopulateReparentJournal(ctx, request.TimeCreatedNs, request.ActionName, topo.ProtoToTabletAlias(request.MasterAlias), position)
	})
}

func (s *server) InitSlave(ctx context.Context, request *pb.InitSlaveRequest) (*pb.InitSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.InitSlaveResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionInitSlave, request, response, true, func() error {
		position, err := myproto.DecodeReplicationPosition(request.ReplicationPosition)
		if err != nil {
			return err
		}
		return s.agent.InitSlave(ctx, topo.ProtoToTabletAlias(request.Parent), position, request.TimeCreatedNs)
	})
}

func (s *server) DemoteMaster(ctx context.Context, request *pb.DemoteMasterRequest) (*pb.DemoteMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.DemoteMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionDemoteMaster, request, response, true, func() error {
		position, err := s.agent.DemoteMaster(ctx)
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) PromoteSlaveWhenCaughtUp(ctx context.Context, request *pb.PromoteSlaveWhenCaughtUpRequest) (*pb.PromoteSlaveWhenCaughtUpResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.PromoteSlaveWhenCaughtUpResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionPromoteSlaveWhenCaughtUp, request, response, true, func() error {
		position, err := myproto.DecodeReplicationPosition(request.Position)
		if err != nil {
			return err
		}
		position, err = s.agent.PromoteSlaveWhenCaughtUp(ctx, position)
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) SlaveWasPromoted(ctx context.Context, request *pb.SlaveWasPromotedRequest) (*pb.SlaveWasPromotedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SlaveWasPromotedResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSlaveWasPromoted, request, response, true, func() error {
		return s.agent.SlaveWasPromoted(ctx)
	})
}

func (s *server) SetMaster(ctx context.Context, request *pb.SetMasterRequest) (*pb.SetMasterResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SetMasterResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSetMaster, request, response, true, func() error {
		return s.agent.SetMaster(ctx, topo.ProtoToTabletAlias(request.Parent), request.TimeCreatedNs, request.ForceStartSlave)
	})
}

func (s *server) SlaveWasRestarted(ctx context.Context, request *pb.SlaveWasRestartedRequest) (*pb.SlaveWasRestartedResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.SlaveWasRestartedResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionSlaveWasRestarted, request, response, true, func() error {
		return s.agent.SlaveWasRestarted(ctx, &actionnode.SlaveWasRestartedArgs{
			Parent: topo.ProtoToTabletAlias(request.Parent),
		})
	})
}

func (s *server) StopReplicationAndGetStatus(ctx context.Context, request *pb.StopReplicationAndGetStatusRequest) (*pb.StopReplicationAndGetStatusResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.StopReplicationAndGetStatusResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionStopReplicationAndGetStatus, request, response, true, func() error {
		status, err := s.agent.StopReplicationAndGetStatus(ctx)
		if err == nil {
			response.Status = myproto.ReplicationStatusToProto(status)
		}
		return err
	})
}

func (s *server) PromoteSlave(ctx context.Context, request *pb.PromoteSlaveRequest) (*pb.PromoteSlaveResponse, error) {
	ctx = callinfo.GRPCCallInfo(ctx)
	response := &pb.PromoteSlaveResponse{}
	return response, s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionPromoteSlave, request, response, true, func() error {
		position, err := s.agent.PromoteSlave(ctx)
		if err == nil {
			response.Position = myproto.EncodeReplicationPosition(position)
		}
		return err
	})
}

func (s *server) Backup(request *pb.BackupRequest, stream pbs.TabletManager_BackupServer) error {
	ctx := callinfo.GRPCCallInfo(stream.Context())
	return s.agent.RPCWrapLockAction(ctx, actionnode.TabletActionBackup, request, nil, true, func() error {
		// create a logger, send the result back to the caller
		logger := logutil.NewChannelLogger(10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for e := range logger {
				// Note we don't interrupt the loop here, as
				// we still need to flush and finish the
				// command, even if the channel to the client
				// has been broken. We'll just keep trying
				// to send.
				stream.Send(&pb.BackupResponse{
					Event: logutil.LoggerEventToProto(&e),
				})

			}
			wg.Done()
		}()

		err := s.agent.Backup(ctx, int(request.Concurrency), logger)
		close(logger)
		wg.Wait()
		return err
	})
}

// registration glue

func init() {
	tabletmanager.RegisterQueryServices = append(tabletmanager.RegisterQueryServices, func(agent *tabletmanager.ActionAgent) {
		if servenv.GRPCCheckServiceMap("tabletmanager") {
			pbs.RegisterTabletManagerServer(servenv.GRPCServer, &server{agent})
		}
	})
}

// RegisterForTest will register the RPC, to be used by test instances only
func RegisterForTest(s *grpc.Server, agent *tabletmanager.ActionAgent) {
	pbs.RegisterTabletManagerServer(s, &server{agent})
}
