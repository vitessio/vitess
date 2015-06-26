// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"

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
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionSleep, request, response, func() error {
		s.agent.Sleep(ctx, time.Duration(request.Duration))
		return nil
	})
}

func (s *server) ExecuteHook(ctx context.Context, request *pb.ExecuteHookRequest) (*pb.ExecuteHookResponse, error) {
	return nil, nil
}

func (s *server) GetSchema(context.Context, *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	return nil, nil
}

func (s *server) GetPermissions(context.Context, *pb.GetPermissionsRequest) (*pb.GetPermissionsResponse, error) {
	return nil, nil
}

func (s *server) SetReadOnly(context.Context, *pb.SetReadOnlyRequest) (*pb.SetReadOnlyResponse, error) {
	return nil, nil
}

func (s *server) SetReadWrite(context.Context, *pb.SetReadWriteRequest) (*pb.SetReadWriteResponse, error) {
	return nil, nil
}

func (s *server) ChangeType(context.Context, *pb.ChangeTypeRequest) (*pb.ChangeTypeResponse, error) {
	return nil, nil
}

func (s *server) Scrap(context.Context, *pb.ScrapRequest) (*pb.ScrapResponse, error) {
	return nil, nil
}

func (s *server) RefreshState(context.Context, *pb.RefreshStateRequest) (*pb.RefreshStateResponse, error) {
	return nil, nil
}

func (s *server) RunHealthCheck(context.Context, *pb.RunHealthCheckRequest) (*pb.RunHealthCheckResponse, error) {
	return nil, nil
}

func (s *server) StreamHealth(request *pb.StreamHealthRequest, stream pbs.TabletManager_StreamHealthServer) error {
	return nil
}

func (s *server) ReloadSchema(context.Context, *pb.ReloadSchemaRequest) (*pb.ReloadSchemaResponse, error) {
	return nil, nil
}

func (s *server) PreflightSchema(context.Context, *pb.PreflightSchemaRequest) (*pb.PreflightSchemaResponse, error) {
	return nil, nil
}

func (s *server) ApplySchema(context.Context, *pb.ApplySchemaRequest) (*pb.ApplySchemaResponse, error) {
	return nil, nil
}

func (s *server) ExecuteFetchAsDba(context.Context, *pb.ExecuteFetchAsDbaRequest) (*pb.ExecuteFetchAsDbaResponse, error) {
	return nil, nil
}

func (s *server) ExecuteFetchAsApp(context.Context, *pb.ExecuteFetchAsAppRequest) (*pb.ExecuteFetchAsAppResponse, error) {
	return nil, nil
}

func (s *server) SlaveStatus(context.Context, *pb.SlaveStatusRequest) (*pb.SlaveStatusResponse, error) {
	return nil, nil
}

func (s *server) MasterPosition(context.Context, *pb.MasterPositionRequest) (*pb.MasterPositionResponse, error) {
	return nil, nil
}

func (s *server) StopSlave(context.Context, *pb.StopSlaveRequest) (*pb.StopSlaveResponse, error) {
	return nil, nil
}

func (s *server) StopSlaveMinimum(context.Context, *pb.StopSlaveMinimumRequest) (*pb.StopSlaveMinimumResponse, error) {
	return nil, nil
}

func (s *server) StartSlave(context.Context, *pb.StartSlaveRequest) (*pb.StartSlaveResponse, error) {
	return nil, nil
}

func (s *server) TabletExternallyReparented(context.Context, *pb.TabletExternallyReparentedRequest) (*pb.TabletExternallyReparentedResponse, error) {
	return nil, nil
}

func (s *server) TabletExternallyElected(context.Context, *pb.TabletExternallyElectedRequest) (*pb.TabletExternallyElectedResponse, error) {
	return nil, nil
}

func (s *server) GetSlaves(context.Context, *pb.GetSlavesRequest) (*pb.GetSlavesResponse, error) {
	return nil, nil
}

func (s *server) WaitBlpPosition(context.Context, *pb.WaitBlpPositionRequest) (*pb.WaitBlpPositionResponse, error) {
	return nil, nil
}

func (s *server) StopBlp(context.Context, *pb.StopBlpRequest) (*pb.StopBlpResponse, error) {
	return nil, nil
}

func (s *server) StartBlp(context.Context, *pb.StartBlpRequest) (*pb.StartBlpResponse, error) {
	return nil, nil
}

func (s *server) RunBlpUntil(context.Context, *pb.RunBlpUntilRequest) (*pb.RunBlpUntilResponse, error) {
	return nil, nil
}

func (s *server) ResetReplication(context.Context, *pb.ResetReplicationRequest) (*pb.ResetReplicationResponse, error) {
	return nil, nil
}

func (s *server) InitMaster(context.Context, *pb.InitMasterRequest) (*pb.InitMasterResponse, error) {
	return nil, nil
}

func (s *server) PopulateReparentJournal(context.Context, *pb.PopulateReparentJournalRequest) (*pb.PopulateReparentJournalResponse, error) {
	return nil, nil
}

func (s *server) InitSlave(context.Context, *pb.InitSlaveRequest) (*pb.InitSlaveResponse, error) {
	return nil, nil
}

func (s *server) DemoteMaster(context.Context, *pb.DemoteMasterRequest) (*pb.DemoteMasterResponse, error) {
	return nil, nil
}

func (s *server) PromoteSlaveWhenCaughtUp(context.Context, *pb.PromoteSlaveWhenCaughtUpRequest) (*pb.PromoteSlaveWhenCaughtUpResponse, error) {
	return nil, nil
}

func (s *server) SlaveWasPromoted(context.Context, *pb.SlaveWasPromotedRequest) (*pb.SlaveWasPromotedResponse, error) {
	return nil, nil
}

func (s *server) SetMaster(context.Context, *pb.SetMasterRequest) (*pb.SetMasterResponse, error) {
	return nil, nil
}

func (s *server) SlaveWasRestarted(context.Context, *pb.SlaveWasRestartedRequest) (*pb.SlaveWasRestartedResponse, error) {
	return nil, nil
}

func (s *server) StopReplicationAndGetStatus(context.Context, *pb.StopReplicationAndGetStatusRequest) (*pb.StopReplicationAndGetStatusResponse, error) {
	return nil, nil
}

func (s *server) PromoteSlave(context.Context, *pb.PromoteSlaveRequest) (*pb.PromoteSlaveResponse, error) {
	return nil, nil
}

func (s *server) Backup(request *pb.BackupRequest, stream pbs.TabletManager_BackupServer) error {
	return nil
}

// registration glue

func init() {
	tabletmanager.RegisterQueryServices = append(tabletmanager.RegisterQueryServices, func(agent *tabletmanager.ActionAgent) {
		if servenv.GRPCCheckServiceMap("tabletmanager") {
			pbs.RegisterTabletManagerServer(servenv.GRPCServer, &server{agent})
		}
	})
}
