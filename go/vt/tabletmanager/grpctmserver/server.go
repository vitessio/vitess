// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/hook"
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
	return response, s.agent.RPCWrap(ctx, actionnode.TabletActionSleep, request, response, func() error {
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

func (s *server) StreamHealth(request *pb.StreamHealthRequest, stream pbs.TabletManager_StreamHealthServer) error {
	ctx := callinfo.GRPCCallInfo(stream.Context())
	return s.agent.RPCWrap(ctx, actionnode.TabletActionHealthStream, request, nil, func() error {
		c := make(chan *actionnode.HealthStreamReply, 10)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for hsr := range c {
				// we send until the client disconnects
				if err := stream.Send(&pb.StreamHealthResponse{
					Tablet:              topo.TabletToProto(hsr.Tablet),
					BinlogPlayerMapSize: hsr.BinlogPlayerMapSize,
					HealthError:         hsr.HealthError,
					ReplicationDelay:    int64(hsr.ReplicationDelay),
				}); err != nil {
					return
				}
			}
		}()

		id, err := s.agent.RegisterHealthStream(c)
		if err != nil {
			close(c)
			wg.Wait()
			return err
		}
		wg.Wait()
		return s.agent.UnregisterHealthStream(id)

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
	return nil, nil
}

func (s *server) ApplySchema(ctx context.Context, request *pb.ApplySchemaRequest) (*pb.ApplySchemaResponse, error) {
	return nil, nil
}

func (s *server) ExecuteFetchAsDba(ctx context.Context, request *pb.ExecuteFetchAsDbaRequest) (*pb.ExecuteFetchAsDbaResponse, error) {
	return nil, nil
}

func (s *server) ExecuteFetchAsApp(ctx context.Context, request *pb.ExecuteFetchAsAppRequest) (*pb.ExecuteFetchAsAppResponse, error) {
	return nil, nil
}

func (s *server) SlaveStatus(ctx context.Context, request *pb.SlaveStatusRequest) (*pb.SlaveStatusResponse, error) {
	return nil, nil
}

func (s *server) MasterPosition(ctx context.Context, request *pb.MasterPositionRequest) (*pb.MasterPositionResponse, error) {
	return nil, nil
}

func (s *server) StopSlave(ctx context.Context, request *pb.StopSlaveRequest) (*pb.StopSlaveResponse, error) {
	return nil, nil
}

func (s *server) StopSlaveMinimum(ctx context.Context, request *pb.StopSlaveMinimumRequest) (*pb.StopSlaveMinimumResponse, error) {
	return nil, nil
}

func (s *server) StartSlave(ctx context.Context, request *pb.StartSlaveRequest) (*pb.StartSlaveResponse, error) {
	return nil, nil
}

func (s *server) TabletExternallyReparented(ctx context.Context, request *pb.TabletExternallyReparentedRequest) (*pb.TabletExternallyReparentedResponse, error) {
	return nil, nil
}

func (s *server) TabletExternallyElected(ctx context.Context, request *pb.TabletExternallyElectedRequest) (*pb.TabletExternallyElectedResponse, error) {
	return nil, nil
}

func (s *server) GetSlaves(ctx context.Context, request *pb.GetSlavesRequest) (*pb.GetSlavesResponse, error) {
	return nil, nil
}

func (s *server) WaitBlpPosition(ctx context.Context, request *pb.WaitBlpPositionRequest) (*pb.WaitBlpPositionResponse, error) {
	return nil, nil
}

func (s *server) StopBlp(ctx context.Context, request *pb.StopBlpRequest) (*pb.StopBlpResponse, error) {
	return nil, nil
}

func (s *server) StartBlp(ctx context.Context, request *pb.StartBlpRequest) (*pb.StartBlpResponse, error) {
	return nil, nil
}

func (s *server) RunBlpUntil(ctx context.Context, request *pb.RunBlpUntilRequest) (*pb.RunBlpUntilResponse, error) {
	return nil, nil
}

func (s *server) ResetReplication(ctx context.Context, request *pb.ResetReplicationRequest) (*pb.ResetReplicationResponse, error) {
	return nil, nil
}

func (s *server) InitMaster(ctx context.Context, request *pb.InitMasterRequest) (*pb.InitMasterResponse, error) {
	return nil, nil
}

func (s *server) PopulateReparentJournal(ctx context.Context, request *pb.PopulateReparentJournalRequest) (*pb.PopulateReparentJournalResponse, error) {
	return nil, nil
}

func (s *server) InitSlave(ctx context.Context, request *pb.InitSlaveRequest) (*pb.InitSlaveResponse, error) {
	return nil, nil
}

func (s *server) DemoteMaster(ctx context.Context, request *pb.DemoteMasterRequest) (*pb.DemoteMasterResponse, error) {
	return nil, nil
}

func (s *server) PromoteSlaveWhenCaughtUp(ctx context.Context, request *pb.PromoteSlaveWhenCaughtUpRequest) (*pb.PromoteSlaveWhenCaughtUpResponse, error) {
	return nil, nil
}

func (s *server) SlaveWasPromoted(ctx context.Context, request *pb.SlaveWasPromotedRequest) (*pb.SlaveWasPromotedResponse, error) {
	return nil, nil
}

func (s *server) SetMaster(ctx context.Context, request *pb.SetMasterRequest) (*pb.SetMasterResponse, error) {
	return nil, nil
}

func (s *server) SlaveWasRestarted(ctx context.Context, request *pb.SlaveWasRestartedRequest) (*pb.SlaveWasRestartedResponse, error) {
	return nil, nil
}

func (s *server) StopReplicationAndGetStatus(ctx context.Context, request *pb.StopReplicationAndGetStatusRequest) (*pb.StopReplicationAndGetStatusResponse, error) {
	return nil, nil
}

func (s *server) PromoteSlave(ctx context.Context, request *pb.PromoteSlaveRequest) (*pb.PromoteSlaveResponse, error) {
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
