// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmclient

import (
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	pbs "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
)

type timeoutError struct {
	error
}

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
		return &Client{}
	})
}

// Client implements tmclient.TabletManagerClient
type Client struct{}

// dial returns a client to use
func (client *Client) dial(ctx context.Context, tablet *topo.TabletInfo) (*grpc.ClientConn, pbs.TabletManagerClient, error) {
	// create the RPC client, using ctx.Deadline if set, or no timeout.
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout < 0 {
			return nil, nil, timeoutError{fmt.Errorf("timeout connecting to TabletManager on %v", tablet.Alias)}
		}
	}

	var cc *grpc.ClientConn
	var err error
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.Portmap["grpc"]))
	if connectTimeout == 0 {
		cc, err = grpc.Dial(addr, grpc.WithBlock())
	} else {
		cc, err = grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(connectTimeout))
	}
	if err != nil {
		return nil, nil, err
	}
	return cc, pbs.NewTabletManagerClient(cc), nil
}

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface
func (client *Client) Ping(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	result, err := c.Ping(ctx, &pb.PingRequest{
		Payload: "payload",
	})
	if err != nil {
		return err
	}
	if result.Payload != "payload" {
		return fmt.Errorf("bad ping result: %v", result.Payload)
	}
	return nil
}

// Sleep is part of the tmclient.TabletManagerClient interface
func (client *Client) Sleep(ctx context.Context, tablet *topo.TabletInfo, duration time.Duration) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.Sleep(ctx, &pb.SleepRequest{
		Duration: int64(duration),
	})
	return err
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface
func (client *Client) ExecuteHook(ctx context.Context, tablet *topo.TabletInfo, hk *hook.Hook) (*hook.HookResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	hr, err := c.ExecuteHook(ctx, &pb.ExecuteHookRequest{
		Name:       hk.Name,
		Parameters: hk.Parameters,
		ExtraEnv:   hk.ExtraEnv,
	})
	if err != nil {
		return nil, err
	}
	return &hook.HookResult{
		ExitStatus: int(hr.ExitStatus),
		Stdout:     hr.Stdout,
		Stderr:     hr.Stderr,
	}, nil
}

// GetSchema is part of the tmclient.TabletManagerClient interface
func (client *Client) GetSchema(ctx context.Context, tablet *topo.TabletInfo, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetSchema(ctx, &pb.GetSchemaRequest{
		Tables:        tables,
		ExcludeTables: excludeTables,
		IncludeViews:  includeViews,
	})
	if err != nil {
		return nil, err
	}
	return myproto.ProtoToSchemaDefinition(response.SchemaDefinition), nil
}

// GetPermissions is part of the tmclient.TabletManagerClient interface
func (client *Client) GetPermissions(ctx context.Context, tablet *topo.TabletInfo) (*myproto.Permissions, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetPermissions(ctx, &pb.GetPermissionsRequest{})
	if err != nil {
		return nil, err
	}
	return myproto.ProtoToPermissions(response.Permissions), nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface
func (client *Client) SetReadOnly(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadOnly(ctx, &pb.SetReadOnlyRequest{})
	return err
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface
func (client *Client) SetReadWrite(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadWrite(ctx, &pb.SetReadWriteRequest{})
	return err
}

// ChangeType is part of the tmclient.TabletManagerClient interface
func (client *Client) ChangeType(ctx context.Context, tablet *topo.TabletInfo, dbType topo.TabletType) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ChangeType(ctx, &pb.ChangeTypeRequest{
		TabletType: topo.TabletTypeToProto(dbType),
	})
	return err
}

// Scrap is part of the tmclient.TabletManagerClient interface
func (client *Client) Scrap(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.Scrap(ctx, &pb.ScrapRequest{})
	return err
}

// RefreshState is part of the tmclient.TabletManagerClient interface
func (client *Client) RefreshState(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RefreshState(ctx, &pb.RefreshStateRequest{})
	return err
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface
func (client *Client) RunHealthCheck(ctx context.Context, tablet *topo.TabletInfo, targetTabletType topo.TabletType) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RunHealthCheck(ctx, &pb.RunHealthCheckRequest{
		TabletType: topo.TabletTypeToProto(targetTabletType),
	})
	return err
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface
func (client *Client) ReloadSchema(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ReloadSchema(ctx, &pb.ReloadSchemaRequest{})
	return err
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface
func (client *Client) PreflightSchema(ctx context.Context, tablet *topo.TabletInfo, change string) (*myproto.SchemaChangeResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.PreflightSchema(ctx, &pb.PreflightSchemaRequest{
		Change: change,
	})
	if err != nil {
		return nil, err
	}
	return &myproto.SchemaChangeResult{
		BeforeSchema: myproto.ProtoToSchemaDefinition(response.BeforeSchema),
		AfterSchema:  myproto.ProtoToSchemaDefinition(response.AfterSchema),
	}, err
}

// ApplySchema is part of the tmclient.TabletManagerClient interface
func (client *Client) ApplySchema(ctx context.Context, tablet *topo.TabletInfo, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ApplySchema(ctx, &pb.ApplySchemaRequest{
		Sql:              change.Sql,
		Force:            change.Force,
		AllowReplication: change.AllowReplication,
		BeforeSchema:     myproto.SchemaDefinitionToProto(change.BeforeSchema),
		AfterSchema:      myproto.SchemaDefinitionToProto(change.AfterSchema),
	})
	if err != nil {
		return nil, err
	}
	return &myproto.SchemaChangeResult{
		BeforeSchema: myproto.ProtoToSchemaDefinition(response.BeforeSchema),
		AfterSchema:  myproto.ProtoToSchemaDefinition(response.AfterSchema),
	}, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface
func (client *Client) ExecuteFetchAsDba(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields, disableBinlogs, reloadSchema bool) (*mproto.QueryResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ExecuteFetchAsDba(ctx, &pb.ExecuteFetchAsDbaRequest{
		Query:          query,
		DbName:         tablet.DbName(),
		MaxRows:        uint64(maxRows),
		WantFields:     wantFields,
		DisableBinlogs: disableBinlogs,
		ReloadSchema:   reloadSchema,
	})
	if err != nil {
		return nil, err
	}
	return mproto.Proto3ToQueryResult(response.Result), nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface
func (client *Client) ExecuteFetchAsApp(ctx context.Context, tablet *topo.TabletInfo, query string, maxRows int, wantFields bool) (*mproto.QueryResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ExecuteFetchAsApp(ctx, &pb.ExecuteFetchAsAppRequest{
		Query:      query,
		MaxRows:    uint64(maxRows),
		WantFields: wantFields,
	})
	if err != nil {
		return nil, err
	}
	return mproto.Proto3ToQueryResult(response.Result), nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface
func (client *Client) SlaveStatus(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationStatus, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationStatus{}, err
	}
	defer cc.Close()
	response, err := c.SlaveStatus(ctx, &pb.SlaveStatusRequest{})
	if err != nil {
		return myproto.ReplicationStatus{}, err
	}
	return myproto.ProtoToReplicationStatus(response.Status), nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface
func (client *Client) MasterPosition(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.MasterPosition(ctx, &pb.MasterPositionRequest{})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

// StopSlave is part of the tmclient.TabletManagerClient interface
func (client *Client) StopSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StopSlave(ctx, &pb.StopSlaveRequest{})
	return err
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface
func (client *Client) StopSlaveMinimum(ctx context.Context, tablet *topo.TabletInfo, minPos myproto.ReplicationPosition, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.StopSlaveMinimum(ctx, &pb.StopSlaveMinimumRequest{
		Position:    myproto.EncodeReplicationPosition(minPos),
		WaitTimeout: int64(waitTime),
	})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

// StartSlave is part of the tmclient.TabletManagerClient interface
func (client *Client) StartSlave(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartSlave(ctx, &pb.StartSlaveRequest{})
	return err
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface
func (client *Client) TabletExternallyReparented(ctx context.Context, tablet *topo.TabletInfo, externalID string) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.TabletExternallyReparented(ctx, &pb.TabletExternallyReparentedRequest{
		ExternalId: externalID,
	})
	return err
}

// GetSlaves is part of the tmclient.TabletManagerClient interface
func (client *Client) GetSlaves(ctx context.Context, tablet *topo.TabletInfo) ([]string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetSlaves(ctx, &pb.GetSlavesRequest{})
	if err != nil {
		return nil, err
	}
	return response.Addrs, nil
}

// WaitBlpPosition is part of the tmclient.TabletManagerClient interface
func (client *Client) WaitBlpPosition(ctx context.Context, tablet *topo.TabletInfo, blpPosition blproto.BlpPosition, waitTime time.Duration) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.WaitBlpPosition(ctx, &pb.WaitBlpPositionRequest{
		BlpPosition: blproto.BlpPositionToProto(&blpPosition),
		WaitTimeout: int64(waitTime),
	})
	return err
}

// StopBlp is part of the tmclient.TabletManagerClient interface
func (client *Client) StopBlp(ctx context.Context, tablet *topo.TabletInfo) (*blproto.BlpPositionList, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.StopBlp(ctx, &pb.StopBlpRequest{})
	if err != nil {
		return nil, err
	}
	return blproto.ProtoToBlpPositionList(response.BlpPositions), nil
}

// StartBlp is part of the tmclient.TabletManagerClient interface
func (client *Client) StartBlp(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartBlp(ctx, &pb.StartBlpRequest{})
	return err
}

// RunBlpUntil is part of the tmclient.TabletManagerClient interface
func (client *Client) RunBlpUntil(ctx context.Context, tablet *topo.TabletInfo, positions *blproto.BlpPositionList, waitTime time.Duration) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.RunBlpUntil(ctx, &pb.RunBlpUntilRequest{
		BlpPositions: blproto.BlpPositionListToProto(positions),
		WaitTimeout:  int64(waitTime),
	})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface
func (client *Client) ResetReplication(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ResetReplication(ctx, &pb.ResetReplicationRequest{})
	return err
}

// InitMaster is part of the tmclient.TabletManagerClient interface
func (client *Client) InitMaster(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.InitMaster(ctx, &pb.InitMasterRequest{})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface
func (client *Client) PopulateReparentJournal(ctx context.Context, tablet *topo.TabletInfo, timeCreatedNS int64, actionName string, masterAlias topo.TabletAlias, pos myproto.ReplicationPosition) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.PopulateReparentJournal(ctx, &pb.PopulateReparentJournalRequest{
		TimeCreatedNs:       timeCreatedNS,
		ActionName:          actionName,
		MasterAlias:         topo.TabletAliasToProto(masterAlias),
		ReplicationPosition: myproto.EncodeReplicationPosition(pos),
	})
	return err
}

// InitSlave is part of the tmclient.TabletManagerClient interface
func (client *Client) InitSlave(ctx context.Context, tablet *topo.TabletInfo, parent topo.TabletAlias, replicationPosition myproto.ReplicationPosition, timeCreatedNS int64) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.InitSlave(ctx, &pb.InitSlaveRequest{
		Parent:              topo.TabletAliasToProto(parent),
		ReplicationPosition: myproto.EncodeReplicationPosition(replicationPosition),
		TimeCreatedNs:       timeCreatedNS,
	})
	return err
}

// DemoteMaster is part of the tmclient.TabletManagerClient interface
func (client *Client) DemoteMaster(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.DemoteMaster(ctx, &pb.DemoteMasterRequest{})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

// PromoteSlaveWhenCaughtUp is part of the tmclient.TabletManagerClient interface
func (client *Client) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topo.TabletInfo, pos myproto.ReplicationPosition) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.PromoteSlaveWhenCaughtUp(ctx, &pb.PromoteSlaveWhenCaughtUpRequest{
		Position: myproto.EncodeReplicationPosition(pos),
	})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface
func (client *Client) SlaveWasPromoted(ctx context.Context, tablet *topo.TabletInfo) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SlaveWasPromoted(ctx, &pb.SlaveWasPromotedRequest{})
	return err
}

// SetMaster is part of the tmclient.TabletManagerClient interface
func (client *Client) SetMaster(ctx context.Context, tablet *topo.TabletInfo, parent topo.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetMaster(ctx, &pb.SetMasterRequest{
		Parent:          topo.TabletAliasToProto(parent),
		TimeCreatedNs:   timeCreatedNS,
		ForceStartSlave: forceStartSlave,
	})
	return err
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface
func (client *Client) SlaveWasRestarted(ctx context.Context, tablet *topo.TabletInfo, args *actionnode.SlaveWasRestartedArgs) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SlaveWasRestarted(ctx, &pb.SlaveWasRestartedRequest{
		Parent: topo.TabletAliasToProto(args.Parent),
	})
	return err
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface
func (client *Client) StopReplicationAndGetStatus(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationStatus, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationStatus{}, err
	}
	defer cc.Close()
	response, err := c.StopReplicationAndGetStatus(ctx, &pb.StopReplicationAndGetStatusRequest{})
	if err != nil {
		return myproto.ReplicationStatus{}, err
	}
	return myproto.ProtoToReplicationStatus(response.Status), nil
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface
func (client *Client) PromoteSlave(ctx context.Context, tablet *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	defer cc.Close()
	response, err := c.PromoteSlave(ctx, &pb.PromoteSlaveRequest{})
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	position, err := myproto.DecodeReplicationPosition(response.Position)
	if err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return position, err
}

//
// Backup related methods
//

// Backup is part of the tmclient.TabletManagerClient interface
func (client *Client) Backup(ctx context.Context, tablet *topo.TabletInfo, concurrency int) (<-chan *logutil.LoggerEvent, tmclient.ErrFunc, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, nil, err
	}

	logstream := make(chan *logutil.LoggerEvent, 10)
	stream, err := c.Backup(ctx, &pb.BackupRequest{
		Concurrency: int64(concurrency),
	})
	if err != nil {
		cc.Close()
		return nil, nil, err
	}

	var finalErr error
	go func() {
		for {
			br, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalErr = err
				}
				close(logstream)
				return
			}
			logstream <- logutil.ProtoToLoggerEvent(br.Event)
		}
	}()
	return logstream, func() error {
		cc.Close()
		return finalErr
	}, nil
}

//
// RPC related methods
//

// IsTimeoutError is part of the tmclient.TabletManagerClient interface
func (client *Client) IsTimeoutError(err error) bool {
	switch err.(type) {
	case timeoutError:
		return true
	default:
		return false
	}
}
