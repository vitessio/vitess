// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmclient

import (
	"flag"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	cert = flag.String("tablet_manager_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("tablet_manager_grpc_key", "", "the key to use to connect")
	ca   = flag.String("tablet_manager_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("tablet_manager_grpc_server_name", "", "the server name to use to validate server certificate")
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
func (client *Client) dial(ctx context.Context, tablet *topodatapb.Tablet) (*grpc.ClientConn, tabletmanagerservicepb.TabletManagerClient, error) {
	// create the RPC client, using ctx.Deadline if set, or no timeout.
	var connectTimeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		connectTimeout = deadline.Sub(time.Now())
		if connectTimeout <= 0 {
			return nil, nil, timeoutError{fmt.Errorf("timeout connecting to TabletManager on %v", tablet.Alias)}
		}
	}

	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, nil, err
	}
	opts := []grpc.DialOption{opt, grpc.WithBlock()}
	if connectTimeout != 0 {
		opts = append(opts, grpc.WithTimeout(connectTimeout))
	}
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, nil, err
	}
	return cc, tabletmanagerservicepb.NewTabletManagerClient(cc), nil
}

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface.
func (client *Client) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	result, err := c.Ping(ctx, &tabletmanagerdatapb.PingRequest{
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

// Sleep is part of the tmclient.TabletManagerClient interface.
func (client *Client) Sleep(ctx context.Context, tablet *topodatapb.Tablet, duration time.Duration) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.Sleep(ctx, &tabletmanagerdatapb.SleepRequest{
		Duration: int64(duration),
	})
	return err
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	hr, err := c.ExecuteHook(ctx, &tabletmanagerdatapb.ExecuteHookRequest{
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

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetSchema(ctx, &tabletmanagerdatapb.GetSchemaRequest{
		Tables:        tables,
		ExcludeTables: excludeTables,
		IncludeViews:  includeViews,
	})
	if err != nil {
		return nil, err
	}
	return response.SchemaDefinition, nil
}

// GetPermissions is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetPermissions(ctx, &tabletmanagerdatapb.GetPermissionsRequest{})
	if err != nil {
		return nil, err
	}
	return response.Permissions, nil
}

//
// Various read-write methods
//

// SetReadOnly is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadOnly(ctx, &tabletmanagerdatapb.SetReadOnlyRequest{})
	return err
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadWrite(ctx, &tabletmanagerdatapb.SetReadWriteRequest{})
	return err
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (client *Client) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ChangeType(ctx, &tabletmanagerdatapb.ChangeTypeRequest{
		TabletType: dbType,
	})
	return err
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (client *Client) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RefreshState(ctx, &tabletmanagerdatapb.RefreshStateRequest{})
	return err
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *Client) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RunHealthCheck(ctx, &tabletmanagerdatapb.RunHealthCheckRequest{})
	return err
}

// IgnoreHealthError is part of the tmclient.TabletManagerClient interface.
func (client *Client) IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.IgnoreHealthError(ctx, &tabletmanagerdatapb.IgnoreHealthErrorRequest{
		Pattern: pattern,
	})
	return err
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ReloadSchema(ctx, &tabletmanagerdatapb.ReloadSchemaRequest{})
	return err
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tmutils.SchemaChangeResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	response, err := c.PreflightSchema(ctx, &tabletmanagerdatapb.PreflightSchemaRequest{
		Changes: changes,
	})
	if err != nil {
		return nil, err
	}

	diffs := make([]*tmutils.SchemaChangeResult, len(changes))
	for i := 0; i < len(changes); i++ {
		diffs[i] = &tmutils.SchemaChangeResult{
			BeforeSchema: response.BeforeSchemas[i],
			AfterSchema:  response.AfterSchemas[i],
		}
	}
	return diffs, nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ApplySchema(ctx, &tabletmanagerdatapb.ApplySchemaRequest{
		Sql:              change.SQL,
		Force:            change.Force,
		AllowReplication: change.AllowReplication,
		BeforeSchema:     change.BeforeSchema,
		AfterSchema:      change.AfterSchema,
	})
	if err != nil {
		return nil, err
	}
	return &tmutils.SchemaChangeResult{
		BeforeSchema: response.BeforeSchema,
		AfterSchema:  response.AfterSchema,
	}, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, query string, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ExecuteFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:          query,
		DbName:         topoproto.TabletDbName(tablet),
		MaxRows:        uint64(maxRows),
		DisableBinlogs: disableBinlogs,
		ReloadSchema:   reloadSchema,
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, query string, maxRows int) (*querypb.QueryResult, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.ExecuteFetchAsApp(ctx, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
		Query:   query,
		MaxRows: uint64(maxRows),
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

//
// Replication related methods
//

// SlaveStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) SlaveStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.SlaveStatus(ctx, &tabletmanagerdatapb.SlaveStatusRequest{})
	if err != nil {
		return nil, err
	}
	return response.Status, nil
}

// MasterPosition is part of the tmclient.TabletManagerClient interface.
func (client *Client) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.MasterPosition(ctx, &tabletmanagerdatapb.MasterPositionRequest{})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// StopSlave is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StopSlave(ctx, &tabletmanagerdatapb.StopSlaveRequest{})
	return err
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, minPos string, waitTime time.Duration) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.StopSlaveMinimum(ctx, &tabletmanagerdatapb.StopSlaveMinimumRequest{
		Position:    minPos,
		WaitTimeout: int64(waitTime),
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// StartSlave is part of the tmclient.TabletManagerClient interface.
func (client *Client) StartSlave(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartSlave(ctx, &tabletmanagerdatapb.StartSlaveRequest{})
	return err
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface.
func (client *Client) TabletExternallyReparented(ctx context.Context, tablet *topodatapb.Tablet, externalID string) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.TabletExternallyReparented(ctx, &tabletmanagerdatapb.TabletExternallyReparentedRequest{
		ExternalId: externalID,
	})
	return err
}

// GetSlaves is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetSlaves(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.GetSlaves(ctx, &tabletmanagerdatapb.GetSlavesRequest{})
	if err != nil {
		return nil, err
	}
	return response.Addrs, nil
}

// WaitBlpPosition is part of the tmclient.TabletManagerClient interface.
func (client *Client) WaitBlpPosition(ctx context.Context, tablet *topodatapb.Tablet, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.WaitBlpPosition(ctx, &tabletmanagerdatapb.WaitBlpPositionRequest{
		BlpPosition: blpPosition,
		WaitTimeout: int64(waitTime),
	})
	return err
}

// StopBlp is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopBlp(ctx context.Context, tablet *topodatapb.Tablet) ([]*tabletmanagerdatapb.BlpPosition, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.StopBlp(ctx, &tabletmanagerdatapb.StopBlpRequest{})
	if err != nil {
		return nil, err
	}
	return response.BlpPositions, nil
}

// StartBlp is part of the tmclient.TabletManagerClient interface.
func (client *Client) StartBlp(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartBlp(ctx, &tabletmanagerdatapb.StartBlpRequest{})
	return err
}

// RunBlpUntil is part of the tmclient.TabletManagerClient interface.
func (client *Client) RunBlpUntil(ctx context.Context, tablet *topodatapb.Tablet, positions []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.RunBlpUntil(ctx, &tabletmanagerdatapb.RunBlpUntilRequest{
		BlpPositions: positions,
		WaitTimeout:  int64(waitTime),
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ResetReplication(ctx, &tabletmanagerdatapb.ResetReplicationRequest{})
	return err
}

// InitMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) InitMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.InitMaster(ctx, &tabletmanagerdatapb.InitMasterRequest{})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (client *Client) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, pos string) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.PopulateReparentJournal(ctx, &tabletmanagerdatapb.PopulateReparentJournalRequest{
		TimeCreatedNs:       timeCreatedNS,
		ActionName:          actionName,
		MasterAlias:         masterAlias,
		ReplicationPosition: pos,
	})
	return err
}

// InitSlave is part of the tmclient.TabletManagerClient interface.
func (client *Client) InitSlave(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.InitSlave(ctx, &tabletmanagerdatapb.InitSlaveRequest{
		Parent:              parent,
		ReplicationPosition: replicationPosition,
		TimeCreatedNs:       timeCreatedNS,
	})
	return err
}

// DemoteMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) DemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.DemoteMaster(ctx, &tabletmanagerdatapb.DemoteMasterRequest{})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// PromoteSlaveWhenCaughtUp is part of the tmclient.TabletManagerClient interface.
func (client *Client) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topodatapb.Tablet, pos string) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.PromoteSlaveWhenCaughtUp(ctx, &tabletmanagerdatapb.PromoteSlaveWhenCaughtUpRequest{
		Position: pos,
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// SlaveWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *Client) SlaveWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SlaveWasPromoted(ctx, &tabletmanagerdatapb.SlaveWasPromotedRequest{})
	return err
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetMaster(ctx, &tabletmanagerdatapb.SetMasterRequest{
		Parent:          parent,
		TimeCreatedNs:   timeCreatedNS,
		ForceStartSlave: forceStartSlave,
	})
	return err
}

// SlaveWasRestarted is part of the tmclient.TabletManagerClient interface.
func (client *Client) SlaveWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SlaveWasRestarted(ctx, &tabletmanagerdatapb.SlaveWasRestartedRequest{
		Parent: parent,
	})
	return err
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.StopReplicationAndGetStatus(ctx, &tabletmanagerdatapb.StopReplicationAndGetStatusRequest{})
	if err != nil {
		return nil, err
	}
	return response.Status, nil
}

// PromoteSlave is part of the tmclient.TabletManagerClient interface.
func (client *Client) PromoteSlave(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	response, err := c.PromoteSlave(ctx, &tabletmanagerdatapb.PromoteSlaveRequest{})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

//
// Backup related methods
//
type eventStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_BackupClient
	cc     *grpc.ClientConn
}

func (e *eventStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.cc.Close()
		return nil, err
	}
	return br.Event, nil
}

// Backup is part of the tmclient.TabletManagerClient interface.
func (client *Client) Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int) (logutil.EventStream, error) {
	cc, c, err := client.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}

	stream, err := c.Backup(ctx, &tabletmanagerdatapb.BackupRequest{
		Concurrency: int64(concurrency),
	})
	if err != nil {
		cc.Close()
		return nil, err
	}
	return &eventStreamAdapter{
		stream: stream,
		cc:     cc,
	}, nil
}

//
// RPC related methods
//

// IsTimeoutError is part of the tmclient.TabletManagerClient interface.
func (client *Client) IsTimeoutError(err error) bool {
	if err == grpc.ErrClientConnTimeout || grpc.Code(err) == codes.DeadlineExceeded {
		return true
	}
	switch err.(type) {
	case timeoutError:
		return true
	default:
		return false
	}
}
