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

package grpctmclient

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	concurrency = flag.Int("tablet_manager_grpc_concurrency", 8, "concurrency to use to talk to a vttablet server for performance-sensitive RPCs (like ExecuteFetchAs{Dba,AllPrivs,App})")
	cert        = flag.String("tablet_manager_grpc_cert", "", "the cert to use to connect")
	key         = flag.String("tablet_manager_grpc_key", "", "the key to use to connect")
	ca          = flag.String("tablet_manager_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name        = flag.String("tablet_manager_grpc_server_name", "", "the server name to use to validate server certificate")
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
		return NewClient()
	})
}

type tmc struct {
	cc     *grpc.ClientConn
	client tabletmanagerservicepb.TabletManagerClient
}

// Client implements tmclient.TabletManagerClient
type Client struct {
	// This cache of connections is to maximize QPS for ExecuteFetch.
	// Note we'll keep the clients open and close them upon Close() only.
	// But that's OK because usually the tasks that use them are
	// one-purpose only.
	// The map is protected by the mutex.
	mu           sync.Mutex
	rpcClientMap map[string]chan *tmc
}

// NewClient returns a new gRPC client.
func NewClient() *Client {
	return &Client{}
}

// dial returns a client to use
func (client *Client) dial(tablet *topodatapb.Tablet) (*grpc.ClientConn, tabletmanagerservicepb.TabletManagerClient, error) {
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, nil, err
	}
	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, nil, err
	}
	return cc, tabletmanagerservicepb.NewTabletManagerClient(cc), nil
}

func (client *Client) dialPool(tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, error) {
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}

	client.mu.Lock()
	if client.rpcClientMap == nil {
		client.rpcClientMap = make(map[string]chan *tmc)
	}
	c, ok := client.rpcClientMap[addr]
	if !ok {
		c = make(chan *tmc, *concurrency)
		client.rpcClientMap[addr] = c
		client.mu.Unlock()

		for i := 0; i < cap(c); i++ {
			cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
			if err != nil {
				return nil, err
			}
			c <- &tmc{
				cc:     cc,
				client: tabletmanagerservicepb.NewTabletManagerClient(cc),
			}
		}
	} else {
		client.mu.Unlock()
	}

	result := <-c
	c <- result
	return result.client, nil
}

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface.
func (client *Client) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadOnly(ctx, &tabletmanagerdatapb.SetReadOnlyRequest{})
	return err
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SetReadWrite(ctx, &tabletmanagerdatapb.SetReadWriteRequest{})
	return err
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (client *Client) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RefreshState(ctx, &tabletmanagerdatapb.RefreshStateRequest{})
	return err
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *Client) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.RunHealthCheck(ctx, &tabletmanagerdatapb.RunHealthCheckRequest{})
	return err
}

// IgnoreHealthError is part of the tmclient.TabletManagerClient interface.
func (client *Client) IgnoreHealthError(ctx context.Context, tablet *topodatapb.Tablet, pattern string) error {
	cc, c, err := client.dial(tablet)
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
func (client *Client) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ReloadSchema(ctx, &tabletmanagerdatapb.ReloadSchemaRequest{
		WaitPosition: waitPosition,
	})
	return err
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	cc, c, err := client.dial(tablet)
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

	return response.ChangeResults, nil
}

// ApplySchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	cc, c, err := client.dial(tablet)
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
	return &tabletmanagerdatapb.SchemaChangeResult{
		BeforeSchema: response.BeforeSchema,
		AfterSchema:  response.AfterSchema,
	}, nil
}

// LockTables is part of the tmclient.TabletManagerClient interface.
func (client *Client) LockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()

	_, err = c.LockTables(ctx, &tabletmanagerdatapb.LockTablesRequest{})
	return err
}

// UnlockTables is part of the tmclient.TabletManagerClient interface.
func (client *Client) UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()

	_, err = c.UnlockTables(ctx, &tabletmanagerdatapb.UnlockTablesRequest{})
	return err
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var err error
	if usePool {
		c, err = client.dialPool(tablet)
		if err != nil {
			return nil, err
		}
	} else {
		var cc *grpc.ClientConn
		cc, c, err = client.dial(tablet)
		if err != nil {
			return nil, err
		}
		defer cc.Close()
	}

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

// ExecuteFetchAsAllPrivs is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, query []byte, maxRows int, reloadSchema bool) (*querypb.QueryResult, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var err error
	var cc *grpc.ClientConn
	cc, c, err = client.dial(tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	response, err := c.ExecuteFetchAsAllPrivs(ctx, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
		Query:        query,
		DbName:       topoproto.TabletDbName(tablet),
		MaxRows:      uint64(maxRows),
		ReloadSchema: reloadSchema,
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int) (*querypb.QueryResult, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var err error
	if usePool {
		c, err = client.dialPool(tablet)
		if err != nil {
			return nil, err
		}
	} else {
		var cc *grpc.ClientConn
		cc, c, err = client.dial(tablet)
		if err != nil {
			return nil, err
		}
		defer cc.Close()
	}

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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StopSlave(ctx, &tabletmanagerdatapb.StopSlaveRequest{})
	return err
}

// StopSlaveMinimum is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopSlaveMinimum(ctx context.Context, tablet *topodatapb.Tablet, minPos string, waitTime time.Duration) (string, error) {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartSlave(ctx, &tabletmanagerdatapb.StartSlaveRequest{})
	return err
}

// StartSlaveUntilAfter is part of the tmclient.TabletManagerClient interface.
func (client *Client) StartSlaveUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, waitTime time.Duration) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.StartSlaveUntilAfter(ctx, &tabletmanagerdatapb.StartSlaveUntilAfterRequest{
		Position:    position,
		WaitTimeout: int64(waitTime),
	})
	return err
}

// TabletExternallyReparented is part of the tmclient.TabletManagerClient interface.
func (client *Client) TabletExternallyReparented(ctx context.Context, tablet *topodatapb.Tablet, externalID string) error {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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

// VReplicationExec is part of the tmclient.TabletManagerClient interface.
func (client *Client) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	response, err := c.VReplicationExec(ctx, &tabletmanagerdatapb.VReplicationExecRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// VReplicationWaitForPos is part of the tmclient.TabletManagerClient interface.
func (client *Client) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	if _, err = c.VReplicationWaitForPos(ctx, &tabletmanagerdatapb.VReplicationWaitForPosRequest{Id: int64(id), Position: pos}); err != nil {
		return err
	}
	return nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.ResetReplication(ctx, &tabletmanagerdatapb.ResetReplicationRequest{})
	return err
}

// InitMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) InitMaster(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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

// UndoDemoteMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) UndoDemoteMaster(ctx context.Context, tablet *topodatapb.Tablet) error {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.UndoDemoteMaster(ctx, &tabletmanagerdatapb.UndoDemoteMasterRequest{})
	return err
}

// PromoteSlaveWhenCaughtUp is part of the tmclient.TabletManagerClient interface.
func (client *Client) PromoteSlaveWhenCaughtUp(ctx context.Context, tablet *topodatapb.Tablet, pos string) (string, error) {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = c.SlaveWasPromoted(ctx, &tabletmanagerdatapb.SlaveWasPromotedRequest{})
	return err
}

// SetMaster is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetMaster(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
	cc, c, err := client.dial(tablet)
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
type backupStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_BackupClient
	cc     *grpc.ClientConn
}

func (e *backupStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.cc.Close()
		return nil, err
	}
	return br.Event, nil
}

// Backup is part of the tmclient.TabletManagerClient interface.
func (client *Client) Backup(ctx context.Context, tablet *topodatapb.Tablet, concurrency int) (logutil.EventStream, error) {
	cc, c, err := client.dial(tablet)
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
	return &backupStreamAdapter{
		stream: stream,
		cc:     cc,
	}, nil
}

type restoreFromBackupStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_RestoreFromBackupClient
	cc     *grpc.ClientConn
}

func (e *restoreFromBackupStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.cc.Close()
		return nil, err
	}
	return br.Event, nil
}

// RestoreFromBackup is part of the tmclient.TabletManagerClient interface.
func (client *Client) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet) (logutil.EventStream, error) {
	cc, c, err := client.dial(tablet)
	if err != nil {
		return nil, err
	}

	stream, err := c.RestoreFromBackup(ctx, &tabletmanagerdatapb.RestoreFromBackupRequest{})
	if err != nil {
		cc.Close()
		return nil, err
	}
	return &restoreFromBackupStreamAdapter{
		stream: stream,
		cc:     cc,
	}, nil
}

// Close is part of the tmclient.TabletManagerClient interface.
func (client *Client) Close() {
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, c := range client.rpcClientMap {
		close(c)
		for ch := range c {
			ch.cc.Close()
		}
	}
	client.rpcClientMap = nil
}
