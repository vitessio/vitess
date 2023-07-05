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

package grpctmclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/servenv"
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
	concurrency = 8
	cert        string
	key         string
	ca          string
	crl         string
	name        string
)

func registerFlags(fs *pflag.FlagSet) {
	fs.IntVar(&concurrency, "tablet_manager_grpc_concurrency", concurrency, "concurrency to use to talk to a vttablet server for performance-sensitive RPCs (like ExecuteFetchAs{Dba,AllPrivs,App})")
	fs.StringVar(&cert, "tablet_manager_grpc_cert", cert, "the cert to use to connect")
	fs.StringVar(&key, "tablet_manager_grpc_key", key, "the key to use to connect")
	fs.StringVar(&ca, "tablet_manager_grpc_ca", ca, "the server ca to use to validate servers when connecting")
	fs.StringVar(&crl, "tablet_manager_grpc_crl", crl, "the server crl to use to validate server certificates when connecting")
	fs.StringVar(&name, "tablet_manager_grpc_server_name", name, "the server name to use to validate server certificate")
}

var _binaries = []string{ // binaries that require the flags in this package
	"vtbackup",
	"vtcombo",
	"vtctl",
	"vtctld",
	"vtctldclient",
	"vtorc",
	"vttablet",
	"vttestserver",
}

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
		return NewClient()
	})
	tmclient.RegisterTabletManagerClientFactory("grpc-oneshot", func() tmclient.TabletManagerClient {
		return NewClient()
	})

	for _, cmd := range _binaries {
		servenv.OnParseFor(cmd, registerFlags)
	}
}

type tmc struct {
	cc     *grpc.ClientConn
	client tabletmanagerservicepb.TabletManagerClient
}

// grpcClient implements both dialer and poolDialer.
type grpcClient struct {
	// This cache of connections is to maximize QPS for ExecuteFetch.
	// Note we'll keep the clients open and close them upon Close() only.
	// But that's OK because usually the tasks that use them are
	// one-purpose only.
	// The map is protected by the mutex.
	mu           sync.Mutex
	rpcClientMap map[string]chan *tmc
}

type dialer interface {
	dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error)
	Close()
}

type poolDialer interface {
	dialPool(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, error)
}

// Client implements tmclient.TabletManagerClient.
//
// Connections are produced by the dialer implementation, which is either the
// grpcClient implementation, which reuses connections only for ExecuteFetch and
// otherwise makes single-purpose connections that are closed after use.
//
// In order to more efficiently use the underlying tcp connections, you can
// instead use the cachedConnDialer implementation by specifying
//
//	-tablet_manager_protocol "grpc-cached"
//
// The cachedConnDialer keeps connections to up to -tablet_manager_grpc_connpool_size distinct
// tablets open at any given time, for faster per-RPC call time, and less
// connection churn.
type Client struct {
	dialer dialer
}

// NewClient returns a new gRPC client.
func NewClient() *Client {
	return &Client{
		dialer: &grpcClient{},
	}
}

// dial returns a client to use
func (client *grpcClient) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, nil, err
	}
	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, nil, err
	}

	return tabletmanagerservicepb.NewTabletManagerClient(cc), cc, nil
}

func (client *grpcClient) dialPool(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, error) {
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, err
	}

	client.mu.Lock()
	if client.rpcClientMap == nil {
		client.rpcClientMap = make(map[string]chan *tmc)
	}
	c, ok := client.rpcClientMap[addr]
	if !ok {
		c = make(chan *tmc, concurrency)
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

// Close is part of the tmclient.TabletManagerClient interface.
func (client *grpcClient) Close() {
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

//
// Various read-only methods
//

// Ping is part of the tmclient.TabletManagerClient interface.
func (client *Client) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
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
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.Sleep(ctx, &tabletmanagerdatapb.SleepRequest{
		Duration: int64(duration),
	})
	return err
}

// ExecuteHook is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteHook(ctx context.Context, tablet *topodatapb.Tablet, hk *hook.Hook) (*hook.HookResult, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
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
func (client *Client) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.GetSchema(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.SchemaDefinition, nil
}

// GetPermissions is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetPermissions(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.Permissions, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
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
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.SetReadOnly(ctx, &tabletmanagerdatapb.SetReadOnlyRequest{})
	return err
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.SetReadWrite(ctx, &tabletmanagerdatapb.SetReadWriteRequest{})
	return err
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (client *Client) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ChangeType(ctx, &tabletmanagerdatapb.ChangeTypeRequest{
		TabletType: dbType,
		SemiSync:   semiSync,
	})
	return err
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (client *Client) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.RefreshState(ctx, &tabletmanagerdatapb.RefreshStateRequest{})
	return err
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *Client) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.RunHealthCheck(ctx, &tabletmanagerdatapb.RunHealthCheckRequest{})
	return err
}

// ReloadSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ReloadSchema(ctx, &tabletmanagerdatapb.ReloadSchemaRequest{
		WaitPosition: waitPosition,
	})
	return err
}

func (client *Client) ResetSequences(ctx context.Context, tablet *topodatapb.Tablet, tables []string) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ResetSequences(ctx, &tabletmanagerdatapb.ResetSequencesRequest{
		Tables: tables,
	})
	return err
}

// PreflightSchema is part of the tmclient.TabletManagerClient interface.
func (client *Client) PreflightSchema(ctx context.Context, tablet *topodatapb.Tablet, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

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
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.ApplySchema(ctx, &tabletmanagerdatapb.ApplySchemaRequest{
		Sql:              change.SQL,
		Force:            change.Force,
		AllowReplication: change.AllowReplication,
		BeforeSchema:     change.BeforeSchema,
		AfterSchema:      change.AfterSchema,
		SqlMode:          change.SQLMode,
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
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = c.LockTables(ctx, &tabletmanagerdatapb.LockTablesRequest{})
	return err
}

// UnlockTables is part of the tmclient.TabletManagerClient interface.
func (client *Client) UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = c.UnlockTables(ctx, &tabletmanagerdatapb.UnlockTablesRequest{})
	return err
}

// ExecuteQuery is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteQuery(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteQueryRequest) (*querypb.QueryResult, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	cid := req.CallerId
	if cid == nil {
		cid = callerid.EffectiveCallerIDFromContext(ctx)
	}

	response, err := c.ExecuteQuery(ctx, &tabletmanagerdatapb.ExecuteQueryRequest{
		Query:    req.Query,
		DbName:   topoproto.TabletDbName(tablet),
		MaxRows:  req.MaxRows,
		CallerId: cid,
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var err error
	if usePool {
		if poolDialer, ok := client.dialer.(poolDialer); ok {
			c, err = poolDialer.dialPool(ctx, tablet)
			if err != nil {
				return nil, err
			}
		}
	}

	if !usePool || c == nil {
		var closer io.Closer
		c, closer, err = client.dialer.dial(ctx, tablet)
		if err != nil {
			return nil, err
		}
		defer closer.Close()
	}

	response, err := c.ExecuteFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:          req.Query,
		DbName:         topoproto.TabletDbName(tablet),
		MaxRows:        req.MaxRows,
		DisableBinlogs: req.DisableBinlogs,
		ReloadSchema:   req.DisableBinlogs,
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// ExecuteFetchAsAllPrivs is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	response, err := c.ExecuteFetchAsAllPrivs(ctx, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
		Query:        req.Query,
		DbName:       topoproto.TabletDbName(tablet),
		MaxRows:      req.MaxRows,
		ReloadSchema: req.ReloadSchema,
	})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// ExecuteFetchAsApp is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var err error
	if usePool {
		if poolDialer, ok := client.dialer.(poolDialer); ok {
			c, err = poolDialer.dialPool(ctx, tablet)
			if err != nil {
				return nil, err
			}
		}
	}

	if !usePool || c == nil {
		var closer io.Closer
		c, closer, err = client.dialer.dial(ctx, tablet)
		if err != nil {
			return nil, err
		}
		defer closer.Close()
	}

	response, err := c.ExecuteFetchAsApp(ctx, req)
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

//
// Replication related methods
//

// ReplicationStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.ReplicationStatus(ctx, &tabletmanagerdatapb.ReplicationStatusRequest{})
	if err != nil {
		return nil, err
	}
	return response.Status, nil
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.FullStatus(ctx, &tabletmanagerdatapb.FullStatusRequest{})
	if err != nil {
		return nil, err
	}
	return response.Status, nil
}

// PrimaryStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.PrimaryStatus(ctx, &tabletmanagerdatapb.PrimaryStatusRequest{})
	if err != nil {
		return nil, err
	}
	return response.Status, nil
}

// PrimaryPosition is part of the tmclient.TabletManagerClient interface.
func (client *Client) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer closer.Close()
	response, err := c.PrimaryPosition(ctx, &tabletmanagerdatapb.PrimaryPositionRequest{})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// WaitForPosition is part of the tmclient.TabletManagerClient interface.
func (client *Client) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.WaitForPosition(ctx, &tabletmanagerdatapb.WaitForPositionRequest{Position: pos})
	return err
}

// StopReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.StopReplication(ctx, &tabletmanagerdatapb.StopReplicationRequest{})
	return err
}

// StopReplicationMinimum is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopReplicationMinimum(ctx context.Context, tablet *topodatapb.Tablet, minPos string, waitTime time.Duration) (string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer closer.Close()

	response, err := c.StopReplicationMinimum(ctx, &tabletmanagerdatapb.StopReplicationMinimumRequest{
		Position:    minPos,
		WaitTimeout: int64(waitTime),
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// StartReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) StartReplication(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.StartReplication(ctx, &tabletmanagerdatapb.StartReplicationRequest{
		SemiSync: semiSync,
	})
	return err
}

// StartReplicationUntilAfter is part of the tmclient.TabletManagerClient interface.
func (client *Client) StartReplicationUntilAfter(ctx context.Context, tablet *topodatapb.Tablet, position string, waitTime time.Duration) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.StartReplicationUntilAfter(ctx, &tabletmanagerdatapb.StartReplicationUntilAfterRequest{
		Position:    position,
		WaitTimeout: int64(waitTime),
	})
	return err
}

// GetReplicas is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetReplicas(ctx context.Context, tablet *topodatapb.Tablet) ([]string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.GetReplicas(ctx, &tabletmanagerdatapb.GetReplicasRequest{})
	if err != nil {
		return nil, err
	}
	return response.Addrs, nil
}

//
// VReplication related methods
//

func (client *Client) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.CreateVReplicationWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (client *Client) DeleteVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (*tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.DeleteVReplicationWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (client *Client) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.ReadVReplicationWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// VReplicationExec is part of the tmclient.TabletManagerClient interface.
func (client *Client) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.VReplicationExec(ctx, &tabletmanagerdatapb.VReplicationExecRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return response.Result, nil
}

// VReplicationWaitForPos is part of the tmclient.TabletManagerClient interface.
func (client *Client) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	if _, err = c.VReplicationWaitForPos(ctx, &tabletmanagerdatapb.VReplicationWaitForPosRequest{Id: id, Position: pos}); err != nil {
		return err
	}
	return nil
}

func (client *Client) UpdateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.UpdateVReplicationWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// VDiff is part of the tmclient.TabletManagerClient interface.
func (client *Client) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	log.Infof("VDiff for tablet %s, request %+v", tablet.Alias.String(), req)
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.VDiff(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

//
// Reparenting related functions
//

// ResetReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) ResetReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ResetReplication(ctx, &tabletmanagerdatapb.ResetReplicationRequest{})
	return err
}

// InitPrimary is part of the tmclient.TabletManagerClient interface.
func (client *Client) InitPrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer closer.Close()

	response, err := c.InitPrimary(ctx, &tabletmanagerdatapb.InitPrimaryRequest{
		SemiSync: semiSync,
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (client *Client) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, tabletAlias *topodatapb.TabletAlias, pos string) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.PopulateReparentJournal(ctx, &tabletmanagerdatapb.PopulateReparentJournalRequest{
		TimeCreatedNs:       timeCreatedNS,
		ActionName:          actionName,
		PrimaryAlias:        tabletAlias,
		ReplicationPosition: pos,
	})
	return err
}

// InitReplica is part of the tmclient.TabletManagerClient interface.
func (client *Client) InitReplica(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, replicationPosition string, timeCreatedNS int64, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.InitReplica(ctx, &tabletmanagerdatapb.InitReplicaRequest{
		Parent:              parent,
		ReplicationPosition: replicationPosition,
		TimeCreatedNs:       timeCreatedNS,
		SemiSync:            semiSync,
	})
	return err
}

// DemotePrimary is part of the tmclient.TabletManagerClient interface.
func (client *Client) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.DemotePrimary(ctx, &tabletmanagerdatapb.DemotePrimaryRequest{})
	if err != nil {
		return nil, err
	}
	return response.PrimaryStatus, nil
}

// UndoDemotePrimary is part of the tmclient.TabletManagerClient interface.
func (client *Client) UndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.UndoDemotePrimary(ctx, &tabletmanagerdatapb.UndoDemotePrimaryRequest{
		SemiSync: semiSync,
	})
	return err
}

// ReplicaWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReplicaWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ReplicaWasPromoted(ctx, &tabletmanagerdatapb.ReplicaWasPromotedRequest{})
	return err
}

// ResetReplicationParameters is part of the tmclient.TabletManagerClient interface.
func (client *Client) ResetReplicationParameters(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ResetReplicationParameters(ctx, &tabletmanagerdatapb.ResetReplicationParametersRequest{})
	return err
}

// SetReplicationSource is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReplicationSource(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = c.SetReplicationSource(ctx, &tabletmanagerdatapb.SetReplicationSourceRequest{
		Parent:                parent,
		TimeCreatedNs:         timeCreatedNS,
		WaitPosition:          waitPosition,
		ForceStartReplication: forceStartReplication,
		SemiSync:              semiSync,
	})
	return err
}

// ReplicaWasRestarted is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReplicaWasRestarted(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ReplicaWasRestarted(ctx, &tabletmanagerdatapb.ReplicaWasRestartedRequest{
		Parent: parent,
	})
	return err
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, stopReplicationMode replicationdatapb.StopReplicationMode) (status *replicationdatapb.StopReplicationStatus, err error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.StopReplicationAndGetStatus(ctx, &tabletmanagerdatapb.StopReplicationAndGetStatusRequest{
		StopReplicationMode: stopReplicationMode,
	})
	if err != nil {
		return nil, err
	}
	return &replicationdatapb.StopReplicationStatus{ //nolint
		Before: response.Status.Before,
		After:  response.Status.After,
	}, nil
}

// PromoteReplica is part of the tmclient.TabletManagerClient interface.
func (client *Client) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return "", err
	}
	defer closer.Close()

	response, err := c.PromoteReplica(ctx, &tabletmanagerdatapb.PromoteReplicaRequest{
		SemiSync: semiSync,
	})
	if err != nil {
		return "", err
	}
	return response.Position, nil
}

// Backup related methods
type backupStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_BackupClient
	closer io.Closer
}

func (e *backupStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.closer.Close()
		return nil, err
	}
	return br.Event, nil
}

// Backup is part of the tmclient.TabletManagerClient interface.
func (client *Client) Backup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.BackupRequest) (logutil.EventStream, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}

	stream, err := c.Backup(ctx, req)
	if err != nil {
		closer.Close()
		return nil, err
	}
	return &backupStreamAdapter{
		stream: stream,
		closer: closer,
	}, nil
}

type restoreFromBackupStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_RestoreFromBackupClient
	closer io.Closer
}

func (e *restoreFromBackupStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.closer.Close()
		return nil, err
	}
	return br.Event, nil
}

// RestoreFromBackup is part of the tmclient.TabletManagerClient interface.
func (client *Client) RestoreFromBackup(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.RestoreFromBackupRequest) (logutil.EventStream, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}

	stream, err := c.RestoreFromBackup(ctx, req)
	if err != nil {
		closer.Close()
		return nil, err
	}
	return &restoreFromBackupStreamAdapter{
		stream: stream,
		closer: closer,
	}, nil
}

// Close is part of the tmclient.TabletManagerClient interface.
func (client *Client) Close() {
	client.dialer.Close()
}
