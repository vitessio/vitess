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
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type DialPoolGroup int

const (
	dialPoolGroupThrottler DialPoolGroup = iota
	dialPoolGroupVTOrc
)

type invalidatorFunc func()

var (
	concurrency = 8
	cert        string
	key         string
	ca          string
	crl         string
	name        string
)

func RegisterFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &concurrency, "tablet-manager-grpc-concurrency", concurrency, "concurrency to use to talk to a vttablet server for performance-sensitive RPCs (like ExecuteFetchAs{Dba,App}, CheckThrottler and FullStatus)")
	utils.SetFlagStringVar(fs, &cert, "tablet-manager-grpc-cert", cert, "the cert to use to connect")
	utils.SetFlagStringVar(fs, &key, "tablet-manager-grpc-key", key, "the key to use to connect")
	utils.SetFlagStringVar(fs, &ca, "tablet-manager-grpc-ca", ca, "the server ca to use to validate servers when connecting")
	utils.SetFlagStringVar(fs, &crl, "tablet-manager-grpc-crl", crl, "the server crl to use to validate server certificates when connecting")
	utils.SetFlagStringVar(fs, &name, "tablet-manager-grpc-server-name", name, "the server name to use to validate server certificate")
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
		servenv.OnParseFor(cmd, RegisterFlags)
	}
}

type tmc struct {
	cc     *grpc.ClientConn
	client tabletmanagerservicepb.TabletManagerClient
}

type tmcEntry struct {
	once sync.Once
	tmc  *tmc
	err  error
}

type addrTmcMap map[string]*tmcEntry

// grpcClient implements both dialer and poolDialer.
type grpcClient struct {
	// This cache of connections is to maximize QPS for ExecuteFetchAs{Dba,App},
	// CheckThrottler and FullStatus. Note we'll keep the clients open and close them upon Close() only.
	// But that's OK because usually the tasks that use them are one-purpose only.
	// rpcClientMapMu protects rpcClientMap.
	rpcClientMapMu sync.Mutex
	rpcClientMap   map[string]chan *tmc
	// rpcDialPoolMapMu protects rpcDialPoolMap.
	rpcDialPoolMapMu sync.Mutex
	rpcDialPoolMap   map[DialPoolGroup]addrTmcMap
}

type dialer interface {
	dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error)
	Close()
}

type poolDialer interface {
	dialPool(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, error)
	dialDedicatedPool(ctx context.Context, dialPoolGroup DialPoolGroup, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, invalidatorFunc, error)
}

// Client implements tmclient.TabletManagerClient.
//
// Connections are produced by the dialer implementation, which is either the
// grpcClient implementation, which reuses connections only for ExecuteFetchAs{Dba,App}
// CheckThrottler, and FullStatus, otherwise making single-purpose connections that are closed
// after use.
//
// In order to more efficiently use the underlying tcp connections, you can
// instead use the cachedConnDialer implementation by specifying
//
//	--tablet-manager-protocol "grpc-cached"
//
// The cachedConnDialer keeps connections to up to --tablet-manager-grpc-connpool-size
// distinct tablets open at any given time, for faster per-RPC call time, and less
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

// validateTablet confirms the tablet record contains the necessary fields
// for talking gRPC.
func validateTablet(tablet *topodatapb.Tablet) error {
	// v24+ adds a TabletShutdownTime field that is set to "now" on clean shutdown.
	if tablet.TabletShutdownTime != nil {
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "tablet is shutdown")
	}

	// <= v23 compatibility to similuate missing TabletShutdownTime field. Remove in v25.
	if tablet.Hostname == "" && tablet.MysqlHostname == "" && tablet.PortMap == nil && tablet.Type != topodatapb.TabletType_UNKNOWN {
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "tablet is shutdown")
	}

	if tablet.Hostname == "" {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "empty tablet hostname")
	}
	if tablet.PortMap == nil {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "no tablet port map")
	}
	grpcPort := int32(tablet.PortMap["grpc"])
	if grpcPort <= 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet grpc port: %d", grpcPort)
	}
	return nil
}

// dial returns a client to use
func (client *grpcClient) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	if err := validateTablet(tablet); err != nil {
		return nil, nil, err
	}

	addr := netutil.JoinHostPort(tablet.Hostname, tablet.PortMap["grpc"])
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, nil, err
	}
	cc, err := grpcclient.DialContext(ctx, addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, nil, err
	}

	return tabletmanagerservicepb.NewTabletManagerClient(cc), cc, nil
}

func (client *grpcClient) createTmc(ctx context.Context, addr string, opt grpc.DialOption) (*tmc, error) {
	cc, err := grpcclient.DialContext(ctx, addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}
	return &tmc{
		cc:     cc,
		client: tabletmanagerservicepb.NewTabletManagerClient(cc),
	}, nil
}

func (client *grpcClient) dialPool(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, error) {
	if err := validateTablet(tablet); err != nil {
		return nil, err
	}

	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}

	c, isEmpty := func() (chan *tmc, bool) {
		client.rpcClientMapMu.Lock()
		defer client.rpcClientMapMu.Unlock()

		if client.rpcClientMap == nil {
			client.rpcClientMap = make(map[string]chan *tmc)
		}
		c, ok := client.rpcClientMap[addr]
		if ok {
			return c, false
		}

		c = make(chan *tmc, concurrency)
		client.rpcClientMap[addr] = c
		return c, true
	}()

	// If the channel is empty, populate it with connections.
	if isEmpty {
		for i := 0; i < cap(c); i++ {
			tm, err := client.createTmc(ctx, addr, opt)
			if err != nil {
				return nil, vterrors.FromGRPC(err)
			}
			c <- tm
		}
	}

	result := <-c
	c <- result
	return result.client, nil
}

func (client *grpcClient) dialDedicatedPool(ctx context.Context, dialPoolGroup DialPoolGroup, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, invalidatorFunc, error) {
	if err := validateTablet(tablet); err != nil {
		return nil, nil, err
	}

	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
	if err != nil {
		return nil, nil, err
	}

	entry := func() *tmcEntry {
		client.rpcDialPoolMapMu.Lock()
		defer client.rpcDialPoolMapMu.Unlock()

		if client.rpcDialPoolMap == nil {
			client.rpcDialPoolMap = make(map[DialPoolGroup]addrTmcMap)
		}
		if _, ok := client.rpcDialPoolMap[dialPoolGroup]; !ok {
			client.rpcDialPoolMap[dialPoolGroup] = make(addrTmcMap)
		}

		poolEntries := client.rpcDialPoolMap[dialPoolGroup]
		entry, ok := poolEntries[addr]
		if ok {
			return entry
		}

		entry = &tmcEntry{}
		poolEntries[addr] = entry
		return entry
	}()

	// Initialize connection exactly once, without holding the mutex
	entry.once.Do(func() {
		entry.tmc, entry.err = client.createTmc(ctx, addr, opt)
	})

	if entry.err != nil {
		return nil, nil, entry.err
	}

	invalidator := func() {
		client.rpcDialPoolMapMu.Lock()
		defer client.rpcDialPoolMapMu.Unlock()

		if entry.tmc != nil && entry.tmc.cc != nil {
			entry.tmc.cc.Close()
		}

		if poolEntries, ok := client.rpcDialPoolMap[dialPoolGroup]; ok {
			delete(poolEntries, addr)
		}
	}
	return entry.tmc.client, invalidator, nil
}

// Close is part of the tmclient.TabletManagerClient interface.
func (client *grpcClient) Close() {
	func() {
		client.rpcClientMapMu.Lock()
		defer client.rpcClientMapMu.Unlock()

		for _, c := range client.rpcClientMap {
			close(c)
			for ch := range c {
				ch.cc.Close()
			}
		}
		client.rpcClientMap = nil
	}()

	// Close dedicated pools
	func() {
		client.rpcDialPoolMapMu.Lock()
		defer client.rpcDialPoolMapMu.Unlock()

		for _, addrMap := range client.rpcDialPoolMap {
			for _, tm := range addrMap {
				if tm != nil && tm.tmc != nil && tm.tmc.cc != nil {
					tm.tmc.cc.Close()
				}
			}
		}
		client.rpcDialPoolMap = nil
	}()
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
		return vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response.Permissions, nil
}

// GetGlobalStatusVars is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetGlobalStatusVars(ctx context.Context, tablet *topodatapb.Tablet, variables []string) (map[string]string, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.GetGlobalStatusVars(ctx, &tabletmanagerdatapb.GetGlobalStatusVarsRequest{
		Variables: variables,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.GetStatusValues(), nil
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
	return vterrors.FromGRPC(err)
}

// SetReadWrite is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReadWrite(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.SetReadWrite(ctx, &tabletmanagerdatapb.SetReadWriteRequest{})
	return vterrors.FromGRPC(err)
}

// ChangeTags is part of the tmclient.TabletManagerClient interface.
func (client *Client) ChangeTags(ctx context.Context, tablet *topodatapb.Tablet, tabletTags map[string]string, replace bool) (*tabletmanagerdatapb.ChangeTagsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	res, err := c.ChangeTags(ctx, &tabletmanagerdatapb.ChangeTagsRequest{
		Tags:    tabletTags,
		Replace: replace,
	})
	return res, vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (client *Client) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.RefreshState(ctx, &tabletmanagerdatapb.RefreshStateRequest{})
	return vterrors.FromGRPC(err)
}

// RunHealthCheck is part of the tmclient.TabletManagerClient interface.
func (client *Client) RunHealthCheck(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.RunHealthCheck(ctx, &tabletmanagerdatapb.RunHealthCheckRequest{})
	return vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		Sql:                     change.SQL,
		Force:                   change.Force,
		AllowReplication:        change.AllowReplication,
		BeforeSchema:            change.BeforeSchema,
		AfterSchema:             change.AfterSchema,
		SqlMode:                 change.SQLMode,
		DisableForeignKeyChecks: change.DisableForeignKeyChecks,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// UnlockTables is part of the tmclient.TabletManagerClient interface.
func (client *Client) UnlockTables(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = c.UnlockTables(ctx, &tabletmanagerdatapb.UnlockTablesRequest{})
	return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		Query:                   req.Query,
		DbName:                  topoproto.TabletDbName(tablet),
		MaxRows:                 req.MaxRows,
		DisableBinlogs:          req.DisableBinlogs,
		ReloadSchema:            req.DisableBinlogs,
		DisableForeignKeyChecks: req.DisableForeignKeyChecks,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Result, nil
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface.
func (client *Client) ExecuteMultiFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest) ([]*querypb.QueryResult, error) {
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

	response, err := c.ExecuteMultiFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
		Sql:                     req.Sql,
		DbName:                  topoproto.TabletDbName(tablet),
		MaxRows:                 req.MaxRows,
		DisableBinlogs:          req.DisableBinlogs,
		ReloadSchema:            req.DisableBinlogs,
		DisableForeignKeyChecks: req.DisableForeignKeyChecks,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Results, nil
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
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response.Result, nil
}

// GetUnresolvedTransactions is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetUnresolvedTransactions(ctx context.Context, tablet *topodatapb.Tablet, abandonAge int64) ([]*querypb.TransactionMetadata, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	response, err := c.GetUnresolvedTransactions(ctx, &tabletmanagerdatapb.GetUnresolvedTransactionsRequest{
		AbandonAge: abandonAge,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Transactions, nil
}

// ConcludeTransaction is part of the tmclient.TabletManagerClient interface.
func (client *Client) ConcludeTransaction(ctx context.Context, tablet *topodatapb.Tablet, dtid string, mm bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = c.ConcludeTransaction(ctx, &tabletmanagerdatapb.ConcludeTransactionRequest{
		Dtid: dtid,
		Mm:   mm,
	})
	return vterrors.FromGRPC(err)
}

func (client *Client) MysqlHostMetrics(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.MysqlHostMetricsRequest) (*tabletmanagerdatapb.MysqlHostMetricsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	resp, err := c.MysqlHostMetrics(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return resp, nil
}

// ReadTransaction is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReadTransaction(ctx context.Context, tablet *topodatapb.Tablet, dtid string) (*querypb.TransactionMetadata, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	resp, err := c.ReadTransaction(ctx, &tabletmanagerdatapb.ReadTransactionRequest{
		Dtid: dtid,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return resp.Transaction, nil
}

// GetTransactionInfo is part of the tmclient.TabletManagerClient interface.
func (client *Client) GetTransactionInfo(ctx context.Context, tablet *topodatapb.Tablet, dtid string) (*tabletmanagerdatapb.GetTransactionInfoResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	resp, err := c.GetTransactionInfo(ctx, &tabletmanagerdatapb.GetTransactionInfoRequest{
		Dtid: dtid,
	})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return resp, nil
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
		return nil, vterrors.FromGRPC(err)
	}
	return response.Status, nil
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
// It always tries to use a cached client via the dialer pool as this is
// called very frequently from VTOrc, and the overhead of creating a new gRPC connection/channel
// and dialing the other tablet every time is not practical.
func (client *Client) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var invalidator invalidatorFunc
	var err error
	if poolDialer, ok := client.dialer.(poolDialer); ok {
		c, invalidator, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupVTOrc, tablet)
		if err != nil {
			return nil, err
		}
	}

	if c == nil {
		var closer io.Closer
		c, closer, err = client.dialer.dial(ctx, tablet)
		if err != nil {
			return nil, err
		}
		defer closer.Close()
	}

	response, err := c.FullStatus(ctx, &tabletmanagerdatapb.FullStatusRequest{})
	if err != nil {
		if invalidator != nil {
			invalidator()
		}
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		return "", vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// StopReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) StopReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.StopReplication(ctx, &tabletmanagerdatapb.StopReplicationRequest{})
	return vterrors.FromGRPC(err)
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
		return "", vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// RestartReplication is part of the tmclient.TabletManagerClient interface.
func (client *Client) RestartReplication(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.RestartReplication(ctx, &tabletmanagerdatapb.RestartReplicationRequest{
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
	return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) DeleteTableData(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.DeleteTableDataRequest) (*tabletmanagerdatapb.DeleteTableDataResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.DeleteTableData(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) HasVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.HasVReplicationWorkflows(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) ReadVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.ReadVReplicationWorkflows(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) ValidateVReplicationPermissions(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ValidateVReplicationPermissionsRequest) (*tabletmanagerdatapb.ValidateVReplicationPermissionsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.ValidateVReplicationPermissions(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
	_, err = c.VReplicationWaitForPos(ctx, &tabletmanagerdatapb.VReplicationWaitForPosRequest{Id: id, Position: pos})
	if err != nil {
		return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) UpdateVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowsResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.UpdateVReplicationWorkflows(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

func (client *Client) GetMaxValueForSequences(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetMaxValueForSequencesRequest) (*tabletmanagerdatapb.GetMaxValueForSequencesResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	res, err := c.GetMaxValueForSequences(ctx, request)
	return res, vterrors.FromGRPC(err)
}

func (client *Client) UpdateSequenceTables(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.UpdateSequenceTablesRequest) (*tabletmanagerdatapb.UpdateSequenceTablesResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	res, err := c.UpdateSequenceTables(ctx, request)
	return res, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
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
		return "", vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// ReadReparentJournalInfo is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReadReparentJournalInfo(ctx context.Context, tablet *topodatapb.Tablet) (int32, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	resp, err := c.ReadReparentJournalInfo(ctx, &tabletmanagerdatapb.ReadReparentJournalInfoRequest{})
	if err != nil {
		return 0, vterrors.FromGRPC(err)
	}
	return resp.Length, nil
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
	return vterrors.FromGRPC(err)
}

// DemotePrimary is part of the tmclient.TabletManagerClient interface.
func (client *Client) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, force bool) (*replicationdatapb.PrimaryStatus, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.DemotePrimary(ctx, &tabletmanagerdatapb.DemotePrimaryRequest{Force: force})
	if err != nil {
		return nil, vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
}

// ReplicaWasPromoted is part of the tmclient.TabletManagerClient interface.
func (client *Client) ReplicaWasPromoted(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ReplicaWasPromoted(ctx, &tabletmanagerdatapb.ReplicaWasPromotedRequest{})
	return vterrors.FromGRPC(err)
}

// ResetReplicationParameters is part of the tmclient.TabletManagerClient interface.
func (client *Client) ResetReplicationParameters(ctx context.Context, tablet *topodatapb.Tablet) error {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = c.ResetReplicationParameters(ctx, &tabletmanagerdatapb.ResetReplicationParametersRequest{})
	return vterrors.FromGRPC(err)
}

// SetReplicationSource is part of the tmclient.TabletManagerClient interface.
func (client *Client) SetReplicationSource(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool, heartbeatInterval float64) error {
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
		HeartbeatInterval:     heartbeatInterval,
	})
	return vterrors.FromGRPC(err)
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
	return vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return &replicationdatapb.StopReplicationStatus{
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
		return "", vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
	}
	return &backupStreamAdapter{
		stream: stream,
		closer: closer,
	}, nil
}

// CheckThrottler is part of the tmclient.TabletManagerClient interface.
// It always tries to use a cached client via the dialer pool as this is
// called very frequently between tablets when the throttler is enabled in
// a keyspace and the overhead of creating a new gRPC connection/channel
// and dialing the other tablet every time is not practical.
func (client *Client) CheckThrottler(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	var c tabletmanagerservicepb.TabletManagerClient
	var invalidator invalidatorFunc
	var err error
	if poolDialer, ok := client.dialer.(poolDialer); ok {
		c, invalidator, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
		if err != nil {
			return nil, err
		}
	}

	if c == nil {
		var closer io.Closer
		c, closer, err = client.dialer.dial(ctx, tablet)
		if err != nil {
			return nil, err
		}
		defer closer.Close()
	}

	response, err := c.CheckThrottler(ctx, req)
	if err != nil {
		if invalidator != nil {
			invalidator()
		}
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

// GetThrottlerStatus is part of the tmclient.TabletManagerClient interface.
// It always tries to use a cached client via the dialer pool as this is
// called very frequently between tablets when the throttler is enabled in
// a keyspace and the overhead of creating a new gRPC connection/channel
// and dialing the other tablet every time is not practical.
func (client *Client) GetThrottlerStatus(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.GetThrottlerStatusRequest) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	c, closer, err := client.dialer.dial(ctx, tablet)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	response, err := c.GetThrottlerStatus(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response, nil
}

type restoreFromBackupStreamAdapter struct {
	stream tabletmanagerservicepb.TabletManager_RestoreFromBackupClient
	closer io.Closer
}

func (e *restoreFromBackupStreamAdapter) Recv() (*logutilpb.Event, error) {
	br, err := e.stream.Recv()
	if err != nil {
		e.closer.Close()
		return nil, vterrors.FromGRPC(err)
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
		return nil, vterrors.FromGRPC(err)
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
