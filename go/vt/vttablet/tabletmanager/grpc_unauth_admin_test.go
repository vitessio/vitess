/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctlserver"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"

	// Register the production tabletmanager gRPC service on servenv.GRPCServer.
	_ "vitess.io/vitess/go/vt/vttablet/grpctmserver"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"

	_ "unsafe"
)

//go:linkname createGRPCServer vitess.io/vitess/go/vt/servenv.createGRPCServer
func createGRPCServer()

//go:linkname serveGRPC vitess.io/vitess/go/vt/servenv.serveGRPC
func serveGRPC() (stop func())

//go:linkname grpcPort vitess.io/vitess/go/vt/servenv.gRPCPort
var grpcPort int

//go:linkname grpcAuth vitess.io/vitess/go/vt/servenv.gRPCAuth
var grpcAuth string

//go:linkname grpcBindAddress vitess.io/vitess/go/vt/servenv.gRPCBindAddress
var grpcBindAddress string

// methodGate is an authenticator that allows exactly one full method name.
// A call is authorized only for that method, so a server that checks a
// different method than the one it then runs fails this gate.
type methodGate struct {
	allowed string
	calls   []string
}

func (m *methodGate) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	m.calls = append(m.calls, fullMethod)
	if fullMethod != m.allowed {
		return nil, status.Errorf(codes.Unauthenticated, "method %s is not authorized", fullMethod)
	}
	return ctx, nil
}

func TestAdminGRPCRequiresAuthentication(t *testing.T) {
	// These tests describe the required behavior. On the current server they
	// fail: with grpc-auth-mode left at its default, the process serves
	// tabletmanager, vtctld, and vtctl RPCs to any TCP client, and the listen
	// address is every interface.
	const (
		dbaQuery      = "select 91001"
		multiQuery    = "select 91002"
		allPrivsQuery = "select 91003"
		appQuery      = "select 91004"
		lockQuery     = "flush tables with read lock"
	)

	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	tm := tabletmanager.NewUnauthProbeTabletManager(db)
	daemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	var superQueries int
	daemon.ExecuteSuperQueryListCallback = func() { superQueries++ }
	daemon.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}

	ts := memorytopo.NewServer(t.Context(), "zone1")
	addr := startAdminGRPCServer(t, "", ts, tm)

	nonLoopback := nonLoopbackIPv4(t)
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", addr.Port)), time.Second)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
	t.Run("bind address", func(t *testing.T) {
		outside, err := net.DialTimeout("tcp", net.JoinHostPort(nonLoopback, fmt.Sprintf("%d", addr.Port)), time.Second)
		if err == nil {
			outside.Close()
		}
		require.Errorf(t, err, "grpc-auth-mode is unset, so the server must not accept TCP on %s", nonLoopback)
	})

	conn := dialInsecure(t, addr.Port)
	tmClient := tabletmanagerservicepb.NewTabletManagerClient(conn)
	vtctldClient := vtctlservicepb.NewVtctldClient(conn)
	vtctlClient := vtctlservicepb.NewVtctlClient(conn)

	t.Run("ExecuteFetchAsDba", func(t *testing.T) {
		_, err := tmClient.ExecuteFetchAsDba(t.Context(), &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(dbaQuery),
			DbName:  "testdb",
			MaxRows: 10,
		})
		require.NotContains(t, db.QueryLog(), dbaQuery)
		requireUnauthenticated(t, err)
	})

	t.Run("ExecuteMultiFetchAsDba", func(t *testing.T) {
		_, err := tmClient.ExecuteMultiFetchAsDba(t.Context(), &tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
			Sql:     []byte(multiQuery),
			DbName:  "testdb",
			MaxRows: 10,
		})
		require.NotContains(t, db.QueryLog(), multiQuery)
		requireUnauthenticated(t, err)
	})

	t.Run("ExecuteFetchAsAllPrivs", func(t *testing.T) {
		_, err := tmClient.ExecuteFetchAsAllPrivs(t.Context(), &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
			Query:   []byte(allPrivsQuery),
			DbName:  "testdb",
			MaxRows: 10,
		})
		require.NotContains(t, db.QueryLog(), allPrivsQuery)
		requireUnauthenticated(t, err)
	})

	t.Run("ExecuteFetchAsApp", func(t *testing.T) {
		_, err := tmClient.ExecuteFetchAsApp(t.Context(), &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
			Query:   []byte(appQuery),
			MaxRows: 10,
		})
		require.NotContains(t, db.QueryLog(), appQuery)
		requireUnauthenticated(t, err)
	})

	t.Run("LockTables", func(t *testing.T) {
		_, err := tmClient.LockTables(t.Context(), &tabletmanagerdatapb.LockTablesRequest{})
		require.NotContains(t, db.QueryLog(), lockQuery)
		requireUnauthenticated(t, err)
	})

	t.Run("StopReplication", func(t *testing.T) {
		before := superQueries
		daemon.Replicating = true
		_, err := tmClient.StopReplication(t.Context(), &tabletmanagerdatapb.StopReplicationRequest{})
		requireUnauthenticated(t, err)
		require.Equal(t, before, superQueries)
		require.True(t, daemon.Replicating, "StopReplication changed the replica state")
	})

	t.Run("ExecuteHook", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(root, "vthook"), 0o755))
		script := filepath.Join(root, "vthook", "unauth_probe")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$HOOK_FROM_CALLER\" > \"$1\"\n"), 0o755))
		t.Setenv("VTROOT", root)
		marker := filepath.Join(t.TempDir(), "hook-ran")
		_, err := tmClient.ExecuteHook(t.Context(), &tabletmanagerdatapb.ExecuteHookRequest{
			Name:       "unauth_probe",
			Parameters: []string{marker},
			ExtraEnv:   map[string]string{"HOOK_FROM_CALLER": "caller-controlled"},
		})
		_, statErr := os.Stat(marker)
		require.ErrorIs(t, statErr, os.ErrNotExist)
		requireUnauthenticated(t, err)
	})

	t.Run("CreateKeyspace", func(t *testing.T) {
		_, err := vtctldClient.CreateKeyspace(t.Context(), &vtctldatapb.CreateKeyspaceRequest{
			Name:              "unauthks",
			Type:              topodatapb.KeyspaceType_NORMAL,
			AllowEmptyVSchema: true,
			Force:             false,
		})
		_, got := ts.GetKeyspace(t.Context(), "unauthks")
		require.Truef(t, topo.IsErrType(got, topo.NoNode), "CreateKeyspace wrote the keyspace: %v", got)
		requireUnauthenticated(t, err)
	})

	t.Run("ApplyShardRoutingRules", func(t *testing.T) {
		_, err := vtctldClient.ApplyShardRoutingRules(t.Context(), &vtctldatapb.ApplyShardRoutingRulesRequest{
			SkipRebuild: true,
			ShardRoutingRules: &vschemapb.ShardRoutingRules{
				Rules: []*vschemapb.ShardRoutingRule{{
					FromKeyspace: "fromks",
					ToKeyspace:   "toks",
					Shard:        "0",
				}},
			},
		})
		rules, rulesErr := ts.GetShardRoutingRules(t.Context())
		require.NoError(t, rulesErr)
		require.Empty(t, rules.GetRules())
		requireUnauthenticated(t, err)
	})

	t.Run("EmergencyReparentShard", func(t *testing.T) {
		_, err := vtctldClient.EmergencyReparentShard(t.Context(), &vtctldatapb.EmergencyReparentShardRequest{
			Keyspace: "unauthks",
			Shard:    "0",
		})
		requireUnauthenticated(t, err)
	})

	t.Run("ApplySchema", func(t *testing.T) {
		_, err := vtctldClient.ApplySchema(t.Context(), &vtctldatapb.ApplySchemaRequest{
			Keyspace: "unauthks",
			Sql:      []string{"create table unauth (id int)"},
		})
		requireUnauthenticated(t, err)
	})

	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	t.Run("vtctld ExecuteFetchAsDBA", func(t *testing.T) {
		_, err := vtctldClient.ExecuteFetchAsDBA(t.Context(), &vtctldatapb.ExecuteFetchAsDBARequest{
			TabletAlias: alias,
			Query:       dbaQuery,
			MaxRows:     10,
		})
		require.NotContains(t, db.QueryLog(), dbaQuery)
		requireUnauthenticated(t, err)
	})
	t.Run("vtctld ExecuteMultiFetchAsDBA", func(t *testing.T) {
		_, err := vtctldClient.ExecuteMultiFetchAsDBA(t.Context(), &vtctldatapb.ExecuteMultiFetchAsDBARequest{
			TabletAlias: alias,
			Sql:         multiQuery,
			MaxRows:     10,
		})
		require.NotContains(t, db.QueryLog(), multiQuery)
		requireUnauthenticated(t, err)
	})
	t.Run("vtctld ExecuteHook", func(t *testing.T) {
		_, err := vtctldClient.ExecuteHook(t.Context(), &vtctldatapb.ExecuteHookRequest{
			TabletAlias: alias,
			TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{
				Name: "unauth_probe",
			},
		})
		requireUnauthenticated(t, err)
	})

	t.Run("ExecuteVtctlCommand", func(t *testing.T) {
		stream, err := vtctlClient.ExecuteVtctlCommand(t.Context(), &vtctldatapb.ExecuteVtctlCommandRequest{
			Args: []string{"CreateKeyspace", "unauthvtctlks"},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		_, got := ts.GetKeyspace(t.Context(), "unauthvtctlks")
		require.Truef(t, topo.IsErrType(got, topo.NoNode), "ExecuteVtctlCommand created the keyspace: %v", got)
		requireUnauthenticated(t, err)
	})

	t.Run("every tabletmanager method", func(t *testing.T) {
		// Empty requests still have to be rejected before the tablet manager
		// runs. HandleRPCPanic recovers handler panics, so this sweep finishes.
		requireEveryMethodUnauthenticated(t, conn, tabletmanagerservicepb.File_tabletmanagerservice_proto.Services().ByName("TabletManager"))
	})

	// An authenticator that allows only Ping must not let any other method run,
	// and Ping must be the method the authenticator was asked about.
	t.Run("authorized method is the method that runs", func(t *testing.T) {
		gate := &methodGate{allowed: tabletmanagerservicepb.TabletManager_Ping_FullMethodName}
		const plugin = "test-method-gate"
		servenv.RegisterAuthPlugin(plugin, func() (servenv.Authenticator, error) {
			return gate, nil
		})
		authed := startAdminGRPCServer(t, plugin, memorytopo.NewServer(t.Context(), "zone1"), tm)
		authedConn := dialInsecure(t, authed.Port)
		authedTM := tabletmanagerservicepb.NewTabletManagerClient(authedConn)

		db.ResetQueryLog()
		_, err := authedTM.ExecuteFetchAsDba(t.Context(), &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(dbaQuery),
			DbName:  "testdb",
			MaxRows: 10,
		})
		require.NotContains(t, db.QueryLog(), dbaQuery)
		requireUnauthenticated(t, err)
		require.Equal(t, []string{tabletmanagerservicepb.TabletManager_ExecuteFetchAsDba_FullMethodName}, gate.calls)

		pong, err := authedTM.Ping(t.Context(), &tabletmanagerdatapb.PingRequest{Payload: "ok"})
		require.NoError(t, err)
		require.Equal(t, "ok", pong.GetPayload())
		require.Equal(t, []string{
			tabletmanagerservicepb.TabletManager_ExecuteFetchAsDba_FullMethodName,
			tabletmanagerservicepb.TabletManager_Ping_FullMethodName,
		}, gate.calls)
	})
}

// startAdminGRPCServer listens with the production server constructor.
// authMode is the grpc-auth-mode value; empty is the flag default.
// The bind address is left empty, which is also the flag default.
func startAdminGRPCServer(t *testing.T, authMode string, ts *topo.Server, tm *tabletmanager.TabletManager) *net.TCPAddr {
	t.Helper()

	oldPort, oldAuth, oldBind := grpcPort, grpcAuth, grpcBindAddress
	oldServer := servenv.GRPCServer
	grpcPort = freePort(t)
	grpcAuth = authMode
	grpcBindAddress = ""
	t.Cleanup(func() {
		grpcPort = oldPort
		grpcAuth = oldAuth
		grpcBindAddress = oldBind
		servenv.GRPCServer = oldServer
	})

	servenv.InitServiceMap("grpc", "tabletmanager")
	servenv.InitServiceMap("grpc", "vtctld")
	servenv.InitServiceMap("grpc", "vtctl")
	createGRPCServer()
	require.NotNil(t, servenv.GRPCServer)

	require.True(t, servenv.GRPCCheckServiceMap("tabletmanager"))
	for _, register := range tabletmanager.RegisterTabletManagers {
		register(tm)
	}
	require.True(t, servenv.GRPCCheckServiceMap("vtctld"))
	env := vtenv.NewTestEnv()
	grpcvtctldserver.StartServer(servenv.GRPCServer, env, ts)
	require.True(t, servenv.GRPCCheckServiceMap("vtctl"))
	grpcvtctlserver.StartServer(servenv.GRPCServer, env, ts)

	stopOrca := serveGRPC()
	t.Cleanup(stopOrca)
	server := servenv.GRPCServer
	t.Cleanup(server.Stop)

	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: grpcPort}
}

func requireEveryMethodUnauthenticated(t *testing.T, conn *grpc.ClientConn, svc protoreflect.ServiceDescriptor) {
	t.Helper()
	require.NotNil(t, svc)
	methods := svc.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		t.Run(string(method.Name()), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			full := "/" + string(svc.FullName()) + "/" + string(method.Name())
			in := dynamicpb.NewMessage(method.Input())
			out := dynamicpb.NewMessage(method.Output())
			var err error
			if method.IsStreamingClient() || method.IsStreamingServer() {
				stream, streamErr := conn.NewStream(ctx, &grpc.StreamDesc{
					StreamName:    string(method.Name()),
					ServerStreams: method.IsStreamingServer(),
					ClientStreams: method.IsStreamingClient(),
				}, full)
				require.NoError(t, streamErr)
				if !method.IsStreamingClient() {
					_ = stream.SendMsg(in)
					_ = stream.CloseSend()
				}
				err = stream.RecvMsg(out)
			} else {
				err = conn.Invoke(ctx, full, in, out)
			}
			requireUnauthenticated(t, err)
		})
	}
}

func requireUnauthenticated(t *testing.T, err error) {
	t.Helper()
	st, ok := status.FromError(err)
	require.Truef(t, ok && (st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied), "got %v", err)
}

func dialInsecure(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func nonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	require.NoError(t, err)
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		require.NoError(t, err)
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil || ipNet.IP.IsLoopback() {
				continue
			}
			return ipNet.IP.String()
		}
	}
	t.Fatal("no up non-loopback IPv4 address")
	return ""
}
