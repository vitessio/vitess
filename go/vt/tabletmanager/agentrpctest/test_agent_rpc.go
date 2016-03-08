// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package agentrpctest

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// fakeRPCAgent implements tabletmanager.RPCAgent and fills in all
// possible values in all APIs
type fakeRPCAgent struct {
	t      *testing.T
	panics bool
	// slow if true will let Ping() sleep and effectively not respond to an RPC.
	slow bool
	// mu guards accesses of "slow".
	mu sync.Mutex
}

func (fra *fakeRPCAgent) setSlow(slow bool) {
	fra.mu.Lock()
	fra.slow = slow
	fra.mu.Unlock()
}

// NewFakeRPCAgent returns a fake tabletmanager.RPCAgent that's just a mirror.
func NewFakeRPCAgent(t *testing.T) tabletmanager.RPCAgent {
	return &fakeRPCAgent{
		t: t,
	}
}

// The way this test is organized is a repetition of:
// - static test data for a call
// - implementation of the tabletmanager.RPCAgent method for fakeRPCAgent
// - static test method for the call (client side)
// for each possible method of the interface.
// This makes the implementations all in the same spot.

func compare(t *testing.T, name string, got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Unexpected %v: got %v expected %v", name, got, want)
	}
}

func compareBool(t *testing.T, name string, got bool) {
	if !got {
		t.Errorf("Unexpected %v: got false expected true", name)
	}
}

func compareError(t *testing.T, name string, err error, got, want interface{}) {
	if err != nil {
		t.Errorf("%v failed: %v", name, err)
	} else {
		compare(t, name+" result", got, want)
	}
}

var testLogString = "test log"

func logStuff(logger logutil.Logger, count int) {
	for i := 0; i < count; i++ {
		logger.Infof(testLogString)
	}
}

func compareLoggedStuff(t *testing.T, name string, logChannel <-chan *logutilpb.Event, count int) {
	for i := 0; i < count; i++ {
		le, ok := <-logChannel
		if !ok {
			t.Errorf("No logged value for %v/%v", name, i)
			return
		}
		if le.Value != testLogString {
			t.Errorf("Unexpected log response for %v: got %v expected %v", name, le.Value, testLogString)
		}
	}
	_, ok := <-logChannel
	if ok {
		t.Fatalf("log channel wasn't closed for %v", name)
	}
}

func expectRPCWrapPanic(t *testing.T, err error) {
	expected := "RPCWrap caught panic"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected a panic error with '%v' but got: %v", expected, err)
	}
}

func expectRPCWrapLockPanic(t *testing.T, err error) {
	expected := "RPCWrapLock caught panic"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected a panic error with '%v' but got: %v", expected, err)
	}
}

func expectRPCWrapLockActionPanic(t *testing.T, err error) {
	expected := "RPCWrapLockAction caught panic"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected a panic error with '%v' but got: %v", expected, err)
	}
}

//
// Various read-only methods
//

func (fra *fakeRPCAgent) Ping(ctx context.Context, args string) string {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	fra.mu.Lock()
	slow := fra.slow
	fra.mu.Unlock()
	if slow {
		time.Sleep(time.Minute)
	}
	return args
}

func agentRPCTestPing(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Ping(ctx, ti)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func agentRPCTestPingPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Ping(ctx, ti)
	expectRPCWrapPanic(t, err)
}

// agentRPCTestIsTimeoutErrorDialExpiredContext verifies that
// client.IsTimeoutError() returns true for RPCs failed due to an expired
// context before .Dial().
func agentRPCTestIsTimeoutErrorDialExpiredContext(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	// Using a timeout of 0 here such that .Dial() will fail immediately.
	expiredCtx, cancel := context.WithTimeout(ctx, 0)
	defer cancel()
	err := client.Ping(expiredCtx, ti)
	if err == nil {
		t.Fatal("agentRPCTestIsTimeoutErrorDialExpiredContext: RPC with expired context did not fail")
	}
	if !client.IsTimeoutError(err) {
		t.Errorf("agentRPCTestIsTimeoutErrorDialExpiredContext: want: IsTimeoutError() = true. error: %v", err)
	}
}

// agentRPCTestIsTimeoutErrorDialTimeout verifies that client.IsTimeoutError()
// returns true for RPCs failed due to a connect timeout during .Dial().
func agentRPCTestIsTimeoutErrorDialTimeout(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	// Connect to a non-existing tablet.
	// For example, this provokes gRPC to return error grpc.ErrClientConnTimeout.
	invalidTi := topo.NewTabletInfo(ti.Tablet, ti.Version())
	invalidTi.Tablet = proto.Clone(invalidTi.Tablet).(*topodatapb.Tablet)
	invalidTi.Tablet.Hostname = "Non-Existent.Server"

	shortCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	err := client.Ping(shortCtx, invalidTi)
	if err == nil {
		t.Fatal("agentRPCTestIsTimeoutErrorDialTimeout: connect to non-existant tablet did not fail")
	}
	if !client.IsTimeoutError(err) {
		t.Errorf("agentRPCTestIsTimeoutErrorDialTimeout: want: IsTimeoutError() = true. error: %v", err)
	}
}

// agentRPCTestIsTimeoutErrorRPC verifies that client.IsTimeoutError() returns
// true for RPCs failed due to an expired context during RPC execution.
func agentRPCTestIsTimeoutErrorRPC(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo, fakeAgent *fakeRPCAgent) {
	// We must use a timeout > 0 such that the context deadline hasn't expired
	// yet in grpctmclient.Client.dial().
	// NOTE: This might still race e.g. when test execution takes too long the
	//       context will be expired in dial() already. In such cases coverage
	//       will be reduced but the test will not flake.
	shortCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	fakeAgent.setSlow(true)
	defer func() { fakeAgent.setSlow(false) }()
	err := client.Ping(shortCtx, ti)
	if err == nil {
		t.Fatal("agentRPCTestIsTimeoutErrorRPC: RPC with expired context did not fail")
	}
	if !client.IsTimeoutError(err) {
		t.Errorf("agentRPCTestIsTimeoutErrorRPC: want: IsTimeoutError() = true. error: %v", err)
	}
}

var testGetSchemaTables = []string{"table1", "table2"}
var testGetSchemaExcludeTables = []string{"etable1", "etable2", "etable3"}
var testGetSchemaReply = &tabletmanagerdatapb.SchemaDefinition{
	DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
	TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
		{
			Name:              "table_name",
			Schema:            "create table_name",
			Columns:           []string{"col1", "col2"},
			PrimaryKeyColumns: []string{"col1"},
			Type:              tmutils.TableView,
			DataLength:        12,
			RowCount:          6,
		},
		{
			Name:              "table_name2",
			Schema:            "create table_name2",
			Columns:           []string{"col1"},
			PrimaryKeyColumns: []string{"col1"},
			Type:              tmutils.TableBaseTable,
			DataLength:        12,
			RowCount:          6,
		},
	},
	Version: "xxx",
}

func (fra *fakeRPCAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "GetSchema tables", tables, testGetSchemaTables)
	compare(fra.t, "GetSchema excludeTables", excludeTables, testGetSchemaExcludeTables)
	compareBool(fra.t, "GetSchema includeViews", includeViews)
	return testGetSchemaReply, nil
}

func agentRPCTestGetSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetSchema(ctx, ti, testGetSchemaTables, testGetSchemaExcludeTables, true)
	compareError(t, "GetSchema", err, result, testGetSchemaReply)
}

func agentRPCTestGetSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.GetSchema(ctx, ti, testGetSchemaTables, testGetSchemaExcludeTables, true)
	expectRPCWrapPanic(t, err)
}

var testGetPermissionsReply = &tabletmanagerdatapb.Permissions{
	UserPermissions: []*tabletmanagerdatapb.UserPermission{
		{
			Host:             "host1",
			User:             "user1",
			PasswordChecksum: 666,
			Privileges: map[string]string{
				"create": "yes",
				"delete": "no",
			},
		},
	},
	DbPermissions: []*tabletmanagerdatapb.DbPermission{
		{
			Host: "host2",
			Db:   "db1",
			User: "user2",
			Privileges: map[string]string{
				"create": "no",
				"delete": "yes",
			},
		},
	},
}

func (fra *fakeRPCAgent) GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testGetPermissionsReply, nil
}

func agentRPCTestGetPermissions(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetPermissions(ctx, ti)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

func agentRPCTestGetPermissionsPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.GetPermissions(ctx, ti)
	expectRPCWrapPanic(t, err)
}

//
// Various read-write methods
//

var testSetReadOnlyExpectedValue bool

func (fra *fakeRPCAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if rdonly != testSetReadOnlyExpectedValue {
		fra.t.Errorf("Wrong SetReadOnly value: got %v expected %v", rdonly, testSetReadOnlyExpectedValue)
	}
	return nil
}

func agentRPCTestSetReadOnly(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	testSetReadOnlyExpectedValue = true
	err := client.SetReadOnly(ctx, ti)
	if err != nil {
		t.Errorf("SetReadOnly failed: %v", err)
	}
	testSetReadOnlyExpectedValue = false
	err = client.SetReadWrite(ctx, ti)
	if err != nil {
		t.Errorf("SetReadWrite failed: %v", err)
	}
}

func agentRPCTestSetReadOnlyPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SetReadOnly(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testChangeTypeValue = topodatapb.TabletType_REPLICA

func (fra *fakeRPCAgent) ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func agentRPCTestChangeType(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ChangeType(ctx, ti, testChangeTypeValue)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

func agentRPCTestChangeTypePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ChangeType(ctx, ti, testChangeTypeValue)
	expectRPCWrapLockActionPanic(t, err)
}

var testSleepDuration = time.Minute

func (fra *fakeRPCAgent) Sleep(ctx context.Context, duration time.Duration) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func agentRPCTestSleep(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Sleep(ctx, ti, testSleepDuration)
	if err != nil {
		t.Errorf("Sleep failed: %v", err)
	}
}

func agentRPCTestSleepPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Sleep(ctx, ti, testSleepDuration)
	expectRPCWrapLockActionPanic(t, err)
}

var testExecuteHookHook = &hook.Hook{
	Name:       "captain hook",
	Parameters: []string{"param1", "param2"},
	ExtraEnv: map[string]string{
		"boat": "blue",
		"sea":  "red",
	},
}
var testExecuteHookHookResult = &hook.HookResult{
	ExitStatus: hook.HOOK_STAT_FAILED,
	Stdout:     "out",
	Stderr:     "err",
}

func (fra *fakeRPCAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteHook hook", hk, testExecuteHookHook)
	return testExecuteHookHookResult
}

func agentRPCTestExecuteHook(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	hr, err := client.ExecuteHook(ctx, ti, testExecuteHookHook)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

func agentRPCTestExecuteHookPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.ExecuteHook(ctx, ti, testExecuteHookHook)
	expectRPCWrapLockActionPanic(t, err)
}

var testRefreshStateCalled = false

func (fra *fakeRPCAgent) RefreshState(ctx context.Context) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testRefreshStateCalled {
		fra.t.Errorf("RefreshState called multiple times?")
	}
	testRefreshStateCalled = true
}

func agentRPCTestRefreshState(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RefreshState(ctx, ti)
	if err != nil {
		t.Errorf("RefreshState failed: %v", err)
	}
	if !testRefreshStateCalled {
		t.Errorf("RefreshState didn't call the server side")
	}
}

func agentRPCTestRefreshStatePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RefreshState(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testRunHealthCheckValue = topodatapb.TabletType_RDONLY

func (fra *fakeRPCAgent) RunHealthCheck(ctx context.Context, targetTabletType topodatapb.TabletType) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "RunHealthCheck tabletType", targetTabletType, testRunHealthCheckValue)
}

var testIgnoreHealthErrorValue = ".*"

func (fra *fakeRPCAgent) IgnoreHealthError(ctx context.Context, pattern string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "IgnoreHealthError pattern", pattern, testIgnoreHealthErrorValue)
	return nil
}

func agentRPCTestRunHealthCheck(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RunHealthCheck(ctx, ti, testRunHealthCheckValue)
	if err != nil {
		t.Errorf("RunHealthCheck failed: %v", err)
	}
}

func agentRPCTestRunHealthCheckPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RunHealthCheck(ctx, ti, testRunHealthCheckValue)
	expectRPCWrapPanic(t, err)
}

func agentRPCTestIgnoreHealthError(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.IgnoreHealthError(ctx, ti, testIgnoreHealthErrorValue)
	if err != nil {
		t.Errorf("IgnoreHealthError failed: %v", err)
	}
}

func agentRPCTestIgnoreHealthErrorPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.IgnoreHealthError(ctx, ti, testIgnoreHealthErrorValue)
	expectRPCWrapPanic(t, err)
}

var testReloadSchemaCalled = false

func (fra *fakeRPCAgent) ReloadSchema(ctx context.Context) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testReloadSchemaCalled {
		fra.t.Errorf("ReloadSchema called multiple times?")
	}
	testReloadSchemaCalled = true
}

func agentRPCTestReloadSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ReloadSchema(ctx, ti)
	if err != nil {
		t.Errorf("ReloadSchema failed: %v", err)
	}
	if !testReloadSchemaCalled {
		t.Errorf("ReloadSchema didn't call the server side")
	}
}

func agentRPCTestReloadSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ReloadSchema(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testPreflightSchema = "change table add table cloth"
var testSchemaChangeResult = &tmutils.SchemaChangeResult{
	BeforeSchema: testGetSchemaReply,
	AfterSchema:  testGetSchemaReply,
}

func (fra *fakeRPCAgent) PreflightSchema(ctx context.Context, change string) (*tmutils.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PreflightSchema result", change, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func agentRPCTestPreflightSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.PreflightSchema(ctx, ti, testPreflightSchema)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

func agentRPCTestPreflightSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.PreflightSchema(ctx, ti, testPreflightSchema)
	expectRPCWrapLockActionPanic(t, err)
}

var testSchemaChange = &tmutils.SchemaChange{
	SQL:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRPCAgent) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ApplySchema change", change, testSchemaChange)
	return testSchemaChangeResult, nil
}

func agentRPCTestApplySchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.ApplySchema(ctx, ti, testSchemaChange)
	compareError(t, "ApplySchema", err, scr, testSchemaChangeResult)
}

func agentRPCTestApplySchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.ApplySchema(ctx, ti, testSchemaChange)
	expectRPCWrapLockActionPanic(t, err)
}

var testExecuteFetchQuery = "fetch this"
var testExecuteFetchMaxRows = 100
var testExecuteFetchResult = &querypb.QueryResult{
	Fields: []*querypb.Field{
		{
			Name: "column1",
			Type: sqltypes.Blob,
		},
		{
			Name: "column2",
			Type: sqltypes.Datetime,
		},
	},
	RowsAffected: 10,
	InsertId:     32,
	Rows: []*querypb.Row{
		{
			Lengths: []int64{
				3,
				-1,
			},
			Values: []byte{
				'A', 'B', 'C',
			},
		},
	},
}

func (fra *fakeRPCAgent) ExecuteFetchAsDba(ctx context.Context, query string, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsDba query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsDba maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetchAsDba disableBinlogs", disableBinlogs)
	compareBool(fra.t, "ExecuteFetchAsDba reloadSchema", reloadSchema)

	return testExecuteFetchResult, nil
}

func (fra *fakeRPCAgent) ExecuteFetchAsApp(ctx context.Context, query string, maxrows int) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsApp query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsApp maxrows", maxrows, testExecuteFetchMaxRows)
	return testExecuteFetchResult, nil
}

func agentRPCTestExecuteFetch(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	qr, err := client.ExecuteFetchAsDba(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true)
	compareError(t, "ExecuteFetchAsDba", err, qr, testExecuteFetchResult)
	qr, err = client.ExecuteFetchAsApp(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows)
	compareError(t, "ExecuteFetchAsApp", err, qr, testExecuteFetchResult)
}

func agentRPCTestExecuteFetchPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.ExecuteFetchAsDba(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, false)
	expectRPCWrapPanic(t, err)

	_, err = client.ExecuteFetchAsApp(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows)
	expectRPCWrapPanic(t, err)
}

//
// Replication related methods
//

var testReplicationStatus = &replicationdatapb.Status{
	Position:            "MariaDB/1-345-789",
	SlaveIoRunning:      true,
	SlaveSqlRunning:     true,
	SecondsBehindMaster: 654,
	MasterHost:          "master.host",
	MasterPort:          3366,
	MasterConnectRetry:  12,
}

func (fra *fakeRPCAgent) SlaveStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationStatus, nil
}

func agentRPCTestSlaveStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.SlaveStatus(ctx, ti)
	compareError(t, "SlaveStatus", err, rs, testReplicationStatus)
}

func agentRPCTestSlaveStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.SlaveStatus(ctx, ti)
	expectRPCWrapPanic(t, err)
}

var testReplicationPosition = "MariaDB/5-456-890"

func (fra *fakeRPCAgent) MasterPosition(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestMasterPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.MasterPosition(ctx, ti)
	compareError(t, "MasterPosition", err, rs, testReplicationPosition)
}

func agentRPCTestMasterPositionPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.MasterPosition(ctx, ti)
	expectRPCWrapPanic(t, err)
}

var testStopSlaveCalled = false

func (fra *fakeRPCAgent) StopSlave(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStopSlaveCalled = true
	return nil
}

func agentRPCTestStopSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StopSlave(ctx, ti)
	compareError(t, "StopSlave", err, true, testStopSlaveCalled)
}

func agentRPCTestStopSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StopSlave(ctx, ti)
	expectRPCWrapLockPanic(t, err)
}

var testStopSlaveMinimumWaitTime = time.Hour

func (fra *fakeRPCAgent) StopSlaveMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "StopSlaveMinimum position", position, testReplicationPosition)
	compare(fra.t, "StopSlaveMinimum waitTime", waitTime, testStopSlaveMinimumWaitTime)
	return testReplicationPositionReturned, nil
}

func agentRPCTestStopSlaveMinimum(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	pos, err := client.StopSlaveMinimum(ctx, ti, testReplicationPosition, testStopSlaveMinimumWaitTime)
	compareError(t, "StopSlaveMinimum", err, pos, testReplicationPositionReturned)
}

func agentRPCTestStopSlaveMinimumPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.StopSlaveMinimum(ctx, ti, testReplicationPosition, testStopSlaveMinimumWaitTime)
	expectRPCWrapLockPanic(t, err)
}

var testStartSlaveCalled = false

func (fra *fakeRPCAgent) StartSlave(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartSlaveCalled = true
	return nil
}

func agentRPCTestStartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartSlave(ctx, ti)
	compareError(t, "StartSlave", err, true, testStartSlaveCalled)
}

func agentRPCTestStartSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartSlave(ctx, ti)
	expectRPCWrapLockPanic(t, err)
}

var testTabletExternallyReparentedCalled = false

func (fra *fakeRPCAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testTabletExternallyReparentedCalled = true
	return nil
}

func agentRPCTestTabletExternallyReparented(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.TabletExternallyReparented(ctx, ti, "")
	compareError(t, "TabletExternallyReparented", err, true, testTabletExternallyReparentedCalled)
}

func agentRPCTestTabletExternallyReparentedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.TabletExternallyReparented(ctx, ti, "")
	expectRPCWrapLockPanic(t, err)
}

var testGetSlavesResult = []string{"slave1", "slave2"}

func (fra *fakeRPCAgent) GetSlaves(ctx context.Context) ([]string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testGetSlavesResult, nil
}

func agentRPCTestGetSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	s, err := client.GetSlaves(ctx, ti)
	compareError(t, "GetSlaves", err, s, testGetSlavesResult)
}

func agentRPCTestGetSlavesPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.GetSlaves(ctx, ti)
	expectRPCWrapPanic(t, err)
}

var testBlpPosition = &tabletmanagerdatapb.BlpPosition{
	Uid:      73,
	Position: "testReplicationPosition",
}
var testWaitBlpPositionWaitTime = time.Hour
var testWaitBlpPositionCalled = false

func (fra *fakeRPCAgent) WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "WaitBlpPosition blpPosition", blpPosition, testBlpPosition)
	compare(fra.t, "WaitBlpPosition waitTime", waitTime, testWaitBlpPositionWaitTime)
	testWaitBlpPositionCalled = true
	return nil
}

func agentRPCTestWaitBlpPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.WaitBlpPosition(ctx, ti, testBlpPosition, testWaitBlpPositionWaitTime)
	compareError(t, "WaitBlpPosition", err, true, testWaitBlpPositionCalled)
}

func agentRPCTestWaitBlpPositionPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.WaitBlpPosition(ctx, ti, testBlpPosition, testWaitBlpPositionWaitTime)
	expectRPCWrapLockPanic(t, err)
}

var testBlpPositionList = []*tabletmanagerdatapb.BlpPosition{
	{
		Uid:      12,
		Position: "testBlpPosition",
	},
}

func (fra *fakeRPCAgent) StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testBlpPositionList, nil
}

func agentRPCTestStopBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	bpl, err := client.StopBlp(ctx, ti)
	compareError(t, "StopBlp", err, bpl, testBlpPositionList)
}

func agentRPCTestStopBlpPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.StopBlp(ctx, ti)
	expectRPCWrapLockPanic(t, err)
}

var testStartBlpCalled = false

func (fra *fakeRPCAgent) StartBlp(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartBlpCalled = true
	return nil
}

func agentRPCTestStartBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartBlp(ctx, ti)
	compareError(t, "StartBlp", err, true, testStartBlpCalled)
}

func agentRPCTestStartBlpPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartBlp(ctx, ti)
	expectRPCWrapLockPanic(t, err)
}

var testRunBlpUntilWaitTime = 3 * time.Minute

func (fra *fakeRPCAgent) RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "RunBlpUntil bpl", bpl, testBlpPositionList)
	compare(fra.t, "RunBlpUntil waitTime", waitTime, testRunBlpUntilWaitTime)
	return testReplicationPosition, nil
}

func agentRPCTestRunBlpUntil(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.RunBlpUntil(ctx, ti, testBlpPositionList, testRunBlpUntilWaitTime)
	compareError(t, "RunBlpUntil", err, rp, testReplicationPosition)
}

func agentRPCTestRunBlpUntilPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.RunBlpUntil(ctx, ti, testBlpPositionList, testRunBlpUntilWaitTime)
	expectRPCWrapLockPanic(t, err)
}

//
// Reparenting related functions
//

var testResetReplicationCalled = false

func (fra *fakeRPCAgent) ResetReplication(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testResetReplicationCalled = true
	return nil
}

func agentRPCTestResetReplication(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ResetReplication(ctx, ti)
	compareError(t, "ResetReplication", err, true, testResetReplicationCalled)
}

func agentRPCTestResetReplicationPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ResetReplication(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

func (fra *fakeRPCAgent) InitMaster(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestInitMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.InitMaster(ctx, ti)
	compareError(t, "InitMaster", err, rp, testReplicationPosition)
}

func agentRPCTestInitMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.InitMaster(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testPopulateReparentJournalCalled = false
var testTimeCreatedNS int64 = 4569900
var testActionName = "TestActionName"
var testMasterAlias = &topodatapb.TabletAlias{
	Cell: "ce",
	Uid:  372,
}

func (fra *fakeRPCAgent) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PopulateReparentJournal timeCreatedNS", timeCreatedNS, testTimeCreatedNS)
	compare(fra.t, "PopulateReparentJournal actionName", actionName, testActionName)
	compare(fra.t, "PopulateReparentJournal masterAlias", masterAlias, testMasterAlias)
	compare(fra.t, "PopulateReparentJournal pos", position, testReplicationPosition)
	testPopulateReparentJournalCalled = true
	return nil
}

func agentRPCTestPopulateReparentJournal(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.PopulateReparentJournal(ctx, ti, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	compareError(t, "PopulateReparentJournal", err, true, testPopulateReparentJournalCalled)
}

func agentRPCTestPopulateReparentJournalPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.PopulateReparentJournal(ctx, ti, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	expectRPCWrapPanic(t, err)
}

var testInitSlaveCalled = false

func (fra *fakeRPCAgent) InitSlave(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "InitSlave parent", parent, testMasterAlias)
	compare(fra.t, "InitSlave pos", position, testReplicationPosition)
	compare(fra.t, "InitSlave timeCreatedNS", timeCreatedNS, testTimeCreatedNS)
	testInitSlaveCalled = true
	return nil
}

func agentRPCTestInitSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.InitSlave(ctx, ti, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	compareError(t, "InitSlave", err, true, testInitSlaveCalled)
}

func agentRPCTestInitSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.InitSlave(ctx, ti, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	expectRPCWrapLockActionPanic(t, err)
}

func (fra *fakeRPCAgent) DemoteMaster(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.DemoteMaster(ctx, ti)
	compareError(t, "DemoteMaster", err, rp, testReplicationPosition)
}

func agentRPCTestDemoteMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.DemoteMaster(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testReplicationPositionReturned = "MariaDB/5-567-3456"

func (fra *fakeRPCAgent) PromoteSlaveWhenCaughtUp(ctx context.Context, position string) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PromoteSlaveWhenCaughtUp pos", position, testReplicationPosition)
	return testReplicationPositionReturned, nil
}

func agentRPCTestPromoteSlaveWhenCaughtUp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.PromoteSlaveWhenCaughtUp(ctx, ti, testReplicationPosition)
	compareError(t, "PromoteSlaveWhenCaughtUp", err, rp, testReplicationPositionReturned)
}

func agentRPCTestPromoteSlaveWhenCaughtUpPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.PromoteSlaveWhenCaughtUp(ctx, ti, testReplicationPosition)
	expectRPCWrapLockActionPanic(t, err)
}

var testSlaveWasPromotedCalled = false

func (fra *fakeRPCAgent) SlaveWasPromoted(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testSlaveWasPromotedCalled = true
	return nil
}

func agentRPCTestSlaveWasPromoted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasPromoted(ctx, ti)
	compareError(t, "SlaveWasPromoted", err, true, testSlaveWasPromotedCalled)
}

func agentRPCTestSlaveWasPromotedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasPromoted(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

var testSetMasterCalled = false
var testForceStartSlave = true

func (fra *fakeRPCAgent) SetMaster(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, forceStartSlave bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "SetMaster parent", parent, testMasterAlias)
	compare(fra.t, "SetMaster timeCreatedNS", timeCreatedNS, testTimeCreatedNS)
	compare(fra.t, "SetMaster forceStartSlave", forceStartSlave, testForceStartSlave)
	testSetMasterCalled = true
	return nil
}

func agentRPCTestSetMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SetMaster(ctx, ti, testMasterAlias, testTimeCreatedNS, testForceStartSlave)
	compareError(t, "SetMaster", err, true, testSetMasterCalled)
}

func agentRPCTestSetMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SetMaster(ctx, ti, testMasterAlias, testTimeCreatedNS, testForceStartSlave)
	expectRPCWrapLockActionPanic(t, err)
}

var testSlaveWasRestartedArgs = &actionnode.SlaveWasRestartedArgs{
	Parent: &topodatapb.TabletAlias{
		Cell: "prison",
		Uid:  42,
	},
}
var testSlaveWasRestartedCalled = false

func (fra *fakeRPCAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "SlaveWasRestarted swrd", swrd, testSlaveWasRestartedArgs)
	testSlaveWasRestartedCalled = true
	return nil
}

func agentRPCTestSlaveWasRestarted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasRestarted(ctx, ti, testSlaveWasRestartedArgs)
	compareError(t, "SlaveWasRestarted", err, true, testSlaveWasRestartedCalled)
}

func agentRPCTestSlaveWasRestartedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasRestarted(ctx, ti, testSlaveWasRestartedArgs)
	expectRPCWrapLockActionPanic(t, err)
}

func (fra *fakeRPCAgent) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationStatus, nil
}

func agentRPCTestStopReplicationAndGetStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.StopReplicationAndGetStatus(ctx, ti)
	compareError(t, "StopReplicationAndGetStatus", err, rp, testReplicationStatus)
}

func agentRPCTestStopReplicationAndGetStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.StopReplicationAndGetStatus(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

func (fra *fakeRPCAgent) PromoteSlave(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestPromoteSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.PromoteSlave(ctx, ti)
	compareError(t, "PromoteSlave", err, rp, testReplicationPosition)
}

func agentRPCTestPromoteSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	_, err := client.PromoteSlave(ctx, ti)
	expectRPCWrapLockActionPanic(t, err)
}

//
// Backup / restore related methods
//

var testBackupConcurrency = 24
var testBackupCalled = false

func (fra *fakeRPCAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Backup args", concurrency, testBackupConcurrency)
	logStuff(logger, 10)
	testBackupCalled = true
	return nil
}

func agentRPCTestBackup(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Backup(ctx, ti, testBackupConcurrency)
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	compareLoggedStuff(t, "Backup", logChannel, 10)
	err = errFunc()
	compareError(t, "Backup", err, true, testBackupCalled)
}

func agentRPCTestBackupPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Backup(ctx, ti, testBackupConcurrency)
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	if e, ok := <-logChannel; ok {
		t.Fatalf("Unexpected Backup logs: %v", e)
	}
	err = errFunc()
	expectRPCWrapLockActionPanic(t, err)
}

//
// RPC helpers
//

// RPCWrap is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrap(ctx context.Context, name string, args, reply interface{}, f func() error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("RPCWrap caught panic during %v", name)
		}
	}()
	return f()
}

// RPCWrapLock is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrapLock(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("RPCWrapLock caught panic during %v", name)
		}
	}()
	return f()
}

// RPCWrapLockAction is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrapLockAction(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("RPCWrapLockAction caught panic during %v", name)
		}
	}()
	return f()
}

// methods to test individual API calls

// Run will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RPCAgent (returned
// by NewFakeRPCAgent)
func Run(t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo, fakeAgent tabletmanager.RPCAgent) {
	ctx := context.Background()

	// Test RPC specific methods of the interface.
	agentRPCTestIsTimeoutErrorDialExpiredContext(ctx, t, client, ti)
	agentRPCTestIsTimeoutErrorDialTimeout(ctx, t, client, ti)
	agentRPCTestIsTimeoutErrorRPC(ctx, t, client, ti, fakeAgent.(*fakeRPCAgent))

	// Various read-only methods
	agentRPCTestPing(ctx, t, client, ti)
	agentRPCTestGetSchema(ctx, t, client, ti)
	agentRPCTestGetPermissions(ctx, t, client, ti)

	// Various read-write methods
	agentRPCTestSetReadOnly(ctx, t, client, ti)
	agentRPCTestChangeType(ctx, t, client, ti)
	agentRPCTestSleep(ctx, t, client, ti)
	agentRPCTestExecuteHook(ctx, t, client, ti)
	agentRPCTestRefreshState(ctx, t, client, ti)
	agentRPCTestRunHealthCheck(ctx, t, client, ti)
	agentRPCTestIgnoreHealthError(ctx, t, client, ti)
	agentRPCTestReloadSchema(ctx, t, client, ti)
	agentRPCTestPreflightSchema(ctx, t, client, ti)
	agentRPCTestApplySchema(ctx, t, client, ti)
	agentRPCTestExecuteFetch(ctx, t, client, ti)

	// Replication related methods
	agentRPCTestSlaveStatus(ctx, t, client, ti)
	agentRPCTestMasterPosition(ctx, t, client, ti)
	agentRPCTestStopSlave(ctx, t, client, ti)
	agentRPCTestStopSlaveMinimum(ctx, t, client, ti)
	agentRPCTestStartSlave(ctx, t, client, ti)
	agentRPCTestTabletExternallyReparented(ctx, t, client, ti)
	agentRPCTestGetSlaves(ctx, t, client, ti)
	agentRPCTestWaitBlpPosition(ctx, t, client, ti)
	agentRPCTestStopBlp(ctx, t, client, ti)
	agentRPCTestStartBlp(ctx, t, client, ti)
	agentRPCTestRunBlpUntil(ctx, t, client, ti)

	// Reparenting related functions
	agentRPCTestResetReplication(ctx, t, client, ti)
	agentRPCTestInitMaster(ctx, t, client, ti)
	agentRPCTestPopulateReparentJournal(ctx, t, client, ti)
	agentRPCTestInitSlave(ctx, t, client, ti)
	agentRPCTestDemoteMaster(ctx, t, client, ti)
	agentRPCTestPromoteSlaveWhenCaughtUp(ctx, t, client, ti)
	agentRPCTestSlaveWasPromoted(ctx, t, client, ti)
	agentRPCTestSetMaster(ctx, t, client, ti)
	agentRPCTestSlaveWasRestarted(ctx, t, client, ti)
	agentRPCTestStopReplicationAndGetStatus(ctx, t, client, ti)
	agentRPCTestPromoteSlave(ctx, t, client, ti)

	// Backup / restore related methods
	agentRPCTestBackup(ctx, t, client, ti)

	//
	// Tests panic handling everywhere now
	//
	fakeAgent.(*fakeRPCAgent).panics = true

	// Various read-only methods
	agentRPCTestPingPanic(ctx, t, client, ti)
	agentRPCTestGetSchemaPanic(ctx, t, client, ti)
	agentRPCTestGetPermissionsPanic(ctx, t, client, ti)

	// Various read-write methods
	agentRPCTestSetReadOnlyPanic(ctx, t, client, ti)
	agentRPCTestChangeTypePanic(ctx, t, client, ti)
	agentRPCTestSleepPanic(ctx, t, client, ti)
	agentRPCTestExecuteHookPanic(ctx, t, client, ti)
	agentRPCTestRefreshStatePanic(ctx, t, client, ti)
	agentRPCTestRunHealthCheckPanic(ctx, t, client, ti)
	agentRPCTestReloadSchemaPanic(ctx, t, client, ti)
	agentRPCTestPreflightSchemaPanic(ctx, t, client, ti)
	agentRPCTestApplySchemaPanic(ctx, t, client, ti)
	agentRPCTestExecuteFetchPanic(ctx, t, client, ti)

	// Replication related methods
	agentRPCTestSlaveStatusPanic(ctx, t, client, ti)
	agentRPCTestMasterPositionPanic(ctx, t, client, ti)
	agentRPCTestStopSlavePanic(ctx, t, client, ti)
	agentRPCTestStopSlaveMinimumPanic(ctx, t, client, ti)
	agentRPCTestStartSlavePanic(ctx, t, client, ti)
	agentRPCTestTabletExternallyReparentedPanic(ctx, t, client, ti)
	agentRPCTestGetSlavesPanic(ctx, t, client, ti)
	agentRPCTestWaitBlpPositionPanic(ctx, t, client, ti)
	agentRPCTestStopBlpPanic(ctx, t, client, ti)
	agentRPCTestStartBlpPanic(ctx, t, client, ti)
	agentRPCTestRunBlpUntilPanic(ctx, t, client, ti)

	// Reparenting related functions
	agentRPCTestResetReplicationPanic(ctx, t, client, ti)
	agentRPCTestInitMasterPanic(ctx, t, client, ti)
	agentRPCTestPopulateReparentJournalPanic(ctx, t, client, ti)
	agentRPCTestInitSlavePanic(ctx, t, client, ti)
	agentRPCTestDemoteMasterPanic(ctx, t, client, ti)
	agentRPCTestPromoteSlaveWhenCaughtUpPanic(ctx, t, client, ti)
	agentRPCTestSlaveWasPromotedPanic(ctx, t, client, ti)
	agentRPCTestSetMasterPanic(ctx, t, client, ti)
	agentRPCTestSlaveWasRestartedPanic(ctx, t, client, ti)
	agentRPCTestStopReplicationAndGetStatusPanic(ctx, t, client, ti)
	agentRPCTestPromoteSlavePanic(ctx, t, client, ti)

	// Backup / restore related methods
	agentRPCTestBackupPanic(ctx, t, client, ti)
}
