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

package agentrpctest

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

func (fra *fakeRPCAgent) LockTables(ctx context.Context) error {
	panic("implement me")
}

func (fra *fakeRPCAgent) UnlockTables(ctx context.Context) error {
	panic("implement me")
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

var protoMessage = reflect.TypeOf((*proto.Message)(nil)).Elem()

func compare(t *testing.T, name string, got, want interface{}) {
	t.Helper()
	typ := reflect.TypeOf(got)
	if reflect.TypeOf(got) != reflect.TypeOf(want) {
		goto fail
	}
	switch {
	case typ.Implements(protoMessage):
		if !proto.Equal(got.(proto.Message), want.(proto.Message)) {
			goto fail
		}
	case typ.Kind() == reflect.Slice && typ.Elem().Implements(protoMessage):
		vx, vy := reflect.ValueOf(got), reflect.ValueOf(want)
		if vx.Len() != vy.Len() {
			goto fail
		}
		for i := 0; i < vx.Len(); i++ {
			if !proto.Equal(vx.Index(i).Interface().(proto.Message), vy.Index(i).Interface().(proto.Message)) {
				goto fail
			}
		}
	default:
		if !reflect.DeepEqual(got, want) {
			goto fail
		}
	}
	return
fail:
	t.Errorf("Unexpected %v:\ngot  %#v\nwant %#v", name, got, want)
}

func compareBool(t *testing.T, name string, got bool) {
	t.Helper()
	if !got {
		t.Errorf("Unexpected %v: got false expected true", name)
	}
}

func compareError(t *testing.T, name string, err error, got, want interface{}) {
	t.Helper()
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

func compareLoggedStuff(t *testing.T, name string, stream logutil.EventStream, count int) error {
	t.Helper()
	for i := 0; i < count; i++ {
		le, err := stream.Recv()
		if err != nil {
			t.Errorf("No logged value for %v/%v", name, i)
			return err
		}
		if le.Value != testLogString {
			t.Errorf("Unexpected log response for %v: got %v expected %v", name, le.Value, testLogString)
		}
	}
	_, err := stream.Recv()
	if err == nil {
		t.Fatalf("log channel wasn't closed for %v", name)
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func expectHandleRPCPanic(t *testing.T, name string, verbose bool, err error) {
	t.Helper()
	expected := fmt.Sprintf("HandleRPCPanic caught panic during %v with verbose %v", name, verbose)
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

func agentRPCTestPing(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Ping(ctx, tablet)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func agentRPCTestPingPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Ping(ctx, tablet)
	expectHandleRPCPanic(t, "Ping", false /*verbose*/, err)
}

// agentRPCTestDialExpiredContext verifies that
// the context returns the right DeadlineExceeded Err() for
// RPCs failed due to an expired context before .Dial().
func agentRPCTestDialExpiredContext(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	// Using a timeout of 0 here such that .Dial() will fail immediately.
	expiredCtx, cancel := context.WithTimeout(ctx, 0)
	defer cancel()
	err := client.Ping(expiredCtx, tablet)
	if err == nil {
		t.Fatal("agentRPCTestDialExpiredContext: RPC with expired context did not fail")
	}
	// The context was already expired when we created it. Here we only verify that it returns the expected error.
	select {
	case <-expiredCtx.Done():
		if err := expiredCtx.Err(); err != context.DeadlineExceeded {
			t.Errorf("agentRPCTestDialExpiredContext: got %v want context.DeadlineExceeded", err)
		}
	default:
		t.Errorf("agentRPCTestDialExpiredContext: context.Done() not closed")
	}
}

// agentRPCTestRPCTimeout verifies that
// the context returns the right DeadlineExceeded Err() for
// RPCs failed due to an expired context during execution.
func agentRPCTestRPCTimeout(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet, fakeAgent *fakeRPCAgent) {
	// We must use a timeout > 0 such that the context deadline hasn't expired
	// yet in grpctmclient.Client.dial().
	// NOTE: This might still race e.g. when test execution takes too long the
	//       context will be expired in dial() already. In such cases coverage
	//       will be reduced but the test will not flake.
	shortCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	fakeAgent.setSlow(true)
	defer func() { fakeAgent.setSlow(false) }()
	err := client.Ping(shortCtx, tablet)
	if err == nil {
		t.Fatal("agentRPCTestRPCTimeout: RPC with expired context did not fail")
	}
	select {
	case <-shortCtx.Done():
		if err := shortCtx.Err(); err != context.DeadlineExceeded {
			t.Errorf("agentRPCTestRPCTimeout: got %v want context.DeadlineExceeded", err)
		}
	default:
		t.Errorf("agentRPCTestRPCTimeout: context.Done() not closed")
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

func agentRPCTestGetSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	result, err := client.GetSchema(ctx, tablet, testGetSchemaTables, testGetSchemaExcludeTables, true)
	compareError(t, "GetSchema", err, result, testGetSchemaReply)
}

func agentRPCTestGetSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.GetSchema(ctx, tablet, testGetSchemaTables, testGetSchemaExcludeTables, true)
	expectHandleRPCPanic(t, "GetSchema", false /*verbose*/, err)
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

func agentRPCTestGetPermissions(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	result, err := client.GetPermissions(ctx, tablet)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

func agentRPCTestGetPermissionsPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.GetPermissions(ctx, tablet)
	expectHandleRPCPanic(t, "GetPermissions", false /*verbose*/, err)
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

func agentRPCTestSetReadOnly(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	testSetReadOnlyExpectedValue = true
	err := client.SetReadOnly(ctx, tablet)
	if err != nil {
		t.Errorf("SetReadOnly failed: %v", err)
	}
	testSetReadOnlyExpectedValue = false
	err = client.SetReadWrite(ctx, tablet)
	if err != nil {
		t.Errorf("SetReadWrite failed: %v", err)
	}
}

func agentRPCTestSetReadOnlyPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetReadOnly(ctx, tablet)
	expectHandleRPCPanic(t, "SetReadOnly", true /*verbose*/, err)
	err = client.SetReadWrite(ctx, tablet)
	expectHandleRPCPanic(t, "SetReadWrite", true /*verbose*/, err)
}

var testChangeTypeValue = topodatapb.TabletType_REPLICA

func (fra *fakeRPCAgent) ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func agentRPCTestChangeType(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ChangeType(ctx, tablet, testChangeTypeValue)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

func agentRPCTestChangeTypePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ChangeType(ctx, tablet, testChangeTypeValue)
	expectHandleRPCPanic(t, "ChangeType", true /*verbose*/, err)
}

var testSleepDuration = time.Minute

func (fra *fakeRPCAgent) Sleep(ctx context.Context, duration time.Duration) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func agentRPCTestSleep(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Sleep(ctx, tablet, testSleepDuration)
	if err != nil {
		t.Errorf("Sleep failed: %v", err)
	}
}

func agentRPCTestSleepPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Sleep(ctx, tablet, testSleepDuration)
	expectHandleRPCPanic(t, "Sleep", true /*verbose*/, err)
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

func agentRPCTestExecuteHook(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	hr, err := client.ExecuteHook(ctx, tablet, testExecuteHookHook)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

func agentRPCTestExecuteHookPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.ExecuteHook(ctx, tablet, testExecuteHookHook)
	expectHandleRPCPanic(t, "ExecuteHook", true /*verbose*/, err)
}

var testRefreshStateCalled = false

func (fra *fakeRPCAgent) RefreshState(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testRefreshStateCalled {
		fra.t.Errorf("RefreshState called multiple times?")
	}
	testRefreshStateCalled = true
	return nil
}

func agentRPCTestRefreshState(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RefreshState(ctx, tablet)
	if err != nil {
		t.Errorf("RefreshState failed: %v", err)
	}
	if !testRefreshStateCalled {
		t.Errorf("RefreshState didn't call the server side")
	}
}

func agentRPCTestRefreshStatePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RefreshState(ctx, tablet)
	expectHandleRPCPanic(t, "RefreshState", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) RunHealthCheck(ctx context.Context) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
}

var testIgnoreHealthErrorValue = ".*"

func (fra *fakeRPCAgent) IgnoreHealthError(ctx context.Context, pattern string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "IgnoreHealthError pattern", pattern, testIgnoreHealthErrorValue)
	return nil
}

func agentRPCTestRunHealthCheck(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RunHealthCheck(ctx, tablet)
	if err != nil {
		t.Errorf("RunHealthCheck failed: %v", err)
	}
}

func agentRPCTestRunHealthCheckPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RunHealthCheck(ctx, tablet)
	expectHandleRPCPanic(t, "RunHealthCheck", false /*verbose*/, err)
}

func agentRPCTestIgnoreHealthError(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.IgnoreHealthError(ctx, tablet, testIgnoreHealthErrorValue)
	if err != nil {
		t.Errorf("IgnoreHealthError failed: %v", err)
	}
}

func agentRPCTestIgnoreHealthErrorPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.IgnoreHealthError(ctx, tablet, testIgnoreHealthErrorValue)
	expectHandleRPCPanic(t, "IgnoreHealthError", false /*verbose*/, err)
}

var testReloadSchemaCalled = false

func (fra *fakeRPCAgent) ReloadSchema(ctx context.Context, waitPosition string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testReloadSchemaCalled {
		fra.t.Errorf("ReloadSchema called multiple times?")
	}
	testReloadSchemaCalled = true
	return nil
}

func agentRPCTestReloadSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReloadSchema(ctx, tablet, "")
	if err != nil {
		t.Errorf("ReloadSchema failed: %v", err)
	}
	if !testReloadSchemaCalled {
		t.Errorf("ReloadSchema didn't call the server side")
	}
}

func agentRPCTestReloadSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReloadSchema(ctx, tablet, "")
	expectHandleRPCPanic(t, "ReloadSchema", false /*verbose*/, err)
}

var testPreflightSchema = []string{"change table add table cloth"}
var testSchemaChangeResult = []*tabletmanagerdatapb.SchemaChangeResult{
	{
		BeforeSchema: testGetSchemaReply,
		AfterSchema:  testGetSchemaReply,
	},
}

func (fra *fakeRPCAgent) PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PreflightSchema result", changes, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func agentRPCTestPreflightSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	scr, err := client.PreflightSchema(ctx, tablet, testPreflightSchema)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

func agentRPCTestPreflightSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.PreflightSchema(ctx, tablet, testPreflightSchema)
	expectHandleRPCPanic(t, "PreflightSchema", true /*verbose*/, err)
}

var testSchemaChange = &tmutils.SchemaChange{
	SQL:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRPCAgent) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !change.Equal(testSchemaChange) {
		fra.t.Errorf("Unexpected ApplySchema change:\ngot  %#v\nwant %#v", change, testSchemaChange)
	}
	return testSchemaChangeResult[0], nil
}

func agentRPCTestApplySchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	scr, err := client.ApplySchema(ctx, tablet, testSchemaChange)
	compareError(t, "ApplySchema", err, scr, testSchemaChangeResult[0])
}

func agentRPCTestApplySchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.ApplySchema(ctx, tablet, testSchemaChange)
	expectHandleRPCPanic(t, "ApplySchema", true /*verbose*/, err)
}

var testExecuteFetchQuery = []byte("fetch this invalid utf8 character \x80")
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

func (fra *fakeRPCAgent) ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsDba query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsDba maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetchAsDba disableBinlogs", disableBinlogs)
	compareBool(fra.t, "ExecuteFetchAsDba reloadSchema", reloadSchema)

	return testExecuteFetchResult, nil
}

func (fra *fakeRPCAgent) ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsAllPrivs query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsAllPrivs maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetchAsAllPrivs reloadSchema", reloadSchema)

	return testExecuteFetchResult, nil
}

func (fra *fakeRPCAgent) ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsApp query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsApp maxrows", maxrows, testExecuteFetchMaxRows)
	return testExecuteFetchResult, nil
}

func agentRPCTestExecuteFetch(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	// using pool
	qr, err := client.ExecuteFetchAsDba(ctx, tablet, true, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true)
	compareError(t, "ExecuteFetchAsDba", err, qr, testExecuteFetchResult)
	qr, err = client.ExecuteFetchAsApp(ctx, tablet, true, testExecuteFetchQuery, testExecuteFetchMaxRows)
	compareError(t, "ExecuteFetchAsApp", err, qr, testExecuteFetchResult)

	// not using pool
	qr, err = client.ExecuteFetchAsDba(ctx, tablet, false, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true)
	compareError(t, "ExecuteFetchAsDba", err, qr, testExecuteFetchResult)
	qr, err = client.ExecuteFetchAsApp(ctx, tablet, false, testExecuteFetchQuery, testExecuteFetchMaxRows)
	compareError(t, "ExecuteFetchAsApp", err, qr, testExecuteFetchResult)
	qr, err = client.ExecuteFetchAsAllPrivs(ctx, tablet, testExecuteFetchQuery, testExecuteFetchMaxRows, true)
	compareError(t, "ExecuteFetchAsAllPrivs", err, qr, testExecuteFetchResult)

}

func agentRPCTestExecuteFetchPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	// using pool
	_, err := client.ExecuteFetchAsDba(ctx, tablet, true, testExecuteFetchQuery, testExecuteFetchMaxRows, true, false)
	expectHandleRPCPanic(t, "ExecuteFetchAsDba", false /*verbose*/, err)
	_, err = client.ExecuteFetchAsApp(ctx, tablet, true, testExecuteFetchQuery, testExecuteFetchMaxRows)
	expectHandleRPCPanic(t, "ExecuteFetchAsApp", false /*verbose*/, err)

	// not using pool
	_, err = client.ExecuteFetchAsDba(ctx, tablet, false, testExecuteFetchQuery, testExecuteFetchMaxRows, true, false)
	expectHandleRPCPanic(t, "ExecuteFetchAsDba", false /*verbose*/, err)
	_, err = client.ExecuteFetchAsApp(ctx, tablet, false, testExecuteFetchQuery, testExecuteFetchMaxRows)
	expectHandleRPCPanic(t, "ExecuteFetchAsApp", false /*verbose*/, err)
	_, err = client.ExecuteFetchAsAllPrivs(ctx, tablet, testExecuteFetchQuery, testExecuteFetchMaxRows, false)
	expectHandleRPCPanic(t, "ExecuteFetchAsAllPrivs", false /*verbose*/, err)
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

func agentRPCTestSlaveStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rs, err := client.SlaveStatus(ctx, tablet)
	compareError(t, "SlaveStatus", err, rs, testReplicationStatus)
}

func agentRPCTestSlaveStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.SlaveStatus(ctx, tablet)
	expectHandleRPCPanic(t, "SlaveStatus", false /*verbose*/, err)
}

var testReplicationPosition = "MariaDB/5-456-890"

func (fra *fakeRPCAgent) MasterPosition(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestMasterPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rs, err := client.MasterPosition(ctx, tablet)
	compareError(t, "MasterPosition", err, rs, testReplicationPosition)
}

func agentRPCTestMasterPositionPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.MasterPosition(ctx, tablet)
	expectHandleRPCPanic(t, "MasterPosition", false /*verbose*/, err)
}

var testStopSlaveCalled = false

func (fra *fakeRPCAgent) StopSlave(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStopSlaveCalled = true
	return nil
}

func agentRPCTestStopSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StopSlave(ctx, tablet)
	compareError(t, "StopSlave", err, true, testStopSlaveCalled)
}

func agentRPCTestStopSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StopSlave(ctx, tablet)
	expectHandleRPCPanic(t, "StopSlave", true /*verbose*/, err)
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

func agentRPCTestStopSlaveMinimum(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	pos, err := client.StopSlaveMinimum(ctx, tablet, testReplicationPosition, testStopSlaveMinimumWaitTime)
	compareError(t, "StopSlaveMinimum", err, pos, testReplicationPositionReturned)
}

func agentRPCTestStopSlaveMinimumPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.StopSlaveMinimum(ctx, tablet, testReplicationPosition, testStopSlaveMinimumWaitTime)
	expectHandleRPCPanic(t, "StopSlaveMinimum", true /*verbose*/, err)
}

var testStartSlaveCalled = false

func (fra *fakeRPCAgent) StartSlave(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartSlaveCalled = true
	return nil
}

var testStartSlaveUntilAfterCalledWith = ""

func (fra *fakeRPCAgent) StartSlaveUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartSlaveUntilAfterCalledWith = position
	return nil
}

func agentRPCTestStartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartSlave(ctx, tablet)
	compareError(t, "StartSlave", err, true, testStartSlaveCalled)
}

func agentRPCTestStartSlaveUntilAfter(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartSlaveUntilAfter(ctx, tablet, "test-position", time.Minute)
	compareError(t, "StartSlaveUntilAfter", err, "test-position", testStartSlaveUntilAfterCalledWith)
}

func agentRPCTestStartSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartSlave(ctx, tablet)
	expectHandleRPCPanic(t, "StartSlave", true /*verbose*/, err)
}

var testTabletExternallyReparentedCalled = false

func (fra *fakeRPCAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testTabletExternallyReparentedCalled = true
	return nil
}

func agentRPCTestTabletExternallyReparented(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.TabletExternallyReparented(ctx, tablet, "")
	compareError(t, "TabletExternallyReparented", err, true, testTabletExternallyReparentedCalled)
}

func agentRPCTestTabletExternallyReparentedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.TabletExternallyReparented(ctx, tablet, "")
	expectHandleRPCPanic(t, "TabletExternallyReparented", false /*verbose*/, err)
}

var testGetSlavesResult = []string{"slave1", "slave2"}

func (fra *fakeRPCAgent) GetSlaves(ctx context.Context) ([]string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testGetSlavesResult, nil
}

func agentRPCTestGetSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	s, err := client.GetSlaves(ctx, tablet)
	compareError(t, "GetSlaves", err, s, testGetSlavesResult)
}

func agentRPCTestGetSlavesPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.GetSlaves(ctx, tablet)
	expectHandleRPCPanic(t, "GetSlaves", false /*verbose*/, err)
}

var testVRQuery = "query"

func (fra *fakeRPCAgent) VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "VReplicationExec query", query, testVRQuery)
	return testExecuteFetchResult, nil
}

func agentRPCTestVReplicationExec(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.VReplicationExec(ctx, tablet, testVRQuery)
	compareError(t, "VReplicationExec", err, rp, testExecuteFetchResult)
}

func agentRPCTestVReplicationExecPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.VReplicationExec(ctx, tablet, testVRQuery)
	expectHandleRPCPanic(t, "VReplicationExec", true /*verbose*/, err)
}

var (
	wfpid  = 3
	wfppos = ""
)

func (fra *fakeRPCAgent) VReplicationWaitForPos(ctx context.Context, id int, pos string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "VReplicationWaitForPos id", id, wfpid)
	compare(fra.t, "VReplicationWaitForPos pos", pos, wfppos)
	return nil
}

func agentRPCTestVReplicationWaitForPos(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.VReplicationWaitForPos(ctx, tablet, wfpid, wfppos)
	compareError(t, "VReplicationWaitForPos", err, true, true)
}

func agentRPCTestVReplicationWaitForPosPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.VReplicationWaitForPos(ctx, tablet, wfpid, wfppos)
	expectHandleRPCPanic(t, "VReplicationWaitForPos", true /*verbose*/, err)
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

func agentRPCTestResetReplication(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ResetReplication(ctx, tablet)
	compareError(t, "ResetReplication", err, true, testResetReplicationCalled)
}

func agentRPCTestResetReplicationPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ResetReplication(ctx, tablet)
	expectHandleRPCPanic(t, "ResetReplication", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) InitMaster(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestInitMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.InitMaster(ctx, tablet)
	compareError(t, "InitMaster", err, rp, testReplicationPosition)
}

func agentRPCTestInitMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.InitMaster(ctx, tablet)
	expectHandleRPCPanic(t, "InitMaster", true /*verbose*/, err)
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

func agentRPCTestPopulateReparentJournal(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.PopulateReparentJournal(ctx, tablet, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	compareError(t, "PopulateReparentJournal", err, true, testPopulateReparentJournalCalled)
}

func agentRPCTestPopulateReparentJournalPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.PopulateReparentJournal(ctx, tablet, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	expectHandleRPCPanic(t, "PopulateReparentJournal", false /*verbose*/, err)
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

func agentRPCTestInitSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.InitSlave(ctx, tablet, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	compareError(t, "InitSlave", err, true, testInitSlaveCalled)
}

func agentRPCTestInitSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.InitSlave(ctx, tablet, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	expectHandleRPCPanic(t, "InitSlave", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) DemoteMaster(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.DemoteMaster(ctx, tablet)
	compareError(t, "DemoteMaster", err, rp, testReplicationPosition)
}

func agentRPCTestDemoteMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.DemoteMaster(ctx, tablet)
	expectHandleRPCPanic(t, "DemoteMaster", true /*verbose*/, err)
}

var testReplicationPositionReturned = "MariaDB/5-567-3456"

func (fra *fakeRPCAgent) PromoteSlaveWhenCaughtUp(ctx context.Context, position string) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PromoteSlaveWhenCaughtUp pos", position, testReplicationPosition)
	return testReplicationPositionReturned, nil
}

func agentRPCTestPromoteSlaveWhenCaughtUp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.PromoteSlaveWhenCaughtUp(ctx, tablet, testReplicationPosition)
	compareError(t, "PromoteSlaveWhenCaughtUp", err, rp, testReplicationPositionReturned)
}

func agentRPCTestPromoteSlaveWhenCaughtUpPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.PromoteSlaveWhenCaughtUp(ctx, tablet, testReplicationPosition)
	expectHandleRPCPanic(t, "PromoteSlaveWhenCaughtUp", true /*verbose*/, err)
}

var testSlaveWasPromotedCalled = false

func (fra *fakeRPCAgent) SlaveWasPromoted(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testSlaveWasPromotedCalled = true
	return nil
}

func agentRPCTestSlaveWasPromoted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SlaveWasPromoted(ctx, tablet)
	compareError(t, "SlaveWasPromoted", err, true, testSlaveWasPromotedCalled)
}

func agentRPCTestSlaveWasPromotedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SlaveWasPromoted(ctx, tablet)
	expectHandleRPCPanic(t, "SlaveWasPromoted", true /*verbose*/, err)
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

func agentRPCTestSetMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetMaster(ctx, tablet, testMasterAlias, testTimeCreatedNS, testForceStartSlave)
	compareError(t, "SetMaster", err, true, testSetMasterCalled)
}

func agentRPCTestSetMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetMaster(ctx, tablet, testMasterAlias, testTimeCreatedNS, testForceStartSlave)
	expectHandleRPCPanic(t, "SetMaster", true /*verbose*/, err)
}

var testSlaveWasRestartedParent = &topodatapb.TabletAlias{
	Cell: "prison",
	Uid:  42,
}
var testSlaveWasRestartedCalled = false

func (fra *fakeRPCAgent) SlaveWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "SlaveWasRestarted parent", parent, testSlaveWasRestartedParent)
	testSlaveWasRestartedCalled = true
	return nil
}

func agentRPCTestSlaveWasRestarted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SlaveWasRestarted(ctx, tablet, testSlaveWasRestartedParent)
	compareError(t, "SlaveWasRestarted", err, true, testSlaveWasRestartedCalled)
}

func agentRPCTestSlaveWasRestartedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SlaveWasRestarted(ctx, tablet, testSlaveWasRestartedParent)
	expectHandleRPCPanic(t, "SlaveWasRestarted", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationStatus, nil
}

func agentRPCTestStopReplicationAndGetStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.StopReplicationAndGetStatus(ctx, tablet)
	compareError(t, "StopReplicationAndGetStatus", err, rp, testReplicationStatus)
}

func agentRPCTestStopReplicationAndGetStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.StopReplicationAndGetStatus(ctx, tablet)
	expectHandleRPCPanic(t, "StopReplicationAndGetStatus", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) PromoteSlave(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func agentRPCTestPromoteSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.PromoteSlave(ctx, tablet)
	compareError(t, "PromoteSlave", err, rp, testReplicationPosition)
}

func agentRPCTestPromoteSlavePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.PromoteSlave(ctx, tablet)
	expectHandleRPCPanic(t, "PromoteSlave", true /*verbose*/, err)
}

//
// Backup / restore related methods
//

var testBackupConcurrency = 24
var testBackupAllowMaster = false
var testBackupCalled = false
var testRestoreFromBackupCalled = false

func (fra *fakeRPCAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger, allowMaster bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Backup args", concurrency, testBackupConcurrency)
	logStuff(logger, 10)
	testBackupCalled = true
	return nil
}

func agentRPCTestBackup(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.Backup(ctx, tablet, testBackupConcurrency, testBackupAllowMaster)
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	err = compareLoggedStuff(t, "Backup", stream, 10)
	compareError(t, "Backup", err, true, testBackupCalled)
}

func agentRPCTestBackupPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.Backup(ctx, tablet, testBackupConcurrency, testBackupAllowMaster)
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	e, err := stream.Recv()
	if err == nil {
		t.Fatalf("Unexpected Backup logs: %v", e)
	}
	expectHandleRPCPanic(t, "Backup", true /*verbose*/, err)
}

func (fra *fakeRPCAgent) RestoreFromBackup(ctx context.Context, logger logutil.Logger) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	logStuff(logger, 10)
	testRestoreFromBackupCalled = true
	return nil
}

func agentRPCTestRestoreFromBackup(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.RestoreFromBackup(ctx, tablet)
	if err != nil {
		t.Fatalf("RestoreFromBackup failed: %v", err)
	}
	err = compareLoggedStuff(t, "RestoreFromBackup", stream, 10)
	compareError(t, "RestoreFromBackup", err, true, testRestoreFromBackupCalled)
}

func agentRPCTestRestoreFromBackupPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.RestoreFromBackup(ctx, tablet)
	if err != nil {
		t.Fatalf("RestoreFromBackup failed: %v", err)
	}
	e, err := stream.Recv()
	if err == nil {
		t.Fatalf("Unexpected RestoreFromBackup logs: %v", e)
	}
	expectHandleRPCPanic(t, "RestoreFromBackup", true /*verbose*/, err)
}

//
// RPC helpers
//

// HandleRPCPanic is part of the RPCAgent interface
func (fra *fakeRPCAgent) HandleRPCPanic(ctx context.Context, name string, args, reply interface{}, verbose bool, err *error) {
	if x := recover(); x != nil {
		// Use the panic case to make sure 'name' and 'verbose' are right.
		*err = fmt.Errorf("HandleRPCPanic caught panic during %v with verbose %v", name, verbose)
	}
}

// methods to test individual API calls

// Run will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RPCAgent (returned
// by NewFakeRPCAgent)
func Run(t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet, fakeAgent tabletmanager.RPCAgent) {
	ctx := context.Background()

	// Test RPC specific methods of the interface.
	agentRPCTestDialExpiredContext(ctx, t, client, tablet)
	agentRPCTestRPCTimeout(ctx, t, client, tablet, fakeAgent.(*fakeRPCAgent))

	// Various read-only methods
	agentRPCTestPing(ctx, t, client, tablet)
	agentRPCTestGetSchema(ctx, t, client, tablet)
	agentRPCTestGetPermissions(ctx, t, client, tablet)

	// Various read-write methods
	agentRPCTestSetReadOnly(ctx, t, client, tablet)
	agentRPCTestChangeType(ctx, t, client, tablet)
	agentRPCTestSleep(ctx, t, client, tablet)
	agentRPCTestExecuteHook(ctx, t, client, tablet)
	agentRPCTestRefreshState(ctx, t, client, tablet)
	agentRPCTestRunHealthCheck(ctx, t, client, tablet)
	agentRPCTestIgnoreHealthError(ctx, t, client, tablet)
	agentRPCTestReloadSchema(ctx, t, client, tablet)
	agentRPCTestPreflightSchema(ctx, t, client, tablet)
	agentRPCTestApplySchema(ctx, t, client, tablet)
	agentRPCTestExecuteFetch(ctx, t, client, tablet)

	// Replication related methods
	agentRPCTestSlaveStatus(ctx, t, client, tablet)
	agentRPCTestMasterPosition(ctx, t, client, tablet)
	agentRPCTestStopSlave(ctx, t, client, tablet)
	agentRPCTestStopSlaveMinimum(ctx, t, client, tablet)
	agentRPCTestStartSlave(ctx, t, client, tablet)
	agentRPCTestTabletExternallyReparented(ctx, t, client, tablet)
	agentRPCTestGetSlaves(ctx, t, client, tablet)

	// VReplication methods
	agentRPCTestVReplicationExec(ctx, t, client, tablet)
	agentRPCTestVReplicationWaitForPos(ctx, t, client, tablet)

	// Reparenting related functions
	agentRPCTestResetReplication(ctx, t, client, tablet)
	agentRPCTestInitMaster(ctx, t, client, tablet)
	agentRPCTestPopulateReparentJournal(ctx, t, client, tablet)
	agentRPCTestInitSlave(ctx, t, client, tablet)
	agentRPCTestDemoteMaster(ctx, t, client, tablet)
	agentRPCTestPromoteSlaveWhenCaughtUp(ctx, t, client, tablet)
	agentRPCTestSlaveWasPromoted(ctx, t, client, tablet)
	agentRPCTestSetMaster(ctx, t, client, tablet)
	agentRPCTestSlaveWasRestarted(ctx, t, client, tablet)
	agentRPCTestStopReplicationAndGetStatus(ctx, t, client, tablet)
	agentRPCTestPromoteSlave(ctx, t, client, tablet)

	// Backup / restore related methods
	agentRPCTestBackup(ctx, t, client, tablet)
	agentRPCTestRestoreFromBackup(ctx, t, client, tablet)

	//
	// Tests panic handling everywhere now
	//
	fakeAgent.(*fakeRPCAgent).panics = true

	// Various read-only methods
	agentRPCTestPingPanic(ctx, t, client, tablet)
	agentRPCTestGetSchemaPanic(ctx, t, client, tablet)
	agentRPCTestGetPermissionsPanic(ctx, t, client, tablet)

	// Various read-write methods
	agentRPCTestSetReadOnlyPanic(ctx, t, client, tablet)
	agentRPCTestChangeTypePanic(ctx, t, client, tablet)
	agentRPCTestSleepPanic(ctx, t, client, tablet)
	agentRPCTestExecuteHookPanic(ctx, t, client, tablet)
	agentRPCTestRefreshStatePanic(ctx, t, client, tablet)
	agentRPCTestRunHealthCheckPanic(ctx, t, client, tablet)
	agentRPCTestIgnoreHealthErrorPanic(ctx, t, client, tablet)
	agentRPCTestReloadSchemaPanic(ctx, t, client, tablet)
	agentRPCTestPreflightSchemaPanic(ctx, t, client, tablet)
	agentRPCTestApplySchemaPanic(ctx, t, client, tablet)
	agentRPCTestExecuteFetchPanic(ctx, t, client, tablet)

	// Replication related methods
	agentRPCTestSlaveStatusPanic(ctx, t, client, tablet)
	agentRPCTestMasterPositionPanic(ctx, t, client, tablet)
	agentRPCTestStopSlavePanic(ctx, t, client, tablet)
	agentRPCTestStopSlaveMinimumPanic(ctx, t, client, tablet)
	agentRPCTestStartSlavePanic(ctx, t, client, tablet)
	agentRPCTestTabletExternallyReparentedPanic(ctx, t, client, tablet)
	agentRPCTestGetSlavesPanic(ctx, t, client, tablet)

	// VReplication methods
	agentRPCTestVReplicationExecPanic(ctx, t, client, tablet)
	agentRPCTestVReplicationWaitForPosPanic(ctx, t, client, tablet)

	// Reparenting related functions
	agentRPCTestResetReplicationPanic(ctx, t, client, tablet)
	agentRPCTestInitMasterPanic(ctx, t, client, tablet)
	agentRPCTestPopulateReparentJournalPanic(ctx, t, client, tablet)
	agentRPCTestInitSlavePanic(ctx, t, client, tablet)
	agentRPCTestDemoteMasterPanic(ctx, t, client, tablet)
	agentRPCTestPromoteSlaveWhenCaughtUpPanic(ctx, t, client, tablet)
	agentRPCTestSlaveWasPromotedPanic(ctx, t, client, tablet)
	agentRPCTestSetMasterPanic(ctx, t, client, tablet)
	agentRPCTestSlaveWasRestartedPanic(ctx, t, client, tablet)
	agentRPCTestStopReplicationAndGetStatusPanic(ctx, t, client, tablet)
	agentRPCTestPromoteSlavePanic(ctx, t, client, tablet)

	// Backup / restore related methods
	agentRPCTestBackupPanic(ctx, t, client, tablet)
	agentRPCTestRestoreFromBackupPanic(ctx, t, client, tablet)

	client.Close()
}
