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

package tmrpctest

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"context"

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

// fakeRPCTM implements tabletmanager.RPCTM and fills in all
// possible values in all APIs
type fakeRPCTM struct {
	t      *testing.T
	panics bool
	// slow if true will let Ping() sleep and effectively not respond to an RPC.
	slow bool
	// mu guards accesses of "slow".
	mu sync.Mutex
}

func (fra *fakeRPCTM) LockTables(ctx context.Context) error {
	panic("implement me")
}

func (fra *fakeRPCTM) UnlockTables(ctx context.Context) error {
	panic("implement me")
}

func (fra *fakeRPCTM) setSlow(slow bool) {
	fra.mu.Lock()
	fra.slow = slow
	fra.mu.Unlock()
}

// NewFakeRPCTM returns a fake tabletmanager.RPCTM that's just a mirror.
func NewFakeRPCTM(t *testing.T) tabletmanager.RPCTM {
	return &fakeRPCTM{
		t: t,
	}
}

// The way this test is organized is a repetition of:
// - static test data for a call
// - implementation of the tabletmanager.RPCTM method for fakeRPCTM
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

func (fra *fakeRPCTM) Ping(ctx context.Context, args string) string {
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

func tmRPCTestPing(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Ping(ctx, tablet)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func tmRPCTestPingPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Ping(ctx, tablet)
	expectHandleRPCPanic(t, "Ping", false /*verbose*/, err)
}

// tmRPCTestDialExpiredContext verifies that
// the context returns the right DeadlineExceeded Err() for
// RPCs failed due to an expired context before .Dial().
func tmRPCTestDialExpiredContext(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	// Using a timeout of 0 here such that .Dial() will fail immediately.
	expiredCtx, cancel := context.WithTimeout(ctx, 0)
	defer cancel()
	err := client.Ping(expiredCtx, tablet)
	if err == nil {
		t.Fatal("tmRPCTestDialExpiredContext: RPC with expired context did not fail")
	}
	// The context was already expired when we created it. Here we only verify that it returns the expected error.
	select {
	case <-expiredCtx.Done():
		if err := expiredCtx.Err(); err != context.DeadlineExceeded {
			t.Errorf("tmRPCTestDialExpiredContext: got %v want context.DeadlineExceeded", err)
		}
	default:
		t.Errorf("tmRPCTestDialExpiredContext: context.Done() not closed")
	}
}

// tmRPCTestRPCTimeout verifies that
// the context returns the right DeadlineExceeded Err() for
// RPCs failed due to an expired context during execution.
func tmRPCTestRPCTimeout(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet, fakeTM *fakeRPCTM) {
	// We must use a timeout > 0 such that the context deadline hasn't expired
	// yet in grpctmclient.Client.dial().
	// NOTE: This might still race e.g. when test execution takes too long the
	//       context will be expired in dial() already. In such cases coverage
	//       will be reduced but the test will not flake.
	shortCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	fakeTM.setSlow(true)
	defer func() { fakeTM.setSlow(false) }()
	err := client.Ping(shortCtx, tablet)
	if err == nil {
		t.Fatal("tmRPCTestRPCTimeout: RPC with expired context did not fail")
	}
	select {
	case <-shortCtx.Done():
		if err := shortCtx.Err(); err != context.DeadlineExceeded {
			t.Errorf("tmRPCTestRPCTimeout: got %v want context.DeadlineExceeded", err)
		}
	default:
		t.Errorf("tmRPCTestRPCTimeout: context.Done() not closed")
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

func (fra *fakeRPCTM) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "GetSchema tables", tables, testGetSchemaTables)
	compare(fra.t, "GetSchema excludeTables", excludeTables, testGetSchemaExcludeTables)
	compareBool(fra.t, "GetSchema includeViews", includeViews)
	return testGetSchemaReply, nil
}

func tmRPCTestGetSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	result, err := client.GetSchema(ctx, tablet, testGetSchemaTables, testGetSchemaExcludeTables, true)
	compareError(t, "GetSchema", err, result, testGetSchemaReply)
}

func tmRPCTestGetSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testGetPermissionsReply, nil
}

func tmRPCTestGetPermissions(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	result, err := client.GetPermissions(ctx, tablet)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

func tmRPCTestGetPermissionsPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.GetPermissions(ctx, tablet)
	expectHandleRPCPanic(t, "GetPermissions", false /*verbose*/, err)
}

//
// Various read-write methods
//

var testSetReadOnlyExpectedValue bool

func (fra *fakeRPCTM) SetReadOnly(ctx context.Context, rdonly bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if rdonly != testSetReadOnlyExpectedValue {
		fra.t.Errorf("Wrong SetReadOnly value: got %v expected %v", rdonly, testSetReadOnlyExpectedValue)
	}
	return nil
}

func tmRPCTestSetReadOnly(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func tmRPCTestSetReadOnlyPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetReadOnly(ctx, tablet)
	expectHandleRPCPanic(t, "SetReadOnly", true /*verbose*/, err)
	err = client.SetReadWrite(ctx, tablet)
	expectHandleRPCPanic(t, "SetReadWrite", true /*verbose*/, err)
}

var testChangeTypeValue = topodatapb.TabletType_REPLICA

func (fra *fakeRPCTM) ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func tmRPCTestChangeType(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ChangeType(ctx, tablet, testChangeTypeValue)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

func tmRPCTestChangeTypePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ChangeType(ctx, tablet, testChangeTypeValue)
	expectHandleRPCPanic(t, "ChangeType", true /*verbose*/, err)
}

var testSleepDuration = time.Minute

func (fra *fakeRPCTM) Sleep(ctx context.Context, duration time.Duration) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func tmRPCTestSleep(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.Sleep(ctx, tablet, testSleepDuration)
	if err != nil {
		t.Errorf("Sleep failed: %v", err)
	}
}

func tmRPCTestSleepPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteHook hook", hk, testExecuteHookHook)
	return testExecuteHookHookResult
}

func tmRPCTestExecuteHook(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	hr, err := client.ExecuteHook(ctx, tablet, testExecuteHookHook)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

func tmRPCTestExecuteHookPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.ExecuteHook(ctx, tablet, testExecuteHookHook)
	expectHandleRPCPanic(t, "ExecuteHook", true /*verbose*/, err)
}

var testRefreshStateCalled = false

func (fra *fakeRPCTM) RefreshState(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testRefreshStateCalled {
		fra.t.Errorf("RefreshState called multiple times?")
	}
	testRefreshStateCalled = true
	return nil
}

func tmRPCTestRefreshState(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RefreshState(ctx, tablet)
	if err != nil {
		t.Errorf("RefreshState failed: %v", err)
	}
	if !testRefreshStateCalled {
		t.Errorf("RefreshState didn't call the server side")
	}
}

func tmRPCTestRefreshStatePanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RefreshState(ctx, tablet)
	expectHandleRPCPanic(t, "RefreshState", true /*verbose*/, err)
}

func (fra *fakeRPCTM) RunHealthCheck(ctx context.Context) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
}

var testIgnoreHealthErrorValue = ".*"

func (fra *fakeRPCTM) IgnoreHealthError(ctx context.Context, pattern string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "IgnoreHealthError pattern", pattern, testIgnoreHealthErrorValue)
	return nil
}

func tmRPCTestRunHealthCheck(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RunHealthCheck(ctx, tablet)
	if err != nil {
		t.Errorf("RunHealthCheck failed: %v", err)
	}
}

func tmRPCTestRunHealthCheckPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.RunHealthCheck(ctx, tablet)
	expectHandleRPCPanic(t, "RunHealthCheck", false /*verbose*/, err)
}

func tmRPCTestIgnoreHealthError(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.IgnoreHealthError(ctx, tablet, testIgnoreHealthErrorValue)
	if err != nil {
		t.Errorf("IgnoreHealthError failed: %v", err)
	}
}

func tmRPCTestIgnoreHealthErrorPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.IgnoreHealthError(ctx, tablet, testIgnoreHealthErrorValue)
	expectHandleRPCPanic(t, "IgnoreHealthError", false /*verbose*/, err)
}

var testReloadSchemaCalled = false

func (fra *fakeRPCTM) ReloadSchema(ctx context.Context, waitPosition string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if testReloadSchemaCalled {
		fra.t.Errorf("ReloadSchema called multiple times?")
	}
	testReloadSchemaCalled = true
	return nil
}

func tmRPCTestReloadSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReloadSchema(ctx, tablet, "")
	if err != nil {
		t.Errorf("ReloadSchema failed: %v", err)
	}
	if !testReloadSchemaCalled {
		t.Errorf("ReloadSchema didn't call the server side")
	}
}

func tmRPCTestReloadSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "PreflightSchema result", changes, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func tmRPCTestPreflightSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	scr, err := client.PreflightSchema(ctx, tablet, testPreflightSchema)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

func tmRPCTestPreflightSchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !change.Equal(testSchemaChange) {
		fra.t.Errorf("Unexpected ApplySchema change:\ngot  %#v\nwant %#v", change, testSchemaChange)
	}
	return testSchemaChangeResult[0], nil
}

func tmRPCTestApplySchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	scr, err := client.ApplySchema(ctx, tablet, testSchemaChange)
	compareError(t, "ApplySchema", err, scr, testSchemaChangeResult[0])
}

func tmRPCTestApplySchemaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) ExecuteFetchAsDba(ctx context.Context, query []byte, dbName string, maxrows int, disableBinlogs bool, reloadSchema bool) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsDba query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsDba maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetchAsDba disableBinlogs", disableBinlogs)
	compareBool(fra.t, "ExecuteFetchAsDba reloadSchema", reloadSchema)

	return testExecuteFetchResult, nil
}

func (fra *fakeRPCTM) ExecuteFetchAsAllPrivs(ctx context.Context, query []byte, dbName string, maxrows int, reloadSchema bool) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsAllPrivs query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsAllPrivs maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetchAsAllPrivs reloadSchema", reloadSchema)

	return testExecuteFetchResult, nil
}

func (fra *fakeRPCTM) ExecuteFetchAsApp(ctx context.Context, query []byte, maxrows int) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ExecuteFetchAsApp query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetchAsApp maxrows", maxrows, testExecuteFetchMaxRows)
	return testExecuteFetchResult, nil
}

func tmRPCTestExecuteFetch(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func tmRPCTestExecuteFetchPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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
	IoThreadRunning:     true,
	SqlThreadRunning:    true,
	SecondsBehindMaster: 654,
	MasterHost:          "master.host",
	MasterPort:          3366,
	MasterConnectRetry:  12,
}

var testMasterStatus = &replicationdatapb.MasterStatus{Position: "MariaDB/1-345-789"}

func (fra *fakeRPCTM) MasterStatus(ctx context.Context) (*replicationdatapb.MasterStatus, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testMasterStatus, nil
}

func (fra *fakeRPCTM) ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationStatus, nil
}

func tmRPCTestReplicationStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rs, err := client.ReplicationStatus(ctx, tablet)
	compareError(t, "ReplicationStatus", err, rs, testReplicationStatus)
}

func tmRPCTestReplicationStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.ReplicationStatus(ctx, tablet)
	expectHandleRPCPanic(t, "ReplicationStatus", false /*verbose*/, err)
}

var testReplicationPosition = "MariaDB/5-456-890"

func (fra *fakeRPCTM) MasterPosition(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func (fra *fakeRPCTM) WaitForPosition(ctx context.Context, pos string) error {
	panic("unimplemented")
}

func tmRPCTestMasterPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rs, err := client.MasterPosition(ctx, tablet)
	compareError(t, "MasterPosition", err, rs, testReplicationPosition)
}

func tmRPCTestMasterPositionPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.MasterPosition(ctx, tablet)
	expectHandleRPCPanic(t, "MasterPosition", false /*verbose*/, err)
}

var testStopReplicationCalled = false

func (fra *fakeRPCTM) StopReplication(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStopReplicationCalled = true
	return nil
}

func tmRPCTestStopReplication(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StopReplication(ctx, tablet)
	compareError(t, "StopReplication", err, true, testStopReplicationCalled)
}

func tmRPCTestStopReplicationPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StopReplication(ctx, tablet)
	expectHandleRPCPanic(t, "StopReplication", true /*verbose*/, err)
}

var testStopReplicationMinimumWaitTime = time.Hour

func (fra *fakeRPCTM) StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "StopReplicationMinimum position", position, testReplicationPosition)
	compare(fra.t, "StopReplicationMinimum waitTime", waitTime, testStopReplicationMinimumWaitTime)
	return testReplicationPositionReturned, nil
}

func tmRPCTestStopReplicationMinimum(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	pos, err := client.StopReplicationMinimum(ctx, tablet, testReplicationPosition, testStopReplicationMinimumWaitTime)
	compareError(t, "StopReplicationMinimum", err, pos, testReplicationPositionReturned)
}

func tmRPCTestStopReplicationMinimumPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.StopReplicationMinimum(ctx, tablet, testReplicationPosition, testStopReplicationMinimumWaitTime)
	expectHandleRPCPanic(t, "StopReplicationMinimum", true /*verbose*/, err)
}

var testStartReplicationCalled = false

func (fra *fakeRPCTM) StartReplication(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartReplicationCalled = true
	return nil
}

func tmRPCTestStartReplication(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartReplication(ctx, tablet)
	compareError(t, "StartReplication", err, true, testStartReplicationCalled)
}

func tmRPCTestStartReplicationPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartReplication(ctx, tablet)
	expectHandleRPCPanic(t, "StartReplication", true /*verbose*/, err)
}

var testStartReplicationUntilAfterCalledWith = ""

func (fra *fakeRPCTM) StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testStartReplicationUntilAfterCalledWith = position
	return nil
}

func tmRPCTestStartReplicationUntilAfter(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.StartReplicationUntilAfter(ctx, tablet, "test-position", time.Minute)
	compareError(t, "StartReplicationUntilAfter", err, "test-position", testStartReplicationUntilAfterCalledWith)
}

var testGetReplicasResult = []string{"replica1", "replica22"}

func (fra *fakeRPCTM) GetReplicas(ctx context.Context) ([]string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testGetReplicasResult, nil
}

func tmRPCTestGetReplicas(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	s, err := client.GetReplicas(ctx, tablet)
	compareError(t, "GetReplicas", err, s, testGetReplicasResult)
}

func tmRPCTestGetReplicasPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.GetReplicas(ctx, tablet)
	expectHandleRPCPanic(t, "GetReplicas", false /*verbose*/, err)
}

func (fra *fakeRPCTM) VExec(ctx context.Context, query, workflow, keyspace string) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "VExec query", query, "query")
	return testExecuteFetchResult, nil
}

var testVRQuery = "query"

func (fra *fakeRPCTM) VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "VReplicationExec query", query, testVRQuery)
	return testExecuteFetchResult, nil
}

func tmRPCTestVReplicationExec(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.VReplicationExec(ctx, tablet, testVRQuery)
	compareError(t, "VReplicationExec", err, rp, testExecuteFetchResult)
}

func tmRPCTestVReplicationExecPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.VReplicationExec(ctx, tablet, testVRQuery)
	expectHandleRPCPanic(t, "VReplicationExec", true /*verbose*/, err)
}

var (
	wfpid  = 3
	wfppos = ""
)

func (fra *fakeRPCTM) VReplicationWaitForPos(ctx context.Context, id int, pos string) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "VReplicationWaitForPos id", id, wfpid)
	compare(fra.t, "VReplicationWaitForPos pos", pos, wfppos)
	return nil
}

func tmRPCTestVReplicationWaitForPos(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.VReplicationWaitForPos(ctx, tablet, wfpid, wfppos)
	compareError(t, "VReplicationWaitForPos", err, true, true)
}

func tmRPCTestVReplicationWaitForPosPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.VReplicationWaitForPos(ctx, tablet, wfpid, wfppos)
	expectHandleRPCPanic(t, "VReplicationWaitForPos", true /*verbose*/, err)
}

//
// Reparenting related functions
//

var testResetReplicationCalled = false

func (fra *fakeRPCTM) ResetReplication(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testResetReplicationCalled = true
	return nil
}

func tmRPCTestResetReplication(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ResetReplication(ctx, tablet)
	compareError(t, "ResetReplication", err, true, testResetReplicationCalled)
}

func tmRPCTestResetReplicationPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ResetReplication(ctx, tablet)
	expectHandleRPCPanic(t, "ResetReplication", true /*verbose*/, err)
}

func (fra *fakeRPCTM) InitMaster(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func tmRPCTestInitMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.InitMaster(ctx, tablet)
	compareError(t, "InitMaster", err, rp, testReplicationPosition)
}

func tmRPCTestInitMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.InitMaster(ctx, tablet)
	expectHandleRPCPanic(t, "InitMaster", true /*verbose*/, err)
}

var testPopulateReparentJournalCalled = false
var testTimeCreatedNS int64 = 4569900
var testWaitPosition = "test wait position"
var testActionName = "TestActionName"
var testMasterAlias = &topodatapb.TabletAlias{
	Cell: "ce",
	Uid:  372,
}

func (fra *fakeRPCTM) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, masterAlias *topodatapb.TabletAlias, position string) error {
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

func tmRPCTestPopulateReparentJournal(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.PopulateReparentJournal(ctx, tablet, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	compareError(t, "PopulateReparentJournal", err, true, testPopulateReparentJournalCalled)
}

func tmRPCTestPopulateReparentJournalPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.PopulateReparentJournal(ctx, tablet, testTimeCreatedNS, testActionName, testMasterAlias, testReplicationPosition)
	expectHandleRPCPanic(t, "PopulateReparentJournal", false /*verbose*/, err)
}

var testInitReplicaCalled = false

func (fra *fakeRPCTM) InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "InitReplica parent", parent, testMasterAlias)
	compare(fra.t, "InitReplica pos", position, testReplicationPosition)
	compare(fra.t, "InitReplica timeCreatedNS", timeCreatedNS, testTimeCreatedNS)
	testInitReplicaCalled = true
	return nil
}

func tmRPCTestInitReplica(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.InitReplica(ctx, tablet, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	compareError(t, "InitReplica", err, true, testInitReplicaCalled)
}

func tmRPCTestInitReplicaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.InitReplica(ctx, tablet, testMasterAlias, testReplicationPosition, testTimeCreatedNS)
	expectHandleRPCPanic(t, "InitReplica", true /*verbose*/, err)
}

func (fra *fakeRPCTM) DemoteMaster(ctx context.Context) (*replicationdatapb.MasterStatus, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testMasterStatus, nil
}

func tmRPCTestDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	masterStatus, err := client.DemoteMaster(ctx, tablet)
	compareError(t, "DemoteMaster", err, masterStatus.Position, testMasterStatus.Position)
}

func tmRPCTestDemoteMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.DemoteMaster(ctx, tablet)
	expectHandleRPCPanic(t, "DemoteMaster", true /*verbose*/, err)
}

var testUndoDemoteMasterCalled = false

func (fra *fakeRPCTM) UndoDemoteMaster(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return nil
}

func tmRPCTestUndoDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.UndoDemoteMaster(ctx, tablet)
	testUndoDemoteMasterCalled = true
	compareError(t, "UndoDemoteMaster", err, true, testUndoDemoteMasterCalled)
}

func tmRPCTestUndoDemoteMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.UndoDemoteMaster(ctx, tablet)
	expectHandleRPCPanic(t, "UndoDemoteMaster", true /*verbose*/, err)
}

var testReplicationPositionReturned = "MariaDB/5-567-3456"

var testReplicaWasPromotedCalled = false

func (fra *fakeRPCTM) ReplicaWasPromoted(ctx context.Context) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	testReplicaWasPromotedCalled = true
	return nil
}

func tmRPCTestReplicaWasPromoted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReplicaWasPromoted(ctx, tablet)
	compareError(t, "ReplicaWasPromoted", err, true, testReplicaWasPromotedCalled)
}

func tmRPCTestReplicaWasPromotedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReplicaWasPromoted(ctx, tablet)
	expectHandleRPCPanic(t, "ReplicaWasPromoted", true /*verbose*/, err)
}

var testSetMasterCalled = false
var testForceStartReplica = true

func (fra *fakeRPCTM) SetMaster(ctx context.Context, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplica bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "SetMaster parent", parent, testMasterAlias)
	compare(fra.t, "SetMaster timeCreatedNS", timeCreatedNS, testTimeCreatedNS)
	compare(fra.t, "SetMaster waitPosition", waitPosition, testWaitPosition)
	compare(fra.t, "SetMaster forceStartReplica", forceStartReplica, testForceStartReplica)
	testSetMasterCalled = true
	return nil
}

func tmRPCTestSetMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetMaster(ctx, tablet, testMasterAlias, testTimeCreatedNS, testWaitPosition, testForceStartReplica)
	compareError(t, "SetMaster", err, true, testSetMasterCalled)
}

func tmRPCTestSetMasterPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.SetMaster(ctx, tablet, testMasterAlias, testTimeCreatedNS, testWaitPosition, testForceStartReplica)
	expectHandleRPCPanic(t, "SetMaster", true /*verbose*/, err)
}

func (fra *fakeRPCTM) StopReplicationAndGetStatus(ctx context.Context, stopReplicationMode replicationdatapb.StopReplicationMode) (tabletmanager.StopReplicationAndGetStatusResponse, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return tabletmanager.StopReplicationAndGetStatusResponse{
		HybridStatus: testReplicationStatus,
		Status: &replicationdatapb.StopReplicationStatus{
			Before: testReplicationStatus,
			After:  testReplicationStatus,
		},
	}, nil
}

var testReplicaWasRestartedParent = &topodatapb.TabletAlias{
	Cell: "prison",
	Uid:  42,
}
var testReplicaWasRestartedCalled = false

func (fra *fakeRPCTM) ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "ReplicaWasRestarted parent", parent, testReplicaWasRestartedParent)
	testReplicaWasRestartedCalled = true
	return nil
}

func tmRPCTestReplicaWasRestarted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReplicaWasRestarted(ctx, tablet, testReplicaWasRestartedParent)
	compareError(t, "ReplicaWasRestarted", err, true, testReplicaWasRestartedCalled)
}

func tmRPCTestReplicaWasRestartedPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	err := client.ReplicaWasRestarted(ctx, tablet, testReplicaWasRestartedParent)
	expectHandleRPCPanic(t, "ReplicaWasRestarted", true /*verbose*/, err)
}

func tmRPCTestStopReplicationAndGetStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, _, err := client.StopReplicationAndGetStatus(ctx, tablet, replicationdatapb.StopReplicationMode_IOANDSQLTHREAD)
	compareError(t, "StopReplicationAndGetStatus", err, rp, testReplicationStatus)
	rp, _, err = client.StopReplicationAndGetStatus(ctx, tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
	compareError(t, "StopReplicationAndGetStatus", err, rp, testReplicationStatus)
}

func tmRPCTestStopReplicationAndGetStatusPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, _, err := client.StopReplicationAndGetStatus(ctx, tablet, replicationdatapb.StopReplicationMode_IOANDSQLTHREAD)
	expectHandleRPCPanic(t, "StopReplicationAndGetStatus", true /*verbose*/, err)
}

func (fra *fakeRPCTM) PromoteReplica(ctx context.Context) (string, error) {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	return testReplicationPosition, nil
}

func tmRPCTestPromoteReplica(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	rp, err := client.PromoteReplica(ctx, tablet)
	compareError(t, "PromoteReplica", err, rp, testReplicationPosition)
}

func tmRPCTestPromoteReplicaPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	_, err := client.PromoteReplica(ctx, tablet)
	expectHandleRPCPanic(t, "PromoteReplica", true /*verbose*/, err)
}

//
// Backup / restore related methods
//

var testBackupConcurrency = 24
var testBackupAllowMaster = false
var testBackupCalled = false
var testRestoreFromBackupCalled = false

func (fra *fakeRPCTM) Backup(ctx context.Context, concurrency int, logger logutil.Logger, allowMaster bool) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	compare(fra.t, "Backup args", concurrency, testBackupConcurrency)
	compare(fra.t, "Backup args", allowMaster, testBackupAllowMaster)
	logStuff(logger, 10)
	testBackupCalled = true
	return nil
}

func tmRPCTestBackup(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.Backup(ctx, tablet, testBackupConcurrency, testBackupAllowMaster)
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	err = compareLoggedStuff(t, "Backup", stream, 10)
	compareError(t, "Backup", err, true, testBackupCalled)
}

func tmRPCTestBackupPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

func (fra *fakeRPCTM) RestoreFromBackup(ctx context.Context, logger logutil.Logger) error {
	if fra.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	logStuff(logger, 10)
	testRestoreFromBackupCalled = true
	return nil
}

func tmRPCTestRestoreFromBackup(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
	stream, err := client.RestoreFromBackup(ctx, tablet)
	if err != nil {
		t.Fatalf("RestoreFromBackup failed: %v", err)
	}
	err = compareLoggedStuff(t, "RestoreFromBackup", stream, 10)
	compareError(t, "RestoreFromBackup", err, true, testRestoreFromBackupCalled)
}

func tmRPCTestRestoreFromBackupPanic(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet) {
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

// HandleRPCPanic is part of the RPCTM interface
func (fra *fakeRPCTM) HandleRPCPanic(ctx context.Context, name string, args, reply interface{}, verbose bool, err *error) {
	if x := recover(); x != nil {
		// Use the panic case to make sure 'name' and 'verbose' are right.
		*err = fmt.Errorf("HandleRPCPanic caught panic during %v with verbose %v", name, verbose)
	}
}

// methods to test individual API calls

// Run will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RPCTM (returned
// by NewFakeRPCTM)
func Run(t *testing.T, client tmclient.TabletManagerClient, tablet *topodatapb.Tablet, fakeTM tabletmanager.RPCTM) {
	ctx := context.Background()

	// Test RPC specific methods of the interface.
	tmRPCTestDialExpiredContext(ctx, t, client, tablet)
	tmRPCTestRPCTimeout(ctx, t, client, tablet, fakeTM.(*fakeRPCTM))

	// Various read-only methods
	tmRPCTestPing(ctx, t, client, tablet)
	tmRPCTestGetSchema(ctx, t, client, tablet)
	tmRPCTestGetPermissions(ctx, t, client, tablet)

	// Various read-write methods
	tmRPCTestSetReadOnly(ctx, t, client, tablet)
	tmRPCTestChangeType(ctx, t, client, tablet)
	tmRPCTestSleep(ctx, t, client, tablet)
	tmRPCTestExecuteHook(ctx, t, client, tablet)
	tmRPCTestRefreshState(ctx, t, client, tablet)
	tmRPCTestRunHealthCheck(ctx, t, client, tablet)
	tmRPCTestIgnoreHealthError(ctx, t, client, tablet)
	tmRPCTestReloadSchema(ctx, t, client, tablet)
	tmRPCTestPreflightSchema(ctx, t, client, tablet)
	tmRPCTestApplySchema(ctx, t, client, tablet)
	tmRPCTestExecuteFetch(ctx, t, client, tablet)

	// Replication related methods
	tmRPCTestMasterPosition(ctx, t, client, tablet)

	tmRPCTestReplicationStatus(ctx, t, client, tablet)
	tmRPCTestMasterPosition(ctx, t, client, tablet)
	tmRPCTestStopReplication(ctx, t, client, tablet)
	tmRPCTestStopReplicationMinimum(ctx, t, client, tablet)
	tmRPCTestStartReplication(ctx, t, client, tablet)
	tmRPCTestStartReplicationUntilAfter(ctx, t, client, tablet)
	tmRPCTestGetReplicas(ctx, t, client, tablet)

	// VReplication methods
	tmRPCTestVReplicationExec(ctx, t, client, tablet)
	tmRPCTestVReplicationWaitForPos(ctx, t, client, tablet)

	// Reparenting related functions
	tmRPCTestResetReplication(ctx, t, client, tablet)
	tmRPCTestInitMaster(ctx, t, client, tablet)
	tmRPCTestPopulateReparentJournal(ctx, t, client, tablet)
	tmRPCTestDemoteMaster(ctx, t, client, tablet)
	tmRPCTestUndoDemoteMaster(ctx, t, client, tablet)
	tmRPCTestSetMaster(ctx, t, client, tablet)
	tmRPCTestStopReplicationAndGetStatus(ctx, t, client, tablet)
	tmRPCTestPromoteReplica(ctx, t, client, tablet)

	tmRPCTestInitReplica(ctx, t, client, tablet)
	tmRPCTestReplicaWasPromoted(ctx, t, client, tablet)
	tmRPCTestReplicaWasRestarted(ctx, t, client, tablet)

	// Backup / restore related methods
	tmRPCTestBackup(ctx, t, client, tablet)
	tmRPCTestRestoreFromBackup(ctx, t, client, tablet)

	//
	// Tests panic handling everywhere now
	//
	fakeTM.(*fakeRPCTM).panics = true

	// Various read-only methods
	tmRPCTestPingPanic(ctx, t, client, tablet)
	tmRPCTestGetSchemaPanic(ctx, t, client, tablet)
	tmRPCTestGetPermissionsPanic(ctx, t, client, tablet)

	// Various read-write methods
	tmRPCTestSetReadOnlyPanic(ctx, t, client, tablet)
	tmRPCTestChangeTypePanic(ctx, t, client, tablet)
	tmRPCTestSleepPanic(ctx, t, client, tablet)
	tmRPCTestExecuteHookPanic(ctx, t, client, tablet)
	tmRPCTestRefreshStatePanic(ctx, t, client, tablet)
	tmRPCTestRunHealthCheckPanic(ctx, t, client, tablet)
	tmRPCTestIgnoreHealthErrorPanic(ctx, t, client, tablet)
	tmRPCTestReloadSchemaPanic(ctx, t, client, tablet)
	tmRPCTestPreflightSchemaPanic(ctx, t, client, tablet)
	tmRPCTestApplySchemaPanic(ctx, t, client, tablet)
	tmRPCTestExecuteFetchPanic(ctx, t, client, tablet)

	// Replication related methods
	tmRPCTestMasterPositionPanic(ctx, t, client, tablet)
	tmRPCTestReplicationStatusPanic(ctx, t, client, tablet)
	tmRPCTestStopReplicationPanic(ctx, t, client, tablet)
	tmRPCTestStopReplicationMinimumPanic(ctx, t, client, tablet)
	tmRPCTestStartReplicationPanic(ctx, t, client, tablet)
	tmRPCTestGetReplicasPanic(ctx, t, client, tablet)
	// VReplication methods
	tmRPCTestVReplicationExecPanic(ctx, t, client, tablet)
	tmRPCTestVReplicationWaitForPosPanic(ctx, t, client, tablet)

	// Reparenting related functions
	tmRPCTestResetReplicationPanic(ctx, t, client, tablet)
	tmRPCTestInitMasterPanic(ctx, t, client, tablet)
	tmRPCTestPopulateReparentJournalPanic(ctx, t, client, tablet)
	tmRPCTestDemoteMasterPanic(ctx, t, client, tablet)
	tmRPCTestUndoDemoteMasterPanic(ctx, t, client, tablet)
	tmRPCTestSetMasterPanic(ctx, t, client, tablet)
	tmRPCTestStopReplicationAndGetStatusPanic(ctx, t, client, tablet)
	tmRPCTestPromoteReplicaPanic(ctx, t, client, tablet)

	tmRPCTestInitReplicaPanic(ctx, t, client, tablet)
	tmRPCTestReplicaWasPromotedPanic(ctx, t, client, tablet)
	tmRPCTestReplicaWasRestartedPanic(ctx, t, client, tablet)
	// Backup / restore related methods
	tmRPCTestBackupPanic(ctx, t, client, tablet)
	tmRPCTestRestoreFromBackupPanic(ctx, t, client, tablet)

	client.Close()
}
