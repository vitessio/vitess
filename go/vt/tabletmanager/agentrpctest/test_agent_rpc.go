// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package agentrpctest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
)

// fakeRpcAgent implements tabletmanager.RpcAgent and fills in all
// possible values in all APIs
type fakeRpcAgent struct {
	t *testing.T
}

// NewFakeRpcAgent returns a fake tabletmanager.RpcAgent that's just a mirror.
func NewFakeRpcAgent(t *testing.T) tabletmanager.RpcAgent {
	return &fakeRpcAgent{t}
}

// The way this test is organized is a repetition of:
// - static test data for a call
// - implementation of the tabletmanager.RpcAgent method for fakeRpcAgent
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

func compareLoggedStuff(t *testing.T, name string, logChannel <-chan *logutil.LoggerEvent, count int) {
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

//
// Various read-only methods
//

func (fra *fakeRpcAgent) Ping(ctx context.Context, args string) string {
	return args
}

func agentRpcTestPing(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Ping(ctx, ti, time.Minute)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

var testGetSchemaTables = []string{"table1", "table2"}
var testGetSchemaExcludeTables = []string{"etable1", "etable2", "etable3"}
var testGetSchemaReply = &myproto.SchemaDefinition{
	DatabaseSchema: "CREATE DATABASE {{.DatabaseName}}",
	TableDefinitions: []*myproto.TableDefinition{
		&myproto.TableDefinition{
			Name:              "table_name",
			Schema:            "create table_name",
			Columns:           []string{"col1", "col2"},
			PrimaryKeyColumns: []string{"col1"},
			Type:              myproto.TABLE_VIEW,
			DataLength:        12,
			RowCount:          6,
		},
		&myproto.TableDefinition{
			Name:              "table_name2",
			Schema:            "create table_name2",
			Columns:           []string{"col1"},
			PrimaryKeyColumns: []string{"col1"},
			Type:              myproto.TABLE_BASE_TABLE,
			DataLength:        12,
			RowCount:          6,
		},
	},
	Version: "xxx",
}

func (fra *fakeRpcAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	compare(fra.t, "GetSchema tables", tables, testGetSchemaTables)
	compare(fra.t, "GetSchema excludeTables", excludeTables, testGetSchemaExcludeTables)
	compareBool(fra.t, "GetSchema includeViews", includeViews)
	return testGetSchemaReply, nil
}

func agentRpcTestGetSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetSchema(ctx, ti, testGetSchemaTables, testGetSchemaExcludeTables, true, time.Minute)
	compareError(t, "GetSchema", err, result, testGetSchemaReply)
}

var testGetPermissionsReply = &myproto.Permissions{
	UserPermissions: []*myproto.UserPermission{
		&myproto.UserPermission{
			Host:             "host1",
			User:             "user1",
			PasswordChecksum: 666,
			Privileges: map[string]string{
				"create": "yes",
				"delete": "no",
			},
		},
	},
	DbPermissions: []*myproto.DbPermission{
		&myproto.DbPermission{
			Host: "host2",
			Db:   "db1",
			User: "user2",
			Privileges: map[string]string{
				"create": "no",
				"delete": "yes",
			},
		},
	},
	HostPermissions: []*myproto.HostPermission{
		&myproto.HostPermission{
			Host: "host3",
			Db:   "db2",
			Privileges: map[string]string{
				"create": "maybe",
				"delete": "whynot",
			},
		},
	},
}

func (fra *fakeRpcAgent) GetPermissions(ctx context.Context) (*myproto.Permissions, error) {
	return testGetPermissionsReply, nil
}

func agentRpcTestGetPermissions(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetPermissions(ctx, ti, time.Minute)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

//
// Various read-write methods
//

var testSetReadOnlyExpectedValue bool

func (fra *fakeRpcAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
	if rdonly != testSetReadOnlyExpectedValue {
		fra.t.Errorf("Wrong SetReadOnly value: got %v expected %v", rdonly, testSetReadOnlyExpectedValue)
	}
	return nil
}

func agentRpcTestSetReadOnly(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	testSetReadOnlyExpectedValue = true
	err := client.SetReadOnly(ctx, ti, time.Minute)
	if err != nil {
		t.Errorf("SetReadOnly failed: %v", err)
	}
	testSetReadOnlyExpectedValue = false
	err = client.SetReadWrite(ctx, ti, time.Minute)
	if err != nil {
		t.Errorf("SetReadWrite failed: %v", err)
	}
}

var testChangeTypeValue = topo.TYPE_REPLICA

func (fra *fakeRpcAgent) ChangeType(ctx context.Context, tabletType topo.TabletType) error {
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func agentRpcTestChangeType(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ChangeType(ctx, ti, testChangeTypeValue, time.Minute)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

var testScrapError = fmt.Errorf("Scrap Failed!")

func (fra *fakeRpcAgent) Scrap(ctx context.Context) error {
	return testScrapError
}

func agentRpcTestScrap(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Scrap(ctx, ti, time.Minute)
	if strings.Index(err.Error(), testScrapError.Error()) == -1 {
		t.Errorf("Unexpected Scrap result: got %v expected %v", err, testScrapError)
	}
}

var testSleepDuration = time.Minute

func (fra *fakeRpcAgent) Sleep(ctx context.Context, duration time.Duration) {
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func agentRpcTestSleep(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Sleep(ctx, ti, testSleepDuration, time.Minute)
	if err != nil {
		t.Errorf("Sleep failed: %v", err)
	}
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

func (fra *fakeRpcAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	compare(fra.t, "ExecuteHook hook", hk, testExecuteHookHook)
	return testExecuteHookHookResult
}

func agentRpcTestExecuteHook(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	hr, err := client.ExecuteHook(ctx, ti, testExecuteHookHook, time.Minute)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

var testRefreshStateCalled = false

func (fra *fakeRpcAgent) RefreshState(ctx context.Context) {
	if testRefreshStateCalled {
		fra.t.Errorf("RefreshState called multiple times?")
	}
	testRefreshStateCalled = true
}

func agentRpcTestRefreshState(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RefreshState(ctx, ti, time.Minute)
	if err != nil {
		t.Errorf("RefreshState failed: %v", err)
	}
	if !testRefreshStateCalled {
		t.Errorf("RefreshState didn't call the server side")
	}
}

var testRunHealthCheckValue = topo.TYPE_RDONLY

func (fra *fakeRpcAgent) RunHealthCheck(ctx context.Context, targetTabletType topo.TabletType) {
	compare(fra.t, "RunHealthCheck tabletType", targetTabletType, testRunHealthCheckValue)
}

func agentRpcTestRunHealthCheck(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RunHealthCheck(ctx, ti, testRunHealthCheckValue, time.Minute)
	if err != nil {
		t.Errorf("RunHealthCheck failed: %v", err)
	}
}

var testReloadSchemaCalled = false

func (fra *fakeRpcAgent) ReloadSchema(ctx context.Context) {
	if testReloadSchemaCalled {
		fra.t.Errorf("ReloadSchema called multiple times?")
	}
	testReloadSchemaCalled = true
}

func agentRpcTestReloadSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ReloadSchema(ctx, ti, time.Minute)
	if err != nil {
		t.Errorf("ReloadSchema failed: %v", err)
	}
	if !testReloadSchemaCalled {
		t.Errorf("ReloadSchema didn't call the server side")
	}
}

var testPreflightSchema = "change table add table cloth"
var testSchemaChangeResult = &myproto.SchemaChangeResult{
	BeforeSchema: testGetSchemaReply,
	AfterSchema:  testGetSchemaReply,
}

func (fra *fakeRpcAgent) PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "PreflightSchema result", change, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func agentRpcTestPreflightSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.PreflightSchema(ctx, ti, testPreflightSchema, time.Minute)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

var testSchemaChange = &myproto.SchemaChange{
	Sql:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRpcAgent) ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "ApplySchema change", change, testSchemaChange)
	return testSchemaChangeResult, nil
}

func agentRpcTestApplySchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.ApplySchema(ctx, ti, testSchemaChange, time.Minute)
	compareError(t, "ApplySchema", err, scr, testSchemaChangeResult)
}

var testExecuteFetchQuery = "fetch this"
var testExecuteFetchMaxRows = 100
var testExecuteFetchResult = &mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "column1",
			Type: mproto.VT_TINY_BLOB,
		},
		mproto.Field{
			Name: "column2",
			Type: mproto.VT_TIMESTAMP,
		},
	},
	RowsAffected: 10,
	InsertId:     32,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("ABC")),
		},
	},
}

func (fra *fakeRpcAgent) ExecuteFetch(ctx context.Context, query string, maxrows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	compare(fra.t, "ExecuteFetch query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetch maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetch wantFields", wantFields)
	compareBool(fra.t, "ExecuteFetch disableBinlogs", disableBinlogs)
	return testExecuteFetchResult, nil
}

func agentRpcTestExecuteFetch(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	qr, err := client.ExecuteFetch(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true, time.Minute)
	compareError(t, "ExecuteFetch", err, qr, testExecuteFetchResult)
}

//
// Replication related methods
//

var testReplicationStatus = &myproto.ReplicationStatus{
	Position: myproto.ReplicationPosition{
		GTIDSet: myproto.GoogleGTID{
			ServerID: 345,
			GroupID:  789,
		},
	},
	SlaveIORunning:      true,
	SlaveSQLRunning:     true,
	SecondsBehindMaster: 654,
	MasterHost:          "master.host",
	MasterPort:          3366,
	MasterConnectRetry:  12,
}

func (fra *fakeRpcAgent) SlaveStatus(ctx context.Context) (*myproto.ReplicationStatus, error) {
	return testReplicationStatus, nil
}

func agentRpcTestSlaveStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.SlaveStatus(ctx, ti, time.Minute)
	compareError(t, "SlaveStatus", err, rs, testReplicationStatus)
}

var testReplicationPosition = myproto.ReplicationPosition{
	GTIDSet: myproto.MariadbGTID{
		Domain:   5,
		Server:   456,
		Sequence: 890,
	},
}
var testWaitSlavePositionWaitTimeout = time.Hour

func (fra *fakeRpcAgent) WaitSlavePosition(ctx context.Context, position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "WaitSlavePosition position", position, testReplicationPosition)
	compare(fra.t, "WaitSlavePosition waitTimeout", waitTimeout, testWaitSlavePositionWaitTimeout)
	return testReplicationStatus, nil
}

func agentRpcTestWaitSlavePosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.WaitSlavePosition(ctx, ti, testReplicationPosition, testWaitSlavePositionWaitTimeout)
	compareError(t, "WaitSlavePosition", err, rs, testReplicationStatus)
}

func (fra *fakeRpcAgent) MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error) {
	return testReplicationPosition, nil
}

func agentRpcTestMasterPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.MasterPosition(ctx, ti, time.Minute)
	compareError(t, "MasterPosition", err, rs, testReplicationPosition)
}

var testRestartSlaveData = &actionnode.RestartSlaveData{
	ReplicationStatus: testReplicationStatus,
	WaitPosition:      testReplicationPosition,
	TimePromoted:      0x7000000000000000,
	Parent: topo.TabletAlias{
		Cell: "ce",
		Uid:  372,
	},
	Force: true,
}

func (fra *fakeRpcAgent) ReparentPosition(ctx context.Context, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	compare(fra.t, "ReparentPosition position", rp.GTIDSet, testReplicationPosition.GTIDSet)
	return testRestartSlaveData, nil
}

func agentRpcTestReparentPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rsd, err := client.ReparentPosition(ctx, ti, &testReplicationPosition, time.Minute)
	compareError(t, "ReparentPosition", err, rsd, testRestartSlaveData)
}

var testStopSlaveCalled = false

func (fra *fakeRpcAgent) StopSlave(ctx context.Context) error {
	testStopSlaveCalled = true
	return nil
}

func agentRpcTestStopSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StopSlave(ctx, ti, time.Minute)
	compareError(t, "StopSlave", err, true, testStopSlaveCalled)
}

var testStopSlaveMinimumWaitTime = time.Hour

func (fra *fakeRpcAgent) StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "StopSlaveMinimum position", position.GTIDSet, testReplicationPosition.GTIDSet)
	compare(fra.t, "StopSlaveMinimum waitTime", waitTime, testStopSlaveMinimumWaitTime)
	return testReplicationStatus, nil
}

func agentRpcTestStopSlaveMinimum(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.StopSlaveMinimum(ctx, ti, testReplicationPosition, testStopSlaveMinimumWaitTime)
	compareError(t, "StopSlave", err, rs, testReplicationStatus)
}

var testStartSlaveCalled = false

func (fra *fakeRpcAgent) StartSlave(ctx context.Context) error {
	testStartSlaveCalled = true
	return nil
}

func agentRpcTestStartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartSlave(ctx, ti, time.Minute)
	compareError(t, "StartSlave", err, true, testStartSlaveCalled)
}

var testTabletExternallyReparentedCalled = false

func (fra *fakeRpcAgent) TabletExternallyReparented(ctx context.Context, actionTimeout time.Duration) error {
	testTabletExternallyReparentedCalled = true
	return nil
}

func agentRpcTestTabletExternallyReparented(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.TabletExternallyReparented(ctx, ti, time.Minute)
	compareError(t, "TabletExternallyReparented", err, true, testTabletExternallyReparentedCalled)
}

var testGetSlavesResult = []string{"slave1", "slave2"}

func (fra *fakeRpcAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return testGetSlavesResult, nil
}

func agentRpcTestGetSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	s, err := client.GetSlaves(ctx, ti, time.Minute)
	compareError(t, "GetSlaves", err, s, testGetSlavesResult)
}

var testBlpPosition = &blproto.BlpPosition{
	Uid:      73,
	Position: testReplicationPosition,
}
var testWaitBlpPositionWaitTime = time.Hour
var testWaitBlpPositionCalled = false

func (fra *fakeRpcAgent) WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	compare(fra.t, "WaitBlpPosition blpPosition", blpPosition, testBlpPosition)
	compare(fra.t, "WaitBlpPosition waitTime", waitTime, testWaitBlpPositionWaitTime)
	testWaitBlpPositionCalled = true
	return nil
}

func agentRpcTestWaitBlpPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.WaitBlpPosition(ctx, ti, *testBlpPosition, testWaitBlpPositionWaitTime)
	compareError(t, "WaitBlpPosition", err, true, testWaitBlpPositionCalled)
}

var testBlpPositionList = &blproto.BlpPositionList{
	Entries: []blproto.BlpPosition{
		*testBlpPosition,
	},
}

func (fra *fakeRpcAgent) StopBlp(ctx context.Context) (*blproto.BlpPositionList, error) {
	return testBlpPositionList, nil
}

func agentRpcTestStopBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	bpl, err := client.StopBlp(ctx, ti, time.Minute)
	compareError(t, "StopBlp", err, bpl, testBlpPositionList)
}

var testStartBlpCalled = false

func (fra *fakeRpcAgent) StartBlp(ctx context.Context) error {
	testStartBlpCalled = true
	return nil
}

func agentRpcTestStartBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartBlp(ctx, ti, time.Minute)
	compareError(t, "StartBlp", err, true, testStartBlpCalled)
}

var testRunBlpUntilWaitTime = 3 * time.Minute

func (fra *fakeRpcAgent) RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	compare(fra.t, "RunBlpUntil bpl", bpl, testBlpPositionList)
	compare(fra.t, "RunBlpUntil waitTime", waitTime, testRunBlpUntilWaitTime)
	return &testReplicationPosition, nil
}

func agentRpcTestRunBlpUntil(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.RunBlpUntil(ctx, ti, testBlpPositionList, testRunBlpUntilWaitTime)
	compareError(t, "RunBlpUntil", err, rp, testReplicationPosition)
}

//
// Reparenting related functions
//

var testDemoteMasterCalled = false

func (fra *fakeRpcAgent) DemoteMaster(ctx context.Context) error {
	testDemoteMasterCalled = true
	return nil
}

func agentRpcTestDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.DemoteMaster(ctx, ti, time.Minute)
	compareError(t, "DemoteMaster", err, true, testDemoteMasterCalled)
}

func (fra *fakeRpcAgent) PromoteSlave(ctx context.Context) (*actionnode.RestartSlaveData, error) {
	return testRestartSlaveData, nil
}

func agentRpcTestPromoteSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rsd, err := client.PromoteSlave(ctx, ti, time.Minute)
	compareError(t, "PromoteSlave", err, rsd, testRestartSlaveData)
}

var testSlaveWasPromotedCalled = false

func (fra *fakeRpcAgent) SlaveWasPromoted(ctx context.Context) error {
	testSlaveWasPromotedCalled = true
	return nil
}

func agentRpcTestSlaveWasPromoted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasPromoted(ctx, ti, time.Minute)
	compareError(t, "SlaveWasPromoted", err, true, testSlaveWasPromotedCalled)
}

var testRestartSlaveCalled = false

func (fra *fakeRpcAgent) RestartSlave(ctx context.Context, rsd *actionnode.RestartSlaveData) error {
	compare(fra.t, "RestartSlave rsd", rsd, testRestartSlaveData)
	testRestartSlaveCalled = true
	return nil
}

func agentRpcTestRestartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RestartSlave(ctx, ti, testRestartSlaveData, time.Minute)
	compareError(t, "RestartSlave", err, true, testRestartSlaveCalled)
}

var testSlaveWasRestartedArgs = &actionnode.SlaveWasRestartedArgs{
	Parent: topo.TabletAlias{
		Cell: "prison",
		Uid:  42,
	},
}
var testSlaveWasRestartedCalled = false

func (fra *fakeRpcAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	compare(fra.t, "SlaveWasRestarted swrd", swrd, testSlaveWasRestartedArgs)
	testSlaveWasRestartedCalled = true
	return nil
}

func agentRpcTestSlaveWasRestarted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasRestarted(ctx, ti, testSlaveWasRestartedArgs, time.Minute)
	compareError(t, "RestartSlave", err, true, testRestartSlaveCalled)
}

var testBreakSlavesCalled = false

func (fra *fakeRpcAgent) BreakSlaves(ctx context.Context) error {
	testBreakSlavesCalled = true
	return nil
}

func agentRpcTestBreakSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.BreakSlaves(ctx, ti, time.Minute)
	compareError(t, "BreakSlaves", err, true, testBreakSlavesCalled)
}

//
// Backup / restore related methods
//

var testSnapshotArgs = &actionnode.SnapshotArgs{
	Concurrency:         42,
	ServerMode:          true,
	ForceMasterSnapshot: true,
}
var testSnapshotReply = &actionnode.SnapshotReply{
	ParentAlias: topo.TabletAlias{
		Cell: "test",
		Uid:  456,
	},
	ManifestPath:       "path",
	SlaveStartRequired: true,
	ReadOnly:           true,
}

func (fra *fakeRpcAgent) Snapshot(ctx context.Context, args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	compare(fra.t, "Snapshot args", args, testSnapshotArgs)
	logStuff(logger, 0)
	return testSnapshotReply, nil
}

func agentRpcTestSnapshot(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Snapshot(ctx, ti, testSnapshotArgs, time.Minute)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	compareLoggedStuff(t, "Snapshot", logChannel, 0)
	sr, err := errFunc()
	compareError(t, "Snapshot", err, sr, testSnapshotReply)
}

var testSnapshotSourceEndArgs = &actionnode.SnapshotSourceEndArgs{
	SlaveStartRequired: true,
	ReadOnly:           true,
	OriginalType:       topo.TYPE_RDONLY,
}
var testSnapshotSourceEndCalled = false

func (fra *fakeRpcAgent) SnapshotSourceEnd(ctx context.Context, args *actionnode.SnapshotSourceEndArgs) error {
	compare(fra.t, "SnapshotSourceEnd args", args, testSnapshotSourceEndArgs)
	testSnapshotSourceEndCalled = true
	return nil
}

func agentRpcTestSnapshotSourceEnd(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SnapshotSourceEnd(ctx, ti, testSnapshotSourceEndArgs, time.Minute)
	compareError(t, "SnapshotSourceEnd", err, true, testSnapshotSourceEndCalled)
}

var testReserveForRestoreArgs = &actionnode.ReserveForRestoreArgs{
	SrcTabletAlias: topo.TabletAlias{
		Cell: "test",
		Uid:  456,
	},
}
var testReserveForRestoreCalled = false

func (fra *fakeRpcAgent) ReserveForRestore(ctx context.Context, args *actionnode.ReserveForRestoreArgs) error {
	compare(fra.t, "ReserveForRestore args", args, testReserveForRestoreArgs)
	testReserveForRestoreCalled = true
	return nil
}

func agentRpcTestReserveForRestore(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ReserveForRestore(ctx, ti, testReserveForRestoreArgs, time.Minute)
	compareError(t, "ReserveForRestore", err, true, testReserveForRestoreCalled)
}

var testRestoreArgs = &actionnode.RestoreArgs{
	SrcTabletAlias: topo.TabletAlias{
		Cell: "jail1",
		Uid:  890,
	},
	SrcFilePath: "source",
	ParentAlias: topo.TabletAlias{
		Cell: "jail2",
		Uid:  901,
	},
	FetchConcurrency:      12,
	FetchRetryCount:       678,
	WasReserved:           true,
	DontWaitForSlaveStart: true,
}
var testRestoreCalled = false

func (fra *fakeRpcAgent) Restore(ctx context.Context, args *actionnode.RestoreArgs, logger logutil.Logger) error {
	compare(fra.t, "Restore args", args, testRestoreArgs)
	logStuff(logger, 10)
	testRestoreCalled = true
	return nil
}

func agentRpcTestRestore(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Restore(ctx, ti, testRestoreArgs, time.Minute)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	compareLoggedStuff(t, "Restore", logChannel, 10)
	err = errFunc()
	compareError(t, "Restore", err, true, testRestoreCalled)
}

var testMultiSnapshotArgs = &actionnode.MultiSnapshotArgs{
	KeyRanges: []key.KeyRange{
		key.KeyRange{
			Start: "",
			End:   "",
		},
	},
	Tables:           []string{"table1", "table2"},
	ExcludeTables:    []string{"etable1", "etable2"},
	Concurrency:      34,
	SkipSlaveRestart: true,
	MaximumFilesize:  0x2000,
}
var testMultiSnapshotReply = &actionnode.MultiSnapshotReply{
	ParentAlias: topo.TabletAlias{
		Cell: "test",
		Uid:  4567,
	},
	ManifestPaths: []string{"path1", "path2"},
}

func (fra *fakeRpcAgent) MultiSnapshot(ctx context.Context, args *actionnode.MultiSnapshotArgs, logger logutil.Logger) (*actionnode.MultiSnapshotReply, error) {
	compare(fra.t, "MultiSnapshot args", args, testMultiSnapshotArgs)
	logStuff(logger, 100)
	return testMultiSnapshotReply, nil
}

func agentRpcTestMultiSnapshot(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.MultiSnapshot(ctx, ti, testMultiSnapshotArgs, time.Minute)
	if err != nil {
		t.Fatalf("MultiSnapshot failed: %v", err)
	}
	compareLoggedStuff(t, "MultiSnapshot", logChannel, 100)
	sr, err := errFunc()
	compareError(t, "MultiSnapshot", err, sr, testMultiSnapshotReply)
}

var testMultiRestoreArgs = &actionnode.MultiRestoreArgs{
	SrcTabletAliases: []topo.TabletAlias{
		topo.TabletAlias{
			Cell: "jail1",
			Uid:  8902,
		},
		topo.TabletAlias{
			Cell: "jail2",
			Uid:  8901,
		},
	},
	Concurrency:            124,
	FetchConcurrency:       162,
	InsertTableConcurrency: 6178,
	FetchRetryCount:        887,
	Strategy:               "cool one",
}
var testMultiRestoreCalled = false

func (fra *fakeRpcAgent) MultiRestore(ctx context.Context, args *actionnode.MultiRestoreArgs, logger logutil.Logger) error {
	compare(fra.t, "MultiRestore args", args, testMultiRestoreArgs)
	logStuff(logger, 1000)
	testMultiRestoreCalled = true
	return nil
}

func agentRpcTestMultiRestore(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.MultiRestore(ctx, ti, testMultiRestoreArgs, time.Minute)
	if err != nil {
		t.Fatalf("MultiRestore failed: %v", err)
	}
	compareLoggedStuff(t, "MultiRestore", logChannel, 1000)
	err = errFunc()
	compareError(t, "MultiRestore", err, true, testMultiRestoreCalled)
}

//
// RPC helpers
//

// RpcWrap is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrap(ctx context.Context, name string, args, reply interface{}, f func() error) error {
	return f()
}

// RpcWrapLock is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrapLock(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// RpcWrapLockAction is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrapLockAction(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// methods to test individual API calls

// AgentRpcTestSuite will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RpcAgent (returned
// by NewFakeRpcAgent)
func AgentRpcTestSuite(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	// Various read-only methods
	agentRpcTestPing(ctx, t, client, ti)
	agentRpcTestGetSchema(ctx, t, client, ti)
	agentRpcTestGetPermissions(ctx, t, client, ti)

	// Various read-write methods
	agentRpcTestSetReadOnly(ctx, t, client, ti)
	agentRpcTestChangeType(ctx, t, client, ti)
	agentRpcTestScrap(ctx, t, client, ti)
	agentRpcTestSleep(ctx, t, client, ti)
	agentRpcTestExecuteHook(ctx, t, client, ti)
	agentRpcTestRefreshState(ctx, t, client, ti)
	agentRpcTestRunHealthCheck(ctx, t, client, ti)
	agentRpcTestReloadSchema(ctx, t, client, ti)
	agentRpcTestPreflightSchema(ctx, t, client, ti)
	agentRpcTestApplySchema(ctx, t, client, ti)
	agentRpcTestExecuteFetch(ctx, t, client, ti)

	// Replication related methods
	agentRpcTestSlaveStatus(ctx, t, client, ti)
	agentRpcTestWaitSlavePosition(ctx, t, client, ti)
	agentRpcTestMasterPosition(ctx, t, client, ti)
	agentRpcTestReparentPosition(ctx, t, client, ti)
	agentRpcTestStopSlave(ctx, t, client, ti)
	agentRpcTestStopSlaveMinimum(ctx, t, client, ti)
	agentRpcTestStartSlave(ctx, t, client, ti)
	agentRpcTestTabletExternallyReparented(ctx, t, client, ti)
	agentRpcTestGetSlaves(ctx, t, client, ti)
	agentRpcTestWaitBlpPosition(ctx, t, client, ti)
	agentRpcTestStopBlp(ctx, t, client, ti)
	agentRpcTestStartBlp(ctx, t, client, ti)
	agentRpcTestRunBlpUntil(ctx, t, client, ti)

	// Reparenting related functions
	agentRpcTestDemoteMaster(ctx, t, client, ti)
	agentRpcTestPromoteSlave(ctx, t, client, ti)
	agentRpcTestSlaveWasPromoted(ctx, t, client, ti)
	agentRpcTestRestartSlave(ctx, t, client, ti)
	agentRpcTestSlaveWasRestarted(ctx, t, client, ti)
	agentRpcTestBreakSlaves(ctx, t, client, ti)

	// Backup / restore related methods
	agentRpcTestSnapshot(ctx, t, client, ti)
	agentRpcTestSnapshotSourceEnd(ctx, t, client, ti)
	agentRpcTestReserveForRestore(ctx, t, client, ti)
	agentRpcTestRestore(ctx, t, client, ti)
	agentRpcTestMultiSnapshot(ctx, t, client, ti)
	agentRpcTestMultiRestore(ctx, t, client, ti)
}
