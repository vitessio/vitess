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

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// fakeRPCAgent implements tabletmanager.RPCAgent and fills in all
// possible values in all APIs
type fakeRPCAgent struct {
	t *testing.T
}

// NewFakeRPCAgent returns a fake tabletmanager.RPCAgent that's just a mirror.
func NewFakeRPCAgent(t *testing.T) tabletmanager.RPCAgent {
	return &fakeRPCAgent{t}
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

func (fra *fakeRPCAgent) Ping(ctx context.Context, args string) string {
	return args
}

func agentRPCTestPing(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Ping(ctx, ti)
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

func (fra *fakeRPCAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	compare(fra.t, "GetSchema tables", tables, testGetSchemaTables)
	compare(fra.t, "GetSchema excludeTables", excludeTables, testGetSchemaExcludeTables)
	compareBool(fra.t, "GetSchema includeViews", includeViews)
	return testGetSchemaReply, nil
}

func agentRPCTestGetSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetSchema(ctx, ti, testGetSchemaTables, testGetSchemaExcludeTables, true)
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

func (fra *fakeRPCAgent) GetPermissions(ctx context.Context) (*myproto.Permissions, error) {
	return testGetPermissionsReply, nil
}

func agentRPCTestGetPermissions(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	result, err := client.GetPermissions(ctx, ti)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

//
// Various read-write methods
//

var testSetReadOnlyExpectedValue bool

func (fra *fakeRPCAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
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

var testChangeTypeValue = topo.TYPE_REPLICA

func (fra *fakeRPCAgent) ChangeType(ctx context.Context, tabletType topo.TabletType) error {
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func agentRPCTestChangeType(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ChangeType(ctx, ti, testChangeTypeValue)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

var errTestScrap = fmt.Errorf("Scrap Failed!")

func (fra *fakeRPCAgent) Scrap(ctx context.Context) error {
	return errTestScrap
}

func agentRPCTestScrap(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Scrap(ctx, ti)
	if strings.Index(err.Error(), errTestScrap.Error()) == -1 {
		t.Errorf("Unexpected Scrap result: got %v expected %v", err, errTestScrap)
	}
}

var testSleepDuration = time.Minute

func (fra *fakeRPCAgent) Sleep(ctx context.Context, duration time.Duration) {
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func agentRPCTestSleep(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.Sleep(ctx, ti, testSleepDuration)
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

func (fra *fakeRPCAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	compare(fra.t, "ExecuteHook hook", hk, testExecuteHookHook)
	return testExecuteHookHookResult
}

func agentRPCTestExecuteHook(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	hr, err := client.ExecuteHook(ctx, ti, testExecuteHookHook)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

var testRefreshStateCalled = false

func (fra *fakeRPCAgent) RefreshState(ctx context.Context) {
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

var testRunHealthCheckValue = topo.TYPE_RDONLY

func (fra *fakeRPCAgent) RunHealthCheck(ctx context.Context, targetTabletType topo.TabletType) {
	compare(fra.t, "RunHealthCheck tabletType", targetTabletType, testRunHealthCheckValue)
}

func agentRPCTestRunHealthCheck(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RunHealthCheck(ctx, ti, testRunHealthCheckValue)
	if err != nil {
		t.Errorf("RunHealthCheck failed: %v", err)
	}
}

// this test is a bit of a hack: we write something on the channel
// upon registration, and we also return an error, so the streaming query
// ends right there. Otherwise we have no real way to trigger a real
// communication error, that ends the streaming.
var testHealthStreamHealthStreamReply = &actionnode.HealthStreamReply{
	Tablet: &topo.Tablet{
		Alias: topo.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Hostname: "host",
		IPAddr:   "1.2.3.4",
		Portmap: map[string]int{
			"vt": 2345,
		},
		Tags: map[string]string{
			"tag1": "value1",
		},
		Health: map[string]string{
			"health1": "value1",
		},
		Keyspace:       "keyspace",
		Shard:          "shard",
		Type:           topo.TYPE_MASTER,
		State:          topo.STATE_READ_WRITE,
		DbNameOverride: "overruled!",
	},
	BinlogPlayerMapSize: 3,
	HealthError:         "bad rep bad",
	ReplicationDelay:    50 * time.Second,
}
var testRegisterHealthStreamError = "to trigger a server error"

func (fra *fakeRPCAgent) RegisterHealthStream(c chan<- *actionnode.HealthStreamReply) (int, error) {
	c <- testHealthStreamHealthStreamReply
	return 0, fmt.Errorf(testRegisterHealthStreamError)
}

func (fra *fakeRPCAgent) UnregisterHealthStream(int) error {
	return nil
}

func agentRPCTestHealthStream(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	c, errFunc, err := client.HealthStream(ctx, ti)
	if err != nil {
		t.Fatalf("HealthStream failed: %v", err)
	}
	// channel should have one response, then closed
	hsr, ok := <-c
	if !ok {
		t.Fatalf("HealthStream got no response")
	}
	_, ok = <-c
	if ok {
		t.Fatalf("HealthStream wasn't closed")
	}
	err = errFunc()
	if !strings.Contains(err.Error(), testRegisterHealthStreamError) {
		t.Fatalf("HealthStream failed with the wrong error: %v", err)
	}
	compareError(t, "HealthStream", nil, *hsr, *testHealthStreamHealthStreamReply)
}

var testReloadSchemaCalled = false

func (fra *fakeRPCAgent) ReloadSchema(ctx context.Context) {
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

var testPreflightSchema = "change table add table cloth"
var testSchemaChangeResult = &myproto.SchemaChangeResult{
	BeforeSchema: testGetSchemaReply,
	AfterSchema:  testGetSchemaReply,
}

func (fra *fakeRPCAgent) PreflightSchema(ctx context.Context, change string) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "PreflightSchema result", change, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func agentRPCTestPreflightSchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.PreflightSchema(ctx, ti, testPreflightSchema)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

var testSchemaChange = &myproto.SchemaChange{
	Sql:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRPCAgent) ApplySchema(ctx context.Context, change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "ApplySchema change", change, testSchemaChange)
	return testSchemaChangeResult, nil
}

func agentRPCTestApplySchema(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	scr, err := client.ApplySchema(ctx, ti, testSchemaChange)
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
var testExecuteFetchDbConfigName dbconfigs.DbConfigName

func (fra *fakeRPCAgent) ExecuteFetch(ctx context.Context, query string, maxrows int, wantFields, disableBinlogs bool, dbconfigName dbconfigs.DbConfigName) (*mproto.QueryResult, error) {
	compare(fra.t, "ExecuteFetch query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetch maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetch wantFields", wantFields)
	compare(fra.t, "ExecuteFetch dbconfigName", dbconfigName, testExecuteFetchDbConfigName)
	switch dbconfigName {
	case dbconfigs.DbaConfigName:
		compareBool(fra.t, "ExecuteFetch disableBinlogs", disableBinlogs)
	case dbconfigs.AppConfigName:
		compare(fra.t, "ExecuteFetch disableBinlogs", disableBinlogs, false)
	}
	return testExecuteFetchResult, nil
}

func agentRPCTestExecuteFetch(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	testExecuteFetchDbConfigName = dbconfigs.DbaConfigName
	qr, err := client.ExecuteFetchAsDba(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true)
	compareError(t, "ExecuteFetch", err, qr, testExecuteFetchResult)
	testExecuteFetchDbConfigName = dbconfigs.AppConfigName
	qr, err = client.ExecuteFetchAsApp(ctx, ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true)
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

func (fra *fakeRPCAgent) SlaveStatus(ctx context.Context) (*myproto.ReplicationStatus, error) {
	return testReplicationStatus, nil
}

func agentRPCTestSlaveStatus(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.SlaveStatus(ctx, ti)
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

func (fra *fakeRPCAgent) WaitSlavePosition(ctx context.Context, position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "WaitSlavePosition position", position, testReplicationPosition)
	compare(fra.t, "WaitSlavePosition waitTimeout", waitTimeout, testWaitSlavePositionWaitTimeout)
	return testReplicationStatus, nil
}

func agentRPCTestWaitSlavePosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.WaitSlavePosition(ctx, ti, testReplicationPosition, testWaitSlavePositionWaitTimeout)
	compareError(t, "WaitSlavePosition", err, rs, testReplicationStatus)
}

func (fra *fakeRPCAgent) MasterPosition(ctx context.Context) (myproto.ReplicationPosition, error) {
	return testReplicationPosition, nil
}

func agentRPCTestMasterPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.MasterPosition(ctx, ti)
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

func (fra *fakeRPCAgent) ReparentPosition(ctx context.Context, rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	compare(fra.t, "ReparentPosition position", rp.GTIDSet, testReplicationPosition.GTIDSet)
	return testRestartSlaveData, nil
}

func agentRPCTestReparentPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rsd, err := client.ReparentPosition(ctx, ti, &testReplicationPosition)
	compareError(t, "ReparentPosition", err, rsd, testRestartSlaveData)
}

var testStopSlaveCalled = false

func (fra *fakeRPCAgent) StopSlave(ctx context.Context) error {
	testStopSlaveCalled = true
	return nil
}

func agentRPCTestStopSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StopSlave(ctx, ti)
	compareError(t, "StopSlave", err, true, testStopSlaveCalled)
}

var testStopSlaveMinimumWaitTime = time.Hour

func (fra *fakeRPCAgent) StopSlaveMinimum(ctx context.Context, position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "StopSlaveMinimum position", position.GTIDSet, testReplicationPosition.GTIDSet)
	compare(fra.t, "StopSlaveMinimum waitTime", waitTime, testStopSlaveMinimumWaitTime)
	return testReplicationStatus, nil
}

func agentRPCTestStopSlaveMinimum(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rs, err := client.StopSlaveMinimum(ctx, ti, testReplicationPosition, testStopSlaveMinimumWaitTime)
	compareError(t, "StopSlave", err, rs, testReplicationStatus)
}

var testStartSlaveCalled = false

func (fra *fakeRPCAgent) StartSlave(ctx context.Context) error {
	testStartSlaveCalled = true
	return nil
}

func agentRPCTestStartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartSlave(ctx, ti)
	compareError(t, "StartSlave", err, true, testStartSlaveCalled)
}

var testTabletExternallyReparentedCalled = false

func (fra *fakeRPCAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	testTabletExternallyReparentedCalled = true
	return nil
}

func agentRPCTestTabletExternallyReparented(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.TabletExternallyReparented(ctx, ti, "")
	compareError(t, "TabletExternallyReparented", err, true, testTabletExternallyReparentedCalled)
}

var testGetSlavesResult = []string{"slave1", "slave2"}

func (fra *fakeRPCAgent) GetSlaves(ctx context.Context) ([]string, error) {
	return testGetSlavesResult, nil
}

func agentRPCTestGetSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	s, err := client.GetSlaves(ctx, ti)
	compareError(t, "GetSlaves", err, s, testGetSlavesResult)
}

var testBlpPosition = &blproto.BlpPosition{
	Uid:      73,
	Position: testReplicationPosition,
}
var testWaitBlpPositionWaitTime = time.Hour
var testWaitBlpPositionCalled = false

func (fra *fakeRPCAgent) WaitBlpPosition(ctx context.Context, blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	compare(fra.t, "WaitBlpPosition blpPosition", blpPosition, testBlpPosition)
	compare(fra.t, "WaitBlpPosition waitTime", waitTime, testWaitBlpPositionWaitTime)
	testWaitBlpPositionCalled = true
	return nil
}

func agentRPCTestWaitBlpPosition(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.WaitBlpPosition(ctx, ti, *testBlpPosition, testWaitBlpPositionWaitTime)
	compareError(t, "WaitBlpPosition", err, true, testWaitBlpPositionCalled)
}

var testBlpPositionList = &blproto.BlpPositionList{
	Entries: []blproto.BlpPosition{
		*testBlpPosition,
	},
}

func (fra *fakeRPCAgent) StopBlp(ctx context.Context) (*blproto.BlpPositionList, error) {
	return testBlpPositionList, nil
}

func agentRPCTestStopBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	bpl, err := client.StopBlp(ctx, ti)
	compareError(t, "StopBlp", err, bpl, testBlpPositionList)
}

var testStartBlpCalled = false

func (fra *fakeRPCAgent) StartBlp(ctx context.Context) error {
	testStartBlpCalled = true
	return nil
}

func agentRPCTestStartBlp(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.StartBlp(ctx, ti)
	compareError(t, "StartBlp", err, true, testStartBlpCalled)
}

var testRunBlpUntilWaitTime = 3 * time.Minute

func (fra *fakeRPCAgent) RunBlpUntil(ctx context.Context, bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	compare(fra.t, "RunBlpUntil bpl", bpl, testBlpPositionList)
	compare(fra.t, "RunBlpUntil waitTime", waitTime, testRunBlpUntilWaitTime)
	return &testReplicationPosition, nil
}

func agentRPCTestRunBlpUntil(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rp, err := client.RunBlpUntil(ctx, ti, testBlpPositionList, testRunBlpUntilWaitTime)
	compareError(t, "RunBlpUntil", err, rp, testReplicationPosition)
}

//
// Reparenting related functions
//

var testDemoteMasterCalled = false

func (fra *fakeRPCAgent) DemoteMaster(ctx context.Context) error {
	testDemoteMasterCalled = true
	return nil
}

func agentRPCTestDemoteMaster(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.DemoteMaster(ctx, ti)
	compareError(t, "DemoteMaster", err, true, testDemoteMasterCalled)
}

func (fra *fakeRPCAgent) PromoteSlave(ctx context.Context) (*actionnode.RestartSlaveData, error) {
	return testRestartSlaveData, nil
}

func agentRPCTestPromoteSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	rsd, err := client.PromoteSlave(ctx, ti)
	compareError(t, "PromoteSlave", err, rsd, testRestartSlaveData)
}

var testSlaveWasPromotedCalled = false

func (fra *fakeRPCAgent) SlaveWasPromoted(ctx context.Context) error {
	testSlaveWasPromotedCalled = true
	return nil
}

func agentRPCTestSlaveWasPromoted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasPromoted(ctx, ti)
	compareError(t, "SlaveWasPromoted", err, true, testSlaveWasPromotedCalled)
}

var testRestartSlaveCalled = false

func (fra *fakeRPCAgent) RestartSlave(ctx context.Context, rsd *actionnode.RestartSlaveData) error {
	compare(fra.t, "RestartSlave rsd", rsd, testRestartSlaveData)
	testRestartSlaveCalled = true
	return nil
}

func agentRPCTestRestartSlave(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.RestartSlave(ctx, ti, testRestartSlaveData)
	compareError(t, "RestartSlave", err, true, testRestartSlaveCalled)
}

var testSlaveWasRestartedArgs = &actionnode.SlaveWasRestartedArgs{
	Parent: topo.TabletAlias{
		Cell: "prison",
		Uid:  42,
	},
}
var testSlaveWasRestartedCalled = false

func (fra *fakeRPCAgent) SlaveWasRestarted(ctx context.Context, swrd *actionnode.SlaveWasRestartedArgs) error {
	compare(fra.t, "SlaveWasRestarted swrd", swrd, testSlaveWasRestartedArgs)
	testSlaveWasRestartedCalled = true
	return nil
}

func agentRPCTestSlaveWasRestarted(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SlaveWasRestarted(ctx, ti, testSlaveWasRestartedArgs)
	compareError(t, "RestartSlave", err, true, testRestartSlaveCalled)
}

var testBreakSlavesCalled = false

func (fra *fakeRPCAgent) BreakSlaves(ctx context.Context) error {
	testBreakSlavesCalled = true
	return nil
}

func agentRPCTestBreakSlaves(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.BreakSlaves(ctx, ti)
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

func (fra *fakeRPCAgent) Snapshot(ctx context.Context, args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	compare(fra.t, "Snapshot args", args, testSnapshotArgs)
	logStuff(logger, 0)
	return testSnapshotReply, nil
}

func agentRPCTestSnapshot(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Snapshot(ctx, ti, testSnapshotArgs)
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

func (fra *fakeRPCAgent) SnapshotSourceEnd(ctx context.Context, args *actionnode.SnapshotSourceEndArgs) error {
	compare(fra.t, "SnapshotSourceEnd args", args, testSnapshotSourceEndArgs)
	testSnapshotSourceEndCalled = true
	return nil
}

func agentRPCTestSnapshotSourceEnd(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.SnapshotSourceEnd(ctx, ti, testSnapshotSourceEndArgs)
	compareError(t, "SnapshotSourceEnd", err, true, testSnapshotSourceEndCalled)
}

var testReserveForRestoreArgs = &actionnode.ReserveForRestoreArgs{
	SrcTabletAlias: topo.TabletAlias{
		Cell: "test",
		Uid:  456,
	},
}
var testReserveForRestoreCalled = false

func (fra *fakeRPCAgent) ReserveForRestore(ctx context.Context, args *actionnode.ReserveForRestoreArgs) error {
	compare(fra.t, "ReserveForRestore args", args, testReserveForRestoreArgs)
	testReserveForRestoreCalled = true
	return nil
}

func agentRPCTestReserveForRestore(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	err := client.ReserveForRestore(ctx, ti, testReserveForRestoreArgs)
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

func (fra *fakeRPCAgent) Restore(ctx context.Context, args *actionnode.RestoreArgs, logger logutil.Logger) error {
	compare(fra.t, "Restore args", args, testRestoreArgs)
	logStuff(logger, 10)
	testRestoreCalled = true
	return nil
}

func agentRPCTestRestore(ctx context.Context, t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	logChannel, errFunc, err := client.Restore(ctx, ti, testRestoreArgs)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	compareLoggedStuff(t, "Restore", logChannel, 10)
	err = errFunc()
	compareError(t, "Restore", err, true, testRestoreCalled)
}

//
// RPC helpers
//

// RPCWrap is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrap(ctx context.Context, name string, args, reply interface{}, f func() error) error {
	return f()
}

// RPCWrapLock is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrapLock(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// RPCWrapLockAction is part of the RPCAgent interface
func (fra *fakeRPCAgent) RPCWrapLockAction(ctx context.Context, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// methods to test individual API calls

// Run will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RPCAgent (returned
// by NewFakeRPCAgent)
func Run(t *testing.T, client tmclient.TabletManagerClient, ti *topo.TabletInfo) {
	ctx := context.Background()

	// Various read-only methods
	agentRPCTestPing(ctx, t, client, ti)
	agentRPCTestGetSchema(ctx, t, client, ti)
	agentRPCTestGetPermissions(ctx, t, client, ti)

	// Various read-write methods
	agentRPCTestSetReadOnly(ctx, t, client, ti)
	agentRPCTestChangeType(ctx, t, client, ti)
	agentRPCTestScrap(ctx, t, client, ti)
	agentRPCTestSleep(ctx, t, client, ti)
	agentRPCTestExecuteHook(ctx, t, client, ti)
	agentRPCTestRefreshState(ctx, t, client, ti)
	agentRPCTestRunHealthCheck(ctx, t, client, ti)
	agentRPCTestHealthStream(ctx, t, client, ti)
	agentRPCTestReloadSchema(ctx, t, client, ti)
	agentRPCTestPreflightSchema(ctx, t, client, ti)
	agentRPCTestApplySchema(ctx, t, client, ti)
	agentRPCTestExecuteFetch(ctx, t, client, ti)

	// Replication related methods
	agentRPCTestSlaveStatus(ctx, t, client, ti)
	agentRPCTestWaitSlavePosition(ctx, t, client, ti)
	agentRPCTestMasterPosition(ctx, t, client, ti)
	agentRPCTestReparentPosition(ctx, t, client, ti)
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
	agentRPCTestDemoteMaster(ctx, t, client, ti)
	agentRPCTestPromoteSlave(ctx, t, client, ti)
	agentRPCTestSlaveWasPromoted(ctx, t, client, ti)
	agentRPCTestRestartSlave(ctx, t, client, ti)
	agentRPCTestSlaveWasRestarted(ctx, t, client, ti)
	agentRPCTestBreakSlaves(ctx, t, client, ti)

	// Backup / restore related methods
	agentRPCTestSnapshot(ctx, t, client, ti)
	agentRPCTestSnapshotSourceEnd(ctx, t, client, ti)
	agentRPCTestReserveForRestore(ctx, t, client, ti)
	agentRPCTestRestore(ctx, t, client, ti)
}
