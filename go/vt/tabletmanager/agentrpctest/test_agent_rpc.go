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
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
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

//
// Various read-only methods
//

func (fra *fakeRpcAgent) Ping(args string) string {
	return args
}

func agentRpcTestPing(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.Ping(ti, time.Minute)
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
			Columns:           []string{},
			PrimaryKeyColumns: []string{},
			Type:              myproto.TABLE_BASE_TABLE,
			DataLength:        12,
			RowCount:          6,
		},
	},
	Version: "xxx",
}

func (fra *fakeRpcAgent) GetSchema(tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	compare(fra.t, "GetSchema tables", tables, testGetSchemaTables)
	compare(fra.t, "GetSchema excludeTables", excludeTables, testGetSchemaExcludeTables)
	compareBool(fra.t, "GetSchema includeViews", includeViews)
	return testGetSchemaReply, nil
}

func agentRpcTestGetSchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	result, err := client.GetSchema(ti, testGetSchemaTables, testGetSchemaExcludeTables, true, time.Minute)
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

func (fra *fakeRpcAgent) GetPermissions() (*myproto.Permissions, error) {
	return testGetPermissionsReply, nil
}

func agentRpcTestGetPermissions(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	result, err := client.GetPermissions(ti, time.Minute)
	compareError(t, "GetPermissions", err, result, testGetPermissionsReply)
}

//
// Various read-write methods
//

var testSetReadOnlyExpectedValue bool

func (fra *fakeRpcAgent) SetReadOnly(rdonly bool) error {
	if rdonly != testSetReadOnlyExpectedValue {
		fra.t.Errorf("Wrong SetReadOnly value: got %v expected %v", rdonly, testSetReadOnlyExpectedValue)
	}
	return nil
}

func agentRpcTestSetReadOnly(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	testSetReadOnlyExpectedValue = true
	err := client.SetReadOnly(ti, time.Minute)
	if err != nil {
		t.Errorf("SetReadOnly failed: %v", err)
	}
	testSetReadOnlyExpectedValue = false
	err = client.SetReadWrite(ti, time.Minute)
	if err != nil {
		t.Errorf("SetReadWrite failed: %v", err)
	}
}

var testChangeTypeValue = topo.TYPE_REPLICA

func (fra *fakeRpcAgent) ChangeType(tabletType topo.TabletType) error {
	compare(fra.t, "ChangeType tabletType", tabletType, testChangeTypeValue)
	return nil
}

func agentRpcTestChangeType(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.ChangeType(ti, testChangeTypeValue, time.Minute)
	if err != nil {
		t.Errorf("ChangeType failed: %v", err)
	}
}

var testScrapError = fmt.Errorf("Scrap Failed!")

func (fra *fakeRpcAgent) Scrap() error {
	return testScrapError
}

func agentRpcTestScrap(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.Scrap(ti, time.Minute)
	if strings.Index(err.Error(), testScrapError.Error()) == -1 {
		t.Errorf("Unexpected Scrap result: got %v expected %v", err, testScrapError)
	}
}

var testSleepDuration = time.Minute

func (fra *fakeRpcAgent) Sleep(duration time.Duration) {
	compare(fra.t, "Sleep duration", duration, testSleepDuration)
}

func agentRpcTestSleep(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.Sleep(ti, testSleepDuration, time.Minute)
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

func (fra *fakeRpcAgent) ExecuteHook(hk *hook.Hook) *hook.HookResult {
	compare(fra.t, "ExecuteHook hook", hk, testExecuteHookHook)
	return testExecuteHookHookResult
}

func agentRpcTestExecuteHook(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	hr, err := client.ExecuteHook(ti, testExecuteHookHook, time.Minute)
	compareError(t, "ExecuteHook", err, hr, testExecuteHookHookResult)
}

var testReloadSchemaCalled = false

func (fra *fakeRpcAgent) ReloadSchema() {
	if testReloadSchemaCalled {
		fra.t.Errorf("ReloadSchema called multiple times?")
	}
	testReloadSchemaCalled = true
}

func agentRpcTestReloadSchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.ReloadSchema(ti, time.Minute)
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

func (fra *fakeRpcAgent) PreflightSchema(change string) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "PreflightSchema result", change, testPreflightSchema)
	return testSchemaChangeResult, nil
}

func agentRpcTestPreflightSchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	scr, err := client.PreflightSchema(ti, testPreflightSchema, time.Minute)
	compareError(t, "PreflightSchema", err, scr, testSchemaChangeResult)
}

var testSchemaChange = &myproto.SchemaChange{
	Sql:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRpcAgent) ApplySchema(change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	compare(fra.t, "ApplySchema change", change, testSchemaChange)
	return testSchemaChangeResult, nil
}

func agentRpcTestApplySchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	scr, err := client.ApplySchema(ti, testSchemaChange, time.Minute)
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

func (fra *fakeRpcAgent) ExecuteFetch(query string, maxrows int, wantFields, disableBinlogs bool) (*mproto.QueryResult, error) {
	compare(fra.t, "ExecuteFetch query", query, testExecuteFetchQuery)
	compare(fra.t, "ExecuteFetch maxrows", maxrows, testExecuteFetchMaxRows)
	compareBool(fra.t, "ExecuteFetch wantFields", wantFields)
	compareBool(fra.t, "ExecuteFetch disableBinlogs", disableBinlogs)
	return testExecuteFetchResult, nil
}

func agentRpcTestExecuteFetch(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	qr, err := client.ExecuteFetch(ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true, time.Minute)
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

func (fra *fakeRpcAgent) SlaveStatus() (*myproto.ReplicationStatus, error) {
	return testReplicationStatus, nil
}

func agentRpcTestSlaveStatus(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rs, err := client.SlaveStatus(ti, time.Minute)
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

func (fra *fakeRpcAgent) WaitSlavePosition(position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "WaitSlavePosition position", position, testReplicationPosition)
	compare(fra.t, "WaitSlavePosition waitTimeout", waitTimeout, testWaitSlavePositionWaitTimeout)
	return testReplicationStatus, nil
}

func agentRpcTestWaitSlavePosition(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rs, err := client.WaitSlavePosition(ti, testReplicationPosition, testWaitSlavePositionWaitTimeout)
	compareError(t, "WaitSlavePosition", err, rs, testReplicationStatus)
}

func (fra *fakeRpcAgent) MasterPosition() (myproto.ReplicationPosition, error) {
	return testReplicationPosition, nil
}

func agentRpcTestMasterPosition(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rs, err := client.MasterPosition(ti, time.Minute)
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

func (fra *fakeRpcAgent) ReparentPosition(rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	compare(fra.t, "ReparentPosition position", rp.GTIDSet, testReplicationPosition.GTIDSet)
	return testRestartSlaveData, nil
}

func agentRpcTestReparentPosition(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rsd, err := client.ReparentPosition(ti, &testReplicationPosition, time.Minute)
	compareError(t, "ReparentPosition", err, rsd, testRestartSlaveData)
}

var testStopSlaveCalled = false

func (fra *fakeRpcAgent) StopSlave() error {
	testStopSlaveCalled = true
	return nil
}

func agentRpcTestStopSlave(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.StopSlave(ti, time.Minute)
	compareError(t, "StopSlave", err, true, testStopSlaveCalled)
}

var testStopSlaveMinimumWaitTime = time.Hour

func (fra *fakeRpcAgent) StopSlaveMinimum(position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	compare(fra.t, "StopSlaveMinimum position", position.GTIDSet, testReplicationPosition.GTIDSet)
	compare(fra.t, "StopSlaveMinimum waitTime", waitTime, testStopSlaveMinimumWaitTime)
	return testReplicationStatus, nil
}

func agentRpcTestStopSlaveMinimum(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rs, err := client.StopSlaveMinimum(ti, testReplicationPosition, testStopSlaveMinimumWaitTime)
	compareError(t, "StopSlave", err, rs, testReplicationStatus)
}

var testStartSlaveCalled = false

func (fra *fakeRpcAgent) StartSlave() error {
	testStartSlaveCalled = true
	return nil
}

func agentRpcTestStartSlave(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.StartSlave(ti, time.Minute)
	compareError(t, "StartSlave", err, true, testStartSlaveCalled)
}

var testTabletExternallyReparentedActionTimeout = 30 * time.Second
var testTabletExternallyReparentedCalled = false

func (fra *fakeRpcAgent) TabletExternallyReparented(actionTimeout time.Duration) error {
	compare(fra.t, "TabletExternallyReparented actionTimeout", actionTimeout, testTabletExternallyReparentedActionTimeout)
	testTabletExternallyReparentedCalled = true
	return nil
}

func agentRpcTestTabletExternallyReparented(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.TabletExternallyReparented(ti, testTabletExternallyReparentedActionTimeout)
	compareError(t, "TabletExternallyReparented", err, true, testTabletExternallyReparentedCalled)
}

var testGetSlavesResult = []string{"slave1", "slave2"}

func (fra *fakeRpcAgent) GetSlaves() ([]string, error) {
	return testGetSlavesResult, nil
}

func agentRpcTestGetSlaves(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	s, err := client.GetSlaves(ti, time.Minute)
	compareError(t, "GetSlaves", err, s, testGetSlavesResult)
}

var testBlpPosition = &blproto.BlpPosition{
	Uid:      73,
	Position: testReplicationPosition,
}
var testWaitBlpPositionWaitTime = time.Hour
var testWaitBlpPositionCalled = false

func (fra *fakeRpcAgent) WaitBlpPosition(blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	compare(fra.t, "WaitBlpPosition blpPosition", blpPosition, testBlpPosition)
	compare(fra.t, "WaitBlpPosition waitTime", waitTime, testWaitBlpPositionWaitTime)
	testWaitBlpPositionCalled = true
	return nil
}

func agentRpcTestWaitBlpPosition(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.WaitBlpPosition(ti, *testBlpPosition, testWaitBlpPositionWaitTime)
	compareError(t, "WaitBlpPosition", err, true, testWaitBlpPositionCalled)
}

var testBlpPositionList = &blproto.BlpPositionList{
	Entries: []blproto.BlpPosition{
		*testBlpPosition,
	},
}

func (fra *fakeRpcAgent) StopBlp() (*blproto.BlpPositionList, error) {
	return testBlpPositionList, nil
}

func agentRpcTestStopBlp(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	bpl, err := client.StopBlp(ti, time.Minute)
	compareError(t, "StopBlp", err, bpl, testBlpPositionList)
}

var testStartBlpCalled = false

func (fra *fakeRpcAgent) StartBlp() error {
	testStartBlpCalled = true
	return nil
}

func agentRpcTestStartBlp(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	err := client.StartBlp(ti, time.Minute)
	compareError(t, "StartBlp", err, true, testStartBlpCalled)
}

var testRunBlpUntilWaitTime = 3 * time.Minute

func (fra *fakeRpcAgent) RunBlpUntil(bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	compare(fra.t, "RunBlpUntil bpl", bpl, testBlpPositionList)
	compare(fra.t, "RunBlpUntil waitTime", waitTime, testRunBlpUntilWaitTime)
	return &testReplicationPosition, nil
}

func agentRpcTestRunBlpUntil(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	rp, err := client.RunBlpUntil(ti, testBlpPositionList, testRunBlpUntilWaitTime)
	compareError(t, "RunBlpUntil", err, rp, testReplicationPosition)
}

//
// Reparenting related functions
//

func (fra *fakeRpcAgent) DemoteMaster() error {
	return nil
}

func agentRpcTestDemoteMaster(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) PromoteSlave() (*actionnode.RestartSlaveData, error) {
	return nil, nil
}

func agentRpcTestPromoteSlave(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) SlaveWasPromoted() error {
	return nil
}

func agentRpcTestSlaveWasPromoted(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) RestartSlave(rsd *actionnode.RestartSlaveData) error {
	return nil
}

func agentRpcTestRestartSlave(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) SlaveWasRestarted(swrd *actionnode.SlaveWasRestartedArgs) error {
	return nil
}

func agentRpcTestSlaveWasRestarted(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) BreakSlaves() error {
	return nil
}

func agentRpcTestBreakSlaves(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

//
// Backup / restore related methods
//

var testLogString = "test log"

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

func (fra *fakeRpcAgent) Snapshot(args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	if !reflect.DeepEqual(args, testSnapshotArgs) {
		fra.t.Errorf("Unexpected SnapshotArgs: got %v expected %v", *args, *testSnapshotArgs)
	}

	logger.Infof(testLogString)
	var result = *testSnapshotReply
	return &result, nil
}

func agentRpcTestSnapshot(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	// Snapshot
	args := *testSnapshotArgs
	logChannel, errFunc := client.Snapshot(ti, &args, time.Minute)
	le := <-logChannel
	if le.Value != testLogString {
		t.Errorf("Unexpected log response: got %v expected %v", le.Value, testLogString)
	}
	le, ok := <-logChannel
	if ok {
		t.Fatalf("log channel wasn't closed")
	}
	sr, err := errFunc()
	if err != nil {
		t.Errorf("Snapshot failed: %v", err)
	} else {
		if !reflect.DeepEqual(sr, testSnapshotReply) {
			t.Errorf("Unexpected SnapshotReply: got %v expected %v", *sr, *testSnapshotReply)
		}
	}
}

func (fra *fakeRpcAgent) SnapshotSourceEnd(args *actionnode.SnapshotSourceEndArgs) error {
	return nil
}

func agentRpcTestSnapshotSourceEnd(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) ReserveForRestore(args *actionnode.ReserveForRestoreArgs) error {
	return nil
}

func agentRpcTestReserveForRestore(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) Restore(args *actionnode.RestoreArgs, logger logutil.Logger) error {
	return nil
}

func agentRpcTestRestore(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) MultiSnapshot(args *actionnode.MultiSnapshotArgs, logger logutil.Logger) (*actionnode.MultiSnapshotReply, error) {
	return nil, nil
}

func agentRpcTestMultiSnapshot(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

func (fra *fakeRpcAgent) MultiRestore(args *actionnode.MultiRestoreArgs, logger logutil.Logger) error {
	return nil
}

func agentRpcTestMultiRestore(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
}

//
// RPC helpers
//

// RpcWrap is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrap(from, name string, args, reply interface{}, f func() error) error {
	return f()
}

// RpcWrapLock is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrapLock(from, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// RpcWrapLockAction is part of the RpcAgent interface
func (fra *fakeRpcAgent) RpcWrapLockAction(from, name string, args, reply interface{}, verbose bool, f func() error) error {
	return f()
}

// methods to test individual API calls

// AgentRpcTestSuite will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RpcAgent (returned
// by NewFakeRpcAgent)
func AgentRpcTestSuite(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	// Various read-only methods
	agentRpcTestPing(t, client, ti)
	agentRpcTestGetSchema(t, client, ti)
	agentRpcTestGetPermissions(t, client, ti)

	// Various read-write methods
	agentRpcTestSetReadOnly(t, client, ti)
	agentRpcTestChangeType(t, client, ti)
	agentRpcTestScrap(t, client, ti)
	agentRpcTestSleep(t, client, ti)
	agentRpcTestExecuteHook(t, client, ti)
	agentRpcTestReloadSchema(t, client, ti)
	agentRpcTestPreflightSchema(t, client, ti)
	agentRpcTestApplySchema(t, client, ti)
	agentRpcTestExecuteFetch(t, client, ti)

	// Replication related methods
	agentRpcTestSlaveStatus(t, client, ti)
	agentRpcTestWaitSlavePosition(t, client, ti)
	agentRpcTestMasterPosition(t, client, ti)
	agentRpcTestReparentPosition(t, client, ti)
	agentRpcTestStopSlave(t, client, ti)
	agentRpcTestStopSlaveMinimum(t, client, ti)
	agentRpcTestStartSlave(t, client, ti)
	agentRpcTestTabletExternallyReparented(t, client, ti)
	agentRpcTestGetSlaves(t, client, ti)
	agentRpcTestWaitBlpPosition(t, client, ti)
	agentRpcTestStopBlp(t, client, ti)
	agentRpcTestStartBlp(t, client, ti)
	agentRpcTestRunBlpUntil(t, client, ti)

	// Reparenting related functions
	agentRpcTestDemoteMaster(t, client, ti)
	agentRpcTestPromoteSlave(t, client, ti)
	agentRpcTestSlaveWasPromoted(t, client, ti)
	agentRpcTestRestartSlave(t, client, ti)
	agentRpcTestSlaveWasRestarted(t, client, ti)
	agentRpcTestBreakSlaves(t, client, ti)

	// Backup / restore related methods
	agentRpcTestSnapshot(t, client, ti)
	agentRpcTestSnapshotSourceEnd(t, client, ti)
	agentRpcTestReserveForRestore(t, client, ti)
	agentRpcTestRestore(t, client, ti)
	agentRpcTestMultiSnapshot(t, client, ti)
	agentRpcTestMultiRestore(t, client, ti)
}
