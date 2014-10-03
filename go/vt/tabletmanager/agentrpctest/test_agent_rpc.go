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
	if !reflect.DeepEqual(tables, testGetSchemaTables) {
		fra.t.Errorf("Unexpected GetSchema tables: got %v expected %v", tables, testGetSchemaTables)
	}
	if !reflect.DeepEqual(excludeTables, testGetSchemaExcludeTables) {
		fra.t.Errorf("Unexpected GetSchema excludeTables: got %v expected %v", excludeTables, testGetSchemaExcludeTables)
	}
	if !includeViews {
		fra.t.Errorf("Unexpected GetSchema includeViews: got false expected true")
	}
	return testGetSchemaReply, nil
}

func agentRpcTestGetSchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	result, err := client.GetSchema(ti, testGetSchemaTables, testGetSchemaExcludeTables, true, time.Minute)
	if err != nil {
		t.Errorf("GetSchema failed: %v", err)
	} else {
		if !reflect.DeepEqual(result, testGetSchemaReply) {
			t.Errorf("Unexpected GetSchema result: got %v expected %v", *result, *testGetSchemaReply)
		}
	}
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
	if err != nil {
		t.Errorf("GetPermissions failed: %v", err)
	} else {
		if !reflect.DeepEqual(result, testGetPermissionsReply) {
			t.Errorf("Unexpected GetPermissions result: got %v expected %v", *result, *testGetPermissionsReply)
		}
	}
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
	if tabletType != testChangeTypeValue {
		fra.t.Errorf("Wrong TabletType value: got %v expected %v", tabletType, testChangeTypeValue)
	}
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
	if duration != testSleepDuration {
		fra.t.Errorf("Unexpected Sleep duration: got %v expected %v", duration, testSleepDuration)
	}
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
	if !reflect.DeepEqual(hk, testExecuteHookHook) {
		fra.t.Errorf("Unexpected ExecuteHook hook: got %v expected %v", hk, testExecuteHookHook)
	}
	return testExecuteHookHookResult
}

func agentRpcTestExecuteHook(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	hr, err := client.ExecuteHook(ti, testExecuteHookHook, time.Minute)
	if err != nil {
		t.Errorf("ExecuteHook failed: %v", err)
	} else {
		if !reflect.DeepEqual(hr, testExecuteHookHookResult) {
			t.Errorf("Unexpected ExecuteHook result: got %v expected %v", hr, testExecuteHookHookResult)
		}
	}
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
	if change != testPreflightSchema {
		fra.t.Errorf("Unexpected PreflightSchema result: got %v expected %v", change, testPreflightSchema)
	}
	return testSchemaChangeResult, nil
}

func agentRpcTestPreflightSchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	scr, err := client.PreflightSchema(ti, testPreflightSchema, time.Minute)
	if err != nil {
		t.Errorf("PreflightSchema failed: %v", err)
	} else {
		if !reflect.DeepEqual(scr, testSchemaChangeResult) {
			t.Errorf("Unexpected PreflightSchema result: got %v expected %v", scr, testSchemaChangeResult)
		}
	}
}

var testSchemaChange = &myproto.SchemaChange{
	Sql:              "alter table add fruit basket",
	Force:            true,
	AllowReplication: true,
	BeforeSchema:     testGetSchemaReply,
	AfterSchema:      testGetSchemaReply,
}

func (fra *fakeRpcAgent) ApplySchema(change *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	if !reflect.DeepEqual(change, testSchemaChange) {
		fra.t.Errorf("Unexpected ApplySchema change: got %v expected %v", change, testSchemaChange)
	}
	return testSchemaChangeResult, nil
}

func agentRpcTestApplySchema(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	scr, err := client.ApplySchema(ti, testSchemaChange, time.Minute)
	if err != nil {
		t.Errorf("ApplySchema failed: %v", err)
	} else {
		if !reflect.DeepEqual(scr, testSchemaChangeResult) {
			t.Errorf("Unexpected ApplySchema result: got %v expected %v", scr, testSchemaChangeResult)
		}
	}
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
	if query != testExecuteFetchQuery {
		fra.t.Errorf("Unexpected ExecuteFetch query: got %v expected %v", query, testExecuteFetchQuery)
	}
	if maxrows != testExecuteFetchMaxRows {
		fra.t.Errorf("Unexpected ExecuteFetch maxrows: got %v expected %v", maxrows, testExecuteFetchQuery)
	}
	if !wantFields {
		fra.t.Errorf("Unexpected ExecuteFetch wantFields: got false expected true")
	}
	if !disableBinlogs {
		fra.t.Errorf("Unexpected ExecuteFetch disableBinlogs: got false expected true")
	}
	return testExecuteFetchResult, nil
}

func agentRpcTestExecuteFetch(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	qr, err := client.ExecuteFetch(ti, testExecuteFetchQuery, testExecuteFetchMaxRows, true, true, time.Minute)
	if err != nil {
		t.Errorf("ApplySchema failed: %v", err)
	} else {
		if !reflect.DeepEqual(qr, testExecuteFetchResult) {
			t.Errorf("Unexpected ExecuteFetch result: got %v expected %v", qr, testExecuteFetchResult)
		}
	}
}

//
// Replication related methods
//

func (fra *fakeRpcAgent) SlaveStatus() (*myproto.ReplicationStatus, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) WaitSlavePosition(position myproto.ReplicationPosition, waitTimeout time.Duration) (*myproto.ReplicationStatus, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) MasterPosition() (myproto.ReplicationPosition, error) {
	return myproto.ReplicationPosition{}, nil
}

func (fra *fakeRpcAgent) ReparentPosition(rp *myproto.ReplicationPosition) (*actionnode.RestartSlaveData, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) StopSlave() error {
	return nil
}

func (fra *fakeRpcAgent) StopSlaveMinimum(position myproto.ReplicationPosition, waitTime time.Duration) (*myproto.ReplicationStatus, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) StartSlave() error {
	return nil
}

func (fra *fakeRpcAgent) TabletExternallyReparented(actionTimeout time.Duration) error {
	return nil
}

func (fra *fakeRpcAgent) GetSlaves() ([]string, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) WaitBlpPosition(blpPosition *blproto.BlpPosition, waitTime time.Duration) error {
	return nil
}

func (fra *fakeRpcAgent) StopBlp() (*blproto.BlpPositionList, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) StartBlp() error {
	return nil
}

func (fra *fakeRpcAgent) RunBlpUntil(bpl *blproto.BlpPositionList, waitTime time.Duration) (*myproto.ReplicationPosition, error) {
	return nil, nil
}

//
// Reparenting related functions
//

func (fra *fakeRpcAgent) DemoteMaster() error {
	return nil
}

func (fra *fakeRpcAgent) PromoteSlave() (*actionnode.RestartSlaveData, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) SlaveWasPromoted() error {
	return nil
}

func (fra *fakeRpcAgent) RestartSlave(rsd *actionnode.RestartSlaveData) error {
	return nil
}

func (fra *fakeRpcAgent) SlaveWasRestarted(swrd *actionnode.SlaveWasRestartedArgs) error {
	return nil
}

func (fra *fakeRpcAgent) BreakSlaves() error {
	return nil
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

func (fra *fakeRpcAgent) ReserveForRestore(args *actionnode.ReserveForRestoreArgs) error {
	return nil
}

func (fra *fakeRpcAgent) Restore(args *actionnode.RestoreArgs, logger logutil.Logger) error {
	return nil
}

func (fra *fakeRpcAgent) MultiSnapshot(args *actionnode.MultiSnapshotArgs, logger logutil.Logger) (*actionnode.MultiSnapshotReply, error) {
	return nil, nil
}

func (fra *fakeRpcAgent) MultiRestore(args *actionnode.MultiRestoreArgs, logger logutil.Logger) error {
	return nil
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

	// Reparenting related functions

	// Backup / restore related methods
	agentRpcTestSnapshot(t, client, ti)
}
