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

package vtgate

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var hcVTGateTest *discovery.FakeHealthCheck

var executeOptions = &querypb.ExecuteOptions{
	IncludedFields: querypb.ExecuteOptions_TYPE_ONLY,
}

var primarySession *vtgatepb.Session

func init() {
	createSandbox(KsTestUnsharded).VSchema = `
{
	"sharded": false,
	"tables": {
		"t1": {}
	}
}
`
	createSandbox(KsTestBadVSchema).VSchema = `
{
	"sharded": true,
	"tables": {
		"t2": {
			"auto_increment": {
				"column": "id",
				"sequence": "id_seq"
			}
		}
	}
}
`
	hcVTGateTest = discovery.NewFakeHealthCheck(nil)
	transactionMode = "MULTI"
	Init(context.Background(), hcVTGateTest, newSandboxForCells([]string{"aa"}), "aa", nil, querypb.ExecuteOptions_Gen4)

	mysqlServerPort = 0
	mysqlAuthServerImpl = "none"
	initMySQLProtocol()
}

func TestVTGateExecute(t *testing.T) {
	counts := rpcVTGate.timings.Timings.Counts()

	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	want := *sandboxconn.SingleRowResult
	want.StatusFlags = 0 // VTGate result set does not contain status flags in sqltypes.Result
	utils.MustMatch(t, &want, qr)
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}

	newCounts := rpcVTGate.timings.Timings.Counts()
	require.Contains(t, newCounts, "All")
	require.Equal(t, counts["All"]+1, newCounts["All"])
	require.Contains(t, newCounts, "Execute..primary")
	require.Equal(t, counts["Execute..primary"]+1, newCounts["Execute..primary"])

	for k, v := range newCounts {
		if strings.HasPrefix(k, "Prepare") {
			require.Equal(t, v, counts[k])
		}
	}
}

func TestVTGateExecuteError(t *testing.T) {
	counts := errorCounts.Counts()

	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"bad select id from t1",
		nil,
	)
	require.Error(t, err)
	require.Nil(t, qr)

	newCounts := errorCounts.Counts()
	require.Contains(t, newCounts, "Execute..primary.INVALID_ARGUMENT")
	require.Equal(t, counts["Execute..primary.INVALID_ARGUMENT"]+1, newCounts["Execute..primary.INVALID_ARGUMENT"])

	for k, v := range newCounts {
		if strings.HasPrefix(k, "Prepare") {
			require.Equal(t, v, counts[k])
		}
	}
}

func TestVTGatePrepare(t *testing.T) {
	counts := rpcVTGate.timings.Timings.Counts()

	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_, qr, err := rpcVTGate.Prepare(
		context.Background(),
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	want := sandboxconn.SingleRowResult.Fields
	utils.MustMatch(t, want, qr)
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}

	newCounts := rpcVTGate.timings.Timings.Counts()
	require.Contains(t, newCounts, "All")
	require.Equal(t, counts["All"]+1, newCounts["All"])
	require.Contains(t, newCounts, "Prepare..primary")
	require.Equal(t, counts["Prepare..primary"]+1, newCounts["Prepare..primary"])

	for k, v := range newCounts {
		if strings.HasPrefix(k, "Execute") {
			require.Equal(t, v, counts[k])
		}
	}
}

func TestVTGatePrepareError(t *testing.T) {
	counts := errorCounts.Counts()

	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_, qr, err := rpcVTGate.Prepare(
		context.Background(),
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"bad select id from t1",
		nil,
	)
	require.Error(t, err)
	require.Nil(t, qr)

	newCounts := errorCounts.Counts()
	require.Contains(t, newCounts, "Prepare..primary.INTERNAL")
	require.Equal(t, counts["Prepare..primary.INTERNAL"]+1, newCounts["Prepare..primary.INTERNAL"])

	for k, v := range newCounts {
		if strings.HasPrefix(k, "Execute") {
			require.Equal(t, v, counts[k])
		}
	}
}

func TestVTGateExecuteWithKeyspaceShard(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)

	// Valid keyspace.
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			TargetString: KsTestUnsharded,
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantQr := *sandboxconn.SingleRowResult
	wantQr.StatusFlags = 0 // VTGate result set does not contain status flags in sqltypes.Result
	utils.MustMatch(t, &wantQr, qr)

	// Invalid keyspace.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			TargetString: "invalid_keyspace",
		},
		"select id from none",
		nil,
	)
	want := "VT05003: unknown database 'invalid_keyspace' in vschema"
	assert.EqualError(t, err, want)

	// Valid keyspace/shard.
	_, qr, err = rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			TargetString: KsTestUnsharded + ":0@primary",
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	utils.MustMatch(t, &wantQr, qr)

	// Invalid keyspace/shard.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			TargetString: KsTestUnsharded + ":noshard@primary",
		},
		"select id from none",
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), `no healthy tablet available for 'keyspace:"TestUnsharded" shard:"noshard" tablet_type:PRIMARY`)
}

func TestVTGateStreamExecute(t *testing.T) {
	ks := KsTestUnsharded
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
	var qrs []*sqltypes.Result
	_, err := rpcVTGate.StreamExecute(
		context.Background(),
		nil,
		&vtgatepb.Session{
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		},
	)
	require.NoError(t, err)
	want := []*sqltypes.Result{{
		Fields: sandboxconn.StreamRowResult.Fields,
	}, {
		Rows: sandboxconn.StreamRowResult.Rows,
	}}
	utils.MustMatch(t, want, qrs)
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateBindVarError(t *testing.T) {
	ks := KsTestUnsharded
	createSandbox(ks)
	hcVTGateTest.Reset()
	ctx := context.Background()
	session := &vtgatepb.Session{}
	bindVars := map[string]*querypb.BindVariable{
		"v": {
			Type:  querypb.Type_EXPRESSION,
			Value: []byte("1"),
		},
	}
	want := "v: invalid type specified for MakeValue: EXPRESSION"

	tcases := []struct {
		name string
		f    func() error
	}{{
		name: "Execute",
		f: func() error {
			_, _, err := rpcVTGate.Execute(ctx, nil, session, "", bindVars)
			return err
		},
	}, {
		name: "ExecuteBatch",
		f: func() error {
			_, _, err := rpcVTGate.ExecuteBatch(ctx, session, []string{""}, []map[string]*querypb.BindVariable{bindVars})
			return err
		},
	}, {
		name: "StreamExecute",
		f: func() error {
			_, err := rpcVTGate.StreamExecute(ctx, nil, session, "", bindVars, func(_ *sqltypes.Result) error { return nil })
			return err
		},
	}}
	for _, tcase := range tcases {
		if err := tcase.f(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v error: %v, must contain %s", tcase.name, err, want)
		}
	}
}

func testErrorPropagation(t *testing.T, sbcs []*sandboxconn.SandboxConn, before func(sbc *sandboxconn.SandboxConn), after func(sbc *sandboxconn.SandboxConn), expected vtrpcpb.Code) {

	// Execute
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, _, err := rpcVTGate.Execute(
		context.Background(),
		nil,
		primarySession,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Errorf("error %v not propagated for Execute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got code %v err %v, want %v", ec, err, expected)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecute
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, err = rpcVTGate.StreamExecute(
		context.Background(),
		nil,
		primarySession,
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			return nil
		},
	)
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}
}

// TestErrorPropagation tests an error returned by sandboxconn is
// properly propagated through vtgate layers.  We need both a primary
// tablet and a rdonly tablet because we don't control the routing of
// Commit.
func TestErrorPropagation(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	// create a new session each time so that ShardSessions don't get re-used across tests
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}

	sbcm := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbcrdonly := hcVTGateTest.AddTestTablet("aa", "1.1.1.2", 1001, KsTestUnsharded, "0", topodatapb.TabletType_RDONLY, true, 1, nil)
	sbcs := []*sandboxconn.SandboxConn{
		sbcm,
		sbcrdonly,
	}

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 0
	}, vtrpcpb.Code_CANCELED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 0
	}, vtrpcpb.Code_UNKNOWN)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 0
	}, vtrpcpb.Code_INVALID_ARGUMENT)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 0
	}, vtrpcpb.Code_DEADLINE_EXCEEDED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
	}, vtrpcpb.Code_ALREADY_EXISTS)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 0
	}, vtrpcpb.Code_PERMISSION_DENIED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0
	}, vtrpcpb.Code_RESOURCE_EXHAUSTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 0
	}, vtrpcpb.Code_FAILED_PRECONDITION)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0
	}, vtrpcpb.Code_ABORTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 0
	}, vtrpcpb.Code_INTERNAL)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 0
	}, vtrpcpb.Code_UNAVAILABLE)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 0
	}, vtrpcpb.Code_UNAUTHENTICATED)
}

// This test makes sure that if we start a transaction and hit a critical
// error, a rollback is issued.
func TestErrorIssuesRollback(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.Code_ABORTED case.
	session, _, err := rpcVTGate.Execute(context.Background(), nil, &vtgatepb.Session{}, "begin", nil)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Load() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Load())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	_, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Load() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Load())
	}
	sbc.RollbackCount.Store(0)
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED case.
	session, _, err = rpcVTGate.Execute(context.Background(), nil, &vtgatepb.Session{}, "begin", nil)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Load() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Load())
	}
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	_, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Load() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Load())
	}
	sbc.RollbackCount.Store(0)
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should *not* trigger a rollback:
	// vtrpcpb.Code_ALREADY_EXISTS case.
	session, _, err = rpcVTGate.Execute(context.Background(), nil, &vtgatepb.Session{}, "begin", nil)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Load() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Load())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	_, _, err = rpcVTGate.Execute(context.Background(), nil, session, "select id from t1", nil)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Load() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Load())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
}

var shardedVSchema = `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		}
	},
	"tables": {
		"sp_tbl": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				}
			]
		}
	}
}
`

var shardedVSchemaUnknownParams = `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash",
			"params": {
				"hello": "world",
				"goodbye": "world"
			}
		},
		"binary_index": {
			"type": "binary",
			"params": {
				"foo": "bar"
			}
		}
	},
	"tables": {
		"sp_tbl": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				}
			]
		}
	}
}
`

func TestMultiInternalSavepointVtGate(t *testing.T) {
	s := createSandbox(KsTestSharded)
	s.ShardSpec = "-40-80-"
	s.VSchema = shardedVSchema
	srvSchema := getSandboxSrvVSchema()
	rpcVTGate.executor.vm.VSchemaUpdate(srvSchema, nil)
	hcVTGateTest.Reset()

	sbc1 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1, KsTestSharded, "-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc2 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 2, KsTestSharded, "40-80", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc3 := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 3, KsTestSharded, "80-", topodatapb.TabletType_PRIMARY, true, 1, nil)

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{Autocommit: true}
	require.True(t, session.GetAutocommit())
	require.False(t, session.InTransaction)

	var err error
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "begin", nil)
	require.NoError(t, err)
	require.True(t, session.GetAutocommit())
	require.True(t, session.InTransaction)

	// this query goes to multiple shards so internal savepoint will be created.
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "insert into sp_tbl(user_id) values (1), (3)", nil)
	require.NoError(t, err)
	require.True(t, session.GetAutocommit())
	require.True(t, session.InTransaction)

	wantQ := []*querypb.BoundQuery{{
		Sql:           "savepoint x",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into sp_tbl(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
			"vtg1":       sqltypes.Int64BindVariable(1),
			"vtg2":       sqltypes.Int64BindVariable(3),
		},
	}}
	assertQueriesWithSavepoint(t, sbc1, wantQ)
	wantQ[1].Sql = "insert into sp_tbl(user_id) values (:_user_id_1)"
	assertQueriesWithSavepoint(t, sbc2, wantQ)
	assert.Len(t, sbc3.Queries, 0)
	// internal savepoint should be removed.
	assert.Len(t, session.Savepoints, 0)
	sbc1.Queries = nil
	sbc2.Queries = nil

	// multi shard so new savepoint will be created.
	session, _, err = rpcVTGate.Execute(context.Background(), nil, session, "insert into sp_tbl(user_id) values (2), (4)", nil)
	require.NoError(t, err)
	wantQ = []*querypb.BoundQuery{{
		Sql:           "savepoint x",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into sp_tbl(user_id) values (:_user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_user_id_1": sqltypes.Int64BindVariable(4),
			"vtg1":       sqltypes.Int64BindVariable(2),
			"vtg2":       sqltypes.Int64BindVariable(4),
		},
	}}
	assertQueriesWithSavepoint(t, sbc3, wantQ)
	// internal savepoint should be removed.
	assert.Len(t, session.Savepoints, 0)
	sbc2.Queries = nil
	sbc3.Queries = nil

	// single shard so no savepoint will be created and neither any old savepoint will be executed
	_, _, err = rpcVTGate.Execute(context.Background(), nil, session, "insert into sp_tbl(user_id) values (5)", nil)
	require.NoError(t, err)
	wantQ = []*querypb.BoundQuery{{
		Sql: "insert into sp_tbl(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(5),
			"vtg1":       sqltypes.Int64BindVariable(5),
		},
	}}
	assertQueriesWithSavepoint(t, sbc2, wantQ)

	testQueryLog(t, logChan, "Execute", "BEGIN", "begin", 0)
	testQueryLog(t, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 0)
	testQueryLog(t, logChan, "Execute", "INSERT", "insert into sp_tbl(user_id) values (:vtg1 /* INT64 */), (:vtg2 /* INT64 */)", 2)
	testQueryLog(t, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint y", 2)
	testQueryLog(t, logChan, "Execute", "INSERT", "insert into sp_tbl(user_id) values (:vtg1 /* INT64 */), (:vtg2 /* INT64 */)", 2)
	testQueryLog(t, logChan, "Execute", "INSERT", "insert into sp_tbl(user_id) values (:vtg1 /* INT64 */)", 1)
}

func TestVSchemaVindexUnknownParams(t *testing.T) {
	s := createSandbox(KsTestSharded)
	s.ShardSpec = "-40-80-"

	s.VSchema = shardedVSchema
	srvSchema := getSandboxSrvVSchema()
	rpcVTGate.executor.vm.VSchemaUpdate(srvSchema, nil)
	hcVTGateTest.Reset()

	unknownParams := vindexUnknownParams.Get()
	require.Equal(t, int64(0), unknownParams)

	s.VSchema = shardedVSchemaUnknownParams
	srvSchema = getSandboxSrvVSchema()
	rpcVTGate.executor.vm.VSchemaUpdate(srvSchema, nil)

	unknownParams = vindexUnknownParams.Get()
	require.Equal(t, int64(3), unknownParams)

	s.VSchema = shardedVSchema
	srvSchema = getSandboxSrvVSchema()
	rpcVTGate.executor.vm.VSchemaUpdate(srvSchema, nil)

	unknownParams = vindexUnknownParams.Get()
	require.Equal(t, int64(0), unknownParams)
}
