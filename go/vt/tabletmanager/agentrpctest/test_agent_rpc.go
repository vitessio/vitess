// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package agentrpctest

import (
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
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

// local variables used as test structures. They should set a value
// for each possible field.

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

// Snapshot is part of the RpcAgent interface
func (fra *fakeRpcAgent) Snapshot(args *actionnode.SnapshotArgs, logger logutil.Logger) (*actionnode.SnapshotReply, error) {
	if !reflect.DeepEqual(args, testSnapshotArgs) {
		fra.t.Errorf("Unexpected SnapshotArgs: got %v expected %v", *args, *testSnapshotArgs)
	}

	logger.Infof(testLogString)
	var result = *testSnapshotReply
	return &result, nil
}

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

// AgentRpcTestSuite will run the test suite using the provided client and
// the provided tablet. Tablet's vt address needs to be configured so
// the client will connect to a server backed by our RpcAgent (returned
// by NewFakeRpcAgent)
func AgentRpcTestSuite(t *testing.T, client initiator.TabletManagerConn, ti *topo.TabletInfo) {
	agentRpcTestSnapshot(t, client, ti)
}
