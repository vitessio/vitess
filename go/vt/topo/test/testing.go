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

// Package test contains utilities to test topo.Conn
// implementations. If you are testing your implementation, you will
// want to call TopoServerTestSuite in your test method. For an
// example, look at the tests in
// vitess.io/vitess/go/vt/topo/memorytopo.
package test

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// LocalCellName is the cell name used by this test suite.
const LocalCellName = "test"

func newKeyRange(value string) *topodatapb.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

func executeTestSuite(f func(*testing.T, context.Context, *topo.Server), t *testing.T, ctx context.Context, ts *topo.Server, ignoreList []string, name string) {
	// some test does not apply every where therefore we ignore them
	for _, n := range ignoreList {
		if n == name {
			t.Logf("=== ignoring test %s", name)
			return
		}
	}
	f(t, ctx, ts)
}

// TopoServerTestSuite runs the full topo.Server/Conn test suite.
// The factory method should return a topo.Server that has a single cell
// called LocalCellName.
// Not all tests are applicable for each Topo server, therefore we provide ignoreList in order to
// avoid them for given Topo server tests. For example `TryLock` implementation is same as `Lock` for some Topo servers.
// Hence, for these Topo servers we ignore executing TryLock Tests.
func TopoServerTestSuite(t *testing.T, ctx context.Context, factory func() *topo.Server, ignoreList []string) {
	var ts *topo.Server

	t.Log("=== checkKeyspace")
	ts = factory()
	executeTestSuite(checkKeyspace, t, ctx, ts, ignoreList, "checkKeyspace")
	ts.Close()

	t.Log("=== checkShard")
	ts = factory()
	executeTestSuite(checkShard, t, ctx, ts, ignoreList, "checkShard")
	ts.Close()

	t.Log("=== checkShardWithLock")
	ts = factory()
	executeTestSuite(checkShardWithLock, t, ctx, ts, ignoreList, "checkShardWithLock")
	ts.Close()

	t.Log("=== checkTablet")
	ts = factory()
	executeTestSuite(checkTablet, t, ctx, ts, ignoreList, "checkTablet")
	ts.Close()

	t.Log("=== checkShardReplication")
	ts = factory()
	executeTestSuite(checkShardReplication, t, ctx, ts, ignoreList, "checkShardReplication")
	ts.Close()

	t.Log("=== checkSrvKeyspace")
	ts = factory()
	executeTestSuite(checkSrvKeyspace, t, ctx, ts, ignoreList, "checkSrvKeyspace")
	ts.Close()

	t.Log("=== checkSrvVSchema")
	ts = factory()
	executeTestSuite(checkSrvVSchema, t, ctx, ts, ignoreList, "checkSrvVSchema")
	ts.Close()

	t.Log("=== checkLock")
	ts = factory()
	executeTestSuite(checkLock, t, ctx, ts, ignoreList, "checkLock")
	ts.Close()

	t.Log("=== checkTryLock")
	ts = factory()
	executeTestSuite(checkTryLock, t, ctx, ts, ignoreList, "checkTryLock")
	ts.Close()

	t.Log("=== checkVSchema")
	ts = factory()
	executeTestSuite(checkVSchema, t, ctx, ts, ignoreList, "checkVSchema")
	ts.Close()

	t.Log("=== checkRoutingRules")
	ts = factory()
	executeTestSuite(checkRoutingRules, t, ctx, ts, ignoreList, "checkRoutingRules")
	ts.Close()

	t.Log("=== checkElection")
	ts = factory()
	executeTestSuite(checkElection, t, ctx, ts, ignoreList, "checkElection")
	ts.Close()

	t.Log("=== checkWaitForNewLeader")
	ts = factory()
	executeTestSuite(checkWaitForNewLeader, t, ctx, ts, ignoreList, "checkWaitForNewLeader")
	ts.Close()

	t.Log("=== checkDirectory")
	ts = factory()
	executeTestSuite(checkDirectory, t, ctx, ts, ignoreList, "checkDirectory")
	ts.Close()

	t.Log("=== checkFile")
	ts = factory()
	executeTestSuite(checkFile, t, ctx, ts, ignoreList, "checkFile")
	ts.Close()

	t.Log("=== checkWatch")
	ts = factory()
	executeTestSuite(checkWatch, t, ctx, ts, ignoreList, "checkWatch")
	ts.Close()

	ts = factory()
	t.Log("=== checkWatchInterrupt")
	executeTestSuite(checkWatchInterrupt, t, ctx, ts, ignoreList, "checkWatchInterrupt")
	ts.Close()

	ts = factory()
	t.Log("=== checkList")
	executeTestSuite(checkList, t, ctx, ts, ignoreList, "checkList")
	ts.Close()

	ts = factory()
	t.Log("=== checkWatchRecursive")
	executeTestSuite(checkWatchRecursive, t, ctx, ts, ignoreList, "checkWatchRecursive")
	ts.Close()
}
