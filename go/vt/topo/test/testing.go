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

// TopoServerTestSuite runs the full topo.Server/Conn test suite.
// The factory method should return a topo.Server that has a single cell
// called LocalCellName.
func TopoServerTestSuite(t *testing.T, factory func() *topo.Server) {
	var ts *topo.Server

	t.Log("=== checkKeyspace")
	ts = factory()
	checkKeyspace(t, ts)
	ts.Close()

	t.Log("=== checkShard")
	ts = factory()
	checkShard(t, ts)
	ts.Close()

	t.Log("=== checkTablet")
	ts = factory()
	checkTablet(t, ts)
	ts.Close()

	t.Log("=== checkShardReplication")
	ts = factory()
	checkShardReplication(t, ts)
	ts.Close()

	t.Log("=== checkSrvKeyspace")
	ts = factory()
	checkSrvKeyspace(t, ts)
	ts.Close()

	t.Log("=== checkSrvVSchema")
	ts = factory()
	checkSrvVSchema(t, ts)
	ts.Close()

	t.Log("=== checkLock")
	ts = factory()
	checkLock(t, ts)
	ts.Close()

	t.Log("=== checkVSchema")
	ts = factory()
	checkVSchema(t, ts)
	ts.Close()

	t.Log("=== checkRoutingRules")
	ts = factory()
	checkRoutingRules(t, ts)
	ts.Close()

	t.Log("=== checkElection")
	ts = factory()
	checkElection(t, ts)
	ts.Close()

	t.Log("=== checkDirectory")
	ts = factory()
	checkDirectory(t, ts)
	ts.Close()

	t.Log("=== checkFile")
	ts = factory()
	checkFile(t, ts)
	ts.Close()

	t.Log("=== checkWatch")
	ts = factory()
	checkWatch(t, ts)
	checkWatchInterrupt(t, ts)
	ts.Close()
}
