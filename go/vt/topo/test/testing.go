/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package test contains utilities to test topo.Impl
// implementations. If you are testing your implementation, you will
// want to call TopoServerTestSuite in your test method. For an
// example, look at the tests in
// github.com/youtube/vitess/go/vt/topo/memorytopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func newKeyRange(value string) *topodatapb.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

func getLocalCell(ctx context.Context, t *testing.T, ts topo.Impl) string {
	cells, err := ts.GetKnownCells(ctx)
	if err != nil {
		t.Fatalf("GetKnownCells: %v", err)
	}
	if len(cells) < 1 {
		t.Fatalf("provided topo.Impl doesn't have enough cells (need at least 1): %v", cells)
	}
	return cells[0]
}

// TopoServerTestSuite runs the full topo.Impl test suite.
// The factory method should return a topo server that has a single cell
// called 'test'.
func TopoServerTestSuite(t *testing.T, factory func() topo.Impl) {
	var ts topo.Impl

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

	t.Log("=== checkKeyspaceLock")
	ts = factory()
	checkKeyspaceLock(t, ts)
	ts.Close()

	t.Log("=== checkShardLock")
	ts = factory()
	checkShardLock(t, ts)
	ts.Close()

	t.Log("=== checkVSchema")
	ts = factory()
	checkVSchema(t, ts)
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
