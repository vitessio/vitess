/*
Copyright 2021 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"io"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"
)

func TestReplicationStress(t *testing.T) {
	if !*cluster.PerfTest {
		t.Skip("performance tests disabled")
	}

	const initialStressVSchema = `
{
  "tables": {
	"largebin": {},
	"customer": {}
  }
}
`
	const initialStressSchema = `
create table largebin(pid int, maindata varbinary(4096), primary key(pid));
create table customer(cid int, name varbinary(128), meta json default null, typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),ts timestamp not null default current_timestamp, primary key(cid))  CHARSET=utf8mb4;
`

	const defaultCellName = "zone1"

	const sourceKs = "stress_src"
	const targetKs = "stress_tgt"

	allCells := []string{defaultCellName}
	allCellNames = defaultCellName

	vc = NewVitessCluster(t, "TestReplicationStress", allCells, mainClusterConfig)
	require.NotNil(t, vc)

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, "0", initialStressVSchema, initialStressSchema, 0, 0, 100, nil)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)

	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1, 30*time.Second)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	verifyClusterHealth(t, vc)

	const insertCount = 16 * 1024 * 1024

	tablet := defaultCell.Keyspaces[sourceKs].Shards["0"].Tablets["zone1-100"].Vttablet
	tablet.BulkLoad(t, "stress_src", "largebin", func(w io.Writer) {
		for i := 0; i < insertCount; i++ {
			fmt.Fprintf(w, "\"%d\",%q\n", i, "foobar")
		}
	})

	waitForRowCount(t, vtgateConn, "stress_src:0", "largebin", insertCount)

	t.Logf("creating new keysepace '%s'", targetKs)
	vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, "0", initialStressVSchema, initialStressSchema, 0, 0, 200, nil)
	waitForRowCount(t, vtgateConn, "stress_tgt:0", "largebin", 0)

	t.Logf("moving 'largebin' table...")
	moveStart := time.Now()

	for _, ks := range defaultCell.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Tablets {
				tablet.Vttablet.ToggleProfiling()
			}
		}
	}

	moveTables(t, defaultCell.Name, "stress_workflow", sourceKs, targetKs, "largebin")

	keyspaceTgt := defaultCell.Keyspaces[targetKs]
	for _, shard := range keyspaceTgt.Shards {
		for _, tablet := range shard.Tablets {
			t.Logf("catchup shard=%v, tablet=%v", shard.Name, tablet.Name)
			tablet.Vttablet.WaitForVReplicationToCatchup(t, "stress_workflow", fmt.Sprintf("vt_%s", tablet.Vttablet.Keyspace), 5*time.Minute)
		}
	}

	for _, ks := range defaultCell.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Tablets {
				tablet.Vttablet.ToggleProfiling()
			}
		}
	}

	t.Logf("finished catching up after MoveTables (%v)", time.Since(moveStart))
	waitForRowCount(t, vtgateConn, "stress_tgt:0", "largebin", insertCount)
}
