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
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"

	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// Validates that a reparent while VStream API is streaming doesn't miss any events
// We stream only from the primary and while streaming we reparent to a replica and then back to the original primary
func TestVStreamFailover(t *testing.T) {
	defaultCellName := "zone1"
	cells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestVStreamFailover", cells, mainClusterConfig)

	require.NotNil(t, vc)
	defaultReplicas = 2
	defaultRdonly = 0
	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 3)
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	verifyClusterHealth(t, vc)
	insertInitialData(t)
	ctx := context.Background()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Fatal(err)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "product",
			Shard:    "0",
			Gtid:     "",
		}}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	flags := &vtgatepb.VStreamFlags{HeartbeatInterval: 3600}
	done := false

	// don't insert while PRS is going on
	var insertMu sync.Mutex
	stopInserting := false
	id := 0

	// first goroutine that keeps inserting rows into table being streamed until some time elapses after second PRS
	go func() {
		for {
			if stopInserting {
				return
			}
			insertMu.Lock()
			id++
			execVtgateQuery(t, vtgateConn, "product", fmt.Sprintf("insert into customer (cid, name) values (%d, 'customer%d')", id+100, id))
			insertMu.Unlock()
		}
	}()

	// stream events from the VStream API
	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	require.NoError(t, err)
	var numRowEvents int64
	// second goroutine that continuously receives events via VStream API and should be resilient to the two PRS events
	go func() {
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					if ev.Type == binlogdatapb.VEventType_ROW {
						numRowEvents++
					}
				}
			case io.EOF:
				log.Infof("Stream Ended")
			default:
				log.Infof("%s:: remote error: %v", time.Now(), err)
			}

			if done {
				return
			}
		}
	}()

	// run two PRS after one second each, wait for events to be received and exit test
	ticker := time.NewTicker(1 * time.Second)
	tickCount := 0
	// this for loop implements a mini state machine that does the two PRSs, waits a bit after the second PRS,
	// stops the insertions, waits for a bit again for the vstream to catchup and signals the test to stop
	for {
		<-ticker.C
		tickCount++
		switch tickCount {
		case 1:
			insertMu.Lock()
			output, err := vc.VtctlClient.ExecuteCommandWithOutput("PlannedReparentShard", "-keyspace_shard=product/0", "-new_primary=zone1-101")
			insertMu.Unlock()
			log.Infof("output of first PRS is %s", output)
			require.NoError(t, err)
		case 2:
			insertMu.Lock()
			output, err := vc.VtctlClient.ExecuteCommandWithOutput("PlannedReparentShard", "-keyspace_shard=product/0", "-new_primary=zone1-100")
			insertMu.Unlock()
			log.Infof("output of second PRS is %s", output)
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
			stopInserting = true
			time.Sleep(2 * time.Second)
			done = true
		}

		if done {
			break
		}
	}
	qr := execVtgateQuery(t, vtgateConn, "product", "select count(*) from customer")
	require.NotNil(t, qr)
	// total number of row events found by the VStream API should match the rows inserted
	insertedRows, err := evalengine.ToInt64(qr.Rows[0][0])
	require.NoError(t, err)
	require.Equal(t, insertedRows, numRowEvents)
}
