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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// Validates that we have a working VStream API
// If Failover is enabled:
//   - We ensure that this works through active reparents and doesn't miss any events
//   - We stream only from the primary and while streaming we reparent to a replica and then back to the original primary
func testVStreamWithFailover(t *testing.T, failover bool) {
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()

	require.NotNil(t, vc)
	defaultReplicas = 2
	defaultRdonly = 0

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	vtgate := defaultCell.Vtgates[0]
	t.Run("VStreamFrom", func(t *testing.T) {
		testVStreamFrom(t, vtgate, "product", 2)
	})
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

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

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
			if failover {
				insertMu.Lock()
				output, err := vc.VtctlClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", "--keyspace_shard=product/0", "--new_primary=zone1-101")
				insertMu.Unlock()
				log.Infof("output of first PRS is %s", output)
				require.NoError(t, err)
			}
		case 2:
			if failover {
				insertMu.Lock()
				output, err := vc.VtctlClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", "--keyspace_shard=product/0", "--new_primary=zone1-100")
				insertMu.Unlock()
				log.Infof("output of second PRS is %s", output)
				require.NoError(t, err)
			}
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
	insertedRows, err := qr.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.Equal(t, insertedRows, numRowEvents)
}

const schemaUnsharded = `
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into customer_seq(id, next_id, cache) values(0, 1, 3);
`
const vschemaUnsharded = `
{
  "tables": {
	"customer_seq": {
		"type": "sequence"
	}
  }
}
`
const schemaSharded = `
create table customer(cid int, name varbinary(128), primary key(cid)) TABLESPACE innodb_system CHARSET=utf8mb4;
`
const vschemaSharded = `
{
  "sharded": true,
  "vindexes": {
	    "reverse_bits": {
	      "type": "reverse_bits"
	    }
	  },
   "tables":  {
	    "customer": {
	      "column_vindexes": [
	        {
	          "column": "cid",
	          "name": "reverse_bits"
	        }
	      ],
	      "auto_increment": {
	        "column": "cid",
	        "sequence": "customer_seq"
	      }
	    }
   }
}
`

func insertRow(keyspace, table string, id int) {
	vtgateConn := getConnectionNoError(vc.t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	// Due to race conditions this call is sometimes made after vtgates have shutdown. In that case just return.
	if vtgateConn == nil {
		return
	}
	vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", keyspace), 1000, false)
	vtgateConn.ExecuteFetch("begin", 1000, false)
	_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into %s (name) values ('%s%d')", table, table, id), 1000, false)
	if err != nil {
		log.Errorf("error inserting row %d: %v", id, err)
	}
	vtgateConn.ExecuteFetch("commit", 1000, false)
}

type numEvents struct {
	numRowEvents, numJournalEvents                            int64
	numDash80Events, num80DashEvents                          int64
	numDash40Events, num40DashEvents                          int64
	numShard0BeforeReshardEvents, numShard0AfterReshardEvents int64
}

// tests the StopOnReshard flag
func testVStreamStopOnReshardFlag(t *testing.T, stopOnReshard bool, baseTabletID int) *numEvents {
	defaultCellName := "zone1"
	vc = NewVitessCluster(t, nil)

	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown()

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "unsharded", "0", vschemaUnsharded, schemaUnsharded, defaultReplicas, defaultRdonly, baseTabletID+100, nil)

	verifyClusterHealth(t, vc)

	// some initial data
	for i := 0; i < 10; i++ {
		insertRow("sharded", "customer", i)
	}

	vc.AddKeyspace(t, []*Cell{defaultCell}, "sharded", "-80,80-", vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+200, nil)

	ctx := context.Background()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Fatal(err)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "sharded",
			Gtid:     "current",
		}}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	flags := &vtgatepb.VStreamFlags{HeartbeatInterval: 3600, StopOnReshard: stopOnReshard}
	done := false

	id := 1000
	// first goroutine that keeps inserting rows into table being streamed until a minute after reshard
	// * if StopOnReshard is false we should keep getting events on the new shards
	// * if StopOnReshard is true we should get a journal event and no events on the new shards
	go func() {
		for {
			if done {
				return
			}
			id++
			time.Sleep(1 * time.Second)
			insertRow("sharded", "customer", id)
		}
	}()
	// stream events from the VStream API
	var ne numEvents
	go func() {
		var reader vtgateconn.VStreamReader
		reader, err = vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		require.NoError(t, err)
		connect := false
		numErrors := 0
		for {
			if connect { // if vtgate returns a transient error try reconnecting from the last seen vgtid
				reader, err = vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
				require.NoError(t, err)
				connect = false
			}
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_VGTID:
						vgtid = ev.Vgtid
					case binlogdatapb.VEventType_ROW:
						shard := ev.RowEvent.Shard
						switch shard {
						case "-80":
							ne.numDash80Events++
						case "80-":
							ne.num80DashEvents++
						case "-40":
							ne.numDash40Events++
						case "40-":
							ne.num40DashEvents++
						}
						ne.numRowEvents++
					case binlogdatapb.VEventType_JOURNAL:
						ne.numJournalEvents++
					}
				}
			case io.EOF:
				log.Infof("Stream Ended")
				done = true
			default:
				log.Infof("%s:: remote error: %v", time.Now(), err)
				numErrors++
				if numErrors > 10 { // if vtgate is continuously unavailable error the test
					return
				}
				if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
					// this should not happen, but maybe the buffering logic might return a transient
					// error during resharding. So adding this logic to reduce future flakiness
					time.Sleep(100 * time.Millisecond)
					connect = true
				} else {
					// failure, stop test
					done = true
				}
			}
			if done {
				return
			}
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	tickCount := 0
	for {
		<-ticker.C
		tickCount++
		switch tickCount {
		case 1:
			reshard(t, "sharded", "customer", "vstreamStopOnReshard", "-80,80-",
				"-40,40-", baseTabletID+400, nil, nil, nil, nil, defaultCellName, 1)
		case 60:
			done = true
		}
		if done {
			break
		}
	}
	return &ne
}

// Validate that we can continue streaming from multiple keyspaces after first copying some tables and then resharding one of the keyspaces
// Ensure that there are no missing row events during the resharding process.
func testVStreamCopyMultiKeyspaceReshard(t *testing.T, baseTabletID int) numEvents {
	vc = NewVitessCluster(t, nil)
	ogdr := defaultReplicas
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func(dr int) { defaultReplicas = dr }(ogdr)

	defer vc.TearDown()

	defaultCell := vc.Cells[vc.CellNames[0]]
	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "unsharded", "0", vschemaUnsharded, schemaUnsharded, defaultReplicas, defaultRdonly, baseTabletID+100, nil)
	require.NoError(t, err)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	_, err = vc.AddKeyspace(t, []*Cell{defaultCell}, "sharded", "-80,80-", vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+200, nil)
	require.NoError(t, err)

	ctx := context.Background()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Fatal(err)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "/.*",
		}}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// We want to confirm that the following two tables are streamed.
			// 1. the customer_seq in the unsharded keyspace
			// 2. the customer table in the sharded keyspace
			Match: "/customer.*/",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	done := false

	id := 1000
	// First goroutine that keeps inserting rows into the table being streamed until a minute after reshard
	// We should keep getting events on the new shards
	go func() {
		for {
			if done {
				return
			}
			id++
			time.Sleep(1 * time.Second)
			insertRow("sharded", "customer", id)
		}
	}()
	// stream events from the VStream API
	var ne numEvents
	reshardDone := false
	go func() {
		var reader vtgateconn.VStreamReader
		reader, err = vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.RowEvent.Shard
						switch shard {
						case "0":
							if reshardDone {
								ne.numShard0AfterReshardEvents++
							} else {
								ne.numShard0BeforeReshardEvents++
							}
						case "-80":
							ne.numDash80Events++
						case "80-":
							ne.num80DashEvents++
						case "-40":
							ne.numDash40Events++
						case "40-":
							ne.num40DashEvents++
						}
						ne.numRowEvents++
					case binlogdatapb.VEventType_JOURNAL:
						ne.numJournalEvents++
					}
				}
			case io.EOF:
				log.Infof("Stream Ended")
				done = true
			default:
				log.Errorf("Returned err %v", err)
				done = true
			}
			if done {
				return
			}
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	tickCount := 0
	for {
		<-ticker.C
		tickCount++
		switch tickCount {
		case 1:
			reshard(t, "sharded", "customer", "vstreamCopyMultiKeyspaceReshard", "-80,80-", "-40,40-", baseTabletID+400, nil, nil, nil, nil, defaultCellName, 1)
			reshardDone = true
		case 60:
			done = true
		}
		if done {
			break
		}
	}
	log.Infof("ne=%v", ne)

	// The number of row events streamed by the VStream API should match the number of rows inserted.
	// This is important for sharded tables, where we need to ensure that no row events are missed during the resharding process.
	//
	// On the other hand, we don't verify the exact number of row events for the unsharded keyspace
	// because the keyspace remains unsharded and the number of rows in the customer_seq table is always 1.
	// We believe that checking the number of row events for the unsharded keyspace, which should always be greater than 0 before and after resharding,
	// is sufficient to confirm that the resharding of one keyspace does not affect another keyspace, while keeping the test straightforward.
	customerResult := execVtgateQuery(t, vtgateConn, "sharded", "select count(*) from customer")
	insertedCustomerRows, err := customerResult.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.Equal(t, insertedCustomerRows, ne.numDash80Events+ne.num80DashEvents+ne.numDash40Events+ne.num40DashEvents)
	return ne
}

// Validate that we can resume a VStream when the keyspace has been resharded
// while not streaming. Ensure that there we successfully transition from the
// old shards -- which are in the VGTID from the previous stream -- and that
// we miss no row events during the process.
func TestMultiVStreamsKeyspaceReshard(t *testing.T) {
	ctx := context.Background()
	ks := "testks"
	wf := "multiVStreamsKeyspaceReshard"
	baseTabletID := 100
	tabletType := topodatapb.TabletType_PRIMARY.String()
	oldShards := "-80,80-"
	newShards := "-40,40-80,80-c0,c0-"
	oldShardRowEvents, newShardRowEvents := 0, 0
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	defaultCell := vc.Cells[vc.CellNames[0]]
	ogdr := defaultReplicas
	defaultReplicas = 0 // Because of CI resource constraints we can only run this test with primary tablets
	defer func(dr int) { defaultReplicas = dr }(ogdr)

	// For our sequences etc.
	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "global", "0", vschemaUnsharded, schemaUnsharded, defaultReplicas, defaultRdonly, baseTabletID, nil)
	require.NoError(t, err)

	// Setup the keyspace with our old/original shards.
	keyspace, err := vc.AddKeyspace(t, []*Cell{defaultCell}, ks, oldShards, vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+1000, nil)
	require.NoError(t, err)

	// Add the new shards.
	err = vc.AddShards(t, []*Cell{defaultCell}, keyspace, newShards, defaultReplicas, defaultRdonly, baseTabletID+2000, targetKsOpts)
	require.NoError(t, err)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	require.NoError(t, err)
	defer vstreamConn.Close()

	// Ensure that we're starting with a clean slate.
	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("delete from %s.customer", ks), 1000, false)
	require.NoError(t, err)

	// Coordinate go-routines.
	streamCtx, streamCancel := context.WithTimeout(ctx, 1*time.Minute)
	defer streamCancel()
	done := make(chan struct{})

	// First goroutine that keeps inserting rows into the table being streamed until the
	// stream context is cancelled.
	go func() {
		id := 1
		for {
			select {
			case <-streamCtx.Done():
				// Give the VStream a little catch-up time before telling it to stop
				// via the done channel.
				time.Sleep(10 * time.Second)
				close(done)
				return
			default:
				insertRow(ks, "customer", id)
				time.Sleep(250 * time.Millisecond)
				id++
			}
		}
	}()

	// Create the Reshard workflow and wait for it to finish the copy phase.
	reshardAction(t, "Create", wf, ks, oldShards, newShards, defaultCellName, tabletType)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", ks, wf), binlogdatapb.VReplicationWorkflowState_Running.String())

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "/.*", // Match all keyspaces just to be more realistic.
		}}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// Only stream the customer table and its sequence backing table.
			Match: "/customer.*",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		IncludeReshardJournalEvents: true,
	}
	journalEvents := 0

	// Stream events but stop once we have a VGTID with positions for the old/original shards.
	var newVGTID *binlogdatapb.VGtid
	func() {
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.GetRowEvent().GetShard()
						switch shard {
						case "-80", "80-":
							oldShardRowEvents++
						case "0":
							// We expect some for the sequence backing table, but don't care.
						default:
							require.FailNow(t, fmt.Sprintf("received event for unexpected shard: %s", shard))
						}
					case binlogdatapb.VEventType_VGTID:
						newVGTID = ev.GetVgtid()
						if len(newVGTID.GetShardGtids()) == 3 {
							// We want a VGTID with a position for the global shard and the old shards.
							canStop := true
							for _, sg := range newVGTID.GetShardGtids() {
								if sg.GetGtid() == "" {
									canStop = false
								}
							}
							if canStop {
								return
							}
						}
					}
				}
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error: %v", err))
			}
			select {
			case <-streamCtx.Done():
				return
			default:
			}
		}
	}()

	// Confirm that we have shard GTIDs for the global shard and the old/original shards.
	require.Len(t, newVGTID.GetShardGtids(), 3)

	// Switch the traffic to the new shards.
	reshardAction(t, "SwitchTraffic", wf, ks, oldShards, newShards, defaultCellName, tabletType)

	// Now start a new VStream from our previous VGTID which only has the old/original shards.
	func() {
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, newVGTID, filter, flags)
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.RowEvent.Shard
						switch shard {
						case "-80", "80-":
							oldShardRowEvents++
						case "-40", "40-80", "80-c0", "c0-":
							newShardRowEvents++
						case "0":
							// Again, we expect some for the sequence backing table, but don't care.
						default:
							require.FailNow(t, fmt.Sprintf("received event for unexpected shard: %s", shard))
						}
					case binlogdatapb.VEventType_JOURNAL:
						require.True(t, ev.Journal.MigrationType == binlogdatapb.MigrationType_SHARDS)
						journalEvents++
					}
				}
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error: %v", err))
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	// We should have a mix of events across the old and new shards.
	require.Greater(t, oldShardRowEvents, 0)
	require.Greater(t, newShardRowEvents, 0)
	// We should have seen a reshard journal event.
	require.Greater(t, journalEvents, 0)

	// The number of row events streamed by the VStream API should match the number of rows inserted.
	customerResult := execVtgateQuery(t, vtgateConn, ks, "select count(*) from customer")
	customerCount, err := customerResult.Rows[0][0].ToInt64()
	require.NoError(t, err)
	require.Equal(t, customerCount, int64(oldShardRowEvents+newShardRowEvents))
}

// TestMultiVStreamsKeyspaceStopOnReshard confirms that journal events are received
// when resuming a VStream after a reshard.
func TestMultiVStreamsKeyspaceStopOnReshard(t *testing.T) {
	ctx := context.Background()
	ks := "testks"
	wf := "multiVStreamsKeyspaceReshard"
	baseTabletID := 100
	tabletType := topodatapb.TabletType_PRIMARY.String()
	oldShards := "-80,80-"
	newShards := "-40,40-80,80-c0,c0-"
	oldShardRowEvents, journalEvents := 0, 0
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	defaultCell := vc.Cells[vc.CellNames[0]]
	ogdr := defaultReplicas
	defaultReplicas = 0 // Because of CI resource constraints we can only run this test with primary tablets
	defer func(dr int) { defaultReplicas = dr }(ogdr)

	// For our sequences etc.
	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "global", "0", vschemaUnsharded, schemaUnsharded, defaultReplicas, defaultRdonly, baseTabletID, nil)
	require.NoError(t, err)

	// Setup the keyspace with our old/original shards.
	keyspace, err := vc.AddKeyspace(t, []*Cell{defaultCell}, ks, oldShards, vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+1000, nil)
	require.NoError(t, err)

	// Add the new shards.
	err = vc.AddShards(t, []*Cell{defaultCell}, keyspace, newShards, defaultReplicas, defaultRdonly, baseTabletID+2000, targetKsOpts)
	require.NoError(t, err)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	require.NoError(t, err)
	defer vstreamConn.Close()

	// Ensure that we're starting with a clean slate.
	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("delete from %s.customer", ks), 1000, false)
	require.NoError(t, err)

	// Coordinate go-routines.
	streamCtx, streamCancel := context.WithTimeout(ctx, 1*time.Minute)
	defer streamCancel()
	done := make(chan struct{})

	// First goroutine that keeps inserting rows into the table being streamed until the
	// stream context is cancelled.
	go func() {
		id := 1
		for {
			select {
			case <-streamCtx.Done():
				// Give the VStream a little catch-up time before telling it to stop
				// via the done channel.
				time.Sleep(10 * time.Second)
				close(done)
				return
			default:
				insertRow(ks, "customer", id)
				time.Sleep(250 * time.Millisecond)
				id++
			}
		}
	}()

	// Create the Reshard workflow and wait for it to finish the copy phase.
	reshardAction(t, "Create", wf, ks, oldShards, newShards, defaultCellName, tabletType)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", ks, wf), binlogdatapb.VReplicationWorkflowState_Running.String())

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			// Only stream the keyspace that we're resharding. Otherwise the client stream
			// will continue to run with only the tablet stream from the global keyspace.
			Keyspace: ks,
		}}}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// Stream all tables.
			Match: "/.*",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		StopOnReshard: true,
	}

	// Stream events but stop once we have a VGTID with positions for the old/original shards.
	var newVGTID *binlogdatapb.VGtid
	func() {
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.GetRowEvent().GetShard()
						switch shard {
						case "-80", "80-":
							oldShardRowEvents++
						default:
							require.FailNow(t, fmt.Sprintf("received event for unexpected shard: %s", shard))
						}
					case binlogdatapb.VEventType_VGTID:
						newVGTID = ev.GetVgtid()
						// We want a VGTID with a ShardGtid for both of the old shards.
						if len(newVGTID.GetShardGtids()) == 2 {
							canStop := true
							for _, sg := range newVGTID.GetShardGtids() {
								if sg.GetGtid() == "" {
									canStop = false
								}
							}
							if canStop {
								return
							}
						}
					}
				}
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error: %v", err))
			}
			select {
			case <-streamCtx.Done():
				return
			default:
			}
		}
	}()

	// Confirm that we have shard GTIDs for the old/original shards.
	require.Len(t, newVGTID.GetShardGtids(), 2)
	t.Logf("Position at end of first stream: %+v", newVGTID.GetShardGtids())

	// Switch the traffic to the new shards.
	reshardAction(t, "SwitchTraffic", wf, ks, oldShards, newShards, defaultCellName, tabletType)

	// Now start a new VStream from our previous VGTID which only has the old/original shards.
	expectedJournalEvents := 2 // One for each old shard: -80,80-
	var streamStopped bool     // We expect the stream to end with io.EOF from the reshard
	runResumeStream := func() {
		journalEvents = 0
		streamStopped = false
		t.Logf("Streaming from position: %+v", newVGTID.GetShardGtids())
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, newVGTID, filter, flags)
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for i, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.RowEvent.Shard
						switch shard {
						case "-80", "80-":
						default:
							require.FailNow(t, fmt.Sprintf("received event for unexpected shard: %s", shard))
						}
					case binlogdatapb.VEventType_JOURNAL:
						t.Logf("Journal event: %+v", ev)
						journalEvents++
						require.Equal(t, binlogdatapb.VEventType_BEGIN, evs[i-1].Type, "JOURNAL event not preceded by BEGIN event")
						require.Equal(t, binlogdatapb.VEventType_VGTID, evs[i+1].Type, "JOURNAL event not followed by VGTID event")
						require.Equal(t, binlogdatapb.VEventType_COMMIT, evs[i+2].Type, "JOURNAL event not followed by COMMIT event")
					}
				}
			case io.EOF:
				streamStopped = true
				return
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error: %v", err))
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}

	// Multiple VStream clients should be able to resume from where they left off and
	// get the reshard journal event.
	for i := 1; i <= expectedJournalEvents; i++ {
		runResumeStream()
		// We should have seen the journal event for each shard in the stream due to
		// using StopOnReshard.
		require.Equal(t, expectedJournalEvents, journalEvents,
			"did not get expected journal events on resume vstream #%d", i)
		// Confirm that the stream stopped on the reshard.
		require.True(t, streamStopped, "the vstream did not stop with io.EOF as expected")
	}
}

func TestVStreamFailover(t *testing.T) {
	testVStreamWithFailover(t, true)
}

func TestVStreamStopOnReshardTrue(t *testing.T) {
	ne := testVStreamStopOnReshardFlag(t, true, 1000)
	require.Greater(t, ne.numJournalEvents, int64(0))
	require.NotZero(t, ne.numRowEvents)
	require.NotZero(t, ne.numDash80Events)
	require.NotZero(t, ne.num80DashEvents)
	require.Zero(t, ne.numDash40Events)
	require.Zero(t, ne.num40DashEvents)
}

func TestVStreamStopOnReshardFalse(t *testing.T) {
	ne := testVStreamStopOnReshardFlag(t, false, 2000)
	require.Equal(t, int64(0), ne.numJournalEvents)
	require.NotZero(t, ne.numRowEvents)
	require.NotZero(t, ne.numDash80Events)
	require.NotZero(t, ne.num80DashEvents)
	require.NotZero(t, ne.numDash40Events)
	require.NotZero(t, ne.num40DashEvents)
}

func TestVStreamWithKeyspacesToWatch(t *testing.T) {
	extraVTGateArgs = append(extraVTGateArgs, []string{
		"--keyspaces_to_watch", "product",
	}...)

	testVStreamWithFailover(t, false)
}

func TestVStreamCopyMultiKeyspaceReshard(t *testing.T) {
	ne := testVStreamCopyMultiKeyspaceReshard(t, 3000)
	require.Equal(t, int64(0), ne.numJournalEvents)
	require.NotZero(t, ne.numRowEvents)
	require.NotZero(t, ne.numShard0BeforeReshardEvents)
	require.NotZero(t, ne.numShard0AfterReshardEvents)
	require.NotZero(t, ne.numDash80Events)
	require.NotZero(t, ne.num80DashEvents)
	require.NotZero(t, ne.numDash40Events)
	require.NotZero(t, ne.num40DashEvents)
}

const (
	vstreamHeartbeatsTestContextTimeout = 20 * time.Second
	// Expect a reasonable number of heartbeats to be received in the test duration, should ideally be ~ timeout
	// since the heartbeat interval is set to 1s. But we set it to 10 to be conservative to avoid CI flakiness.
	numExpectedHeartbeats = 10
)

func doVStream(t *testing.T, vc *VitessCluster, flags *vtgatepb.VStreamFlags) (numRowEvents map[string]int, numFieldEvents map[string]int) {
	// Stream for a while to ensure heartbeats are sent.
	ctx, cancel := context.WithTimeout(context.Background(), vstreamHeartbeatsTestContextTimeout)
	defer cancel()

	numRowEvents = make(map[string]int)
	numFieldEvents = make(map[string]int)
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	require.NoError(t, err)
	defer vstreamConn.Close()

	done := false
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
	// Stream events from the VStream API.
	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	require.NoError(t, err)
	for !done {
		evs, err := reader.Recv()
		switch err {
		case nil:
			for _, ev := range evs {
				switch ev.Type {
				case binlogdatapb.VEventType_ROW:
					rowEvent := ev.RowEvent
					arr := strings.Split(rowEvent.TableName, ".")
					require.Equal(t, len(arr), 2)
					tableName := arr[1]
					require.Equal(t, "product", rowEvent.Keyspace)
					require.Equal(t, "0", rowEvent.Shard)
					numRowEvents[tableName]++

				case binlogdatapb.VEventType_FIELD:
					fieldEvent := ev.FieldEvent
					arr := strings.Split(fieldEvent.TableName, ".")
					require.Equal(t, len(arr), 2)
					tableName := arr[1]
					require.Equal(t, "product", fieldEvent.Keyspace)
					require.Equal(t, "0", fieldEvent.Shard)
					numFieldEvents[tableName]++
				default:
				}
			}
		case io.EOF:
			log.Infof("Stream Ended")
			done = true
		default:
			log.Errorf("remote error: %v", err)
			done = true
		}
	}
	return numRowEvents, numFieldEvents
}

// TestVStreamHeartbeats enables streaming of the internal Vitess heartbeat tables in the VStream API and
// ensures that the heartbeat events are received as expected by the client.
func TestVStreamHeartbeats(t *testing.T) {
	// Enable continuous heartbeats.
	extraVTTabletArgs = append(extraVTTabletArgs,
		"--heartbeat_enable",
		"--heartbeat_interval", "1s",
		"--heartbeat_on_demand_duration", "0",
	)
	setSidecarDBName("_vt")
	config := *mainClusterConfig
	config.overrideHeartbeatOptions = true
	vc = NewVitessCluster(t, &clusterOptions{
		clusterConfig: &config,
	})
	defer vc.TearDown()

	require.NotNil(t, vc)
	defaultReplicas = 0
	defaultRdonly = 0

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema,
		defaultReplicas, defaultRdonly, 100, nil)
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	expectedNumRowEvents := make(map[string]int)
	expectedNumRowEvents["customer"] = 3 // 3 rows inserted in the customer table in insertInitialData()

	type testCase struct {
		name               string
		flags              *vtgatepb.VStreamFlags
		expectedHeartbeats int
	}
	testCases := []testCase{
		{
			name: "With Keyspace Heartbeats On",
			flags: &vtgatepb.VStreamFlags{
				StreamKeyspaceHeartbeats: true,
			},
			expectedHeartbeats: numExpectedHeartbeats,
		},
		{
			name:               "With Keyspace Heartbeats Off",
			flags:              nil,
			expectedHeartbeats: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotNumRowEvents, gotNumFieldEvents := doVStream(t, vc, tc.flags)
			for k := range expectedNumRowEvents {
				require.Equalf(t, 1, gotNumFieldEvents[k], "incorrect number of field events for table %s, got %d", k, gotNumFieldEvents[k])
			}
			require.GreaterOrEqual(t, gotNumRowEvents["heartbeat"], tc.expectedHeartbeats, "incorrect number of heartbeat events received")
			log.Infof("Total number of heartbeat events received: %v", gotNumRowEvents["heartbeat"])
			delete(gotNumRowEvents, "heartbeat")
			require.Equal(t, expectedNumRowEvents, gotNumRowEvents)
		})
	}
}
