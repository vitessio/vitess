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
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"

	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
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
		log.Infof("error inserting row %d: %v", id, err)
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
	vc.AddKeyspace(t, []*Cell{defaultCell}, "unsharded", "0", vschemaUnsharded, schemaUnsharded, defaultReplicas, defaultRdonly, baseTabletID+100, nil)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	vc.AddKeyspace(t, []*Cell{defaultCell}, "sharded", "-80,80-", vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+200, nil)

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
