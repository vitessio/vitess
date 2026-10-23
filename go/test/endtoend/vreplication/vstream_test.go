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
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestVStreamWithTablesToSkipCopyFlag(t *testing.T) {
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()

	require.NotNil(t, vc)
	defaultReplicas = 2
	defaultRdonly = 0

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)
	verifyClusterHealth(t, vc)

	ctx := t.Context()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: defaultSourceKs,
			Shard:    "0",
			Gtid:     "",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{
			{
				Match:  "customer",
				Filter: "select * from customer",
			}, {
				Match:  "product",
				Filter: "select * from product",
			}, {
				Match:  "merchant",
				Filter: "select * from merchant",
			},
		},
	}
	flags := &vtgatepb.VStreamFlags{
		TablesToCopy:         []string{"product", "customer"},
		TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
	}
	id := 0
	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	// To test the copy phase, let's insert 10 rows intitally in each table
	// present in the filter before running the VStream.
	for range 10 {
		id++
		_, err = execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into customer (cid, name) values (%d, 'customer%d')", id+100, id))
		require.NoError(t, err)
		_, err = execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into product (pid, description) values (%d, 'description%d')", id+100, id))
		require.NoError(t, err)
		_, err = execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into merchant (mname, category) values ('mname%d', 'category%d')", id+100, id))
		require.NoError(t, err)
	}

	require.NoError(t, insertLargeTransactionForChunkTesting(vtgateConn, defaultSourceKs, 10000))

	// Stream events from the VStream API
	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	require.NoError(t, err)
	var numRowEvents atomic.Int64
	var sentinelSeen atomic.Bool

	copyPhaseCompleted := atomic.Bool{}
	copyPhaseCompleted.Store(false)

	done := atomic.Bool{}
	done.Store(false)

	copiedTables := make(sets.Set[string])
	// Start reading events from the VStream.
	go func() {
		for {
			evs, err := reader.Recv()
			switch err {
			case nil:
				for _, ev := range evs {
					if ev.Type == binlogdatapb.VEventType_ROW {
						if !copyPhaseCompleted.Load() {
							escapedTableNameParts := strings.Split(ev.RowEvent.TableName, ".")
							if !assert.Len(t, escapedTableNameParts, 2) {
								return
							}
							copiedTables.Insert(escapedTableNameParts[1])
						}
						numRowEvents.Add(1)
						if isSentinelRow(ev.RowEvent) {
							sentinelSeen.Store(true)
						}
					}
					if ev.Type == binlogdatapb.VEventType_COPY_COMPLETED {
						copyPhaseCompleted.Store(true)
					}
				}
			case io.EOF:
				log.Info("Stream Ended")
			default:
				log.Info(fmt.Sprintf("%s:: remote error: %v", time.Now(), err))
			}

			if done.Load() {
				return
			}
		}
	}()

	// Wait for copy phase to complete.
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		<-ticker.C
		if copyPhaseCompleted.Load() {
			break
		}
	}

	stopInserting := atomic.Bool{}
	stopInserting.Store(false)
	var insertMu sync.Mutex
	var inserts sync.WaitGroup
	inserts.Go(func() {
		insertCount := 0
		for {
			if stopInserting.Load() {
				return
			}
			insertMu.Lock()
			id++
			if _, err := execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into customer (cid, name) values (%d, 'customer%d')", id+100, id)); err != nil {
				assert.NoError(t, err)
				insertMu.Unlock()
				return
			}
			if _, err := execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into product (pid, description) values (%d, 'description%d')", id+100, id)); err != nil {
				assert.NoError(t, err)
				insertMu.Unlock()
				return
			}
			if _, err := execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into merchant (mname, category) values ('mname%d', 'category%d')", id+100, id)); err != nil {
				assert.NoError(t, err)
				insertMu.Unlock()
				return
			}
			insertCount++
			if insertCount%5 == 0 {
				if err := insertLargeTransactionForChunkTesting(vtgateConn, defaultSourceKs, 20000+insertCount*10); err != nil {
					assert.NoError(t, err)
					insertMu.Unlock()
					return
				}
			}
			insertMu.Unlock()
		}
	})

	time.Sleep(100 * time.Millisecond)
	stopInserting.Store(true)
	inserts.Wait()
	waitForSentinelRow(t, vtgateConn, defaultSourceKs, &sentinelSeen)
	done.Store(true)

	qr1, err := execVtgateQuery(vtgateConn, defaultSourceKs, "select count(*) from customer")
	require.NoError(t, err)
	qr2, err := execVtgateQuery(vtgateConn, defaultSourceKs, "select count(*) from product")
	require.NoError(t, err)
	qr3, err := execVtgateQuery(vtgateConn, defaultSourceKs, "select count(*) from merchant")
	require.NoError(t, err)
	require.NotNil(t, qr1)
	require.NotNil(t, qr2)
	require.NotNil(t, qr3)

	// Total number of rows.
	insertedRows1, err := qr1.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.NotZero(t, insertedRows1)
	insertedRows2, err := qr2.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.NotZero(t, insertedRows2)
	insertedRows3, err := qr3.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.NotZero(t, insertedRows3)

	assert.Len(t, copiedTables, 2)
	for _, expectedCopiedTableName := range flags.TablesToCopy {
		assert.Truef(t, copiedTables.Has(expectedCopiedTableName), "expected table %s to be copied", expectedCopiedTableName)
	}
	// We don't expect merchant table to be part of copy phase.
	assert.False(t, copiedTables.Has("merchant"), "expected table merchant not to be copied")

	// Since we don't expect merchant table to be part of copy phase, we can
	// subtract 10 from the total rows found in the 3 tables.
	wantTotalRows := insertedRows1 + insertedRows2 + insertedRows3 - 10
	assert.Equal(t, wantTotalRows, numRowEvents.Load())

	// Confirm that the copy phase queries used by the VStream did NOT include the MAX_EXECUTION_TIME
	// query hint. Adding the hint forces unnecessary copy resume cycles for VStream clients that may
	// not support resume well, or at all (see issue #20039).
	logFiles := path.Join(vc.ClusterConfig.tmpDir, "*-vttablet-stderr.txt")
	totalCmd := fmt.Sprintf("grep -h 'Streaming rows for query:' %s | wc -l", logFiles)
	totalOut, err := exec.Command("bash", "-c", totalCmd).Output()
	require.NoError(t, err)
	totalCount, err := strconv.Atoi(strings.TrimSpace(string(totalOut)))
	require.NoError(t, err)
	require.Positive(t, totalCount, "expected at least one rowstreamer 'Streaming rows for query:' log line during copy phase")

	withHintCmd := fmt.Sprintf("grep -h 'Streaming rows for query:' %s | grep -c MAX_EXECUTION_TIME || true", logFiles)
	withHintOut, err := exec.Command("bash", "-c", withHintCmd).Output()
	require.NoError(t, err)
	withHintCount, err := strconv.Atoi(strings.TrimSpace(string(withHintOut)))
	require.NoError(t, err)
	require.Zero(t, withHintCount, "expected no VStream copy/sync queries to include MAX_EXECUTION_TIME hint")
}

// TestVStreamLaggingDDLRowEvents confirms that when the schema historian is enabled via the --track-schema-versions flag, we don't
// fail to handle older ROW events in the VStream that were created prior to a DDL which modified the table structure and thus
// impacted the binlog event field number to table column mapping. This includes recovering the value mappings for an ENUM or SET
// column that was added and then dropped — also when the column has a binary collation and thus reports as BINARY rather than
// ENUM/SET in field metadata — which requires the historian to have recorded the column's type definition (issue #20175).
func TestVStreamLaggingDDLRowEvents(t *testing.T) {
	oldArgs := slices.Clone(extraVTTabletArgs)
	extraVTTabletArgs = append(extraVTTabletArgs,
		"--track-schema-versions",
	)
	defer func() {
		extraVTTabletArgs = oldArgs
	}()

	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()

	oldDefaultReplicas := defaultReplicas
	oldDefaultRdonly := defaultRdonly
	defaultReplicas = 0
	defaultRdonly = 0
	defer func() {
		defaultReplicas = oldDefaultReplicas
		defaultRdonly = oldDefaultRdonly
	}()

	defaultCell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{defaultCell}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)
	verifyClusterHealth(t, vc)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	require.NoError(t, err)
	defer vstreamConn.Close()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "loadtest",
			Filter: "select * from loadtest",
		}},
		FieldEventMode: binlogdatapb.Filter_ERR_ON_MISMATCH,
	}
	startVGTID := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: defaultSourceKs,
			Shard:    "0",
			Gtid:     "",
		}},
	}

	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, startVGTID, filter, nil)
	require.NoError(t, err)
	var resumeVGTID *binlogdatapb.VGtid
	for resumeVGTID == nil {
		evs, err := reader.Recv()
		require.NoError(t, err)
		for _, ev := range evs {
			if ev.Type == binlogdatapb.VEventType_VGTID {
				resumeVGTID = ev.Vgtid
				break
			}
		}
	}

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	_, err = vtgateConn.ExecuteFetch("use `"+defaultSourceKs+"`", 1000, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch("alter table loadtest add column age int default 1 after id, "+
		"add column plan enum('free','standard','enterprise') not null default 'free' after age, "+
		"add column roles set('admin','user') not null default 'user' after plan, "+
		// A binary collation makes the column report as BINARY rather than ENUM in
		// field metadata, exercising the historian's name-based type enrichment.
		"add column tier enum('gold','silver') collate utf8mb4_bin not null default 'gold' after roles", 1000, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch("insert into loadtest (id, name, plan, roles, tier) values "+
		"(2001, 'cust1', 'standard', 'admin,user', 'silver'), (2002, 'cust2', 'enterprise', 'user', 'gold'), (2003, 'cust3', 'free', 'admin', 'silver')", 1000, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch("alter table loadtest drop column age, drop column plan, drop column roles, drop column tier", 1000, false)
	require.NoError(t, err)

	streamCtx, streamCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer streamCancel()
	rowEvents := 0
	var loadtestFields []*querypb.Field
	var decodedValues []string
	// We use eventually here as it's still possible that the schema tracker's stream encounters an error and
	// pauses for a few seconds before restarting.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		reader, err = vstreamConn.VStream(streamCtx, topodatapb.TabletType_PRIMARY, resumeVGTID, filter, nil)
		assert.NoError(c, err)
	}, 30*time.Second, 500*time.Millisecond)
	deadline := time.Now().Add(1 * time.Minute)
	for rowEvents < 3 {
		if time.Now().After(deadline) {
			require.FailNowf(t, "timed out waiting for row events", "row events seen: %d", rowEvents)
		}
		evs, err := reader.Recv()
		require.NoError(t, err)
		for _, ev := range evs {
			switch ev.Type {
			case binlogdatapb.VEventType_FIELD:
				if strings.HasSuffix(ev.FieldEvent.TableName, ".loadtest") {
					loadtestFields = ev.FieldEvent.Fields
				}
			case binlogdatapb.VEventType_ROW:
				if strings.HasSuffix(ev.RowEvent.TableName, ".loadtest") {
					rowEvents += len(ev.RowEvent.RowChanges)
					for _, rowChange := range ev.RowEvent.RowChanges {
						if rowChange.After == nil {
							continue
						}
						// MakeRowTrusted indexes fields by position without bounds checks, so
						// confirm the FIELD event arrived first and matches the row's shape.
						require.NotNilf(t, loadtestFields, "received a ROW event for the loadtest table before its FIELD event")
						require.Lenf(t, rowChange.After.Lengths, len(loadtestFields),
							"loadtest ROW event value count does not match the preceding FIELD event's field count")
						for _, val := range sqltypes.MakeRowTrusted(loadtestFields, rowChange.After) {
							decodedValues = append(decodedValues, val.ToString())
						}
					}
				}
			}
		}
	}
	// The resumed stream replays the inserts that reference the since-dropped ENUM (plan, tier)
	// and SET (roles) columns. With --track-schema-versions the historian supplies their
	// type definitions, so the values must decode to the exact strings inserted (issue #20175).
	for _, want := range []string{"free", "standard", "enterprise", "admin,user", "user", "admin", "gold", "silver"} {
		assert.Containsf(t, decodedValues, want, "expected decoded ENUM/SET value %q in resumed loadtest rows; got %v", want, decodedValues)
	}
}

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
	vc.AddKeyspace(t, []*Cell{defaultCell}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	vtgate := defaultCell.Vtgates[0]
	t.Run("VStreamFrom", func(t *testing.T) {
		testVStreamFrom(t, vtgate, "product", 2)
	})
	ctx := t.Context()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: defaultSourceKs,
			Shard:    "0",
			Gtid:     "",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		HeartbeatInterval:    3600,
		TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
	}
	done := atomic.Bool{}
	done.Store(false)

	// don't insert while PRS is going on
	var insertMu sync.Mutex
	stopInserting := atomic.Bool{}
	stopInserting.Store(false)
	id := 0

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	// first goroutine that keeps inserting rows into table being streamed until some time elapses after second PRS
	var inserts sync.WaitGroup
	inserts.Go(func() {
		insertCount := 0
		for {
			if stopInserting.Load() {
				return
			}
			insertMu.Lock()
			id++
			if _, err := execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into customer (cid, name) values (%d, 'customer%d')", id+100, id)); err != nil {
				assert.NoError(t, err)
				insertMu.Unlock()
				return
			}
			insertCount++
			if insertCount%3 == 0 {
				if err := insertLargeTransactionForChunkTesting(vtgateConn, defaultSourceKs, 40000+insertCount*10); err != nil {
					assert.NoError(t, err)
					insertMu.Unlock()
					return
				}
			}
			insertMu.Unlock()
		}
	})

	// stream events from the VStream API
	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	require.NoError(t, err)
	var numRowEvents atomic.Int64
	var copyCompleted, sentinelSeen atomic.Bool
	// second goroutine that continuously receives events via VStream API and should be resilient to the two PRS events
	go func() {
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						numRowEvents.Add(1)
						if isSentinelRow(ev.RowEvent) {
							sentinelSeen.Store(true)
						}
					case binlogdatapb.VEventType_COPY_COMPLETED:
						copyCompleted.Store(true)
					}
				}
			case io.EOF:
				log.Info("Stream Ended")
			default:
				log.Info(fmt.Sprintf("%s:: remote error: %v", time.Now(), err))
			}

			if done.Load() {
				return
			}
		}
	}()

	// run two PRS after one second each, wait for events to be received and exit test
	ticker := time.NewTicker(1 * time.Second)
	tickCount := 0
	// this for loop implements a mini state machine that does the two PRSs, waits a bit after the second PRS,
	// stops the insertions, waits for the vstream to catch up and signals the test to stop
	for {
		<-ticker.C
		tickCount++
		switch tickCount {
		case 1:
			if failover {
				insertMu.Lock()
				output, err := vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", defaultSourceKs+"/0", "--new-primary=zone1-101")
				insertMu.Unlock()
				log.Info("output of first PRS is " + output)
				require.NoError(t, err)
			}
		case 2:
			if failover {
				insertMu.Lock()
				output, err := vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", defaultSourceKs+"/0", "--new-primary=zone1-100")
				insertMu.Unlock()
				log.Info("output of second PRS is " + output)
				require.NoError(t, err)
			}
			time.Sleep(100 * time.Millisecond)
			stopInserting.Store(true)
			inserts.Wait()
			// The stream starts with a copy phase, which a reparent can
			// interrupt and resume. While a table is being copied, its binlog
			// events, the sentinel's among them, can arrive before the rest of
			// its rows, so the sentinel only shows the stream caught up once
			// the copy phase is done.
			require.Eventually(t, copyCompleted.Load, 30*time.Second, 10*time.Millisecond, "the vstream did not complete its copy phase")
			waitForSentinelRow(t, vtgateConn, defaultSourceKs, &sentinelSeen)
			done.Store(true)
		}

		if done.Load() {
			break
		}
	}

	qr, err := execVtgateQuery(vtgateConn, defaultSourceKs, "select count(*) from customer")
	require.NoError(t, err)
	require.NotNil(t, qr)
	// total number of row events found by the VStream API should match the rows inserted
	insertedRows, err := qr.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.Equal(t, insertedRows, numRowEvents.Load())
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
	vtgateConn.ExecuteFetch("use "+keyspace, 1000, false)
	vtgateConn.ExecuteFetch("begin", 1000, false)
	_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into %s (name) values ('%s%d')", table, table, id), 1000, false)
	if err != nil {
		log.Error(fmt.Sprintf("error inserting row %d: %v", id, err))
	}
	vtgateConn.ExecuteFetch("commit", 1000, false)
}

// sentinelCID is the cid of the customer row that a test inserts after its
// other inserts. A single-shard stream delivers commits in order, so once it
// delivers the sentinel row it has delivered every earlier insert. It is
// negative so that it collides with neither explicit nor auto-increment ids.
const sentinelCID = -1

// rowValues returns the column values of row, with NULL as an empty string.
func rowValues(row *querypb.Row) []string {
	values := make([]string, 0, len(row.GetLengths()))
	var offset int64
	for _, length := range row.GetLengths() {
		if length < 0 {
			values = append(values, "")
			continue
		}
		values = append(values, string(row.Values[offset:offset+length]))
		offset += length
	}
	return values
}

// isSentinelRow reports whether ev inserts the customer row with cid sentinelCID.
func isSentinelRow(ev *binlogdatapb.RowEvent) bool {
	if !strings.HasSuffix(ev.GetTableName(), ".customer") {
		return false
	}
	for _, rowChange := range ev.GetRowChanges() {
		if values := rowValues(rowChange.GetAfter()); len(values) > 0 && values[0] == strconv.Itoa(sentinelCID) {
			return true
		}
	}
	return false
}

// waitForSentinelRow inserts the sentinel customer row into keyspace and waits
// until the stream reports it in seen.
func waitForSentinelRow(t *testing.T, vtgateConn *mysql.Conn, keyspace string, seen *atomic.Bool) {
	_, err := execVtgateQuery(vtgateConn, keyspace, fmt.Sprintf("insert into customer (cid, name) values (%d, 'sentinel')", sentinelCID))
	require.NoError(t, err)
	require.Eventually(t, seen.Load, 30*time.Second, 10*time.Millisecond, "the vstream did not deliver the sentinel row")
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
	for i := range 10 {
		insertRow("sharded", "customer", i)
	}

	vc.AddKeyspace(t, []*Cell{defaultCell}, "sharded", "-80,80-", vschemaSharded, schemaSharded, defaultReplicas, defaultRdonly, baseTabletID+200, nil)

	ctx := t.Context()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "sharded",
			Gtid:     "current",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		HeartbeatInterval:    3600,
		StopOnReshard:        stopOnReshard,
		TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
	}
	var done, streamEnded, stopInserts atomic.Bool

	// first goroutine that keeps inserting rows into table being streamed until the test has seen the reshard
	// * if StopOnReshard is false we should keep getting events on the new shards
	// * if StopOnReshard is true we should get a journal event and no events on the new shards
	var inserts sync.WaitGroup
	inserts.Go(func() {
		for id := 1001; !stopInserts.Load(); id++ {
			time.Sleep(250 * time.Millisecond)
			insertRow("sharded", "customer", id)
		}
	})
	stopInserting := func() {
		stopInserts.Store(true)
		inserts.Wait()
	}
	// Stop inserting before the deferred teardown, also when the test fails.
	defer stopInserting()
	// stream events from the VStream API
	var mu sync.Mutex // protects ne
	var ne numEvents
	go func() {
		var reader vtgateconn.VStreamReader
		reader, err = vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		if !assert.NoError(t, err) {
			return
		}
		connect := false
		numErrors := 0
		for {
			if connect { // if vtgate returns a transient error try reconnecting from the last seen vgtid
				reader, err = vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
				if !assert.NoError(t, err) {
					return
				}
				connect = false
			}
			evs, err := reader.Recv()

			switch err {
			case nil:
				mu.Lock()
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
				mu.Unlock()
			case io.EOF:
				log.Info("Stream Ended")
				streamEnded.Store(true)
				done.Store(true)
			default:
				log.Info(fmt.Sprintf("%s:: remote error: %v", time.Now(), err))
				numErrors++
				if numErrors > 10 { // if vtgate is continuously unavailable error the test
					done.Store(true)
					return
				}
				if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
					// this should not happen, but maybe the buffering logic might return a transient
					// error during resharding. So adding this logic to reduce future flakiness
					time.Sleep(100 * time.Millisecond)
					connect = true
				} else {
					// failure, stop test
					done.Store(true)
				}
			}
			if done.Load() {
				return
			}
		}
	}()

	reshard(t, "sharded", "customer", "vstreamStopOnReshard", "-80,80-",
		"-40,40-", baseTabletID+400, nil, nil, nil, nil, defaultCellName, 1,
		reshardOptions{skipSwitchWritesErrorHandling: true})

	// With StopOnReshard the stream ends cleanly on the reshard journal.
	// Without it, the stream must keep going: vtgate starts streaming the new
	// shards only after every old shard has reached the journal, and it sends
	// a journal to the client before that. Once rows from both new shards
	// have arrived, a journal sent by mistake would already have arrived too.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		if stopOnReshard {
			assert.True(c, streamEnded.Load(), "the vstream did not end with io.EOF after the reshard")
			return
		}
		assert.False(c, done.Load(), "the vstream ended")
		mu.Lock()
		defer mu.Unlock()
		assert.Positive(c, ne.numDash40Events, "no row events from shard -40")
		assert.Positive(c, ne.num40DashEvents, "no row events from shard 40-")
	}, time.Minute, 100*time.Millisecond)
	stopInserting()
	done.Store(true)

	mu.Lock()
	defer mu.Unlock()
	result := ne
	return &result
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

	ctx := t.Context()
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}
	defer vstreamConn.Close()
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "/.*",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// We want to confirm that the following two tables are streamed.
			// 1. the customer_seq in the unsharded keyspace
			// 2. the customer table in the sharded keyspace
			Match: "/customer.*/",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
	}
	var done, stopInserts, reshardDone atomic.Bool

	// First goroutine that keeps inserting rows into the table being streamed until the new shards have streamed rows
	// We should keep getting events on the new shards
	var inserts sync.WaitGroup
	inserts.Go(func() {
		for id := 1001; !stopInserts.Load(); id++ {
			time.Sleep(250 * time.Millisecond)
			insertRow("sharded", "customer", id)
		}
	})
	stopInserting := func() {
		stopInserts.Store(true)
		inserts.Wait()
	}
	// Stop inserting before the deferred teardown, also when the test fails.
	defer stopInserting()
	// stream events from the VStream API
	var mu sync.Mutex // protects ne and streamedNames
	var ne numEvents
	streamedNames := make(map[string]bool) // names of the streamed rows of the sharded customer table
	go func() {
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
		if !assert.NoError(t, err) {
			return
		}
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				mu.Lock()
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_ROW:
						shard := ev.RowEvent.Shard
						switch shard {
						case "0":
							if reshardDone.Load() {
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
						if shard != "0" {
							for _, rowChange := range ev.RowEvent.RowChanges {
								if values := rowValues(rowChange.GetAfter()); len(values) > 1 {
									streamedNames[values[1]] = true
								}
							}
						}
						ne.numRowEvents++
					case binlogdatapb.VEventType_JOURNAL:
						ne.numJournalEvents++
					}
				}
				mu.Unlock()
			case io.EOF:
				log.Info("Stream Ended")
				done.Store(true)
			default:
				log.Error(fmt.Sprintf("Returned err %v", err))
				done.Store(true)
			}
			if done.Load() {
				return
			}
		}
	}()

	reshard(t, "sharded", defaultTargetKs, "vstreamCopyMultiKeyspaceReshard", "-80,80-", "-40,40-", baseTabletID+400, nil, nil, nil, nil, defaultCellName, 1)
	reshardDone.Store(true)

	// Once rows from both new shards have arrived, vtgate has finished the
	// reshard transition, so a journal sent by mistake would already have
	// arrived (see testVStreamStopOnReshardFlag).
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, done.Load(), "the vstream ended")
		mu.Lock()
		defer mu.Unlock()
		assert.Positive(c, ne.numDash40Events, "no row events from shard -40")
		assert.Positive(c, ne.num40DashEvents, "no row events from shard 40-")
		assert.Positive(c, ne.numShard0AfterReshardEvents, "no row events from the unsharded keyspace after the reshard")
	}, time.Minute, 100*time.Millisecond)
	stopInserting()

	// Wait until every inserted row has been streamed.
	nameResult, err := execVtgateQuery(vtgateConn, "sharded", "select name from customer")
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		for _, row := range nameResult.Rows {
			assert.Truef(c, streamedNames[row[0].ToString()], "row %s was not streamed", row[0].ToString())
		}
	}, 30*time.Second, 10*time.Millisecond)
	done.Store(true)

	mu.Lock()
	result := ne
	mu.Unlock()
	log.Info(fmt.Sprintf("ne=%v", result))

	// The number of row events streamed by the VStream API should match the number of rows inserted.
	// This is important for sharded tables, where we need to ensure that no row events are missed during the resharding process.
	//
	// On the other hand, we don't verify the exact number of row events for the unsharded keyspace
	// because the keyspace remains unsharded and the number of rows in the customer_seq table is always 1.
	// We believe that checking the number of row events for the unsharded keyspace, which should always be greater than 0 before and after resharding,
	// is sufficient to confirm that the resharding of one keyspace does not affect another keyspace, while keeping the test straightforward.
	customerResult, err := execVtgateQuery(vtgateConn, "sharded", "select count(*) from customer")
	require.NoError(t, err)
	insertedCustomerRows, err := customerResult.Rows[0][0].ToCastInt64()
	require.NoError(t, err)
	require.Equal(t, insertedCustomerRows, result.numDash80Events+result.num80DashEvents+result.numDash40Events+result.num40DashEvents)
	return result
}

// Validate that we can resume a VStream when the keyspace has been resharded
// while not streaming. Ensure that there we successfully transition from the
// old shards -- which are in the VGTID from the previous stream -- and that
// we miss no row events during the process.
func TestMultiVStreamsKeyspaceReshard(t *testing.T) {
	ctx := t.Context()
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
	err = vc.AddShards(t, []*Cell{defaultCell}, keyspace, newShards, defaultReplicas, defaultRdonly, baseTabletID+2000, defaultTargetKsOpts)
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
	stopInserts := make(chan struct{})
	insertsDone := make(chan struct{})

	// First goroutine that keeps inserting rows into the table being streamed until the
	// resumed stream has crossed the reshard, or the stream context is cancelled.
	go func() {
		defer close(insertsDone)
		for id := 1; ; id++ {
			select {
			case <-streamCtx.Done():
				return
			case <-stopInserts:
				return
			default:
				insertRow(ks, "customer", id)
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()

	// Create the Reshard workflow and wait for it to finish the copy phase.
	reshardAction(t, "Create", wf, ks, oldShards, newShards, defaultCellName, tabletType)
	require.NoError(t, waitForWorkflowState(vc, fmt.Sprintf("%s.%s", ks, wf), binlogdatapb.VReplicationWorkflowState_Running.String()))

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "/.*", // Match all keyspaces just to be more realistic.
		}},
	}
	// The names of the customer rows seen in either stream.
	streamedNames := make(map[string]bool)
	recordNames := func(ev *binlogdatapb.RowEvent) {
		for _, rowChange := range ev.RowChanges {
			if values := rowValues(rowChange.GetAfter()); len(values) > 1 {
				streamedNames[values[1]] = true
			}
		}
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// Only stream the customer table and its sequence backing table.
			Match: "/customer.*",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		IncludeReshardJournalEvents: true,
		TransactionChunkSize:        1024, // 1KB - test chunking for all transactions
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
							recordNames(ev.RowEvent)
						case "0":
							// We expect some for the sequence backing table, but don't care.
						default:
							require.FailNow(t, "received event for unexpected shard: "+shard)
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

	require.NoError(t, waitForWorkflowState(vc, fmt.Sprintf("%s.%s", ks, wf), binlogdatapb.VReplicationWorkflowState_Running.String()))

	// Switch the traffic to the new shards.
	reshardAction(t, "SwitchTraffic", wf, ks, oldShards, newShards, defaultCellName, tabletType)

	// Now start a new VStream from our previous VGTID which only has the old/original shards.
	// Once it has crossed the reshard, stop inserting and stream until every inserted
	// row has been seen.
	func() {
		resumeCtx, resumeCancel := context.WithTimeout(ctx, 2*time.Minute)
		defer resumeCancel()
		reader, err := vstreamConn.VStream(resumeCtx, topodatapb.TabletType_PRIMARY, newVGTID, filter, flags)
		require.NoError(t, err)
		var insertedNames []string // nil until the inserts have stopped
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
							recordNames(ev.RowEvent)
						case "-40", "40-80", "80-c0", "c0-":
							newShardRowEvents++
							recordNames(ev.RowEvent)
						case "0":
							// Again, we expect some for the sequence backing table, but don't care.
						default:
							require.FailNow(t, "received event for unexpected shard: "+shard)
						}
					case binlogdatapb.VEventType_JOURNAL:
						require.Equal(t, binlogdatapb.MigrationType_SHARDS, ev.Journal.MigrationType)
						journalEvents++
					}
				}
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error: %v", err),
					"old shard rows: %d, new shard rows: %d, journal events: %d, inserts stopped: %t",
					oldShardRowEvents, newShardRowEvents, journalEvents, insertedNames != nil)
			}
			if insertedNames == nil && newShardRowEvents > 0 && journalEvents > 0 {
				close(stopInserts)
				<-insertsDone
				qr, err := execVtgateQuery(vtgateConn, ks, "select name from customer")
				require.NoError(t, err)
				insertedNames = make([]string, 0, len(qr.Rows))
				for _, row := range qr.Rows {
					insertedNames = append(insertedNames, row[0].ToString())
				}
			}
			if insertedNames != nil && !slices.ContainsFunc(insertedNames, func(name string) bool { return !streamedNames[name] }) {
				return
			}
		}
	}()

	// We should have a mix of events across the old and new shards.
	require.Positive(t, oldShardRowEvents)
	require.Positive(t, newShardRowEvents)
	// We should have seen a reshard journal event.
	require.Positive(t, journalEvents)

	// The number of row events streamed by the VStream API should match the number of rows inserted.
	customerResult, err := execVtgateQuery(vtgateConn, ks, "select count(*) from customer")
	require.NoError(t, err)
	customerCount, err := customerResult.Rows[0][0].ToInt64()
	require.NoError(t, err)
	require.Equal(t, customerCount, int64(oldShardRowEvents+newShardRowEvents))
}

// TestMultiVStreamsKeyspaceStopOnReshard confirms that journal events are received
// when resuming a VStream after a reshard.
func TestMultiVStreamsKeyspaceStopOnReshard(t *testing.T) {
	ctx := t.Context()
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
	err = vc.AddShards(t, []*Cell{defaultCell}, keyspace, newShards, defaultReplicas, defaultRdonly, baseTabletID+2000, defaultTargetKsOpts)
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
	require.NoError(t, waitForWorkflowState(vc, fmt.Sprintf("%s.%s", ks, wf), binlogdatapb.VReplicationWorkflowState_Running.String()))

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			// Only stream the keyspace that we're resharding. Otherwise the client stream
			// will continue to run with only the tablet stream from the global keyspace.
			Keyspace: ks,
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			// Stream all tables.
			Match: "/.*",
		}},
	}
	flags := &vtgatepb.VStreamFlags{
		StopOnReshard:        true,
		TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
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
							require.FailNow(t, "received event for unexpected shard: "+shard)
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
							require.FailNow(t, "received event for unexpected shard: "+shard)
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
	require.Positive(t, ne.numJournalEvents)
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
		"--keyspaces-to-watch", defaultSourceKs,
		// The harness starts vtgate before the keyspace has any tablets, and
		// with --keyspaces-to-watch set, vtgate only becomes ready once the
		// gateway's initial-tablet wait on the watched-but-empty keyspace
		// gives up: the default 30s is a guaranteed dead wait here. The
		// gateway keeps discovering tablets after this timeout either way.
		"--gateway-initial-tablet-timeout", "2s",
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

// doVStream streams the customer and, if flags ask for them, heartbeat events
// until ctx ends. It calls onHeartbeat, if set, with the number of heartbeats
// received so far. It returns an error if the stream could not start or ended
// before ctx did.
func doVStream(t *testing.T, ctx context.Context, vc *VitessCluster, flags *vtgatepb.VStreamFlags, onHeartbeat func(int)) (numRowEvents map[string]int, numFieldEvents map[string]int, err error) {
	numRowEvents = make(map[string]int)
	numFieldEvents = make(map[string]int)
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	if err != nil {
		return nil, nil, err
	}
	defer vstreamConn.Close()

	done := false
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: defaultSourceKs,
			Shard:    "0",
			Gtid:     "",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	// Stream events from the VStream API.
	reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	if err != nil {
		return nil, nil, err
	}
	for !done {
		evs, err := reader.Recv()
		switch err {
		case nil:
			for _, ev := range evs {
				switch ev.Type {
				case binlogdatapb.VEventType_ROW:
					rowEvent := ev.RowEvent
					arr := strings.Split(rowEvent.TableName, ".")
					if !assert.Len(t, arr, 2) {
						continue
					}
					tableName := arr[1]
					assert.Equal(t, defaultSourceKs, rowEvent.Keyspace)
					assert.Equal(t, "0", rowEvent.Shard)
					numRowEvents[tableName]++
					if tableName == "heartbeat" && onHeartbeat != nil {
						onHeartbeat(numRowEvents[tableName])
					}

				case binlogdatapb.VEventType_FIELD:
					fieldEvent := ev.FieldEvent
					arr := strings.Split(fieldEvent.TableName, ".")
					if !assert.Len(t, arr, 2) {
						continue
					}
					tableName := arr[1]
					assert.Equal(t, defaultSourceKs, fieldEvent.Keyspace)
					assert.Equal(t, "0", fieldEvent.Shard)
					numFieldEvents[tableName]++
				default:
				}
			}
		default:
			// The stream must run until ctx ends. Any other end, io.EOF
			// included, would let a stream that stopped early pass as one
			// that received no heartbeats.
			if ctx.Err() == nil {
				return nil, nil, vterrors.Wrapf(err, "the vstream ended before its context")
			}
			done = true
		}
	}
	return numRowEvents, numFieldEvents, nil
}

// TestVStreamHeartbeats enables streaming of the internal Vitess heartbeat tables in the VStream API and
// ensures that the heartbeat events are received as expected by the client.
func TestVStreamHeartbeats(t *testing.T) {
	// Enable continuous heartbeats.
	extraVTTabletArgs = append(extraVTTabletArgs,
		"--heartbeat-enable",
		"--heartbeat-interval", "1s",
		"--heartbeat-on-demand-duration", "0",
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
	vc.AddKeyspace(t, []*Cell{defaultCell}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema,
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
				TransactionChunkSize:     1024, // 1KB - test chunking for all transactions
			},
			expectedHeartbeats: numExpectedHeartbeats,
		},
		{
			name:               "With Keyspace Heartbeats Off",
			flags:              nil,
			expectedHeartbeats: 0,
		},
	}

	// Run both streams at the same time, and stop them once the stream with
	// heartbeats on has received the expected heartbeats. The stream with
	// heartbeats off was open while those heartbeats were written.
	ctx, cancel := context.WithTimeout(t.Context(), vstreamHeartbeatsTestContextTimeout)
	defer cancel()
	gotNumRowEvents := make([]map[string]int, len(testCases))
	gotNumFieldEvents := make([]map[string]int, len(testCases))
	streamErrs := make([]error, len(testCases))
	var streams sync.WaitGroup
	for i, tc := range testCases {
		var onHeartbeat func(int)
		if tc.expectedHeartbeats > 0 {
			onHeartbeat = func(heartbeats int) {
				if heartbeats >= tc.expectedHeartbeats {
					cancel()
				}
			}
		}
		streams.Go(func() {
			gotNumRowEvents[i], gotNumFieldEvents[i], streamErrs[i] = doVStream(t, ctx, vc, tc.flags, onHeartbeat)
		})
	}
	streams.Wait()

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, streamErrs[i])
			for k := range expectedNumRowEvents {
				require.Equalf(t, 1, gotNumFieldEvents[i][k], "incorrect number of field events for table %s, got %d", k, gotNumFieldEvents[i][k])
			}
			if tc.expectedHeartbeats == 0 {
				require.Zero(t, gotNumRowEvents[i]["heartbeat"], "received heartbeat events with keyspace heartbeats off")
			} else {
				require.GreaterOrEqual(t, gotNumRowEvents[i]["heartbeat"], tc.expectedHeartbeats, "incorrect number of heartbeat events received")
			}
			log.Info(fmt.Sprintf("Total number of heartbeat events received: %v", gotNumRowEvents[i]["heartbeat"]))
			delete(gotNumRowEvents[i], "heartbeat")
			require.Equal(t, expectedNumRowEvents, gotNumRowEvents[i])
		})
	}
}

// TestVStreamPushdownFilters confirms that pushdown filters are applied correctly
// when they are specified in the VStream API via the rule.Filter.
// It also confirms that we use the proper collation for the VStream filter when
// using VARCHAR fields.
func TestVStreamPushdownFilters(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	setSidecarDBName("_vt")
	config := *mainClusterConfig
	vc = NewVitessCluster(t, &clusterOptions{
		clusterConfig: &config,
	})
	defer vc.TearDown()
	require.NotNil(t, vc)
	ks := defaultSourceKs
	shard := "0"
	defaultCell := vc.Cells[vc.CellNames[0]]

	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, ks, shard, initialProductVSchema, initialProductSchema, 0, 0, 100, nil)
	require.NoError(t, err)
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	// Make sure that we get at least one paul row event in the copy phase.
	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("insert into `%s`.customer (name) values ('PAUĹ')", ks), 1, false)
	require.NoError(t, err)
	res, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select count(*) from `%s`.customer where name = 'pauĺ'", ks), 1, false)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	startingPauls, err := res.Rows[0][0].ToInt()
	require.NoError(t, err)

	// Coordinate go-routines.
	streamCtx, streamCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer streamCancel()
	insertsDone := make(chan struct{})

	// First goroutine that keeps inserting rows into the table being streamed until the
	// stream context is cancelled, and then inserts the sentinel row, which is a paul too.
	createdPauls := startingPauls
	createdNonPauls := 0
	go func() {
		defer close(insertsDone)
		id := 1
		for {
			select {
			case <-streamCtx.Done():
				_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into `%s`.customer (cid, name) values (%d, 'paÜl')", ks, sentinelCID), 1, false)
				if assert.NoError(t, err) {
					createdPauls++
				}
				return
			default:
				if id%10 == 0 {
					_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("insert into `%s`.customer (name) values ('paÜl')", ks), 1, false)
					if !assert.NoError(t, err) {
						return
					}
					createdPauls++
				} else {
					insertRow(ks, "customer", id)
					createdNonPauls++
				}
				time.Sleep(10 * time.Millisecond)
				id++
			}
		}
	}()

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    shard,
			Gtid:     "",
		}},
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer where name = 'påul'",
		}},
	}
	vstreamConn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateGrpcPort))
	require.NoError(t, err)
	defer vstreamConn.Close()

	// So we should have at least one paul row event in the copy phase, and
	// we should have many paul row events in the running phase.
	copyPhaseRowEvents, runningPhaseRowEvents := runVStreamAndGetNumOfRowEvents(t, ctx, vstreamConn, vgtid, filter)
	<-insertsDone

	require.NotZero(t, createdPauls)
	require.NotZero(t, createdNonPauls)
	require.Greater(t, createdNonPauls, createdPauls)
	require.NotZero(t, copyPhaseRowEvents)
	require.NotZero(t, runningPhaseRowEvents)

	t.Logf("Created pauls: %d, pauls copied: %d, pauls replicated: %d", createdPauls, copyPhaseRowEvents, runningPhaseRowEvents)
	require.Equal(t, createdPauls, copyPhaseRowEvents+runningPhaseRowEvents)
}

// runVStreamAndGetNumOfRowEvents runs VStream with the specified filter until
// it has received the sentinel row and completed the copy phase, and returns
// number of copy phase and running phase row events. The sentinel can arrive
// during the copy phase, before the rest of the copied rows.
func runVStreamAndGetNumOfRowEvents(t *testing.T, ctx context.Context, vstreamConn *vtgateconn.VTGateConn,
	vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter,
) (copyPhaseRowEvents int, runningPhaseRowEvents int) {
	copyPhase := true
	sentinelSeen := false
	func() {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, &vtgatepb.VStreamFlags{
			TransactionChunkSize: 1024, // 1KB - test chunking for all transactions
		})
		require.NoError(t, err)
		for {
			evs, err := reader.Recv()

			switch err {
			case nil:
				for _, ev := range evs {
					switch ev.Type {
					case binlogdatapb.VEventType_COPY_COMPLETED:
						copyPhase = false
					case binlogdatapb.VEventType_ROW:
						if copyPhase {
							copyPhaseRowEvents++
						} else {
							runningPhaseRowEvents++
						}
						sentinelSeen = sentinelSeen || isSentinelRow(ev.RowEvent)
					}
				}
				if sentinelSeen && !copyPhase {
					return
				}
			default:
				require.FailNow(t, fmt.Sprintf("VStream returned unexpected error before the sentinel row: %v", err))
			}
		}
	}()
	return
}
