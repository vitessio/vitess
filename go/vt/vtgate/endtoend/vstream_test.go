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

package endtoend

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func initialize(ctx context.Context, t *testing.T) (*vtgateconn.VTGateConn, *mysql.Conn, *mysql.Conn, func()) {
	gconn, err := vtgateconn.Dial(ctx, grpcAddress)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	mconn, err := mysql.Connect(ctx, &mysqlParams)
	if err != nil {
		t.Fatal(err)
	}
	close := func() {
		gconn.Close()
		conn.Close()
		mconn.Close()
	}
	return gconn, conn, mconn, close
}
func TestVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gconn, conn, mconn, closeConnections := initialize(ctx, t)
	defer closeConnections()

	mpos, err := mconn.PrimaryPosition()
	if err != nil {
		t.Fatal(err)
	}
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "ks",
			Shard:    "-80",
			Gtid:     fmt.Sprintf("%s/%s", mpos.GTIDSet.Flavor(), mpos),
		}},
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecuteFetch("insert into vstream_test(id,val) values(1,1), (4,4)", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	// We expect two events because the insert goes to two shards (-80 and 80-),
	// and both of them are in the same mysql server.
	// The row that goes to 80- will have events.
	// The other will be an empty transaction.
	// In a real world scenario where every mysql instance hosts only one
	// keyspace/shard, we should expect only a single event.
	// The events could come in any order as the scatter insert runs in parallel.
	emptyEventSkipped := false
	for i := 0; i < 2; i++ {
		events, err := reader.Recv()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("events: %v\n", events)
		// An empty transaction has three events: begin, gtid and commit.
		if len(events) == 3 && !emptyEventSkipped {
			emptyEventSkipped = true
			continue
		}
		if len(events) != 5 {
			t.Errorf("Unexpected event length: %v", events)
			continue
		}
		wantFields := &binlogdatapb.FieldEvent{
			TableName: "ks.vstream_test",
			Keyspace:  "ks",
			Shard:     "-80",
			Fields: []*querypb.Field{{
				Name: "id",
				Type: querypb.Type_INT64,
			}, {
				Name: "val",
				Type: querypb.Type_INT64,
			}},
		}

		gotFields := events[1].FieldEvent
		filteredFields := &binlogdatapb.FieldEvent{
			TableName: gotFields.TableName,
			Keyspace:  gotFields.Keyspace,
			Shard:     gotFields.Shard,
			Fields:    []*querypb.Field{},
		}
		for _, field := range gotFields.Fields {
			filteredFields.Fields = append(filteredFields.Fields, &querypb.Field{
				Name: field.Name,
				Type: field.Type,
			})
		}
		if !proto.Equal(filteredFields, wantFields) {
			t.Errorf("FieldEvent:\n%v, want\n%v", filteredFields, wantFields)
		}
		wantRows := &binlogdatapb.RowEvent{
			TableName: "ks.vstream_test",
			Keyspace:  "ks",
			Shard:     "-80",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &query.Row{
					Lengths: []int64{1, 1},
					Values:  []byte("11"),
				},
			}},
		}
		gotRows := events[2].RowEvent
		if !proto.Equal(gotRows, wantRows) {
			t.Errorf("RowEvent:\n%v, want\n%v", gotRows, wantRows)
		}
	}
	cancel()
}

func TestVStreamCopyBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gconn, conn, mconn, closeConnections := initialize(ctx, t)
	defer closeConnections()

	_, err := conn.ExecuteFetch("insert into t1(id1,id2) values(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)", 1, false)
	if err != nil {
		t.Fatal(err)
	}

	lastPK := sqltypes.Result{
		Fields: []*query.Field{{Name: "id1", Type: query.Type_INT32}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewInt32(4)}},
	}
	qr := sqltypes.ResultToProto3(&lastPK)
	tablePKs := []*binlogdatapb.TableLastPK{{
		TableName: "t1",
		Lastpk:    qr,
	}}
	var shardGtids []*binlogdatapb.ShardGtid
	var vgtid = &binlogdatapb.VGtid{}
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "-80",
		Gtid:     "",
		TablePKs: tablePKs,
	})
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "80-",
		Gtid:     "",
		TablePKs: tablePKs,
	})
	vgtid.ShardGtids = shardGtids
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	_, _ = conn, mconn
	if err != nil {
		t.Fatal(err)
	}
	numExpectedEvents := 2 /* num shards */ * (7 /* begin/field/vgtid:pos/2 rowevents avg/vgitd: lastpk/commit) */ + 3 /* begin/vgtid/commit for completed table */)
	require.NotNil(t, reader)
	var evs []*binlogdatapb.VEvent
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			evs = append(evs, e...)
			if len(evs) == numExpectedEvents {
				t.Logf("TestVStreamCopyBasic was successful")
				return
			}
			printEvents(evs) // for debugging ci failures
		case io.EOF:
			log.Infof("stream ended\n")
			cancel()
		default:
			log.Errorf("Returned err %v", err)
			t.Fatalf("remote error: %v\n", err)
		}
	}
}

func TestVStreamCopyResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gconn, conn, mconn, closeConnections := initialize(ctx, t)
	defer closeConnections()

	_, err := conn.ExecuteFetch("insert into t1_copy_resume(id1,id2) values(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)", 1, false)
	if err != nil {
		t.Fatal(err)
	}

	// Any subsequent GTIDs will be part of the stream
	mpos, err := mconn.PrimaryPosition()
	require.NoError(t, err)

	// lastPK is id1=4, meaning we should only copy rows for id1 IN(5,6,7,8,9)
	lastPK := sqltypes.Result{
		Fields: []*query.Field{{Name: "id1", Type: query.Type_INT64}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewInt64(4)}},
	}
	tableLastPK := []*binlogdatapb.TableLastPK{{
		TableName: "t1_copy_resume",
		Lastpk:    sqltypes.ResultToProto3(&lastPK),
	}}

	catchupQueries := []string{
		"insert into t1_copy_resume(id1,id2) values(9,9)", // this row will show up twice: once in catchup and copy
		"update t1_copy_resume set id2 = 10 where id1 = 1",
		"insert into t1(id1, id2) values(100,100)",
		"delete from t1_copy_resume where id1 = 1",
		"update t1_copy_resume set id2 = 90 where id1 = 9",
	}
	for _, query := range catchupQueries {
		_, err = conn.ExecuteFetch(query, 1, false)
		require.NoError(t, err)
	}

	var shardGtids []*binlogdatapb.ShardGtid
	var vgtid = &binlogdatapb.VGtid{}
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "-80",
		Gtid:     fmt.Sprintf("%s/%s", mpos.GTIDSet.Flavor(), mpos),
		TablePKs: tableLastPK,
	})
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "80-",
		Gtid:     fmt.Sprintf("%s/%s", mpos.GTIDSet.Flavor(), mpos),
		TablePKs: tableLastPK,
	})
	vgtid.ShardGtids = shardGtids
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1_copy_resume",
			Filter: "select * from t1_copy_resume",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, reader)

	expectedRowCopyEvents := 5                       // id1 and id2 IN(5,6,7,8,9)
	expectedCatchupEvents := len(catchupQueries) - 1 // insert into t1 should never reach
	rowCopyEvents, replCatchupEvents := 0, 0
	expectedEvents := []string{
		`type:ROW timestamp:[0-9]+ row_event:{table_name:"ks.t1_copy_resume" row_changes:{before:{lengths:1 lengths:1 values:"11"} after:{lengths:1 lengths:2 values:"110"}} keyspace:"ks" shard:"-80"} current_time:[0-9]+ keyspace:"ks" shard:"-80"`,
		`type:ROW timestamp:[0-9]+ row_event:{table_name:"ks.t1_copy_resume" row_changes:{before:{lengths:1 lengths:2 values:"110"}} keyspace:"ks" shard:"-80"} current_time:[0-9]+ keyspace:"ks" shard:"-80"`,
		`type:ROW row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:1 values:"55"}} keyspace:"ks" shard:"-80"} keyspace:"ks" shard:"-80"`,
		`type:ROW row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:1 values:"66"}} keyspace:"ks" shard:"80-"} keyspace:"ks" shard:"80-"`,
		`type:ROW row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:1 values:"77"}} keyspace:"ks" shard:"80-"} keyspace:"ks" shard:"80-"`,
		`type:ROW row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:1 values:"88"}} keyspace:"ks" shard:"80-"} keyspace:"ks" shard:"80-"`,
		`type:ROW timestamp:[0-9]+ row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:1 values:"99"}} keyspace:"ks" shard:"-80"} current_time:[0-9]+ keyspace:"ks" shard:"-80"`,
		`type:ROW row_event:{table_name:"ks.t1_copy_resume" row_changes:{after:{lengths:1 lengths:2 values:"990"}} keyspace:"ks" shard:"-80"} keyspace:"ks" shard:"-80"`,
		`type:ROW timestamp:[0-9]+ row_event:{table_name:"ks.t1_copy_resume" row_changes:{before:{lengths:1 lengths:1 values:"99"} after:{lengths:1 lengths:2 values:"990"}} keyspace:"ks" shard:"-80"} current_time:[0-9]+ keyspace:"ks" shard:"-80"`,
	}
	var evs []*binlogdatapb.VEvent
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			for _, ev := range e {
				if ev.Type == binlogdatapb.VEventType_ROW {
					evs = append(evs, ev)
					if ev.Timestamp == 0 {
						rowCopyEvents++
					} else {
						replCatchupEvents++
					}
					printEvents(evs) // for debugging ci failures
				}
			}
			if expectedCatchupEvents == replCatchupEvents && expectedRowCopyEvents == rowCopyEvents {
				sort.Sort(VEventSorter(evs))
				for i, ev := range evs {
					require.Regexp(t, expectedEvents[i], ev.String())
				}
				t.Logf("TestVStreamCopyResume was successful")
				return
			}
		case io.EOF:
			log.Infof("stream ended\n")
			cancel()
		default:
			log.Errorf("Returned err %v", err)
			t.Fatalf("remote error: %v\n", err)
		}
	}
}

func TestVStreamCurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gconn, conn, mconn, closeConnections := initialize(ctx, t)
	defer closeConnections()

	var shardGtids []*binlogdatapb.ShardGtid
	var vgtid = &binlogdatapb.VGtid{}
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "-80",
		Gtid:     "current",
	})
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "80-",
		Gtid:     "current",
	})
	vgtid.ShardGtids = shardGtids
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	_, _ = conn, mconn
	if err != nil {
		t.Fatal(err)
	}
	numExpectedEvents := 4 // vgtid+other per shard for "current"
	require.NotNil(t, reader)
	var evs []*binlogdatapb.VEvent
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			evs = append(evs, e...)
			printEvents(evs) // for debugging ci failures
			if len(evs) == numExpectedEvents {
				t.Logf("TestVStreamCurrent was successful")
				return
			}
		case io.EOF:
			log.Infof("stream ended\n")
			cancel()
		default:
			log.Errorf("Returned err %v", err)
			t.Fatalf("remote error: %v\n", err)
		}
	}
}

func TestVStreamSharded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gconn, conn, mconn, closeConnections := initialize(ctx, t)
	defer closeConnections()

	var shardGtids []*binlogdatapb.ShardGtid
	var vgtid = &binlogdatapb.VGtid{}
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "-80",
		Gtid:     "",
	})
	shardGtids = append(shardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "80-",
		Gtid:     "",
	})
	vgtid.ShardGtids = shardGtids
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1_sharded",
			Filter: "select * from t1_sharded",
		}},
	}
	_, err := conn.ExecuteFetch("insert into t1_sharded(id1,id2) values(1,1), (4,4)", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	_, _ = conn, mconn
	if err != nil {
		t.Fatal(err)
	}
	numExpectedEvents := 4
	require.NotNil(t, reader)
	var evs []*binlogdatapb.VEvent

	type expectedEvent struct {
		ev       string
		received bool
	}
	expectedEvents := []*expectedEvent{
		{`type:FIELD field_event:{table_name:"ks.t1_sharded" fields:{name:"id1" type:INT64 table:"t1_sharded" org_table:"t1_sharded" database:"vt_ks_-80" org_name:"id1" column_length:20 charset:63 flags:53251} fields:{name:"id2" type:INT64 table:"t1_sharded" org_table:"t1_sharded" database:"vt_ks_-80" org_name:"id2" column_length:20 charset:63 flags:32768} keyspace:"ks" shard:"-80"}`, false},
		{`type:ROW row_event:{table_name:"ks.t1_sharded" row_changes:{after:{lengths:1 lengths:1 values:"11"}} keyspace:"ks" shard:"-80"}`, false},
		{`type:FIELD field_event:{table_name:"ks.t1_sharded" fields:{name:"id1" type:INT64 table:"t1_sharded" org_table:"t1_sharded" database:"vt_ks_80-" org_name:"id1" column_length:20 charset:63 flags:53251} fields:{name:"id2" type:INT64 table:"t1_sharded" org_table:"t1_sharded" database:"vt_ks_80-" org_name:"id2" column_length:20 charset:63 flags:32768} keyspace:"ks" shard:"80-"}`, false},
		{`type:ROW row_event:{table_name:"ks.t1_sharded" row_changes:{after:{lengths:1 lengths:1 values:"44"}} keyspace:"ks" shard:"80-"}`, false},
	}
	for {
		events, err := reader.Recv()
		switch err {
		case nil:
			for _, event := range events {
				// check for Keyspace/Shard values first and then reset them so that we don't have to add them to each expected event proto string
				require.Equal(t, "ks", event.Keyspace, "event has an incorrect keyspace attribute: %s", event.Keyspace)
				require.True(t, event.Shard == "-80" || event.Shard == "80-", "event has an incorrect shard attribute: %s", event.Shard)
				event.Keyspace = ""
				event.Shard = ""
				switch event.Type {
				case binlogdatapb.VEventType_ROW, binlogdatapb.VEventType_FIELD:
					evs = append(evs, event)
				default:
				}
			}
			printEvents(evs) // for debugging ci failures
			if len(evs) == numExpectedEvents {
				// events from the two shards -80 and 80- can come in any order, hence this logic
				for _, ev := range evs {
					s := fmt.Sprintf("%v", ev)
					for _, expectedEv := range expectedEvents {
						if expectedEv.ev == s {
							expectedEv.received = true
							break
						}
					}
				}
				for _, expectedEv := range expectedEvents {
					require.Truef(t, expectedEv.received, "event %s not received", expectedEv.ev)
				}

				t.Logf("TestVStreamCurrent was successful")
				return
			}
		case io.EOF:
			log.Infof("stream ended\n")
			cancel()
		default:
			log.Errorf("Returned err %v", err)
			t.Fatalf("remote error: %v\n", err)
		}
	}

}

var printMu sync.Mutex

func printEvents(evs []*binlogdatapb.VEvent) {
	printMu.Lock()
	defer printMu.Unlock()
	if len(evs) == 0 {
		return
	}
	s := "\n===START===" + "\n"
	for i, ev := range evs {
		s += fmt.Sprintf("Event %d; %v\n", i, ev)
	}
	s += "===END===" + "\n"
	log.Infof("%s", s)
}

// Sort the VEvents by the first row change's after value bytes primarily, with
// secondary ordering by timestamp (ASC). Note that row copy events do not have
// a timestamp and the value will be 0.
type VEventSorter []*binlogdatapb.VEvent

func (v VEventSorter) Len() int {
	return len(v)
}
func (v VEventSorter) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
func (v VEventSorter) Less(i, j int) bool {
	valsI := v[i].GetRowEvent().RowChanges[0].After
	if valsI == nil {
		valsI = v[i].GetRowEvent().RowChanges[0].Before
	}
	valsJ := v[j].GetRowEvent().RowChanges[0].After
	if valsJ == nil {
		valsJ = v[j].GetRowEvent().RowChanges[0].Before
	}
	valI := string(valsI.Values)
	valJ := string(valsJ.Values)
	if valI == valJ {
		return v[i].Timestamp < v[j].Timestamp
	}
	return valI < valJ
}
