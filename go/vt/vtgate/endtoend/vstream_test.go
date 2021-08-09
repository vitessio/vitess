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
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter, flags)
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
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter, flags)
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
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter, flags)
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
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter, flags)
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
