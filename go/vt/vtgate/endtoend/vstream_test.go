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
	"regexp"
	"sort"
	"sync"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
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

	_, err := conn.ExecuteFetch("insert into t1_copy_basic(id1,id2) values(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)", 1, false)
	if err != nil {
		t.Fatal(err)
	}

	lastPK := sqltypes.Result{
		Fields: []*query.Field{{Name: "id1", Type: query.Type_INT32}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewInt32(4)}},
	}
	qr := sqltypes.ResultToProto3(&lastPK)
	tablePKs := []*binlogdatapb.TableLastPK{{
		TableName: "t1_copy_basic",
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
			Match:  "t1_copy_basic",
			Filter: "select * from t1_copy_basic",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	_, _ = conn, mconn
	if err != nil {
		t.Fatal(err)
	}
	numExpectedEvents := 2 /* num shards */ *(7 /* begin/field/vgtid:pos/2 rowevents avg/vgitd: lastpk/commit) */ +3 /* begin/vgtid/commit for completed table */ +1 /* copy operation completed */) + 1 /* fully copy operation completed */
	expectedCompletedEvents := []string{
		`type:COPY_COMPLETED keyspace:"ks" shard:"-80"`,
		`type:COPY_COMPLETED keyspace:"ks" shard:"80-"`,
		`type:COPY_COMPLETED`,
	}
	require.NotNil(t, reader)
	var evs []*binlogdatapb.VEvent
	var completedEvs []*binlogdatapb.VEvent
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			evs = append(evs, e...)

			for _, ev := range e {
				if ev.Type == binlogdatapb.VEventType_COPY_COMPLETED {
					completedEvs = append(completedEvs, ev)
				}
			}

			printEvents(evs) // for debugging ci failures

			if len(evs) == numExpectedEvents {
				sortCopyCompletedEvents(completedEvs)
				for i, ev := range completedEvs {
					require.Regexp(t, expectedCompletedEvents[i], ev.String())
				}
				t.Logf("TestVStreamCopyBasic was successful")
				return
			} else if numExpectedEvents < len(evs) {
				t.Fatalf("len(events)=%v are not expected\n", len(evs))
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

// TestVStreamCopyUnspecifiedShardGtid tests the case where the keyspace contains wildcards and/or the shard is not specified in the request.
// Verify that the Vstream API resolves the unspecified ShardGtid input to a list of all the matching keyspaces and all the shards in the topology.
// - If the keyspace contains wildcards and the shard is not specified, the copy operation should be performed on all shards of all matching keyspaces.
// - If the keyspace is specified and the shard is not specified, the copy operation should be performed on all shards of the specified keyspace.
func TestVStreamCopyUnspecifiedShardGtid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		require.NoError(t, err)
	}
	defer conn.Close()

	_, err = conn.ExecuteFetch("insert into t1_copy_all(id1,id2) values(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)", 1, false)
	if err != nil {
		require.NoError(t, err)
	}

	_, err = conn.ExecuteFetch("insert into t1_copy_all_ks2(id1,id2) values(10,10), (20,20)", 1, false)
	if err != nil {
		require.NoError(t, err)
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/t1_copy_all.*/",
		}},
	}
	flags := &vtgatepb.VStreamFlags{}

	// We have 2 shards in each keyspace. We assume the rows are
	// evenly split across each shard. For each INSERT statement, which
	// is a transaction and gets a global transaction identifier or GTID, we
	// have 1 each of the following events:
	//    begin, field, position, lastpk, commit (5)
	// For each row created in the INSERT statement -- 8 on ks1 and
	// 2 on ks2 -- we have 1 row event between the begin and commit.
	// When we have copied all rows for a table in the shard, the shard
	// also gets events marking the transition from the copy phase to
	// the streaming phase for that table with 1 each of the following:
	//    begin, vgtid, commit (3)
	// As the copy phase completes for all tables on the shard, the shard
	// gets 1 copy phase completed event.
	// Lastly the stream has 1 final event to mark the final end to all
	// copy phase operations in the vstream.
	expectedKs1EventNum := 2 /* num shards */ * (9 /* begin/field/vgtid:pos/4 rowevents avg/vgitd: lastpk/commit) */ + 3 /* begin/vgtid/commit for completed table */ + 1 /* copy operation completed */)
	expectedKs2EventNum := 2 /* num shards */ * (6 /* begin/field/vgtid:pos/1 rowevents avg/vgitd: lastpk/commit) */ + 3 /* begin/vgtid/commit for completed table */ + 1 /* copy operation completed */)
	expectedFullyCopyCompletedNum := 1

	cases := []struct {
		name                    string
		shardGtid               *binlogdatapb.ShardGtid
		expectedEventNum        int
		expectedCompletedEvents []string
	}{
		{
			name: "copy from all keyspaces",
			shardGtid: &binlogdatapb.ShardGtid{
				Keyspace: "/.*",
			},
			expectedEventNum: expectedKs1EventNum + expectedKs2EventNum + expectedFullyCopyCompletedNum,
			expectedCompletedEvents: []string{
				`type:COPY_COMPLETED keyspace:"ks" shard:"-80"`,
				`type:COPY_COMPLETED keyspace:"ks" shard:"80-"`,
				`type:COPY_COMPLETED keyspace:"ks2" shard:"-80"`,
				`type:COPY_COMPLETED keyspace:"ks2" shard:"80-"`,
				`type:COPY_COMPLETED`,
			},
		},
		{
			name: "copy from all shards in one keyspace",
			shardGtid: &binlogdatapb.ShardGtid{
				Keyspace: "ks",
			},
			expectedEventNum: expectedKs1EventNum + expectedFullyCopyCompletedNum,
			expectedCompletedEvents: []string{
				`type:COPY_COMPLETED keyspace:"ks" shard:"-80"`,
				`type:COPY_COMPLETED keyspace:"ks" shard:"80-"`,
				`type:COPY_COMPLETED`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gconn, conn, mconn, closeConnections := initialize(ctx, t)
			defer closeConnections()

			var vgtid = &binlogdatapb.VGtid{}
			vgtid.ShardGtids = []*binlogdatapb.ShardGtid{c.shardGtid}
			reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
			_, _ = conn, mconn
			if err != nil {
				require.NoError(t, err)
			}
			require.NotNil(t, reader)
			var evs []*binlogdatapb.VEvent
			var completedEvs []*binlogdatapb.VEvent
			for {
				e, err := reader.Recv()
				switch err {
				case nil:
					evs = append(evs, e...)

					for _, ev := range e {
						if ev.Type == binlogdatapb.VEventType_COPY_COMPLETED {
							completedEvs = append(completedEvs, ev)
						}
					}

					if len(evs) == c.expectedEventNum {
						sortCopyCompletedEvents(completedEvs)
						for i, ev := range completedEvs {
							require.Equal(t, c.expectedCompletedEvents[i], ev.String())
						}
						t.Logf("TestVStreamCopyUnspecifiedShardGtid was successful")
						return
					} else if c.expectedEventNum < len(evs) {
						printEvents(evs) // for debugging ci failures
						require.FailNow(t, fmt.Sprintf("len(events)=%d are not expected\n", len(evs)))
					}
				case io.EOF:
					log.Infof("stream ended\n")
					cancel()
				default:
					log.Errorf("Returned err %v", err)
					require.FailNow(t, "remote error: %v\n", err)
				}
			}
		})
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
		Fields: []*query.Field{{Name: "id1", Type: query.Type_INT64, Charset: collations.CollationBinaryID, Flags: uint32(query.MySqlFlag_NUM_FLAG | query.MySqlFlag_BINARY_FLAG)}},
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
	redash80 := regexp.MustCompile(`(?i)type:VGTID vgtid:{shard_gtids:{keyspace:"ks" shard:"-80" gtid:".+" table_p_ks:{table_name:"t1_copy_resume" lastpk:{fields:{name:"id1" type:INT64 charset:63 flags:[0-9]+} rows:{lengths:1 values:"[0-9]"}}}} shard_gtids:{keyspace:"ks" shard:"80-" gtid:".+"}} keyspace:"ks" shard:"(-80|80-)"`)
	re80dash := regexp.MustCompile(`(?i)type:VGTID vgtid:{shard_gtids:{keyspace:"ks" shard:"-80" gtid:".+"} shard_gtids:{keyspace:"ks" shard:"80-" gtid:".+" table_p_ks:{table_name:"t1_copy_resume" lastpk:{fields:{name:"id1" type:INT64 charset:63 flags:[0-9]+} rows:{lengths:1 values:"[0-9]"}}}}} keyspace:"ks" shard:"(-80|80-)"`)
	both := regexp.MustCompile(`(?i)type:VGTID vgtid:{shard_gtids:{keyspace:"ks" shard:"-80" gtid:".+" table_p_ks:{table_name:"t1_copy_resume" lastpk:{fields:{name:"id1" type:INT64 charset:63 flags:[0-9]+} rows:{lengths:1 values:"[0-9]"}}}} shard_gtids:{keyspace:"ks" shard:"80-" gtid:".+" table_p_ks:{table_name:"t1_copy_resume" lastpk:{fields:{name:"id1" type:INT64 charset:63 flags:[0-9]+} rows:{lengths:1 values:"[0-9]"}}}}} keyspace:"ks" shard:"(-80|80-)"`)
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
				if ev.Type == binlogdatapb.VEventType_VGTID {
					// Validate that the vgtid event the client receives from the vstream copy
					// has a complete TableLastPK proto message.
					// Also, to ensure that the client can resume properly, make sure that
					// the Fields value is present in the sqltypes.Result field and not missing.
					// It's not guaranteed that BOTH shards have streamed a row yet as the order
					// of events in the stream is non-determinstic. So we check to be sure that
					// at least one shard has copied rows and thus has a full TableLastPK proto
					// message.
					eventStr := ev.String()
					require.True(t, redash80.MatchString(eventStr) || re80dash.MatchString(eventStr) || both.MatchString(eventStr),
						"VGTID event does not have a complete TableLastPK proto message for either shard; event: %s", eventStr)
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

// TestVStreamCopyTransactions tests that we are properly wrapping
// ROW events in the stream with BEGIN and COMMIT events.
func TestVStreamCopyTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keyspace := "ks"
	shards := []string{"-80", "80-"}
	table := "t1_copy_basic"
	beginEventSeen, commitEventSeen := false, false
	numResultInTrx := 0
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{
			{
				Keyspace: keyspace,
				Shard:    shards[0],
				Gtid:     "", // Start a vstream copy
			},
			{
				Keyspace: keyspace,
				Shard:    shards[1],
				Gtid:     "", // Start a vstream copy
			},
		},
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  table,
			Filter: fmt.Sprintf("select * from %s", table),
		}},
	}

	gconn, conn, _, closeConnections := initialize(ctx, t)
	defer closeConnections()

	// Clear any existing data.
	q := fmt.Sprintf("delete from %s", table)
	_, err := conn.ExecuteFetch(q, -1, false)
	require.NoError(t, err, "error clearing data: %v", err)

	// Generate some test data. Enough to cross the default
	// vstream_packet_size threshold.
	for i := 1; i <= 100000; i++ {
		values := fmt.Sprintf("(%d, %d)", i, i)
		q := fmt.Sprintf("insert into %s (id1, id2) values %s", table, values)
		_, err := conn.ExecuteFetch(q, 1, false)
		require.NoError(t, err, "error inserting data: %v", err)
	}

	// Start a vstream.
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, nil)
	require.NoError(t, err, "error starting vstream: %v", err)

recvLoop:
	for {
		vevents, err := reader.Recv()
		numResultInTrx++
		eventCount := len(vevents)
		t.Logf("------------------ Received %d events in response #%d for the transaction ------------------\n",
			eventCount, numResultInTrx)
		switch err {
		case nil:
			for _, event := range vevents {
				switch event.Type {
				case binlogdatapb.VEventType_BEGIN:
					require.False(t, beginEventSeen, "received a second BEGIN event within the transaction: numResultInTrx=%d\n",
						numResultInTrx)
					beginEventSeen = true
					t.Logf("Found BEGIN event, beginEventSeen=%t, commitEventSeen=%t, eventType=%v, numResultInTrx=%d\n",
						beginEventSeen, commitEventSeen, event.Type, numResultInTrx)
					require.False(t, commitEventSeen, "received a BEGIN event when expecting a COMMIT event: numResultInTrx=%d\n",
						numResultInTrx)
				case binlogdatapb.VEventType_VGTID:
					t.Logf("Found VGTID event, beginEventSeen=%t, commitEventSeen=%t, eventType=%v, numResultInTrx=%d, event=%+v\n",
						beginEventSeen, commitEventSeen, event.Type, numResultInTrx, event)
				case binlogdatapb.VEventType_FIELD:
					t.Logf("Found FIELD event, beginEventSeen=%t, commitEventSeen=%t, eventType=%v, numResultInTrx=%d, event=%+v\n",
						beginEventSeen, commitEventSeen, event.Type, numResultInTrx, event)
				case binlogdatapb.VEventType_ROW:
					// Uncomment if you need to do more debugging.
					// t.Logf("Found ROW event, beginEventSeen=%t, commitEventSeen=%t, eventType=%v, numResultInTrx=%d, event=%+v\n",
					//	beginEventSeen, commitEventSeen, event.Type, numResultInTrx, event)
				case binlogdatapb.VEventType_COMMIT:
					commitEventSeen = true
					t.Logf("Found COMMIT event, beginEventSeen=%t, commitEventSeen=%t, eventType=%v, numResultInTrx=%d, event=%+v\n",
						beginEventSeen, commitEventSeen, event.Type, numResultInTrx, event)
					require.True(t, beginEventSeen, "received COMMIT event before receiving BEGIN event: numResultInTrx=%d\n",
						numResultInTrx)
				case binlogdatapb.VEventType_COPY_COMPLETED:
					t.Logf("Finished vstream copy\n")
					t.Logf("-------------------------------------------------------------------\n\n")
					cancel()
					break recvLoop
				default:
					t.Logf("Found extraneous event: %+v\n", event)
				}
				if beginEventSeen && commitEventSeen {
					t.Logf("Received both BEGIN and COMMIT, so resetting transactional state\n")
					beginEventSeen = false
					commitEventSeen = false
					numResultInTrx = 0
				}
			}
		case io.EOF:
			t.Logf("vstream ended\n")
			t.Logf("-------------------------------------------------------------------\n\n")
			cancel()
			return
		default:
			require.FailNowf(t, "unexpected error", "encountered error in vstream: %v", err)
			return
		}
	}
	// The last response, when the vstream copy completes, does not
	// typically contain ROW events.
	if beginEventSeen || commitEventSeen {
		require.True(t, (beginEventSeen && commitEventSeen), "did not receive both BEGIN and COMMIT events in the final ROW event set")
	}
}

func removeAnyDeprecatedDisplayWidths(orig string) string {
	var adjusted string
	baseIntType := "int"
	intRE := regexp.MustCompile(`(?i)int\(([0-9]*)?\)`)
	adjusted = intRE.ReplaceAllString(orig, baseIntType)
	baseYearType := "year"
	yearRE := regexp.MustCompile(`(?i)year\(([0-9]*)?\)`)
	adjusted = yearRE.ReplaceAllString(adjusted, baseYearType)
	return adjusted
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

// The arrival order of COPY_COMPLETED events with keyspace/shard is not constant.
// On the other hand, the last event should always be a fully COPY_COMPLETED event.
// That's why the sort.Slice doesn't have to handle the last element in completedEvs.
func sortCopyCompletedEvents(completedEvs []*binlogdatapb.VEvent) {
	sortVEventByKeyspaceAndShard(completedEvs[:len(completedEvs)-1])
}

func sortVEventByKeyspaceAndShard(evs []*binlogdatapb.VEvent) {
	sort.Slice(evs, func(i, j int) bool {
		if evs[i].Keyspace == evs[j].Keyspace {
			return evs[i].Shard < evs[j].Shard
		}
		return evs[i].Keyspace < evs[j].Keyspace
	})
}
