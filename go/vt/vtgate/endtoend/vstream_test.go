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
	"testing"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/mysql"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func TestVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gconn, err := vtgateconn.Dial(ctx, grpcAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer gconn.Close()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mconn, err := mysql.Connect(ctx, &mysqlParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mpos, err := mconn.MasterPosition()
	if err != nil {
		t.Fatal(err)
	}
	pos := fmt.Sprintf("ks:-80@%s/%s\n", mpos.GTIDSet.Flavor(), mpos)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	reader, err := gconn.VStream(ctx, topodatapb.TabletType_MASTER, pos, filter)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecuteFetch("insert into vstream_test(id,val) values(1,1), (4,4)", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	// We expect two events because the insert goes to two shards, and they're
	// both to the same mysql server.
	// But only one will have rows in it. The other will be an empty transaction.
	for i := 0; i < 2; i++ {
		events, err := reader.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 3 {
			continue
		}
		if len(events) != 5 {
			t.Errorf("Unexpected event length: %v", events)
			continue
		}
		wantFields := &binlogdatapb.FieldEvent{
			TableName: "ks.vstream_test",
			Fields: []*querypb.Field{{
				Name: "id",
				Type: querypb.Type_INT64,
			}, {
				Name: "val",
				Type: querypb.Type_INT64,
			}},
		}
		gotFields := events[2].FieldEvent
		if !proto.Equal(gotFields, wantFields) {
			t.Errorf("FieldEvent:\n%v, want\n%v", gotFields, wantFields)
		}
		wantRows := &binlogdatapb.RowEvent{
			TableName: "ks.vstream_test",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &query.Row{
					Lengths: []int64{1, 1},
					Values:  []byte("11"),
				},
			}},
		}
		gotRows := events[3].RowEvent
		if !proto.Equal(gotRows, wantRows) {
			t.Errorf("RowEvent:\n%v, want\n%v", gotRows, wantRows)
		}
	}
	cancel()
}
