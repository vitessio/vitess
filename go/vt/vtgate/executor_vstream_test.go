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

package vtgate

import (
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	"context"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestVStreamFrom(t *testing.T) {
	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{
			TableName: "simple",
			Fields: []*querypb.Field{
				{
					Name: "id",
					Type: querypb.Type_INT64,
				},
				{
					Name: "val",
					Type: querypb.Type_VARCHAR,
				},
			},
		},
		},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{
					After: &querypb.Row{
						Lengths: []int64{1, 3},
						Values:  []byte("1abc"),
					},
				},
				{
					After: &querypb.Row{
						Lengths: []int64{1, 5},
						Values:  []byte("2defgh"),
					},
					Before: &querypb.Row{
						Lengths: []int64{1, 5},
						Values:  []byte("1xefgh"),
					},
				},
				{
					Before: &querypb.Row{
						Lengths: []int64{1, 3},
						Values:  []byte("0xyz"),
					},
				},
			},
		},
		},
		{Type: binlogdatapb.VEventType_COMMIT},
	}

	executor, _, _, sbclookup := createExecutorEnv()
	sbclookup.AddVStreamEvents(send1, nil)
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "vstream * from simple"
	result, err := vstreamEvents(executor, sql)
	require.NoError(t, err)
	want := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "op", Type: sqltypes.VarChar},
			{Name: "id", Type: sqltypes.Int64},
			{Name: "val", Type: sqltypes.VarChar},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("+"),
			sqltypes.NewInt64(1),
			sqltypes.NewVarChar("abc"),
		}, {
			sqltypes.NewVarChar("*"),
			sqltypes.NewInt64(2),
			sqltypes.NewVarChar("defgh"),
		}, {
			sqltypes.NewVarChar("-"),
			sqltypes.NewInt64(0),
			sqltypes.NewVarChar("xyz"),
		}},
	}
	utils.MustMatch(t, want, result)
}

func vstreamEvents(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = executor.StreamExecute(
		ctx,
		"TestVStream",
		NewSafeSession(masterSession),
		sql,
		nil,
		querypb.Target{
			Keyspace:   "TestUnsharded",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
			Cell:       "aa",
		},
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &sqltypes.Result{Fields: r.Fields, RowsAffected: r.RowsAffected}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
	}
	return qr, nil
}
