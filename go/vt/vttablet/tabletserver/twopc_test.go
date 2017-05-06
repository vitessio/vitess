/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestReadAllRedo(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	tpc := tsv.te.twoPC
	ctx := context.Background()

	conn, err := tsv.qe.conns.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Recycle()

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{})
	prepared, failed, err := tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []*PreparedTx
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %s, want %s", jsonStr(prepared), jsonStr(want))
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", jsonStr(failed))
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt01")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*PreparedTx{{
		Dtid:    "dtid0",
		Queries: []string{"stmt01"},
		Time:    time.Unix(0, 1),
	}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %s, want %s", jsonStr(prepared), jsonStr(want))
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", jsonStr(failed))
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt02")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*PreparedTx{{
		Dtid:    "dtid0",
		Queries: []string{"stmt01", "stmt02"},
		Time:    time.Unix(0, 1),
	}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %s, want %s", jsonStr(prepared), jsonStr(want))
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", jsonStr(failed))
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt11")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*PreparedTx{{
		Dtid:    "dtid0",
		Queries: []string{"stmt01", "stmt02"},
		Time:    time.Unix(0, 1),
	}, {
		Dtid:    "dtid1",
		Queries: []string{"stmt11"},
		Time:    time.Unix(0, 1),
	}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %s, want %s", jsonStr(prepared), jsonStr(want))
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", jsonStr(failed))
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt11")),
		}, {
			sqltypes.MakeString([]byte("dtid2")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt21")),
		}, {
			sqltypes.MakeString([]byte("dtid2")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt22")),
		}, {
			sqltypes.MakeString([]byte("dtid3")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("stmt31")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*PreparedTx{{
		Dtid:    "dtid0",
		Queries: []string{"stmt01", "stmt02"},
		Time:    time.Unix(0, 1),
	}, {
		Dtid:    "dtid3",
		Queries: []string{"stmt31"},
		Time:    time.Unix(0, 1),
	}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %s, want %s", jsonStr(prepared), jsonStr(want))
	}
	wantFailed := []*PreparedTx{{
		Dtid:    "dtid1",
		Queries: []string{"stmt11"},
		Time:    time.Unix(0, 1),
	}, {
		Dtid:    "dtid2",
		Queries: []string{"stmt21", "stmt22"},
		Time:    time.Unix(0, 1),
	}}
	if !reflect.DeepEqual(failed, wantFailed) {
		t.Errorf("ReadAllRedo failed): %s, want %s", jsonStr(failed), jsonStr(wantFailed))
	}
}

func TestReadAllTransactions(t *testing.T) {
	_, tsv, db := newTestTxExecutor(t)
	defer db.Close()
	defer tsv.StopService()
	tpc := tsv.te.twoPC
	ctx := context.Background()

	conn, err := tsv.qe.conns.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Recycle()

	db.AddQuery(tpc.readAllTransactions, &sqltypes.Result{})
	distributed, err := tpc.ReadAllTransactions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []*DistributedTx
	if !reflect.DeepEqual(distributed, want) {
		t.Errorf("ReadAllTransactions: %s, want %s", jsonStr(distributed), jsonStr(want))
	}

	db.AddQuery(tpc.readAllTransactions, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks01")),
			sqltypes.MakeString([]byte("shard01")),
		}},
	})
	distributed, err = tpc.ReadAllTransactions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*DistributedTx{{
		Dtid:    "dtid0",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []querypb.Target{{
			Keyspace: "ks01",
			Shard:    "shard01",
		}},
	}}
	if !reflect.DeepEqual(distributed, want) {
		t.Errorf("ReadAllTransactions:\n%s, want\n%s", jsonStr(distributed), jsonStr(want))
	}

	db.AddQuery(tpc.readAllTransactions, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks01")),
			sqltypes.MakeString([]byte("shard01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks02")),
			sqltypes.MakeString([]byte("shard02")),
		}},
	})
	distributed, err = tpc.ReadAllTransactions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*DistributedTx{{
		Dtid:    "dtid0",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []querypb.Target{{
			Keyspace: "ks01",
			Shard:    "shard01",
		}, {
			Keyspace: "ks02",
			Shard:    "shard02",
		}},
	}}
	if !reflect.DeepEqual(distributed, want) {
		t.Errorf("ReadAllTransactions:\n%s, want\n%s", jsonStr(distributed), jsonStr(want))
	}

	db.AddQuery(tpc.readAllTransactions, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
			{Type: sqltypes.Int64},
			{Type: sqltypes.Int64},
			{Type: sqltypes.VarChar},
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks01")),
			sqltypes.MakeString([]byte("shard01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks02")),
			sqltypes.MakeString([]byte("shard02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte(strconv.Itoa(RedoStatePrepared))),
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("ks11")),
			sqltypes.MakeString([]byte("shard11")),
		}},
	})
	distributed, err = tpc.ReadAllTransactions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []*DistributedTx{{
		Dtid:    "dtid0",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []querypb.Target{{
			Keyspace: "ks01",
			Shard:    "shard01",
		}, {
			Keyspace: "ks02",
			Shard:    "shard02",
		}},
	}, {
		Dtid:    "dtid1",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []querypb.Target{{
			Keyspace: "ks11",
			Shard:    "shard11",
		}},
	}}
	if !reflect.DeepEqual(distributed, want) {
		t.Errorf("ReadAllTransactions:\n%s, want\n%s", jsonStr(distributed), jsonStr(want))
	}
}

func jsonStr(v interface{}) string {
	out, _ := json.Marshal(v)
	return string(out)
}
