// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

func TestReadAllRedo(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	tpc := tsv.te.twoPC
	ctx := context.Background()

	conn, err := tsv.qe.connPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Recycle()

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{})
	prepared, failed, err := tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %#v, want %#v", prepared, want)
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", failed)
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{"dtid0": {"stmt01"}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %#v, want %#v", prepared, want)
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", failed)
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt02")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{"dtid0": {"stmt01", "stmt02"}}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %#v, want %#v", prepared, want)
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", failed)
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt11")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{
		"dtid0": {"stmt01", "stmt02"},
		"dtid1": {"stmt11"},
	}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %#v, want %#v", prepared, want)
	}
	if len(failed) != 0 {
		t.Errorf("ReadAllRedo (failed): %v, must be empty", failed)
	}

	db.AddQuery(tpc.readAllRedo, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt11")),
		}, {
			sqltypes.MakeString([]byte("dtid2")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt21")),
		}, {
			sqltypes.MakeString([]byte("dtid2")),
			sqltypes.MakeString([]byte("Failed")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt22")),
		}, {
			sqltypes.MakeString([]byte("dtid3")),
			sqltypes.MakeString([]byte("Prepared")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt31")),
		}},
	})
	prepared, failed, err = tpc.ReadAllRedo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{
		"dtid0": {"stmt01", "stmt02"},
		"dtid3": {"stmt31"},
	}
	if !reflect.DeepEqual(prepared, want) {
		t.Errorf("ReadAllRedo: %#v, want %#v", prepared, want)
	}
	wantFailed := []string{"dtid1", "dtid2"}
	if !reflect.DeepEqual(failed, wantFailed) {
		t.Errorf("ReadAllRedo failed): %#v, want %#v", failed, wantFailed)
	}
}
