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

func TestReadPrepared(t *testing.T) {
	// Reuse code from tx_executor_test.
	_, tsv, db := newTestTxExecutor()
	defer tsv.StopService()
	tpc := tsv.qe.twoPC
	ctx := context.Background()

	conn, err := tsv.qe.connPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Recycle()

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{})
	got, err := tpc.ReadPrepared(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadPrepared: %#v, want %#v", got, want)
	}

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}},
	})
	got, err = tpc.ReadPrepared(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{"dtid0": {"stmt01"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadPrepared: %#v, want %#v", got, want)
	}

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt02")),
		}},
	})
	got, err = tpc.ReadPrepared(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{"dtid0": {"stmt01", "stmt02"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadPrepared: %#v, want %#v", got, want)
	}

	db.AddQuery(tpc.readPrepared, &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt01")),
		}, {
			sqltypes.MakeString([]byte("dtid0")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt02")),
		}, {
			sqltypes.MakeString([]byte("dtid1")),
			sqltypes.MakeString([]byte("")),
			sqltypes.MakeString([]byte("stmt11")),
		}},
	})
	got, err = tpc.ReadPrepared(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string][]string{
		"dtid0": {"stmt01", "stmt02"},
		"dtid1": {"stmt11"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadPrepared: %#v, want %#v", got, want)
	}
}
