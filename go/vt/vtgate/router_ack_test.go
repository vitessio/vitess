// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"context"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestRouterMessageAckSharded(t *testing.T) {
	router, sbc1, sbc2, _ := createRouterEnv()

	// Constant in IN is just a number, not a bind variable.
	ids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	count, err := router.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("count: %d, want 1", count)
	}
	if !reflect.DeepEqual(sbc1.MessageIDs, ids) {
		t.Errorf("sbc1.MessageIDs: %v, want %v", sbc1.MessageIDs, ids)
	}
	if sbc2.MessageIDs != nil {
		t.Errorf("sbc2.MessageIDs: %+v, want nil\n", sbc2.MessageIDs)
	}

	// Constant in IN is just a couple numbers, not bind variables.
	// They result in two different MessageIDs on two shards.
	sbc1.MessageIDs = nil
	sbc2.MessageIDs = nil
	ids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	count, err = router.MessageAck(context.Background(), "", "user", ids)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	if !reflect.DeepEqual(sbc1.MessageIDs, wantids) {
		t.Errorf("sbc1.MessageIDs: %+v, want %+v\n", sbc1.MessageIDs, wantids)
	}
	wantids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	if !reflect.DeepEqual(sbc2.MessageIDs, wantids) {
		t.Errorf("sbc2.MessageIDs: %+v, want %+v\n", sbc2.MessageIDs, wantids)
	}
}
