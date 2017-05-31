/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dtids

import (
	"testing"

	"github.com/golang/protobuf/proto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

func TestDTID(t *testing.T) {
	in := &vtgatepb.Session_ShardSession{
		Target: &querypb.Target{
			Keyspace:   "aa",
			Shard:      "0",
			TabletType: topodatapb.TabletType_MASTER,
		},
		TransactionId: 1,
	}
	dtid := New(in)
	want := "aa:0:1"
	if dtid != want {
		t.Errorf("generateDTID: %s, want %s", dtid, want)
	}
	out, err := ShardSession(dtid)
	if err != nil {
		t.Error(err)
	}
	if !proto.Equal(in, out) {
		t.Errorf("ShardSession: %+v, want %+v", out, in)
	}
	_, err = ShardSession("badParts")
	want = "invalid parts in dtid: badParts"
	if err == nil || err.Error() != want {
		t.Errorf("ShardSession(\"badParts\"): %v, want %s", err, want)
	}
	_, err = ShardSession("a:b:badid")
	want = "invalid transaction id in dtid: a:b:badid"
	if err == nil || err.Error() != want {
		t.Errorf("ShardSession(\"a:b:badid\"): %v, want %s", err, want)
	}
}

func TestTransactionID(t *testing.T) {
	out, err := TransactionID("aa:0:1")
	if err != nil {
		t.Error(err)
	}
	if out != 1 {
		t.Errorf("TransactionID(aa:0:1): %d, want 1", out)
	}
	_, err = TransactionID("badParts")
	want := "invalid parts in dtid: badParts"
	if err == nil || err.Error() != want {
		t.Errorf("TransactionID(\"badParts\"): %v, want %s", err, want)
	}
	_, err = TransactionID("a:b:badid")
	want = "invalid transaction id in dtid: a:b:badid"
	if err == nil || err.Error() != want {
		t.Errorf("TransactionID(\"a:b:badid\"): %v, want %s", err, want)
	}
}
