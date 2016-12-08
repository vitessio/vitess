package dtids

import (
	"reflect"
	"testing"

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
	if !reflect.DeepEqual(in, out) {
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
