package vtgate

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestConvert(t *testing.T) {
	cases := []struct {
		in  uint64
		out string
	}{
		{1, "\x16k@\xb4J\xbaK\xd6"},
		{0, "\x8c\xa6M\xe9\xc1\xb1#\xa7"},
		{11, "\xae\xfcDI\x1c\xfeGL"},
		{0x100000000000000, "\r\x9f'\x9b\xa5\xd8r`"},
		{0x800000000000000, " \xb9\xe7g\xb2\xfb\x14V"},
		{11, "\xae\xfcDI\x1c\xfeGL"},
		{0, "\x8c\xa6M\xe9\xc1\xb1#\xa7"},
	}
	for _, c := range cases {
		got := string(vhash(c.in))
		want := c.out
		if got != want {
			t.Errorf("For %d: got: %#v, want %q", c.in, got, want)
		}
	}
}

func BenchmarkConvert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vhash(uint64(i))
	}
}

type topoMocker struct {
}

func (tpm *topoMocker) GetSrvKeyspaceNames(context context.Context, cell string) ([]string, error) {
	return nil, nil
}

func (tpm *topoMocker) GetSrvKeyspace(context context.Context, cell, keyspace string) (*topo.SrvKeyspace, error) {
	noval := key.KeyspaceId("")
	v1 := key.Uint64Key(4 * (1 << 60)).KeyspaceId()
	v2 := key.Uint64Key(8 * (1 << 60)).KeyspaceId()
	v3 := key.Uint64Key(12 * (1 << 60)).KeyspaceId()
	var shards = []topo.SrvShard{
		topo.SrvShard{
			Name: "-40",
			KeyRange: key.KeyRange{
				Start: noval,
				End:   v1,
			},
		},
		topo.SrvShard{
			Name: "40-80",
			KeyRange: key.KeyRange{
				Start: v1,
				End:   v2,
			},
		},
		topo.SrvShard{
			Name: "80-c0",
			KeyRange: key.KeyRange{
				Start: v2,
				End:   v3,
			},
		},
		topo.SrvShard{
			Name: "c0-",
			KeyRange: key.KeyRange{
				Start: v3,
				End:   noval,
			},
		},
	}
	return &topo.SrvKeyspace{
		ServedFrom: map[topo.TabletType]string{
			topo.TabletType("replica"): "other",
		},
		Partitions: map[topo.TabletType]*topo.KeyspacePartition{
			topo.TabletType("master"):  {Shards: shards},
			topo.TabletType("replica"): {Shards: shards},
		},
	}, nil
}

func (tpm *topoMocker) GetEndPoints(context context.Context, cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	return nil, nil
}

var srv = topoMocker{}

func TestHashResolve(t *testing.T) {
	hind := NewHashIndex("main", &srv, "")
	nn, _ := sqltypes.BuildNumeric("11")
	ks, shards, err := hind.Resolve(topo.TabletType("master"), []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6), nn})
	if err != nil {
		t.Error(err)
	}
	want := map[string][]interface{}{
		"-40":   []interface{}{1, int32(2)},
		"40-80": []interface{}{int64(3), uint32(5)},
		"80-c0": []interface{}{nn},
		"c0-":   []interface{}{uint(4), uint64(6)},
	}
	if !reflect.DeepEqual(shards, want) {
		t.Errorf("got\n%#v, want\n%#v", shards, want)
	}
	if ks != "main" {
		t.Errorf("got %v, want main", ks)
	}
	_, _, err = hind.Resolve(topo.TabletType("master"), []interface{}{"aa"})
	wantErr := "unexpected type for aa: string"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}
	ks, _, _ = hind.Resolve(topo.TabletType("replica"), []interface{}{1})
	if ks != "other" {
		t.Errorf("got %v, want other", ks)
	}
}
