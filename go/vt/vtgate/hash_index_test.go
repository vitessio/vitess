package vtgate

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

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
			t.Errorf("vhash(%d): %#v, want %q", c.in, got, want)
		}
		back := vunhash(key.KeyspaceId(got))
		if back != c.in {
			t.Errorf("vunhash(%q): %d, want %d", got, back, c.in)
		}
	}
}

func BenchmarkConvert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vhash(uint64(i))
	}
}

func TestHashResolve(t *testing.T) {
	hind := NewHashIndex(TEST_SHARDED, new(sandboxTopo), "")
	nn, _ := sqltypes.BuildNumeric("11")
	ks, shards, err := hind.Resolve(topo.TabletType("master"), []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6), nn})
	if err != nil {
		t.Error(err)
	}
	want := []string{"-20", "-20", "40-60", "c0-e0", "60-80", "e0-", "a0-c0"}
	if !reflect.DeepEqual(shards, want) {
		t.Errorf("got\n%#v, want\n%#v", shards, want)
	}
	if ks != TEST_SHARDED {
		t.Errorf("got %v, want TEST_SHARDED", ks)
	}
	_, _, err = hind.Resolve(topo.TabletType("master"), []interface{}{"aa"})
	wantErr := "unexpected type for aa: string"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}
}
