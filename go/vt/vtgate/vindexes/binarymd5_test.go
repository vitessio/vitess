package vindexes

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

var binVindex Vindex

func init() {
	binVindex, _ = CreateVindex("binary_md5", "vch", nil)
}

func TestBinaryMD5Cost(t *testing.T) {
	if binVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", binVindex.Cost())
	}
}

func TestBinaryMD5(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za[",
	}, {
		in:  "TEST",
		out: "\x03;\xd9K\x11h\xd7\xe4\xf0\xd6D\xc3\xc9^5\xbf",
	}, {
		in:  "Test",
		out: "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za[",
	}}
	for _, tcase := range tcases {
		got, err := binVindex.(Unique).Map(nil, []interface{}{[]byte(tcase.in)})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0])
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := binVindex.Verify(nil, []byte(tcase.in), []byte(tcase.out))
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}
}

func TestSQLValue(t *testing.T) {
	val := sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("Test"))
	got, err := binVindex.(Unique).Map(nil, []interface{}{val})
	if err != nil {
		t.Error(err)
	}
	out := string(got[0])
	want := "\f\xbcf\x11\xf5T\vЀ\x9a8\x8d\xc9Za["
	if out != want {
		t.Errorf("Map(%#v): %#v, want %#v", val, out, want)
	}
}
