package vindexes

import (
	"testing"
	"bytes"
)

var binOnlyVindex Vindex

func init() {
	binOnlyVindex, _ = CreateVindex("binary", "vch", nil)
}

func TestBinaryCost(t *testing.T) {
	if binOnlyVindex.Cost() != 0 {
		t.Errorf("Cost(): %d, want 0", binOnlyVindex.Cost())
	}
}

func TestBinary(t *testing.T) {
	tcases := []struct {
		in, out []byte
	}{{
		in:  []byte("test"),
		out: []byte("test"),
	}, {
		in:  []byte("test2"),
		out: []byte("test2"),
	}, {
		in:  []byte("test3"),
		out: []byte("test3"),
	}}
	for _, tcase := range tcases {
		got, err := binOnlyVindex.(Unique).Map(nil, []interface{}{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := []byte(got[0])
		if bytes.Compare(tcase.in, out) != 0 {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := binOnlyVindex.Verify(nil, tcase.in, tcase.out)
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}
}

func TestBinaryReverseMap(t *testing.T) {
	got, err := binOnlyVindex.(Reversible).ReverseMap(nil, []byte("\x00\x00\x00\x00\x00\x00\x00\x01"))
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(got.([]byte), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")) != 0 {
		t.Errorf("ReverseMap(): %+v, want %+v", got, []byte("\x00\x00\x00\x00\x00\x00\x00\x01"))
	}
}
