package vindexes

import (
	"bytes"
	"strings"
	"testing"
)

var binOnlyVindex Vindex

func init() {
	binOnlyVindex, _ = CreateVindex("binary", "binary_varchar", nil)
}

func TestBinaryCost(t *testing.T) {
	if binOnlyVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", binOnlyVindex.Cost())
	}
}

func TestBinaryString(t *testing.T) {
	if strings.Compare("binary_varchar", binOnlyVindex.String()) != 0 {
		t.Errorf("String(): %s, want binary_varchar", binOnlyVindex.String())
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
		ok, err := binOnlyVindex.Verify(nil, []interface{}{tcase.in}, [][]byte{tcase.out})
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}

	//Negative Test Case
	_, err := binOnlyVindex.(Unique).Map(nil, []interface{}{1})
	want := "Binary.Map :unexpected data type for getBytes: int"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestBinaryVerifyNeg(t *testing.T) {
	_, err := binOnlyVindex.Verify(nil, []interface{}{[]byte("test1"), []byte("test2")}, [][]byte{[]byte("test1")})
	want := "Binary.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	ok, err := binOnlyVindex.Verify(nil, []interface{}{[]byte("test2")}, [][]byte{[]byte("test1")})
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Errorf("Verify(%#v): true, want false", []byte("test2"))
	}

	_, err = binOnlyVindex.Verify(nil, []interface{}{1}, [][]byte{[]byte("test1")})
	want = "Binary.Verify: unexpected data type for getBytes: int"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestBinaryReverseMap(t *testing.T) {
	got, err := binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(got[0].([]byte), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")) != 0 {
		t.Errorf("ReverseMap(): %+v, want %+v", got, []byte("\x00\x00\x00\x00\x00\x00\x00\x01"))
	}

	//Negative Test
	_, err = binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte(nil)})
	want := "Binary.ReverseMap: keyspaceId is nil"
	if err.Error() != want {
		t.Error(err)
	}
}
