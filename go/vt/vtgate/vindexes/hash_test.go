// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
)

var hash *HashVindex

func init() {
	h, err := NewHashVindex(nil)
	if err != nil {
		panic(err)
	}
	hash = h.(*HashVindex)
}

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

func TestCost(t *testing.T) {
	if hash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", hash.Cost())
	}
}

func TestMap(t *testing.T) {
	nn, _ := sqltypes.BuildNumeric("11")
	got, err := hash.Map([]interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6), nn})
	if err != nil {
		t.Error(err)
	}
	want := []key.KeyspaceId{
		"\x16k@\xb4J\xbaK\xd6",
		"\x06\xe7\xea\"Βp\x8f",
		"N\xb1\x90ɢ\xfa\x16\x9c",
		"\xd2\xfd\x88g\xd5\r-\xfe",
		"p\xbb\x02<\x81\f\xa8z",
		"\xf0\x98H\n\xc4ľq",
		"\xae\xfcDI\x1c\xfeGL",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestVerify(t *testing.T) {
	success, err := hash.Verify(1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestReverseMap(t *testing.T) {
	got, err := hash.ReverseMap("\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if got.(uint64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}
