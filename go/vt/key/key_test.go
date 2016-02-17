// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"encoding/hex"
	"reflect"
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestKey(t *testing.T) {
	k0 := Uint64Key(0)
	k1 := Uint64Key(1)
	k2 := Uint64Key(0x7FFFFFFFFFFFFFFF)
	k3 := Uint64Key(0x8000000000000000)
	k4 := Uint64Key(0xFFFFFFFFFFFFFFFF)

	f := func(k Uint64Key, x string) {
		hexK := hex.EncodeToString(k.Bytes())
		if x != hexK {
			t.Errorf("byte mismatch %#v != %#v", k, x)
		}
	}

	f(k0, "0000000000000000")
	f(k1, "0000000000000001")
	f(k2, "7fffffffffffffff")
	f(k3, "8000000000000000")
	f(k4, "ffffffffffffffff")
}

func TestParseShardingSpec(t *testing.T) {
	x40 := []byte{0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	x80 := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	goodTable := map[string][]*topodatapb.KeyRange{
		"-": {{}},
		"-4000000000000000-8000000000000000-": {
			{End: x40},
			{Start: x40, End: x80},
			{Start: x80},
		},
	}
	badTable := []string{
		"4000000000000000",
		"---",
		"4000000000000000--8000000000000000",
		"4000000000000000-3000000000000000", // not in order
	}
	for key, wanted := range goodTable {
		r, err := ParseShardingSpec(key)
		if err != nil {
			t.Errorf("Unexpected error: %v.", err)
		}
		if len(r) != len(wanted) {
			t.Errorf("Wrong result: wanted %v, got %v", wanted, r)
			continue
		}
		for i, w := range wanted {
			if !reflect.DeepEqual(r[i], w) {
				t.Errorf("Wrong result: wanted %v, got %v", w, r[i])
				break
			}
		}
	}
	for _, bad := range badTable {
		_, err := ParseShardingSpec(bad)
		if err == nil {
			t.Errorf("Didn't get expected error for %v.", bad)
		}
	}
}

func TestContains(t *testing.T) {
	var table = []struct {
		kid       string
		start     string
		end       string
		contained bool
	}{
		{kid: "3000000000000000", start: "3000000000000000", end: "", contained: true},
		{kid: "3000000000000000", start: "", end: "3000000000000000", contained: false},
		{kid: "4000000000000000", start: "3000000000000000", end: "", contained: true},
		{kid: "2000000000000000", start: "3000000000000000", end: "", contained: false},
	}

	for _, el := range table {
		s, err := hex.DecodeString(el.start)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		e, err := hex.DecodeString(el.end)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		kr := &topodatapb.KeyRange{
			Start: s,
			End:   e,
		}
		k, err := hex.DecodeString(el.kid)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if c := KeyRangeContains(kr, k); c != el.contained {
			t.Errorf("Unexpected result: contains for %v and (%v-%v) yields %v.", el.kid, el.start, el.end, c)
		}
		if !KeyRangeContains(nil, k) {
			t.Errorf("KeyRangeContains(nil, x) should always be true")
		}
	}
}

func TestIntersectOverlap(t *testing.T) {
	var table = []struct {
		a          string
		b          string
		c          string
		d          string
		intersects bool
		overlap    string
	}{
		{a: "40", b: "80", c: "c0", d: "d0", intersects: false},
		{a: "", b: "80", c: "80", d: "", intersects: false},
		{a: "", b: "80", c: "", d: "40", intersects: true, overlap: "-40"},
		{a: "80", b: "", c: "c0", d: "", intersects: true, overlap: "c0-"},
		{a: "", b: "80", c: "40", d: "80", intersects: true, overlap: "40-80"},
		{a: "40", b: "80", c: "60", d: "a0", intersects: true, overlap: "60-80"},
		{a: "40", b: "80", c: "50", d: "60", intersects: true, overlap: "50-60"},
		{a: "40", b: "80", c: "10", d: "50", intersects: true, overlap: "40-50"},
		{a: "40", b: "80", c: "40", d: "80", intersects: true, overlap: "40-80"},
		{a: "", b: "80", c: "", d: "80", intersects: true, overlap: "-80"},
		{a: "40", b: "", c: "40", d: "", intersects: true, overlap: "40-"},
		{a: "40", b: "80", c: "20", d: "40", intersects: false},
		{a: "80", b: "", c: "80", d: "c0", intersects: true, overlap: "80-c0"},
		{a: "", b: "", c: "c0", d: "d0", intersects: true, overlap: "c0-d0"},
	}

	for _, el := range table {
		a, err := hex.DecodeString(el.a)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		b, err := hex.DecodeString(el.b)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		left := &topodatapb.KeyRange{Start: a, End: b}
		c, err := hex.DecodeString(el.c)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		d, err := hex.DecodeString(el.d)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		right := &topodatapb.KeyRange{Start: c, End: d}
		if c := KeyRangesIntersect(left, right); c != el.intersects {
			t.Errorf("Unexpected result: KeyRangesIntersect for %v and %v yields %v.", left, right, c)
		}
		overlap, err := KeyRangesOverlap(left, right)
		if el.intersects {
			if err != nil {
				t.Errorf("Unexpected result: KeyRangesOverlap for overlapping %v and %v returned an error: %v", left, right, err)
			} else {
				got := hex.EncodeToString(overlap.Start) + "-" + hex.EncodeToString(overlap.End)
				if got != el.overlap {
					t.Errorf("Unexpected result: KeyRangesOverlap for overlapping %v and %v should have returned: %v but got: %v", left, right, el.overlap, got)
				}
			}
		} else {
			if err == nil {
				t.Errorf("Unexpected result: KeyRangesOverlap for non-overlapping %v and %v should have returned an error", left, right)
			}
		}
	}
}

func BenchmarkUint64KeyBytes(b *testing.B) {
	keys := []Uint64Key{
		0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			key.Bytes()
		}
	}
}

func BenchmarkUint64KeyString(b *testing.B) {
	keys := []Uint64Key{
		0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			key.String()
		}
	}
}

func BenchmarkKeyRangeContains(b *testing.B) {
	kr := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	keys := [][]byte{
		{0x30, 0, 0, 0, 0, 0, 0, 0},
		{0x40, 0, 0, 0, 0, 0, 0, 0},
		{0x50, 0, 0, 0, 0, 0, 0, 0},
		{0x80, 0, 0, 0, 0, 0, 0, 0},
		{0x90, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			KeyRangeContains(kr, key)
		}
	}
}

func BenchmarkKeyRangesIntersect(b *testing.B) {
	kr1 := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	kr2 := &topodatapb.KeyRange{
		Start: []byte{0x30, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x50, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		KeyRangesIntersect(kr1, kr2)
	}
}

func BenchmarkKeyRangesOverlap(b *testing.B) {
	kr1 := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	kr2 := &topodatapb.KeyRange{
		Start: []byte{0x30, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x50, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		KeyRangesOverlap(kr1, kr2)
	}
}
