// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"encoding/json"
	"testing"
)

func TestKey(t *testing.T) {
	k0 := Uint64Key(0).KeyspaceId()
	k1 := Uint64Key(1).KeyspaceId()
	k2 := Uint64Key(0x7FFFFFFFFFFFFFFF).KeyspaceId()
	k3 := Uint64Key(0x8000000000000000).KeyspaceId()
	k4 := Uint64Key(0xFFFFFFFFFFFFFFFF).KeyspaceId()

	f := func(k KeyspaceId, x string) {
		if x != string(k) {
			t.Errorf("byte mismatch %#v != %#v", k, x)
		}
		data, err := json.MarshalIndent(k, "  ", "  ")
		if err != nil {
			t.Errorf("serialize error: %v", err)
		} else {
			t.Logf("json: %v", string(data))
		}

		k_r := new(KeyspaceId)
		err = json.Unmarshal(data, k_r)
		if err != nil {
			t.Errorf("reserialize error: %v", err)
		}

		if k != *k_r {
			t.Errorf("keyspace compare failed: %#v != %#v", k, k_r)
		}
	}

	f(MinKey, "")
	f(k0, "\x00\x00\x00\x00\x00\x00\x00\x00")
	f(k1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	f(k2, "\x7f\xff\xff\xff\xff\xff\xff\xff")
	f(k3, "\x80\x00\x00\x00\x00\x00\x00\x00")
	f(k4, "\xff\xff\xff\xff\xff\xff\xff\xff")

	hv := k4.Hex()
	if hv != "FFFFFFFFFFFFFFFF" {
		t.Errorf("Was expecting FFFFFFFFFFFFFFFF but got %v", hv)
	}
}

func TestKeyUint64Sort(t *testing.T) {
	k0 := Uint64Key(0).KeyspaceId()
	k1 := Uint64Key(1).KeyspaceId()
	k2 := Uint64Key(0x7FFFFFFFFFFFFFFF).KeyspaceId()
	k3 := Uint64Key(0x8000000000000000).KeyspaceId()
	k4 := Uint64Key(0xFFFFFFFFFFFFFFFF).KeyspaceId()
	kl := make([]KeyspaceId, 0, 16)
	klSorted := make([]KeyspaceId, 0, 16)
	kl = append(kl, MinKey, MaxKey, k4, k3, k2, k1, k0)
	klSorted = append(kl, MinKey, k0, k1, k2, k3, k4, MaxKey)
	KeyspaceIdArray(kl).Sort()

	for i, k := range kl {
		if k != klSorted[i] {
			t.Errorf("key order error: %v %v", i, k, klSorted[i])
		}
	}
}

func TestKeyStringSort(t *testing.T) {
	k0 := KeyspaceId("0")
	k1 := KeyspaceId("9")
	k2 := KeyspaceId("Zzzz")
	k3 := KeyspaceId("a")
	k4 := KeyspaceId("z")
	kl := make([]KeyspaceId, 0, 16)
	klSorted := make([]KeyspaceId, 0, 16)
	kl = append(kl, MinKey, MaxKey, k4, k3, k2, k1, k0)
	klSorted = append(kl, MinKey, k0, k1, k2, k3, k4, MaxKey)
	KeyspaceIdArray(kl).Sort()

	for i, k := range kl {
		if k != klSorted[i] {
			t.Errorf("key order error: %v %v", i, k, klSorted[i])
		}
	}
}

func TestParseShardingSpec(t *testing.T) {
	x40, err := HexKeyspaceId("4000000000000000").Unhex()
	if err != nil {
		t.Errorf("Unexpected error: %v.", err)
	}
	x80, err := HexKeyspaceId("8000000000000000").Unhex()
	if err != nil {
		t.Errorf("Unexpected error: %v.", err)
	}

	goodTable := map[string]KeyRangeArray{
		"-": {{Start: MinKey, End: MaxKey}},
		"-4000000000000000-8000000000000000-": {
			{Start: MinKey, End: x40},
			{Start: x40, End: x80},
			{Start: x80, End: MaxKey},
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
			if r[i] != w {
				t.Errorf("Wrong result: wanted %v, got %v", wanted, r)
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
		s, err := HexKeyspaceId(el.start).Unhex()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		e, err := HexKeyspaceId(el.end).Unhex()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		kr := KeyRange{Start: s, End: e}
		k, err := HexKeyspaceId(el.kid).Unhex()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if c := kr.Contains(k); c != el.contained {
			t.Errorf("Unexpected result: contains for %v and (%v-%v) yields %v.", el.kid, el.start, el.end, c)
		}
	}
}
