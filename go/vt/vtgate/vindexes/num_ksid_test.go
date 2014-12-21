// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
)

var numksid NumKSID

func init() {
	vind, _ := NewNumKSID(nil)
	numksid = vind.(NumKSID)
}

func TestNumKSIDCost(t *testing.T) {
	if numksid.Cost() != 0 {
		t.Errorf("Cost(): %d, want 0", numksid.Cost())
	}
}

func TestNumKSIDMap(t *testing.T) {
	got, err := numksid.Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
	if err != nil {
		t.Error(err)
	}
	want := []key.KeyspaceId{
		"\x00\x00\x00\x00\x00\x00\x00\x01",
		"\x00\x00\x00\x00\x00\x00\x00\x02",
		"\x00\x00\x00\x00\x00\x00\x00\x03",
		"\x00\x00\x00\x00\x00\x00\x00\x04",
		"\x00\x00\x00\x00\x00\x00\x00\x05",
		"\x00\x00\x00\x00\x00\x00\x00\x06",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestNumKSIDMapBadData(t *testing.T) {
	_, err := numksid.Map(nil, []interface{}{1.1})
	want := `NumKSID.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numksid.Map: %v, want %v", err, want)
	}
}

func TestNumKSIDVerify(t *testing.T) {
	success, err := numksid.Verify(nil, 1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestNumKSIDVerifyBadData(t *testing.T) {
	_, err := numksid.Verify(nil, 1.1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	want := `NumKSID.Verify: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numksid.Map: %v, want %v", err, want)
	}
}

func TestNumKSIDReverseMap(t *testing.T) {
	got, err := numksid.ReverseMap(nil, "\x00\x00\x00\x00\x00\x00\x00\x01")
	if err != nil {
		t.Error(err)
	}
	if got.(uint64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

func TestNumKSIDReverseMapBadData(t *testing.T) {
	_, err := numksid.ReverseMap(nil, "aa")
	want := `NumKSID.ReverseMap: length of keyspace is not 8: 2`
	if err == nil || err.Error() != want {
		t.Errorf("numksid.Map: %v, want %v", err, want)
	}
}
