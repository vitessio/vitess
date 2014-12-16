// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
)

var numksid = NumKSID{}

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

func TestNumKSIDVerify(t *testing.T) {
	success, err := numksid.Verify(nil, 1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
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
