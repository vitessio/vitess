// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var numeric planbuilder.Vindex

func init() {
	numeric, _ = planbuilder.CreateVindex("numeric", nil)
}

func TestNumericCost(t *testing.T) {
	if numeric.Cost() != 0 {
		t.Errorf("Cost(): %d, want 0", numeric.Cost())
	}
}

func TestNumericMap(t *testing.T) {
	got, err := numeric.(planbuilder.Unique).Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
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

func TestNumericMapBadData(t *testing.T) {
	_, err := numeric.(planbuilder.Unique).Map(nil, []interface{}{1.1})
	want := `Numeric.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}

func TestNumericVerify(t *testing.T) {
	success, err := numeric.Verify(nil, 1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestNumericVerifyBadData(t *testing.T) {
	_, err := numeric.Verify(nil, 1.1, "\x00\x00\x00\x00\x00\x00\x00\x01")
	want := `Numeric.Verify: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}

func TestNumericReverseMap(t *testing.T) {
	got, err := numeric.(planbuilder.Reversible).ReverseMap(nil, "\x00\x00\x00\x00\x00\x00\x00\x01")
	if err != nil {
		t.Error(err)
	}
	if got.(uint64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

func TestNumericReverseMapBadData(t *testing.T) {
	_, err := numeric.(planbuilder.Reversible).ReverseMap(nil, "aa")
	want := `Numeric.ReverseMap: length of keyspace is not 8: 2`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}
