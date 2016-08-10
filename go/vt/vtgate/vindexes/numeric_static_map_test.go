// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
	"reflect"
	"testing"
)

var numericStaticMap Vindex

func init() {
	m := make(map[string]string)
	m["json_path"] = testfiles.Locate("vtgate/numeric_static_map_test.json")
	numericStaticMap, _ = CreateVindex("numeric_static_map", "numericStaticMap", m)
}

func TestNumericStaticMapCost(t *testing.T) {
	if numericStaticMap.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", numericStaticMap.Cost())
	}
}

func TestNumericStaticMapMap(t *testing.T) {
	sqlVal, _ := sqltypes.BuildIntegral("8")
	got, err := numericStaticMap.(Unique).Map(nil, []interface{}{
		1,
		int32(2),
		int64(3),
		uint(4),
		uint32(5),
		uint64(6),
		[]byte("7"),
		sqlVal,
	})
	if err != nil {
		t.Error(err)
	}

	// in the third slice, we expect 2 instead of 3 as numeric_static_map_test.json
	// has 3 mapped to 2
	want := [][]byte{
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x04"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x05"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x06"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x07"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x08"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %+v, want %+v", got, want)
	}
}

func TestNumericStaticMapMapBadData(t *testing.T) {
	_, err := numericStaticMap.(Unique).Map(nil, []interface{}{1.1})
	want := `NumericStaticMap.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("NumericStaticMap.Map: %v, want %v", err, want)
	}
}

func TestNumericStaticMapVerify(t *testing.T) {
	success, err := numericStaticMap.Verify(nil, 1, []byte("\x00\x00\x00\x00\x00\x00\x00\x01"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestNumericStaticMapVerifyBadData(t *testing.T) {
	_, err := numericStaticMap.Verify(nil, 1.1, []byte("\x00\x00\x00\x00\x00\x00\x00\x01"))
	want := `NumericStaticMap.Verify: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numericStaticMap.Map: %v, want %v", err, want)
	}
}
