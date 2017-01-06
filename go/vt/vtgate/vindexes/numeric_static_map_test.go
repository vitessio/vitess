// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
)

// createVindex creates the "numeric_static_map" vindex object which is used by
// each test.
//
// IMPORTANT: This code is called per test and must not be called from init()
// because our internal implementation of testfiles.Locate() does not support to
// be called from init().
func createVindex() (Vindex, error) {
	m := make(map[string]string)
	m["json_path"] = testfiles.Locate("vtgate/numeric_static_map_test.json")
	return CreateVindex("numeric_static_map", "numericStaticMap", m)
}

func TestNumericStaticMapCost(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	if numericStaticMap.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", numericStaticMap.Cost())
	}
}

func TestNumericStaticMapString(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	if strings.Compare("numericStaticMap", numericStaticMap.String()) != 0 {
		t.Errorf("String(): %s, want num", numericStaticMap.String())
	}
}

func TestNumericStaticMapMap(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
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
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	_, err = numericStaticMap.(Unique).Map(nil, []interface{}{1.1})
	want := `NumericStaticMap.Map: getNumber: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("NumericStaticMap.Map: %v, want %v", err, want)
	}
}

func TestNumericStaticMapVerify(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	success, err := numericStaticMap.Verify(nil, []interface{}{1}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestNumericStaticMapVerifyNeg(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	_, err = numericStaticMap.Verify(nil, []interface{}{1, 2}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "NumericStaticMap.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	_, err = numericStaticMap.Verify(nil, []interface{}{1.1}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	want = `NumericStaticMap.Verify: getNumber: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numericStaticMap.Map: %v, want %v", err, want)
	}

	success, err := numericStaticMap.Verify(nil, []interface{}{1}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x02")})
	if err != nil {
		t.Errorf(err.Error())
	}
	if success {
		t.Errorf("Numeric.Verify(): %+v, want false", success)
	}

}
