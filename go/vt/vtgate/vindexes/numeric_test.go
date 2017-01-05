// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"strings"

	"github.com/youtube/vitess/go/sqltypes"
)

var numeric Vindex

func init() {
	numeric, _ = CreateVindex("numeric", "num", nil)
}

func TestNumericCost(t *testing.T) {
	if numeric.Cost() != 0 {
		t.Errorf("Cost(): %d, want 0", numeric.Cost())
	}
}

func TestNumericString(t *testing.T) {
	if strings.Compare("num", numeric.String()) != 0 {
		t.Errorf("String(): %s, want num", numeric.String())
	}
}

func TestNumericMap(t *testing.T) {
	sqlVal, _ := sqltypes.BuildIntegral("8")
	got, err := numeric.(Unique).Map(nil, []interface{}{
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
	want := [][]byte{
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x03"),
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

func TestNumericMapBadData(t *testing.T) {
	_, err := numeric.(Unique).Map(nil, []interface{}{1.1})
	want := `Numeric.Map: getNumber: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}

func TestNumericVerify(t *testing.T) {
	success, err := numeric.Verify(nil, []interface{}{1}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestNumericVerifyNeg(t *testing.T) {
	_, err := numeric.Verify(nil, []interface{}{1, 2}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "Numeric.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	_, err = numeric.Verify(nil, []interface{}{1.1}, [][]byte{[]byte("test1")})
	want = "Numeric.Verify: getNumber: unexpected type for 1.1: float64"
	if err.Error() != want {
		t.Error(err)
	}

	success, err := numeric.Verify(nil, []interface{}{uint(4)}, [][]byte{[]byte("\x06\xe7\xea\"Βp\x8f")})
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Numeric.Verify(): %+v, want false", success)
	}
}

func TestNumericReverseMap(t *testing.T) {
	got, err := numeric.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	if got[0].(uint64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

func TestNumericReverseMapBadData(t *testing.T) {
	_, err := numeric.(Reversible).ReverseMap(nil, [][]byte{[]byte("aa")})
	want := `Numeric.ReverseMap: length of keyspaceId is not 8: 2`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}
