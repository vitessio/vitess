/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	got, err := numeric.(Unique).Map(nil, [][]sqltypes.Value{
		[]sqltypes.Value{sqltypes.NewInt64(1)},
		[]sqltypes.Value{sqltypes.NewInt64(2)},
		[]sqltypes.Value{sqltypes.NewInt64(3)},
		[]sqltypes.Value{sqltypes.NewFloat64(1.1)},
		[]sqltypes.Value{sqltypes.NewInt64(4)},
		[]sqltypes.Value{sqltypes.NewInt64(5)},
		[]sqltypes.Value{sqltypes.NewInt64(6)},
		[]sqltypes.Value{sqltypes.NewInt64(7)},
		[]sqltypes.Value{sqltypes.NewInt64(8)},
		[]sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(10)},
	})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x03"),
		nil,
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x04"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x05"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x06"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x07"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x08"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %+v, want %+v", got, want)
	}
}

func TestNumericVerify(t *testing.T) {
	got, err := numeric.Verify(nil,
		[][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1)}, []sqltypes.Value{sqltypes.NewInt64(2)}},
		[][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}

	// Multi column test
	got, err = numeric.Verify(nil,
		[][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}},
		[][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02")})
	if err != nil {
		t.Error(err)
	}
	want = []bool{true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}
}

func TestNumericReverseMap(t *testing.T) {
	got, err := numeric.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewUint64(1)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %v, want %v", got, want)
	}
	// Multi column reverse
	got, err = numeric.(Reversible).ReverseMap(
		nil,
		[][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02")},
	)
	if err != nil {
		t.Error(err)
	}
	want = [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewUint64(1), sqltypes.NewUint64(2)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %v, want %v", got, want)
	}
}

func TestNumericReverseMapBadData(t *testing.T) {
	_, err := numeric.(Reversible).ReverseMap(nil, [][]byte{[]byte("aa")})
	want := `Numeric.ReverseMap: length of keyspaceId is not valid: 2`
	if err == nil || err.Error() != want {
		t.Errorf("numeric.Map: %v, want %v", err, want)
	}
}
