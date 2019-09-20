/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

// createVindex creates the "numeric_static_map" vindex object which is used by
// each test.
func createVindex() (Vindex, error) {
	m := make(map[string]string)
	m["json_path"] = "testdata/numeric_static_map_test.json"
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
	got, err := numericStaticMap.Map(nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewFloat64(1.1),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(7),
		sqltypes.NewInt64(8),
	})
	if err != nil {
		t.Error(err)
	}

	// in the third slice, we expect 2 instead of 3 as numeric_static_map_test.json
	// has 3 mapped to 2
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x01")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x04")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x05")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x06")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x07")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x08")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %+v, want %+v", got, want)
	}
}

func TestNumericStaticMapVerify(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := numericStaticMap.Verify(nil,
		[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		[][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}

	// Failure test
	_, err = numericStaticMap.Verify(nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	wantErr := "NumericStaticMap.Verify: could not parse value: 'aa'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("hash.Verify err: %v, want %s", err, wantErr)
	}
}
