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
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var null Vindex

func init() {
	hv, err := CreateVindex("null", "nn", map[string]string{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	null = hv
}

func TestNullCost(t *testing.T) {
	if null.Cost() != 0 {
		t.Errorf("Cost(): %d, want 0", null.Cost())
	}
}

func TestNullString(t *testing.T) {
	if strings.Compare("nn", null.String()) != 0 {
		t.Errorf("String(): %s, want null", null.String())
	}
}

func TestNullMap(t *testing.T) {
	got, err := null.Map(nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
	})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte{0}),
		key.DestinationKeyspaceID([]byte{0}),
		key.DestinationKeyspaceID([]byte{0}),
		key.DestinationKeyspaceID([]byte{0}),
		key.DestinationKeyspaceID([]byte{0}),
		key.DestinationKeyspaceID([]byte{0}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestNullVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	ksids := [][]byte{{0}, {1}}
	got, err := null.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binaryMD5.Verify: %v, want %v", got, want)
	}
}
