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

var reverseBits Vindex

func init() {
	hv, err := CreateVindex("reverse_bits", "rr", map[string]string{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	reverseBits = hv
}

func TestReverseBitsCost(t *testing.T) {
	if reverseBits.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", reverseBits.Cost())
	}
}

func TestReverseBitsString(t *testing.T) {
	if strings.Compare("rr", reverseBits.String()) != 0 {
		t.Errorf("String(): %s, want hash", reverseBits.String())
	}
}

func TestReverseBitsMap(t *testing.T) {
	got, err := reverseBits.Map(nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NULL,
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
	})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x80\x00\x00\x00\x00\x00\x00\x00")),
		key.DestinationKeyspaceID([]byte("@\x00\x00\x00\x00\x00\x00\x00")),
		key.DestinationKeyspaceID([]byte("\xc0\x00\x00\x00\x00\x00\x00\x00")),
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte(" \x00\x00\x00\x00\x00\x00\x00")),
		key.DestinationKeyspaceID([]byte("\xa0\x00\x00\x00\x00\x00\x00\x00")),
		key.DestinationKeyspaceID([]byte("`\x00\x00\x00\x00\x00\x00\x00")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestReverseBitsVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	ksids := [][]byte{[]byte("\x80\x00\x00\x00\x00\x00\x00\x00"), []byte("\x80\x00\x00\x00\x00\x00\x00\x00")}
	got, err := reverseBits.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("reverseBits.Verify: %v, want %v", got, want)
	}

	// Failure test
	_, err = reverseBits.Verify(nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	wantErr := "reverseBits.Verify: could not parse value: 'aa'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("reverseBits.Verify err: %v, want %s", err, wantErr)
	}
}

func TestReverseBitsReverseMap(t *testing.T) {
	got, err := reverseBits.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x80\x00\x00\x00\x00\x00\x00\x00")})
	if err != nil {
		t.Error(err)
	}
	want := []sqltypes.Value{sqltypes.NewUint64(uint64(1))}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %v, want %v", got, want)
	}
}

func TestReverseBitsReverseMapNeg(t *testing.T) {
	_, err := reverseBits.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x80\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00")})
	want := "invalid keyspace id: 80000000000000008000000000000000"
	if err.Error() != want {
		t.Error(err)
	}
}
