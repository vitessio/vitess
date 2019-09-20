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

var hash Vindex

func init() {
	hv, err := CreateVindex("hash", "nn", map[string]string{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	hash = hv
}

func TestHashCost(t *testing.T) {
	if hash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", hash.Cost())
	}
}

func TestHashString(t *testing.T) {
	if strings.Compare("nn", hash.String()) != 0 {
		t.Errorf("String(): %s, want hash", hash.String())
	}
}

func TestHashMap(t *testing.T) {
	got, err := hash.Map(nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NULL,
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(0),
		sqltypes.NewInt64(-1),
		sqltypes.NewUint64(18446744073709551615), // 2^64 - 1
		sqltypes.NewInt64(9223372036854775807),   // 2^63 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewInt64(-9223372036854775808),  // - 2^63
	})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x06\xe7\xea\"Βp\x8f")),
		key.DestinationKeyspaceID([]byte("N\xb1\x90ɢ\xfa\x16\x9c")),
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("\xd2\xfd\x88g\xd5\r-\xfe")),
		key.DestinationKeyspaceID([]byte("p\xbb\x02<\x81\f\xa8z")),
		key.DestinationKeyspaceID([]byte("\xf0\x98H\n\xc4ľq")),
		key.DestinationKeyspaceID([]byte("\x8c\xa6M\xe9\xc1\xb1#\xa7")),
		key.DestinationKeyspaceID([]byte("5UP\xb2\x15\x0e$Q")),
		key.DestinationKeyspaceID([]byte("5UP\xb2\x15\x0e$Q")),
		key.DestinationKeyspaceID([]byte("\xf7}H\xaaݡ\xf1\xbb")),
		key.DestinationKeyspaceID([]byte("\xf7}H\xaaݡ\xf1\xbb")),
		key.DestinationKeyspaceID([]byte("\x95\xf8\xa5\xe5\xdd1\xd9\x00")),
	}
	if !reflect.DeepEqual(got, want) {
		for i, v := range got {
			if v.String() != want[i].String() {
				t.Errorf("Map() %d: %#v, want %#v", i, v, want[i])
			}
		}
	}
}

func TestHashVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	ksids := [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x16k@\xb4J\xbaK\xd6")}
	got, err := hash.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binaryMD5.Verify: %v, want %v", got, want)
	}

	// Failure test
	_, err = hash.Verify(nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	wantErr := "hash.Verify: could not parse value: 'aa'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("hash.Verify err: %v, want %s", err, wantErr)
	}
}

func TestHashReverseMap(t *testing.T) {
	got, err := hash.(Reversible).ReverseMap(nil, [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
		[]byte("N\xb1\x90ɢ\xfa\x16\x9c"),
		[]byte("\xd2\xfd\x88g\xd5\r-\xfe"),
		[]byte("p\xbb\x02<\x81\f\xa8z"),
		[]byte("\xf0\x98H\n\xc4ľq"),
		[]byte("\x8c\xa6M\xe9\xc1\xb1#\xa7"),
		[]byte("5UP\xb2\x15\x0e$Q"),
		[]byte("5UP\xb2\x15\x0e$Q"),
		[]byte("\xf7}H\xaaݡ\xf1\xbb"),
		[]byte("\xf7}H\xaaݡ\xf1\xbb"),
		[]byte("\x95\xf8\xa5\xe5\xdd1\xd9\x00"),
	})
	if err != nil {
		t.Error(err)
	}
	neg1 := int64(-1)
	negmax := int64(-9223372036854775808)
	want := []sqltypes.Value{
		sqltypes.NewUint64(uint64(1)),
		sqltypes.NewUint64(2),
		sqltypes.NewUint64(3),
		sqltypes.NewUint64(4),
		sqltypes.NewUint64(5),
		sqltypes.NewUint64(6),
		sqltypes.NewUint64(0),
		sqltypes.NewUint64(uint64(neg1)),
		sqltypes.NewUint64(18446744073709551615), // 2^64 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewUint64(9223372036854775807),  // 2^63 - 1
		sqltypes.NewUint64(uint64(negmax)),       // - 2^63
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %v, want %v", got, want)
	}
}

func TestHashReverseMapNeg(t *testing.T) {
	_, err := hash.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6\x16k@\xb4J\xbaK\xd6")})
	want := "invalid keyspace id: 166b40b44aba4bd6166b40b44aba4bd6"
	if err.Error() != want {
		t.Error(err)
	}
}
