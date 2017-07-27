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
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
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
	got, err := hash.(Unique).Map(nil, []sqltypes.Value{
		testVal(1),
		testVal(2),
		testVal(3),
		testVal(4),
		testVal(5),
		testVal(6),
	})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
		[]byte("N\xb1\x90ɢ\xfa\x16\x9c"),
		[]byte("\xd2\xfd\x88g\xd5\r-\xfe"),
		[]byte("p\xbb\x02<\x81\f\xa8z"),
		[]byte("\xf0\x98H\n\xc4ľq"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// Negative Test Case.
	_, err = hash.(Unique).Map(nil, []sqltypes.Value{testVal(1.2)})
	wanterr := "hash.Map: could not parse value: 1.2"
	if err == nil || err.Error() != wanterr {
		t.Errorf("hash.Map() error: %v, want %s", err, wanterr)
	}
}

func TestHashVerify(t *testing.T) {
	success, err := hash.Verify(nil, []sqltypes.Value{testVal(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashVerifyNeg(t *testing.T) {
	_, err := hash.Verify(nil, []sqltypes.Value{testVal(1), testVal(2)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "hash.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	_, err = hash.Verify(nil, []sqltypes.Value{testVal(1.2)}, [][]byte{[]byte("test1")})
	wanterr := "hash.Verify: could not parse value: 1.2"
	if err == nil || err.Error() != wanterr {
		t.Errorf("hash.Verify() error: %v, want %s", err, wanterr)
	}

	success, err := hash.Verify(nil, []sqltypes.Value{testVal(4)}, [][]byte{[]byte("\x06\xe7\xea\"Βp\x8f")})
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Verify(): %+v, want false", success)
	}
}

func TestHashReverseMap(t *testing.T) {
	got, err := hash.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	want := []sqltypes.Value{testVal(uint64(1))}
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

func testVal(v interface{}) sqltypes.Value {
	val, err := sqltypes.BuildValue(v)
	if err != nil {
		panic(err)
	}
	return val
}
