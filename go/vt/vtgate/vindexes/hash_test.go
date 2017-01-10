// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"strings"
	"testing"
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
	got, err := hash.(Unique).Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
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

	//Negative Test Case
	_, err = hash.(Unique).Map(nil, []interface{}{1.2})
	wanterr := "hash.Map: getNumber: unexpected type for 1.2: float64"
	if err.Error() != wanterr {
		t.Error(err)
	}
}

func TestHashVerify(t *testing.T) {
	success, err := hash.Verify(nil, []interface{}{1}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashVerifyNeg(t *testing.T) {
	_, err := hash.Verify(nil, []interface{}{1, 2}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "hash.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	_, err = hash.Verify(nil, []interface{}{1.2}, [][]byte{[]byte("test1")})
	want = "hash.Verify: getNumber: unexpected type for 1.2: float64"
	if err.Error() != want {
		t.Error(err)
	}

	success, err := hash.Verify(nil, []interface{}{uint(4)}, [][]byte{[]byte("\x06\xe7\xea\"Βp\x8f")})
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
	if got[0].(int64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

func TestHashReverseMapNeg(t *testing.T) {
	_, err := hash.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6\x16k@\xb4J\xbaK\xd6")})
	want := "invalid keyspace id: 166b40b44aba4bd6166b40b44aba4bd6"
	if err.Error() != want {
		t.Error(err)
	}
}
