// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
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
}

func TestHashVerify(t *testing.T) {
	success, err := hash.Verify(nil, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashReverseMap(t *testing.T) {
	got, err := hash.(Reversible).ReverseMap(nil, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got.(int64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}
