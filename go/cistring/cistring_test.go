// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cistring

import (
	"encoding/json"
	"reflect"
	"testing"
	"unsafe"
)

func TestCIString(t *testing.T) {
	str := New("Ab")
	if str.String() != "Ab" {
		t.Errorf("String=%s, want Ab", str.Original())
	}
	if str.Original() != "Ab" {
		t.Errorf("Val=%s, want Ab", str.Original())
	}
	if str.Lowered() != "ab" {
		t.Errorf("Val=%s, want ab", str.Lowered())
	}
	str2 := New("aB")
	if !str.Equal(str2) {
		t.Error("str.Equal(New(aB))=false, want true")
	}
	if !str.EqualString("ab") {
		t.Error("str.Equal(ab)=false, want true")
	}
}

func TestCIStringMarshal(t *testing.T) {
	str := New("Ab")
	b, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `"Ab"`
	if got != want {
		t.Errorf("json.Marshal()= %s, want %s", got, want)
	}
	var out CIString
	err = json.Unmarshal(b, &out)
	if !reflect.DeepEqual(out, str) {
		t.Errorf("Unmarshal: %v, want %v", out, str)
	}
}

func TestToStrings(t *testing.T) {
	in := []CIString{
		New("Ab"),
		New("aB"),
	}
	want := []string{"Ab", "aB"}
	got := ToStrings(in)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ToStrings(in)=%+v, want %+v", got, want)
	}
}

func TestSize(t *testing.T) {
	size := unsafe.Sizeof(New(""))
	want := 2 * unsafe.Sizeof("")
	if size != want {
		t.Errorf("Size of CIString: %d, want 32", want)
	}
}
