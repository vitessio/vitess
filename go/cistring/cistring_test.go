// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cistring

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCIString(t *testing.T) {
	str := NewCIString("Ab")
	if str.String() != "Ab" {
		t.Errorf("String=%s, want Ab", str.Val())
	}
	if str.Val() != "Ab" {
		t.Errorf("Val=%s, want Ab", str.Val())
	}
	if str.Lowered() != "ab" {
		t.Errorf("Val=%s, want ab", str.Lowered())
	}
	if !str.Equal("ab") {
		t.Error("str.Equal(ab)=false, want true")
	}
}

func TestCIStringMarshal(t *testing.T) {
	str := NewCIString("Ab")
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
		NewCIString("Ab"),
		NewCIString("aB"),
	}
	want := []string{"Ab", "aB"}
	got := ToStrings(in)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ToStrings(in)=%+v, want %+v", got, want)
	}
}
