// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cistring

import (
	"encoding/json"
	"testing"
)

func TestCIString(t *testing.T) {
	str := NewCIString("Ab")
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
}
