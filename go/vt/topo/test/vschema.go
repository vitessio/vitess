// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func CheckVSchema(t *testing.T, ts topo.Server) {
	schemafier, ok := ts.(topo.Schemafier)
	if !ok {
		t.Errorf("%T is not a Schemafier", ts)
		return
	}
	got, err := schemafier.GetVSchema()
	if err != nil {
		t.Error(err)
	}
	want := "{}"
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = schemafier.SaveVSchema("aa")
	if err != nil {
		t.Error(err)
	}

	got, err = schemafier.GetVSchema()
	if err != nil {
		t.Error(err)
	}
	want = "aa"
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = schemafier.SaveVSchema("bb")
	if err != nil {
		t.Error(err)
	}

	got, err = schemafier.GetVSchema()
	if err != nil {
		t.Error(err)
	}
	want = "bb"
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}
}
