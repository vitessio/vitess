// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// CheckVSchema runs the tests on the VSchema part of the API
func CheckVSchema(ctx context.Context, t *testing.T, ts topo.Impl) {
	got, err := ts.GetVSchema(ctx)
	if err != nil {
		t.Error(err)
	}
	want := "{}"
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, `{ "Keyspaces": {}}`)
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx)
	if err != nil {
		t.Error(err)
	}
	want = `{ "Keyspaces": {}}`
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, `{ "Keyspaces": { "aa": { "Sharded": false}}}`)
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx)
	if err != nil {
		t.Error(err)
	}
	want = `{ "Keyspaces": { "aa": { "Sharded": false}}}`
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, "invalid")
	want = "Unmarshal failed:"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("SaveVSchema: %v, must start with %s", err, want)
	}
}
