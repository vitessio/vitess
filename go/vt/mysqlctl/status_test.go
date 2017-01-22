// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import "testing"

func TestStatusSlaveRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  true,
		SlaveSQLRunning: true,
	}
	want := true
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveIONotRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  false,
		SlaveSQLRunning: true,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveSQLNotRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  true,
		SlaveSQLRunning: false,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusMasterAddr(t *testing.T) {
	table := map[string]*Status{
		"master-host:1234": {
			MasterHost: "master-host",
			MasterPort: 1234,
		},
		"[::1]:4321": {
			MasterHost: "::1",
			MasterPort: 4321,
		},
	}
	for want, input := range table {
		if got := input.MasterAddr(); got != want {
			t.Errorf("%#v.MasterAddr() = %v, want %v", input, got, want)
		}
	}
}

func TestNewStatus(t *testing.T) {
	table := map[string]*Status{
		"master-host:1234": {
			MasterHost: "master-host",
			MasterPort: 1234,
		},
		"[::1]:4321": {
			MasterHost: "::1",
			MasterPort: 4321,
		},
	}
	for input, want := range table {
		got, err := NewStatus(input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got.MasterHost != want.MasterHost || got.MasterPort != want.MasterPort {
			t.Errorf("NewStatus(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
