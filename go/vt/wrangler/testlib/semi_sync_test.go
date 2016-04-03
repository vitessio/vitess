// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"flag"
	"testing"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

func init() {
	// Enable semi-sync for all testlib tests.
	flag.Set("enable_semi_sync", "true")
}

func checkSemiSyncEnabled(t *testing.T, master, slave bool, tablets ...*FakeTablet) {
	for _, tablet := range tablets {
		if got, want := tablet.FakeMysqlDaemon.SemiSyncMasterEnabled, master; got != want {
			t.Errorf("%v: SemiSyncMasterEnabled = %v, want %v", topoproto.TabletAliasString(tablet.Tablet.Alias), got, want)
		}
		if got, want := tablet.FakeMysqlDaemon.SemiSyncSlaveEnabled, slave; got != want {
			t.Errorf("%v: SemiSyncSlaveEnabled = %v, want %v", topoproto.TabletAliasString(tablet.Tablet.Alias), got, want)
		}
	}
}
