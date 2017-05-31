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
