package testlib

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/topo/topoproto"
)

func init() {
	// Enable semi-sync for all testlib tests.
	flag.Set("enable_semi_sync", "true")
}

func checkSemiSyncEnabled(t *testing.T, primary, replica bool, tablets ...*FakeTablet) {
	for _, tablet := range tablets {
		assert.Equal(t, primary, tablet.FakeMysqlDaemon.SemiSyncPrimaryEnabled, "%v: SemiSyncPrimaryEnabled", topoproto.TabletAliasString(tablet.Tablet.Alias))
		assert.Equal(t, replica, tablet.FakeMysqlDaemon.SemiSyncReplicaEnabled, "%v: SemiSyncReplicaEnabled", topoproto.TabletAliasString(tablet.Tablet.Alias))
	}
}
