package tabletmanager

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestPromoteReplicaHealthTicksStopped checks that the health ticks are not running on the
// replication manager after running PromoteReplica
func TestPromoteReplicaHealthTicksStopped(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	_, err := tm.PromoteReplica(ctx, false)
	require.NoError(t, err)
	require.False(t, tm.replManager.ticks.Running())
}

func captureStderr(f func()) (string, error) {
	old := os.Stderr // keep backup of the real stderr
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stderr = w

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// calling function which stderr we are going to capture:
	f()

	// back to normal state
	w.Close()
	os.Stderr = old // restoring the real stderr
	return <-outC, nil
}

func TestTabletManager_fixSemiSync(t *testing.T) {
	tests := []struct {
		name                 string
		tabletType           topodata.TabletType
		semiSync             SemiSyncAction
		logOutput            string
		shouldEnableSemiSync bool
	}{
		{
			name:                 "enableSemiSync=true(primary eligible),durabilitySemiSync=true",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionSet,
			logOutput:            "",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=true(primary eligible),durabilitySemiSync=false",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionUnset,
			logOutput:            "invalid configuration - enabling semi sync even though not specified by durability policies.",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=true(primary eligible),durabilitySemiSync=none",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionNone,
			logOutput:            "",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=true(primary not-eligible),durabilitySemiSync=true",
			tabletType:           topodata.TabletType_DRAINED,
			semiSync:             SemiSyncActionSet,
			logOutput:            "invalid configuration - semi-sync should be setup according to durability policies, but the tablet is not primaryEligible",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=true(primary not-eligible),durabilitySemiSync=false",
			tabletType:           topodata.TabletType_DRAINED,
			semiSync:             SemiSyncActionUnset,
			logOutput:            "",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=true(primary not-eligible),durabilitySemiSync=none",
			tabletType:           topodata.TabletType_DRAINED,
			semiSync:             SemiSyncActionNone,
			logOutput:            "",
			shouldEnableSemiSync: true,
		}, {
			name:                 "enableSemiSync=false,durabilitySemiSync=true",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionSet,
			logOutput:            "invalid configuration - semi-sync should be setup according to durability policies, but enable_semi_sync is not set",
			shouldEnableSemiSync: false,
		}, {
			name:                 "enableSemiSync=false,durabilitySemiSync=false",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionUnset,
			logOutput:            "",
			shouldEnableSemiSync: false,
		}, {
			name:                 "enableSemiSync=false,durabilitySemiSync=none",
			tabletType:           topodata.TabletType_REPLICA,
			semiSync:             SemiSyncActionNone,
			logOutput:            "",
			shouldEnableSemiSync: false,
		},
	}
	oldEnableSemiSync := *enableSemiSync
	defer func() {
		*enableSemiSync = oldEnableSemiSync
	}()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*enableSemiSync = tt.shouldEnableSemiSync
			fakeMysql := fakemysqldaemon.NewFakeMysqlDaemon(nil)
			tm := &TabletManager{
				MysqlDaemon: fakeMysql,
			}
			logOutput, err := captureStderr(func() {
				err := tm.fixSemiSync(tt.tabletType, tt.semiSync)
				require.NoError(t, err)
			})
			require.NoError(t, err)
			if tt.logOutput != "" {
				require.Contains(t, logOutput, tt.logOutput)
			} else {
				require.Equal(t, "", logOutput)
			}
		})
	}
}
