/*
Copyright 2021 The Vitess Authors.

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

package tabletmanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestPromoteReplicaReplicationManagerSuccess checks that the replication manager is not running after running PromoteReplica
// We also assert that replication manager is stopped before we make any changes to MySQL.
func TestPromoteReplicaReplicationManagerSuccess(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	// Stop the replication manager and set the interval to 100 milliseconds
	tm.replManager.ticks.Stop()
	tm.replManager.ticks.SetInterval(100 * time.Millisecond)
	// Change the ticks function of the replication manager so that we can keep the count of how many times it is called
	numTicksRan := 0
	tm.replManager.ticks.Start(func() {
		numTicksRan++
	})
	// Set the promotion lag to a second and then run PromoteReplica
	tm.MysqlDaemon.(*fakemysqldaemon.FakeMysqlDaemon).PromoteLag = time.Second
	_, err := tm.PromoteReplica(ctx, false)
	require.NoError(t, err)
	// At the end we expect the replication manager to be stopped.
	require.False(t, tm.replManager.ticks.Running())
	// We want the replication manager to be stopped before we call Promote on the MySQL instance.
	// Since that call will take over a second to complete, if the replication manager is running, then it will have ticked
	// 9 to 10 times. If we had stopped it before, then the replication manager would have only ticked 1-2 times.
	// So we assert that the numTicksRan is less than 5 to check that the replication manager was closed before we call Promote.
	require.Less(t, numTicksRan, 5)
}

// TestPromoteReplicaReplicationManagerFailure checks that the replication manager is running after running PromoteReplica fails.
func TestPromoteReplicaReplicationManagerFailure(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	require.True(t, tm.replManager.ticks.Running())
	// Set the promotion lag to a second and then run PromoteReplica
	tm.MysqlDaemon.(*fakemysqldaemon.FakeMysqlDaemon).PromoteError = fmt.Errorf("promote error")
	_, err := tm.PromoteReplica(ctx, false)
	require.Error(t, err)
	// At the end we expect the replication manager to be stopped.
	require.True(t, tm.replManager.ticks.Running())
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
