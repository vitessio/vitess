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
	"context"
	"fmt"
	"testing"
	"time"

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

// TestDisableReplicationManager checks that the replication manager doesn't start if it is disabled
func TestDisableReplicationManager(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	prevDisableReplicationManager := *disableReplicationManager
	*disableReplicationManager = true
	defer func() {
		*disableReplicationManager = prevDisableReplicationManager
	}()

	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	require.False(t, tm.replManager.ticks.Running())
}
