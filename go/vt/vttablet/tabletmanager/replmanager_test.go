/*
Copyright 2020 The Vitess Authors.

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
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestReplManagerSetTabletType(t *testing.T) {
	defer func(saved bool) { mysqlctl.DisableActiveReparents = saved }(mysqlctl.DisableActiveReparents)
	mysqlctl.DisableActiveReparents = true

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)

	// DisableActiveReparents == true should result in no-op
	mysqlctl.DisableActiveReparents = true
	tm.replManager.SetTabletType(topodatapb.TabletType_REPLICA)
	assert.False(t, tm.replManager.ticks.Running())

	// primary should stop the manager
	mysqlctl.DisableActiveReparents = false
	tm.replManager.ticks.Start(nil)
	tm.replManager.SetTabletType(topodatapb.TabletType_PRIMARY)
	assert.False(t, tm.replManager.ticks.Running())

	// If replcation is stopped, the manager should not start
	tm.replManager.setReplicationStopped(true)
	tm.replManager.ticks.Start(nil)
	tm.replManager.SetTabletType(topodatapb.TabletType_REPLICA)
	assert.False(t, tm.replManager.ticks.Running())

	// If the manager is already running, rm.check should not be called
	tm.replManager.setReplicationStopped(false)
	tm.replManager.ticks.Start(nil)
	tm.replManager.SetTabletType(topodatapb.TabletType_REPLICA)
	assert.True(t, tm.replManager.ticks.Running())
	tm.replManager.ticks.Stop()
}

func TestReplManagerSetReplicationStopped(t *testing.T) {
	defer func(saved bool) { mysqlctl.DisableActiveReparents = saved }(mysqlctl.DisableActiveReparents)
	mysqlctl.DisableActiveReparents = true

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)

	// DisableActiveReparents == true should result in no-op
	mysqlctl.DisableActiveReparents = true
	tm.replManager.setReplicationStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
	tm.replManager.setReplicationStopped(false)
	assert.False(t, tm.replManager.ticks.Running())

	mysqlctl.DisableActiveReparents = false
	tm.replManager.setReplicationStopped(false)
	assert.True(t, tm.replManager.ticks.Running())
	tm.replManager.setReplicationStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
}

func TestReplManagerSetReplicationPermanentlyStopped(t *testing.T) {
	defer func(saved bool) { mysqlctl.DisableActiveReparents = saved }(mysqlctl.DisableActiveReparents)
	mysqlctl.DisableActiveReparents = true

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)
	tm.replManager.markerFile = path.Join(os.TempDir(), "markerfile")
	defer func() {
		os.Remove(tm.replManager.markerFile)
	}()

	// DisableActiveReparents == true should result in no-op
	mysqlctl.DisableActiveReparents = true
	tm.replManager.setReplicationPermanentlyStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
	checkIfMarkerFileExists(t, tm, false)
	tm.replManager.setReplicationPermanentlyStopped(false)
	assert.False(t, tm.replManager.ticks.Running())
	checkIfMarkerFileExists(t, tm, false)

	mysqlctl.DisableActiveReparents = false
	tm.replManager.setReplicationPermanentlyStopped(false)
	checkIfMarkerFileExists(t, tm, false)
	assert.True(t, tm.replManager.ticks.Running())
	tm.replManager.setReplicationPermanentlyStopped(true)
	checkIfMarkerFileExists(t, tm, true)
	assert.False(t, tm.replManager.ticks.Running())
}

func checkIfMarkerFileExists(t *testing.T, tm *TabletManager, exists bool) {
	_, err := os.Stat(tm.replManager.markerFile)
	fileExists := err == nil
	require.Equal(t, exists, fileExists)
}

func TestReplManagerReset(t *testing.T) {
	defer func(saved bool) { mysqlctl.DisableActiveReparents = saved }(mysqlctl.DisableActiveReparents)

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)

	mysqlctl.DisableActiveReparents = false
	tm.replManager.setReplicationStopped(false)
	assert.True(t, tm.replManager.ticks.Running())
	// reset should not affect the ticks, but the replication stopped should be false
	tm.replManager.reset()
	assert.True(t, tm.replManager.ticks.Running())
	assert.False(t, tm.replManager.replicationStopped())

	tm.replManager.setReplicationStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
	tm.replManager.reset()
	// reset should not affect the ticks, but the replication stopped should be false
	assert.False(t, tm.replManager.ticks.Running())
	assert.False(t, tm.replManager.replicationStopped())

	tm.replManager.setReplicationStopped(true)
	assert.True(t, tm.replManager.replicationStopped())
	// DisableActiveReparents == true should result in no-op
	mysqlctl.DisableActiveReparents = true
	tm.replManager.reset()
	assert.True(t, tm.replManager.replicationStopped())
}
