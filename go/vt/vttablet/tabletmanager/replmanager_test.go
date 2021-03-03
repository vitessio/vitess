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
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestReplManagerSetTabletType(t *testing.T) {
	defer func(saved bool) { *mysqlctl.DisableActiveReparents = saved }(*mysqlctl.DisableActiveReparents)
	*mysqlctl.DisableActiveReparents = true

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)

	// DisableActiveReparents == true should result in no-op
	*mysqlctl.DisableActiveReparents = true
	tm.replManager.SetTabletType(topodatapb.TabletType_REPLICA)
	assert.False(t, tm.replManager.ticks.Running())

	// Master should stop the manager
	*mysqlctl.DisableActiveReparents = false
	tm.replManager.ticks.Start(nil)
	tm.replManager.SetTabletType(topodatapb.TabletType_MASTER)
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
	defer func(saved bool) { *mysqlctl.DisableActiveReparents = saved }(*mysqlctl.DisableActiveReparents)
	*mysqlctl.DisableActiveReparents = true

	tm := &TabletManager{}
	tm.replManager = newReplManager(context.Background(), tm, 100*time.Millisecond)

	// DisableActiveReparents == true should result in no-op
	*mysqlctl.DisableActiveReparents = true
	tm.replManager.setReplicationStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
	tm.replManager.setReplicationStopped(false)
	assert.False(t, tm.replManager.ticks.Running())

	*mysqlctl.DisableActiveReparents = false
	tm.replManager.setReplicationStopped(false)
	assert.True(t, tm.replManager.ticks.Running())
	tm.replManager.setReplicationStopped(true)
	assert.False(t, tm.replManager.ticks.Running())
}
