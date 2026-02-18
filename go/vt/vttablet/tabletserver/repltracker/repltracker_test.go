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

package repltracker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestReplTracker(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	cfg := tabletenv.NewDefaultConfig()
	cfg.ReplicationTracker.Mode = tabletenv.Heartbeat
	cfg.ReplicationTracker.HeartbeatInterval = time.Second
	params := db.ConnParams()
	cp := *params
	cfg.DB = dbconfigs.NewTestDBConfigs(cp, cp, "")
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	target := &querypb.Target{}
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)

	t.Run("always-on heartbeat", func(t *testing.T) {
		rt := NewReplTracker(env, alias)
		rt.InitDBConfig(target, mysqld)
		assert.Equal(t, tabletenv.Heartbeat, rt.mode)
		assert.Equal(t, HeartbeatConfigTypeAlways, rt.hw.configType)
		assert.Zero(t, rt.hw.onDemandDuration)
		assert.True(t, rt.hr.enabled)

		rt.MakePrimary()
		assert.True(t, rt.hw.isOpen)
		assert.False(t, rt.hr.isOpen)
		assert.True(t, rt.isPrimary)

		lag, err := rt.Status()
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(0), lag)

		rt.MakeNonPrimary()
		assert.False(t, rt.hw.isOpen)
		assert.True(t, rt.hr.isOpen)
		assert.False(t, rt.isPrimary)

		rt.hr.lastKnownLag = 1 * time.Second
		lag, err = rt.Status()
		assert.NoError(t, err)
		assert.Equal(t, 1*time.Second, lag)

		rt.Close()
		assert.False(t, rt.hw.isOpen)
		assert.False(t, rt.hr.isOpen)
	})
	t.Run("disabled heartbeat", func(t *testing.T) {
		cfg.ReplicationTracker.Mode = tabletenv.Polling
		rt := NewReplTracker(env, alias)
		rt.InitDBConfig(target, mysqld)
		assert.Equal(t, tabletenv.Polling, rt.mode)
		assert.Equal(t, mysqld, rt.poller.mysqld)
		assert.Equal(t, HeartbeatConfigTypeNone, rt.hw.configType)
		require.NotZero(t, defaultOnDemandDuration)
		assert.Equal(t, defaultOnDemandDuration, rt.hw.onDemandDuration)
		assert.False(t, rt.hr.enabled)

		rt.MakeNonPrimary()
		assert.False(t, rt.hw.isOpen)
		assert.False(t, rt.hr.isOpen)
		assert.False(t, rt.isPrimary)

		mysqld.ReplicationStatusError = errors.New("err")
		_, err := rt.Status()
		assert.Equal(t, "err", err.Error())
	})
	t.Run("on-demand heartbeat", func(t *testing.T) {
		cfg.ReplicationTracker.Mode = tabletenv.Heartbeat
		cfg.ReplicationTracker.HeartbeatOnDemand = time.Second

		rt := NewReplTracker(env, alias)
		rt.InitDBConfig(target, mysqld)
		assert.Equal(t, tabletenv.Heartbeat, rt.mode)
		assert.Equal(t, HeartbeatConfigTypeOnDemand, rt.hw.configType)
		assert.Equal(t, minimalOnDemandDuration, rt.hw.onDemandDuration)
		assert.True(t, rt.hr.enabled)

		rt.MakePrimary()
		assert.True(t, rt.hw.isOpen)
		assert.False(t, rt.hr.isOpen)
		assert.True(t, rt.isPrimary)

		lag, err := rt.Status()
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(0), lag)

		rt.MakeNonPrimary()
		assert.False(t, rt.hw.isOpen)
		assert.True(t, rt.hr.isOpen)
		assert.False(t, rt.isPrimary)

		rt.hr.lastKnownLag = 1 * time.Second
		lag, err = rt.Status()
		assert.NoError(t, err)
		assert.Equal(t, 1*time.Second, lag)

		rt.Close()
		assert.False(t, rt.hw.isOpen)
		assert.False(t, rt.hr.isOpen)
	})
	t.Run("metric reset on promotion", func(t *testing.T) {
		// Clean up the global metric after test
		defer replicationLagSeconds.Reset()

		rt := NewReplTracker(env, alias)
		rt.InitDBConfig(target, mysqld)

		// Start as replica
		rt.MakeNonPrimary()
		assert.False(t, rt.isPrimary)

		// Simulate having lag (would normally be set by poller)
		replicationLagSeconds.Set(42)
		assert.Equal(t, int64(42), replicationLagSeconds.Get())

		// Promote to primary
		rt.MakePrimary()
		assert.True(t, rt.isPrimary)

		// Verify metric is reset
		assert.Equal(t, int64(0), replicationLagSeconds.Get())

		rt.Close()
	})
	t.Run("metric reset on status when primary", func(t *testing.T) {
		// Clean up the global metric after test
		defer replicationLagSeconds.Reset()

		rt := NewReplTracker(env, alias)
		rt.InitDBConfig(target, mysqld)

		// Set as primary
		rt.MakePrimary()
		assert.True(t, rt.isPrimary)

		// Simulate metric having a stale value (shouldn't happen, but be defensive)
		replicationLagSeconds.Set(99)
		assert.Equal(t, int64(99), replicationLagSeconds.Get())

		// Call Status() which should reset the metric
		lag, err := rt.Status()
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(0), lag)

		// Verify metric is reset
		assert.Equal(t, int64(0), replicationLagSeconds.Get())

		rt.Close()
	})
}
