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

	rt := NewReplTracker(env, alias)
	rt.InitDBConfig(target, mysqld)
	assert.Equal(t, tabletenv.Heartbeat, rt.mode)
	assert.True(t, rt.hw.enabled)
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

	cfg.ReplicationTracker.Mode = tabletenv.Polling
	rt = NewReplTracker(env, alias)
	rt.InitDBConfig(target, mysqld)
	assert.Equal(t, tabletenv.Polling, rt.mode)
	assert.Equal(t, mysqld, rt.poller.mysqld)
	assert.False(t, rt.hw.enabled)
	assert.False(t, rt.hr.enabled)

	rt.MakeNonPrimary()
	assert.False(t, rt.hw.isOpen)
	assert.False(t, rt.hr.isOpen)
	assert.False(t, rt.isPrimary)

	mysqld.ReplicationStatusError = errors.New("err")
	_, err = rt.Status()
	assert.Equal(t, "err", err.Error())
}
