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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
)

func TestPublishState(t *testing.T) {
	defer func(saved time.Duration) { *publishRetryInterval = saved }(*publishRetryInterval)
	*publishRetryInterval = 1 * time.Millisecond

	// This flow doesn't test the failure scenario, which
	// we can't do using memorytopo, but we do test the retry
	// code path.

	ctx := context.Background()
	agent := createTestTM(ctx, t, nil)
	ttablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, agent.Tablet(), ttablet.Tablet)

	tab1 := agent.Tablet()
	tab1.Keyspace = "tab1"
	agent.setTablet(tab1)
	agent.publishState(ctx)
	ttablet, err = agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab1, ttablet.Tablet)

	tab2 := agent.Tablet()
	tab2.Keyspace = "tab2"
	agent.setTablet(tab2)
	agent.retryPublish()
	ttablet, err = agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)

	// If hostname doesn't match, it should not update.
	tab3 := agent.Tablet()
	tab3.Hostname = "tab3"
	agent.setTablet(tab3)
	agent.publishState(ctx)
	ttablet, err = agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)

	// Same for retryPublish.
	agent.retryPublish()
	ttablet, err = agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, tab2, ttablet.Tablet)
}

func TestFindMysqlPort(t *testing.T) {
	defer func(saved time.Duration) { mysqlPortRetryInterval = saved }(mysqlPortRetryInterval)
	mysqlPortRetryInterval = 1 * time.Millisecond

	ctx := context.Background()
	agent := createTestTM(ctx, t, nil)
	err := agent.checkMysql(ctx)
	require.NoError(t, err)
	ttablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	require.NoError(t, err)
	assert.Equal(t, ttablet.MysqlPort, int32(0))

	agent.pubMu.Lock()
	agent.MysqlDaemon.(*fakemysqldaemon.FakeMysqlDaemon).MysqlPort.Set(3306)
	agent.pubMu.Unlock()
	for i := 0; i < 10; i++ {
		ttablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
		require.NoError(t, err)
		if ttablet.MysqlPort == 3306 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Fail(t, "mysql port was not updated")
}
