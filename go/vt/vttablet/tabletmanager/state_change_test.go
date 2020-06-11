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
)

func TestPublishState(t *testing.T) {
	defer func(saved time.Duration) { *publishRetryInterval = saved }(*publishRetryInterval)
	*publishRetryInterval = 1 * time.Millisecond

	// This flow doesn't test the failure scenario, which
	// we can't do using memorytopo, but we do test the retry
	// code path.

	ctx := context.Background()
	agent := createTestAgent(ctx, t, nil)
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
