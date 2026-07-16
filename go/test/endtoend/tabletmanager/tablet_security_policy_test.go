/*
Copyright 2019 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

func TestFallbackSecurityPolicy(t *testing.T) {
	setup(t)
	ctx := t.Context()
	mTablet, err := clusterInstance.AddTablet(t, ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)

	// Requesting an unregistered security-policy should fallback to deny-all.
	err = mTablet.StopVttablet(ctx)
	require.NoError(t, err)
	err = mTablet.StartVttablet(ctx, "--security-policy", "bogus")
	require.NoError(t, err)
	err = mTablet.WaitForTabletStatus(ctx, vttabletStateTimeout, "SERVING")
	require.NoError(t, err)

	// It should deny ADMIN role.
	assertNotAllowedURLTest(t, mTablet, "/livequeryz/terminate")

	// It should deny MONITORING role.
	assertNotAllowedURLTest(t, mTablet, "/debug/health")

	// It should deny DEBUGGING role.
	assertNotAllowedURLTest(t, mTablet, "/queryz")

	// Tear down custom processes
	killTablets(ctx, mTablet)
}

func assertNotAllowedURLTest(t *testing.T, tablet *vitesst.Tablet, path string) {
	status, body, err := tablet.MakeAPICall(t.Context(), path)
	require.NoError(t, err)

	assert.True(t, status > 400)
	assert.Contains(t, body, "Access denied: not allowed")
}

func assertAllowedURLTest(t *testing.T, tablet *vitesst.Tablet, path string) {
	_, body, err := tablet.MakeAPICall(t.Context(), path)
	require.NoError(t, err)

	assert.NotContains(t, body, "Access denied: not allowed")
}

func TestDenyAllSecurityPolicy(t *testing.T) {
	setup(t)
	ctx := t.Context()
	mTablet, err := clusterInstance.AddTablet(t, ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)

	// Requesting a deny-all security-policy.
	err = mTablet.StopVttablet(ctx)
	require.NoError(t, err)
	err = mTablet.StartVttablet(ctx, "--security-policy", "deny-all")
	require.NoError(t, err)
	err = mTablet.WaitForTabletStatus(ctx, vttabletStateTimeout, "SERVING")
	require.NoError(t, err)

	// It should deny ADMIN role.
	assertNotAllowedURLTest(t, mTablet, "/livequeryz/terminate")

	// It should deny MONITORING role.
	assertNotAllowedURLTest(t, mTablet, "/debug/health")

	// It should deny DEBUGGING role.
	assertNotAllowedURLTest(t, mTablet, "/queryz")

	// Tear down custom processes
	killTablets(ctx, mTablet)
}

func TestReadOnlySecurityPolicy(t *testing.T) {
	setup(t)
	ctx := t.Context()
	mTablet, err := clusterInstance.AddTablet(t, ctx, cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)

	// Requesting a read-only security-policy.
	err = mTablet.StopVttablet(ctx)
	require.NoError(t, err)
	err = mTablet.StartVttablet(ctx, "--security-policy", "read-only")
	require.NoError(t, err)
	err = mTablet.WaitForTabletStatus(ctx, vttabletStateTimeout, "SERVING")
	require.NoError(t, err)

	// It should deny ADMIN role.
	assertNotAllowedURLTest(t, mTablet, "/livequeryz/terminate")

	// It should deny MONITORING role.
	assertAllowedURLTest(t, mTablet, "/debug/health")

	// It should deny DEBUGGING role.
	assertAllowedURLTest(t, mTablet, "/queryz")

	// Tear down custom processes
	killTablets(ctx, mTablet)
}
