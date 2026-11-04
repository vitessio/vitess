/*
Copyright 2026 The Vitess Authors.

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

package servenv

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv/testutils"
)

func TestFlushLogsHandler(t *testing.T) {
	onInitHooks.Fire()
	server := testutils.HTTPTestServer()
	defer server.Close()

	// 1. Allowed with default policy
	resp, err := http.Get(server.URL + "/debug/flushlogs")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "flushed", string(body))

	// 2. Denied with deny-all policy
	cleanup := acl.SetPolicyForTest("deny-all")
	defer cleanup()

	respDeny, err := http.Get(server.URL + "/debug/flushlogs")
	require.NoError(t, err)
	defer respDeny.Body.Close()

	require.Equal(t, http.StatusForbidden, respDeny.StatusCode)
}

func TestDebugVarsHandler(t *testing.T) {
	server := testutils.HTTPTestServer()
	defer server.Close()

	// 1. Allowed with default policy
	resp, err := http.Get(server.URL + "/debug/vars")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Denied with deny-all policy
	cleanup := acl.SetPolicyForTest("deny-all")
	defer cleanup()

	respDeny, err := http.Get(server.URL + "/debug/vars")
	require.NoError(t, err)
	defer respDeny.Body.Close()

	require.Equal(t, http.StatusForbidden, respDeny.StatusCode)
}

func TestLivenessHandlerACL(t *testing.T) {
	server := testutils.HTTPTestServer()
	defer server.Close()

	// 1. Allowed with default policy
	resp, err := http.Get(server.URL + "/debug/liveness")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Must remain reachable (200 OK) even under deny-all policy
	cleanup := acl.SetPolicyForTest("deny-all")
	defer cleanup()

	respDeny, err := http.Get(server.URL + "/debug/liveness")
	require.NoError(t, err)
	defer respDeny.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
