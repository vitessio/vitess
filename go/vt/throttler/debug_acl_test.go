/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    10|Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv/testutils"
)

// The process-wide security policy is selected once, from an unexported flag.
// These aliases let the test install a policy the same way vttablet does, then
// put that selection back so other tests in this package still see allow-all.
//
//go:linkname aclSecurityPolicy vitess.io/vitess/go/acl.securityPolicy
var aclSecurityPolicy string

//go:linkname aclOnce vitess.io/vitess/go/acl.once
var aclOnce sync.Once

const (
	// alphaThrottleACL and zetaThrottleACL are registered on GlobalManager,
	// which is the manager the HTTP handlers close over.
	alphaThrottleACL = "alpha-throttle-acl"
	zetaThrottleACL  = "zeta-throttle-acl"

	// debuggingRolePolicyName allows only the debugging role. deny-all is the
	// built-in policy an operator sets to hide the debug UI.
	debuggingRolePolicyName = "throttler-debug-role"
)

// alphaLogMark is the backlog pair from resultDecreased. The emergency row
// does not contain this pair, so one throttler's log is not the other's.
const alphaLogMark = "      <td>10</td>\n      <td>20</td>\n      <td>decreased the rate</td>"

// zetaLogMark is the emergency decision from resultEmergency.
const zetaLogMark = "emergency state decreased the rate"

const denyAllBody = "Access denied: not allowed: deny-all security-policy enforced"

// debuggingRolePolicy is the policy installed under debuggingRolePolicyName.
// It allows the request the handler actually received, and only for debugging.
type debuggingRolePolicy struct {
	mu       sync.Mutex
	lastPath string
	lastRole string
}

func (p *debuggingRolePolicy) CheckAccessHTTP(req *http.Request, role string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastRole = role
	p.lastPath = ""
	if req != nil && req.URL != nil {
		p.lastPath = req.URL.Path
	}
	if role != acl.DEBUGGING || req == nil || req.URL == nil {
		return errors.New("not allowed: debugging role required")
	}
	return nil
}

func (p *debuggingRolePolicy) last() (path, role string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastPath, p.lastRole
}

var debugRolePolicy = &debuggingRolePolicy{}

func init() {
	acl.RegisterPolicy(debuggingRolePolicyName, debugRolePolicy)
}

// resetACLOnce clears the one-shot policy load so the next CheckAccessHTTP
// reads aclSecurityPolicy again. sync.Once.done is an atomic.Bool whose value
// is the uint32 field v (sync/atomic.Bool in Go 1.27.4).
func resetACLOnce() {
	done := reflect.ValueOf(&aclOnce).Elem().FieldByName("done")
	v := done.FieldByName("v")
	if !v.IsValid() || v.Kind() != reflect.Uint32 || !v.CanAddr() {
		panic("cannot reset acl.once: sync.Once layout changed")
	}
	atomic.StoreUint32((*uint32)(unsafe.Pointer(v.UnsafeAddr())), 0)
}

func useSecurityPolicy(t *testing.T, name string) {
	t.Helper()
	aclSecurityPolicy = name
	resetACLOnce()
}

// TestThrottlerDebugHTTPHandlersHonorSecurityPolicy requests the throttler
// debug pages through the process HTTP mux. deny-all must hide every throttler
// name and the adaptive-throttling log. A policy that allows only the debugging
// role must still serve the throttler that the request named.
func TestThrottlerDebugHTTPHandlersHonorSecurityPolicy(t *testing.T) {
	origPolicy := aclSecurityPolicy
	t.Cleanup(func() {
		aclSecurityPolicy = origPolicy
		resetACLOnce()
	})

	alpha, err := newThrottler(GlobalManager, alphaThrottleACL, "queries", 1, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	require.NoError(t, err)
	t.Cleanup(alpha.Close)
	zeta, err := newThrottler(GlobalManager, zetaThrottleACL, "queries", 1, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	require.NoError(t, err)
	t.Cleanup(zeta.Close)
	alpha.(*ThrottlerImpl).maxReplicationLagModule.results.add(resultDecreased)
	zeta.(*ThrottlerImpl).maxReplicationLagModule.results.add(resultEmergency)

	srv := testutils.HTTPTestServer()
	t.Cleanup(srv.Close)
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Operator policy that hides the rest of the debug UI.
	useSecurityPolicy(t, "deny-all")
	for _, path := range []string{
		"/throttlerz/",
		"/throttlerz/" + alphaThrottleACL,
		"/throttlerz/" + zetaThrottleACL,
		"/throttlerlogz/",
		"/throttlerlogz/" + alphaThrottleACL,
		"/throttlerlogz/" + zetaThrottleACL,
		"/throttlerlogz/missing-throttle-acl",
	} {
		status, body, location := getThrottlerDebug(t, client, srv.URL+path)
		assert.Equalf(t, http.StatusForbidden, status, "GET %s body=%s", path, body)
		assert.Emptyf(t, location, "GET %s body=%s", path, body)
		assert.Containsf(t, body, denyAllBody, "GET %s body=%s", path, body)
		assert.NotContainsf(t, body, alphaThrottleACL, "GET %s body=%s", path, body)
		assert.NotContainsf(t, body, zetaThrottleACL, "GET %s body=%s", path, body)
		assert.NotContainsf(t, body, alphaLogMark, "GET %s body=%s", path, body)
		assert.NotContainsf(t, body, zetaLogMark, "GET %s body=%s", path, body)
		assert.NotContainsf(t, body, "throttler not found", "GET %s body=%s", path, body)
	}
	if t.Failed() {
		return
	}

	// Same pages, same throttlers, with a policy that allows debugging only.
	// The handler must have asked about this request path, then rendered it.
	useSecurityPolicy(t, debuggingRolePolicyName)

	status, body, location := getThrottlerDebug(t, client, srv.URL+"/throttlerz/")
	require.Equal(t, http.StatusOK, status, body)
	require.Empty(t, location)
	require.Contains(t, body, alphaThrottleACL)
	require.Contains(t, body, zetaThrottleACL)
	require.NotContains(t, body, alphaLogMark)
	require.NotContains(t, body, zetaLogMark)
	requireAuthorizedPath(t, "/throttlerz/")

	status, body, location = getThrottlerDebug(t, client, srv.URL+"/throttlerz/"+zetaThrottleACL)
	require.Equal(t, http.StatusOK, status, body)
	require.Empty(t, location)
	require.Contains(t, body, "Details for Throttler '"+zetaThrottleACL+"'")
	require.NotContains(t, body, alphaThrottleACL)
	require.NotContains(t, body, alphaLogMark)
	require.NotContains(t, body, zetaLogMark)
	requireAuthorizedPath(t, "/throttlerz/"+zetaThrottleACL)

	status, body, location = getThrottlerDebug(t, client, srv.URL+"/throttlerlogz/"+zetaThrottleACL)
	require.Equal(t, http.StatusOK, status, body)
	require.Empty(t, location)
	require.Contains(t, body, zetaLogMark)
	require.Contains(t, body, "<td>23s</td>")
	require.Contains(t, body, "<td>50</td>")
	require.NotContains(t, body, alphaThrottleACL)
	require.NotContains(t, body, alphaLogMark)
	requireAuthorizedPath(t, "/throttlerlogz/"+zetaThrottleACL)

	status, body, location = getThrottlerDebug(t, client, srv.URL+"/throttlerlogz/"+alphaThrottleACL)
	require.Equal(t, http.StatusOK, status, body)
	require.Empty(t, location)
	require.Contains(t, body, alphaLogMark)
	require.Contains(t, body, "<td>2s</td>")
	require.NotContains(t, body, zetaThrottleACL)
	require.NotContains(t, body, zetaLogMark)
	requireAuthorizedPath(t, "/throttlerlogz/"+alphaThrottleACL)

	status, body, location = getThrottlerDebug(t, client, srv.URL+"/throttlerlogz/")
	require.Equal(t, http.StatusTemporaryRedirect, status, body)
	require.Equal(t, "/throttlerz", location)
	require.NotContains(t, body, alphaThrottleACL)
	require.NotContains(t, body, zetaThrottleACL)
	require.NotContains(t, body, alphaLogMark)
	require.NotContains(t, body, zetaLogMark)
	requireAuthorizedPath(t, "/throttlerlogz/")

	status, body, location = getThrottlerDebug(t, client, srv.URL+"/throttlerlogz/missing-throttle-acl")
	require.Equal(t, http.StatusNotFound, status, body)
	require.Empty(t, location)
	require.Contains(t, body, "throttler not found")
	require.NotContains(t, body, alphaThrottleACL)
	require.NotContains(t, body, zetaThrottleACL)
	require.NotContains(t, body, alphaLogMark)
	require.NotContains(t, body, zetaLogMark)
	requireAuthorizedPath(t, "/throttlerlogz/missing-throttle-acl")
}

func requireAuthorizedPath(t *testing.T, path string) {
	t.Helper()
	gotPath, gotRole := debugRolePolicy.last()
	require.Equal(t, path, gotPath)
	require.Equal(t, acl.DEBUGGING, gotRole)
}

func getThrottlerDebug(t *testing.T, client *http.Client, url string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body), resp.Header.Get("Location")
}
