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

package debugvars_acl

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
)

const (
	childEnv       = "DEBUG_VARS_ACL_CHILD"
	policyEnv      = "DEBUG_VARS_ACL_POLICY"
	actorHeader    = "X-Acl-Actor"
	lockedActor    = "locked"
	customPolicy   = "debugvars-acl-test"
	probeVar       = "debug_vars_acl_probe_counter"
	denyAllBody    = "Access denied: not allowed: deny-all security-policy enforced\n"
	customDenyBody = "Access denied: custom policy denied\n"
)

// requestBoundPolicy denies DEBUGGING and MONITORING only for the request
// that carries the locked actor. A check against any other request allows
// the role, so serving this request afterward is still a bypass.
type requestBoundPolicy struct{}

func (requestBoundPolicy) CheckAccessHTTP(req *http.Request, role string) error {
	if role != acl.DEBUGGING && role != acl.MONITORING {
		return errors.New("custom policy denied")
	}
	if req != nil && req.Header.Get(actorHeader) == lockedActor {
		return errors.New("custom policy denied")
	}
	return nil
}

func init() {
	acl.RegisterPolicy(customPolicy, requestBoundPolicy{})
}

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		fs := pflag.NewFlagSet("debugvars_acl", pflag.ContinueOnError)
		acl.RegisterFlags(fs)
		policy := os.Getenv(policyEnv)
		var args []string
		if policy != "" {
			args = []string{"--security-policy", policy}
		}
		if err := fs.Parse(args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func TestDebugVarsDeniedByDenyAll(t *testing.T) {
	if !child(t, "deny-all") {
		return
	}
	assertDebugEndpoints(t, true, denyAllBody)
}

func TestDebugVarsDeniedByRequestBoundPolicy(t *testing.T) {
	if !child(t, customPolicy) {
		return
	}
	assertDebugEndpoints(t, true, customDenyBody)
}

func TestDebugVarsAllowedByReadOnly(t *testing.T) {
	if !child(t, "read-only") {
		return
	}
	// read-only grants DEBUGGING and MONITORING, so the debug UI stays open.
	assertDebugEndpoints(t, false, "")
}

func TestDebugVarsAllowedByDefaultPolicy(t *testing.T) {
	if !child(t, "") {
		return
	}
	assertDebugEndpoints(t, false, "")
}

// child re-executes this test in a process whose security-policy is latched
// before the first CheckAccessHTTP call. The parent returns false.
func child(t *testing.T, policy string) bool {
	t.Helper()
	if os.Getenv(childEnv) == "1" {
		require.Equal(t, policy, os.Getenv(policyEnv))
		return true
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"=1", policyEnv+"="+policy)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("policy %q: %v\n%s", policy, err, out)
	}
	return false
}

func assertDebugEndpoints(t *testing.T, denied bool, denyBody string) {
	t.Helper()

	stats.NewCounter(probeVar, "probe published into the expvar map").Add(1)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- servenv.HTTPServe(listener)
	}()
	t.Cleanup(func() {
		listener.Close()
		require.NoError(t, <-serveErr)
	})

	base := "http://" + listener.Addr().String()
	statusCode, statusBody := get(t, base+"/debug/status")
	varsCode, varsBody := get(t, base+"/debug/vars")

	if !denied {
		require.Equal(t, http.StatusOK, statusCode, "status body: %s", clip(statusBody))
		require.Contains(t, statusBody, "Status for")
		require.NotContains(t, statusBody, "Access denied")
		require.Equal(t, http.StatusOK, varsCode, "vars body: %s", clip(varsBody))
		require.Contains(t, varsBody, `"`+probeVar+`"`)
		return
	}

	require.Equalf(t, http.StatusForbidden, statusCode, "status body: %s", clip(statusBody))
	require.Equal(t, denyBody, statusBody)
	require.Equalf(t, http.StatusForbidden, varsCode, "vars body: %s", clip(varsBody))
	require.NotContains(t, varsBody, probeVar)
	require.Equal(t, denyBody, varsBody)
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()

	client := &http.Client{Timeout: 5 * time.Second}
	var (
		resp *http.Response
		err  error
	)
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequest(http.MethodGet, url, nil)
		if reqErr != nil {
			err = reqErr
			return false
		}
		req.Header.Set(actorHeader, lockedActor)
		resp, err = client.Do(req)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func clip(body string) string {
	body = strings.ReplaceAll(body, "\n", " ")
	if len(body) > 180 {
		return body[:180] + "..."
	}
	return body
}
