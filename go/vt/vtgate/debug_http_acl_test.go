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

package vtgate

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/streamlog"
	viperdebug "vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	queryLogLiteral  = "ql-literal-a91c2e"
	queryLogzLiteral = "qlz-literal-a91c2e"
	queryPlanLiteral = "qp-literal-a91c2e"
	queryLogSQL      = "select customer_email from accounts where session_token = '" + queryLogLiteral + "'"
	queryLogzSQL     = "select customer_email from accounts where session_token = '" + queryLogzLiteral + "'"
	queryPlanSQL     = "select customer_email from accounts where session_token = '" + queryPlanLiteral + "'"
)

// TestUnauthenticatedDebugHTTPDoesNotLeakQueryTextOrChangeServingTablets is the
// spec for VTGate's default debug HTTP server. An anonymous client that can
// open the port must not see statement text and must not move the replication-lag
// thresholds that choose which tablets serve reads.
func TestUnauthenticatedDebugHTTPDoesNotLeakQueryTextOrChangeServingTablets(t *testing.T) {
	origLow := discovery.GetLowReplicationLag()
	origHigh := discovery.GetHighReplicationLagMinServing()
	origMin := discovery.GetMinNumTablets()
	t.Cleanup(func() {
		discovery.SetLowReplicationLag(origLow)
		discovery.SetHighReplicationLagMinServing(origHigh)
		discovery.SetMinNumTablets(origMin)
	})
	restoreDiscoveryDefaults()

	executor := &Executor{
		env:   vtenv.NewTestEnv(),
		plans: theine.NewStore[PlanCacheKey, *engine.Plan](queryPlanCacheMemory, false),
	}
	t.Cleanup(func() { executor.plans.Close() })
	require.NoError(t, executor.defaultQueryLogger())
	registerUnlessPresent(t, "/debug/env", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		debugEnvHandler(nil, w, r)
	}))
	registerUnlessPresent(t, "/debug/config", http.HandlerFunc(viperdebug.HandlerFunc))
	registerUnlessPresent(t, pathQueryPlans, executor)

	var planKey PlanCacheKey
	planKey[0] = 1
	require.True(t, executor.plans.Set(planKey, &engine.Plan{Original: queryPlanSQL}, 1, executor.epoch.Load()))

	base := listenLikeRun(t)
	client := &http.Client{Timeout: 10 * time.Second}

	t.Run("querylog", func(t *testing.T) {
		body, code := anonymousQueryLog(t, base, executor)
		require.NotContains(t, body, queryLogLiteral)
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/querylog body: %s", body)
	})

	t.Run("querylogz", func(t *testing.T) {
		body, code := anonymousGet(t, client, base+QueryLogzHandler+"?timeout=1&limit=1", func() {
			sendQueryLog(t.Context(), executor, queryLogzSQL)
		})
		require.NotContains(t, body, queryLogzLiteral)
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/querylogz body: %s", body)
	})

	t.Run("queryz", func(t *testing.T) {
		body, code := anonymousGet(t, client, base+QueryzHandler, nil)
		require.NotContains(t, body, queryPlanLiteral)
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/queryz body: %s", body)
	})

	t.Run("query_plans", func(t *testing.T) {
		body, code := anonymousGet(t, client, base+pathQueryPlans, nil)
		require.NotContains(t, body, queryPlanLiteral)
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/query_plans body: %s", body)
	})

	t.Run("config", func(t *testing.T) {
		var body string
		var code int
		created := createdTempFilesDuring(t, "viper_debug", func() {
			body, code = anonymousGet(t, client, base+"/debug/config?format=json", nil)
		})
		require.Emptyf(t, created, "config handler created a temp file before refusing: %v", created)
		require.NotContains(t, body, "discovery_low_replication_lag")
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/config body: %s", body)
	})

	t.Run("status", func(t *testing.T) {
		_, code := anonymousGet(t, client, base+"/debug/status", nil)
		require.Equal(t, http.StatusForbidden, code)
	})

	t.Run("mutex_profile", func(t *testing.T) {
		before := runtime.SetMutexProfileFraction(-1)
		t.Cleanup(func() { runtime.SetMutexProfileFraction(before) })
		body, code := anonymousPost(t, client, base+"/debug/mutexprofilefraction", url.Values{"fraction": {"1"}})
		after := runtime.SetMutexProfileFraction(-1)
		require.Equal(t, before, after)
		require.Equalf(t, http.StatusForbidden, code, "anonymous /debug/mutexprofilefraction body: %s", body)
	})

	t.Run("discovery_low_replication_lag", func(t *testing.T) {
		restoreDiscoveryDefaults()
		lagging := tabletWithLag(3, "lagging-replica", 90)
		tablets := []*discovery.TabletHealth{
			tabletWithLag(1, "fast-replica", 1),
			tabletWithLag(2, "ok-replica", 2),
			lagging,
		}
		require.False(t, servingContains(tablets, lagging))
		body, code := anonymousPost(t, client, base+"/debug/env", url.Values{
			"varname": {"discovery_low_replication_lag"},
			"value":   {"120s"},
		})
		require.Falsef(t, servingContains(tablets, lagging), "lagging replica became serving")
		require.Equal(t, 30*time.Second, discovery.GetLowReplicationLag())
		require.NotContains(t, body, "120s")
		require.Equalf(t, http.StatusForbidden, code, "anonymous POST discovery_low_replication_lag body: %s", body)
	})

	t.Run("discovery_high_replication_lag_minimum_serving", func(t *testing.T) {
		restoreDiscoveryDefaults()
		lagging := tabletWithLag(5, "very-lagging-replica", 3*60*60)
		tablets := []*discovery.TabletHealth{
			tabletWithLag(4, "fast-replica", 1),
			lagging,
		}
		require.False(t, servingContains(tablets, lagging))
		body, code := anonymousPost(t, client, base+"/debug/env", url.Values{
			"varname": {"discovery_high_replication_lag_minimum_serving"},
			"value":   {"4h"},
		})
		require.Falsef(t, servingContains(tablets, lagging), "very lagging replica became serving")
		require.Equal(t, 2*time.Hour, discovery.GetHighReplicationLagMinServing())
		require.NotContains(t, body, "4h")
		require.Equalf(t, http.StatusForbidden, code, "anonymous POST discovery_high_replication_lag_minimum_serving body: %s", body)
	})

	t.Run("min_num_tablets", func(t *testing.T) {
		restoreDiscoveryDefaults()
		lagging := tabletWithLag(8, "third-replica", 90)
		tablets := []*discovery.TabletHealth{
			tabletWithLag(6, "fast-replica", 1),
			tabletWithLag(7, "slow-replica", 40),
			lagging,
		}
		require.False(t, servingContains(tablets, lagging))
		body, code := anonymousPost(t, client, base+"/debug/env", url.Values{
			"varname": {"min_num_tablets"},
			"value":   {"3"},
		})
		require.Falsef(t, servingContains(tablets, lagging), "third replica became serving")
		require.Equal(t, 2, discovery.GetMinNumTablets())
		require.Equalf(t, http.StatusForbidden, code, "anonymous POST min_num_tablets body: %s", body)
	})
}

func restoreDiscoveryDefaults() {
	discovery.SetLowReplicationLag(30 * time.Second)
	discovery.SetHighReplicationLagMinServing(2 * time.Hour)
	discovery.SetMinNumTablets(2)
}

func tabletWithLag(uid uint32, host string, lagSeconds uint32) *discovery.TabletHealth {
	return &discovery.TabletHealth{
		Tablet:  topo.NewTablet(uid, "cell", host),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: lagSeconds},
	}
}

// servingContains reports whether FilterStatsByReplicationLag, the function
// healthcheck uses to publish the healthy tablet list, still selects want.
func servingContains(tablets []*discovery.TabletHealth, want *discovery.TabletHealth) bool {
	for _, got := range discovery.FilterStatsByReplicationLag(tablets) {
		if got.Tablet.Alias.Uid == want.Tablet.Alias.Uid {
			return true
		}
	}
	return false
}

func listenLikeRun(t *testing.T) string {
	t.Helper()
	// Run calls Listen on an empty bind address and the port flag. The default
	// port is 0, which is the address ":0".
	ln, err := servenv.Listen("tcp", net.JoinHostPort("", "0"))
	if err != nil {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() { _ = servenv.HTTPServe(ln) }()

	base := "http://" + ln.Addr().String()
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/debug/liveness")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond)
	t.Logf("HTTP listener %s", ln.Addr().String())
	return base
}

func registerUnlessPresent(t *testing.T, pattern string, handler http.Handler) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		msg := strings.ToLower(recoverString(recovered))
		if !strings.Contains(msg, "multiple registrations") && !strings.Contains(msg, "conflicts with") {
			panic(recovered)
		}
	}()
	servenv.HTTPHandle(pattern, handler)
}

func recoverString(recovered any) string {
	switch v := recovered.(type) {
	case string:
		return v
	case error:
		return v.Error()
	default:
		return ""
	}
}

func sendQueryLog(ctx context.Context, executor *Executor, sql string) {
	stats := logstats.NewLogStats(ctx, "Execute", sql, "session", nil, streamlog.NewQueryLogConfigForTest())
	stats.EndTime = stats.StartTime.Add(time.Millisecond)
	stats.StmtType = "select"
	executor.queryLogger.Send(stats)
}

func anonymousQueryLog(t *testing.T, base string, executor *Executor) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+QueryLogHandler, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		sendQueryLog(t.Context(), executor, queryLogSQL)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return string(body), resp.StatusCode
}

func anonymousGet(t *testing.T, client *http.Client, rawURL string, whileWaiting func()) (string, int) {
	t.Helper()
	if whileWaiting != nil {
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					whileWaiting()
				}
			}
		}()
		defer close(stop)
	}
	resp, err := client.Get(rawURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return string(body), resp.StatusCode
}

func anonymousPost(t *testing.T, client *http.Client, rawURL string, form url.Values) (string, int) {
	t.Helper()
	resp, err := client.PostForm(rawURL, form)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return string(body), resp.StatusCode
}

// createdTempFilesDuring records temp files whose names contain prefix and
// that are created while fn runs. os.CreateTemp uses os.TempDir.
func createdTempFilesDuring(t *testing.T, prefix string, fn func()) []string {
	t.Helper()
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK)
	require.NoError(t, err)
	t.Cleanup(func() { unix.Close(fd) })
	_, err = unix.InotifyAddWatch(fd, os.TempDir(), unix.IN_CREATE)
	require.NoError(t, err)

	fn()

	var created []string
	buf := make([]byte, 4096)
	for {
		n, err := unix.Read(fd, buf)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				break
			}
			require.NoError(t, err)
		}
		var offset uint32
		for offset < uint32(n) {
			event := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			nameLen := event.Len
			start := offset + unix.SizeofInotifyEvent
			name := strings.TrimRight(string(buf[start:start+nameLen]), "\x00")
			if strings.Contains(name, prefix) {
				created = append(created, name)
			}
			offset += unix.SizeofInotifyEvent + nameLen
		}
	}
	return created
}
