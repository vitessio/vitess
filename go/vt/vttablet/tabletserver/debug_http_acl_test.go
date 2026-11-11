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

package tabletserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const tabletQueryLiteral = "tablet-qlz-literal-a91c2e"

// TestUnauthenticatedTabletQuerylogzDoesNotReturnSQL checks the vttablet
// query log page. It uses the same default ACL as VTGate, so an anonymous
// client must not receive statement text.
func TestUnauthenticatedTabletQuerylogzDoesNotReturnSQL(t *testing.T) {
	sql := "select customer_email from accounts where session_token = '" + tabletQueryLiteral + "'"
	stats := tabletenv.NewLogStats(t.Context(), "Execute", streamlog.NewQueryLogConfigForTest())
	stats.OriginalSQL = sql
	stats.EndTime = stats.StartTime.Add(time.Millisecond)
	ch := make(chan *tabletenv.LogStats, 1)
	ch <- stats

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		querylogzHandler(ch, w, r, sqlparser.NewTestParser())
	}))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/debug/querylogz?timeout=1&limit=1")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(body), tabletQueryLiteral)
	require.Equalf(t, http.StatusForbidden, resp.StatusCode, "anonymous tablet /debug/querylogz body: %s", body)
}

// TestUnauthenticatedLiveQueryzIsForbidden checks the in-flight query page.
// Terminating a connection is the admin action on the same page and is covered
// by the same ACL check; this request does not pass a connection id.
func TestUnauthenticatedLiveQueryzIsForbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		livequeryzHandler(nil, w, r)
	}))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/debug/livequeryz")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusForbidden, resp.StatusCode, "anonymous /debug/livequeryz body: %s", body)
}
