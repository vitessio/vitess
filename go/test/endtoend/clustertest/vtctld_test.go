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

package clustertest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var oneTableOutput = `+---+
| a |
+---+
| 1 |
+---+
`

func TestVtctldProcess(t *testing.T) {
	healthCheckURL := fmt.Sprintf("http://%s:%d/debug/health", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, healthCheckURL, "vtctld health check url")

	testNoHTTPAPI(t)
	testGetTablets(t)
	testTabletStatus(t)
	testExecuteAsDba(t)
	testExecuteAsApp(t)
}

// testNoHTTPAPI checks that vtctld does not serve the HTTP API that its web
// UI used, which was removed: it served topology data, arbitrary vtctl
// commands, and more over plain HTTP without a consistent ACL check.
func testNoHTTPAPI(t *testing.T) {
	for _, path := range []string{"/api/keyspaces/", "/api/topodata/", "/api/vtctl/"} {
		url := fmt.Sprintf("http://%s:%d%s", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort, path)
		assert.Equal(t, http.StatusNotFound, getStatusForURL(url), "%s must not be served", url)
	}
}

func testGetTablets(t *testing.T) {
	// first w/o any filters, aside from cell
	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetTablets", "--cell", clusterInstance.Cell)
	require.NoError(t, err)

	tablets := getAllTablets()

	tabletsFromCMD := strings.Split(result, "\n")
	tabletCountFromCMD := 0

	for _, line := range tabletsFromCMD {
		if len(line) > 0 {
			tabletCountFromCMD = tabletCountFromCMD + 1
			assert.Contains(t, tablets, strings.Split(line, " ")[0])
		}
	}
	assert.Equal(t, tabletCountFromCMD, len(tablets))

	// now filtering with the first keyspace and tablet type of primary, in
	// addition to the cell
	result, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"GetTablets",
		"--keyspace", clusterInstance.Keyspaces[0].Name,
		"--tablet-type", "primary",
		"--cell", clusterInstance.Cell,
	)
	require.NoError(t, err)

	// We should only return a single primary tablet per shard in the first keyspace
	tabletsFromCMD = strings.Split(result, "\n")
	// We don't count the final newline with nothing after it (it becomes an empty
	// line at the end of the slice)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), len(tabletsFromCMD)-1)
}

// TestGetTabletsStalePrimary adds a tablet record that still says it is the
// primary of a shard, with a primary term older than the shard's current one,
// as a former primary that never updated its record would. GetTablets must not
// report it as the primary.
func TestGetTabletsStalePrimary(t *testing.T) {
	keyspace := clusterInstance.Keyspaces[0]
	shard := keyspace.Shards[0]
	ts := clusterInstance.TopoProcess.Server

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	si, err := ts.GetShard(ctx, keyspace.Name, shard.Name)
	require.NoError(t, err)
	require.False(t, si.GetPrimaryTermStartTime().IsZero())

	stale := &topodatapb.Tablet{
		Alias:                &topodatapb.TabletAlias{Cell: clusterInstance.Cell, Uid: 99999},
		Keyspace:             keyspace.Name,
		Shard:                shard.Name,
		Type:                 topodatapb.TabletType_PRIMARY,
		PrimaryTermStartTime: protoutil.TimeToProto(si.GetPrimaryTermStartTime().Add(-time.Hour)),
	}
	require.NoError(t, ts.CreateTablet(ctx, stale))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		assert.NoError(t, topotools.DeleteTablet(ctx, ts, stale))
	})
	staleAlias := topoproto.TabletAliasString(stale.Alias)

	t.Run("tablet alias", func(t *testing.T) {
		types := getTabletTypes(t, "GetTablets", "--tablet-alias", staleAlias)
		assert.Equal(t, map[string]string{staleAlias: "unknown"}, types)
	})

	t.Run("keyspace and shard with primary tablet type", func(t *testing.T) {
		types := getTabletTypes(t, "GetTablets", "--keyspace", keyspace.Name, "--shard", shard.Name, "--tablet-type", "primary")
		assert.NotContains(t, types, staleAlias)
		assert.Equal(t, map[string]string{shard.PrimaryTablet().Alias: "primary"}, types)
	})
}

// getTabletTypes runs a vtctldclient GetTablets command and returns the type
// of each tablet in its output, keyed by tablet alias.
func getTabletTypes(t *testing.T, args ...string) map[string]string {
	t.Helper()

	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(args...)
	require.NoError(t, err)

	types := make(map[string]string)
	for line := range strings.Lines(result) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		require.GreaterOrEqual(t, len(fields), 4, "unexpected GetTablets output line: %q", line)
		types[fields[0]] = fields[3]
	}
	return types
}

func testTabletStatus(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d", clusterInstance.Hostname, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort))
	require.NoError(t, err)
	defer resp.Body.Close()
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	result := string(respByte)
	log.Info(fmt.Sprintf("Tablet status response: %v", result))
	assert.True(t, strings.Contains(result, `/debug/health`))
	assert.True(t, strings.Contains(result, `</html>`))
}

func testExecuteAsDba(t *testing.T) {
	tcases := []struct {
		query     string
		result    string
		expectErr bool
	}{
		{
			query:     "",
			expectErr: true,
		},
		{
			query:  "SELECT 1 AS a",
			result: oneTableOutput,
		},
		{
			query:     "SELECT 1 AS a; SELECT 1 AS a",
			expectErr: true,
		},
		{
			query:  "create table t(id int)",
			result: "",
		},
		{
			query:  "create table if not exists t(id int)",
			result: "",
		},
		{
			query:     "create table if not exists t(id int); create table if not exists t(id int);",
			expectErr: true,
		},
		{
			query:     "create table if not exists t(id int); create table if not exists t(id int); SELECT 1 AS a",
			expectErr: true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, tcase.query)
			if tcase.expectErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tcase.result, result)
			}
		})
	}
}

func testExecuteAsApp(t *testing.T) {
	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsApp", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	require.NoError(t, err)
	assert.Equal(t, result, oneTableOutput)
}

func getAllTablets() []string {
	tablets := make([]string, 0)
	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				tablets = append(tablets, tablet.Alias)
			}
		}
	}
	return tablets
}
