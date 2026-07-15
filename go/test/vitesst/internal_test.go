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

package vitesst

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestSpliceInitDBSQL(t *testing.T) {
	// Use the repo's real init_db.sql so this test also pins that the marker
	// the framework depends on still exists there.
	base, err := os.ReadFile("../../../config/init_db.sql")
	require.NoError(t, err)

	out, err := spliceInitDBSQL(string(base), "")
	require.NoError(t, err)
	assert.Contains(t, out, "CREATE USER 'vt_dba'@'%'")
	assert.Contains(t, out, "GRANT PROXY ON ''@'' TO 'vt_dba'@'%' WITH GRANT OPTION;")
	assert.Contains(t, out, customSQLMarker, "the marker must survive so GetInitDBSQL keeps working on the result")

	out, err = spliceInitDBSQL(string(base), "CREATE USER 'extra'@'%';")
	require.NoError(t, err)
	assert.Contains(t, out, "CREATE USER 'extra'@'%';")
	assert.Less(t,
		strings.Index(out, "CREATE USER 'vt_dba'@'%'"),
		strings.Index(out, "CREATE USER 'extra'@'%';"),
		"extra SQL goes after the framework grants",
	)

	_, err = spliceInitDBSQL("no marker in here", "")
	require.ErrorContains(t, err, "missing")
}

func TestShellQuoteAll(t *testing.T) {
	got := shellQuoteAll([]string{"--flag", "a b", "it's", `he said "hi"`, "$HOME"})
	want := []string{
		`'--flag'`,
		`'a b'`,
		`'it'\''s'`,
		`'he said "hi"'`,
		`'$HOME'`,
	}
	assert.Equal(t, want, got)
}

func TestRingLogConsumer(t *testing.T) {
	rc := newRingLogConsumer("test", nil)

	rc.Accept(testcontainers.Log{Content: []byte("first\n")})
	rc.Accept(testcontainers.Log{Content: []byte("second\n")})
	assert.Equal(t, []string{"first", "second"}, rc.snapshot())
	assert.Equal(t, []string{"second"}, rc.tail(1))

	for i := range logRingCapacity + 10 {
		rc.Accept(testcontainers.Log{Content: fmt.Appendf(nil, "line-%d\n", i)})
	}
	all := rc.snapshot()
	require.Len(t, all, logRingCapacity)
	assert.Equal(t, fmt.Sprintf("line-%d", logRingCapacity+9), all[len(all)-1])

	var dumped []string
	rc.dump(func(format string, args ...any) {
		dumped = append(dumped, fmt.Sprintf(format, args...))
	}, 5)
	require.Len(t, dumped, 6, "5 lines plus the omission notice")
	assert.Contains(t, dumped[0], "earlier lines omitted")
	assert.Equal(t, fmt.Sprintf("[test] line-%d", logRingCapacity+9), dumped[len(dumped)-1])
}

func TestClusterOptionsValidation(t *testing.T) {
	err := newClusterOptions(nil).validate()
	require.ErrorContains(t, err, "at least one keyspace")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks"), WithMySQLVersion("5.7")}).validate()
	require.ErrorContains(t, err, "unsupported MySQL version")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks"), WithKeyspace("ks")}).validate()
	require.ErrorContains(t, err, "configured twice")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks"), WithCells("zone1", "")}).validate()
	require.ErrorContains(t, err, "cell names must not be empty")

	err = newClusterOptions([]ClusterOption{WithKeyspace("")}).validate()
	require.ErrorContains(t, err, "name cannot be empty")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks").WithShards(0)}).validate()
	require.ErrorContains(t, err, "at least one shard")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks").WithShards(2).WithShardNames("-80", "80-")}).validate()
	require.ErrorContains(t, err, "both WithShards and WithShardNames")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks").WithShardNames("-41", "")}).validate()
	require.ErrorContains(t, err, "shard names must not be empty")

	err = newClusterOptions([]ClusterOption{WithKeyspace("ks").WithShardNames("-41", "41-4180", "4180-")}).validate()
	require.NoError(t, err)

	config := newClusterOptions([]ClusterOption{
		WithKeyspace("ks").WithShards(2).WithReplicas(1).WithRDOnly(1),
		WithVTTabletArgs("--foo"),
		WithVTGateArgs("--bar"),
		WithVTCtldArgs("--baz"),
	})
	require.NoError(t, config.validate())
	require.Len(t, config.keyspaces, 1)
	assert.Equal(t, 3, config.keyspaces[0].tabletsPerShard(), "primary + 1 replica + 1 rdonly")
	assert.Equal(t, []string{"--foo"}, config.vttabletArgs)
	assert.Equal(t, []string{"--bar"}, config.vtgateArgs)
	assert.Equal(t, []string{"--baz"}, config.vtctldArgs)
	assert.Equal(t, defaultMySQLVersion, config.mysqlVersion)
	assert.Equal(t, []string{defaultCell}, config.cells)
}

func TestMatchesDebugVar(t *testing.T) {
	body := `{"TabletStateName": "SERVING", "TabletType": "replica", "Other": 3}`
	assert.True(t, matchesDebugVar(body, "TabletStateName", []string{"SERVING", "NOT_SERVING"}))
	assert.True(t, matchesDebugVar(body, "TabletType", []string{"replica"}))
	assert.False(t, matchesDebugVar(body, "TabletStateName", []string{"NOT_SERVING"}))
	assert.False(t, matchesDebugVar(body, "Other", []string{"3"}), "non-string vars never match")
	assert.False(t, matchesDebugVar("not json", "TabletStateName", []string{"SERVING"}))
}
