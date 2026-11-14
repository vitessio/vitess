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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartKeyspaceRejectsFailedKeyspace fails a keyspace's first shard before it starts any
// process: with no mysqld to find, AddShard cannot build the tablet's mysqlctl. The keyspace
// must stay registered for Teardown, and must not be started again.
func TestStartKeyspaceRejectsFailedKeyspace(t *testing.T) {
	empty := t.TempDir()
	t.Setenv("PATH", empty)
	t.Setenv("VT_MYSQL_ROOT", empty)
	cluster := &LocalProcessCluster{Cell: "zone1", Hostname: "localhost"}
	keyspace := Keyspace{Name: "ks"}

	require.Error(t, cluster.StartKeyspace(keyspace, []string{"0"}, 0, false, cluster.Cell))
	require.Len(t, cluster.Keyspaces, 1, "the failed keyspace was not registered")
	assert.True(t, cluster.Keyspaces[0].startFailed)
	shards := cluster.Keyspaces[0].Shards

	err := cluster.StartKeyspace(keyspace, []string{"1"}, 0, false, cluster.Cell)
	require.ErrorContains(t, err, "cannot be started again")
	assert.Len(t, cluster.Keyspaces, 1)
	assert.Equal(t, shards, cluster.Keyspaces[0].Shards, "the rejected start added shards")
}
