/*
Copyright 2021 The Vitess Authors.

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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestPromoteReplicaHealthTicksStopped checks that the health ticks are not running on the
// replication manager after running PromoteReplica
func TestPromoteReplicaHealthTicksStopped(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	statsTabletTypeCount.ResetAll()
	tm := newTestTM(t, ts, 100, keyspace, shard)
	defer tm.Stop()

	_, err := tm.PromoteReplica(ctx, false)
	require.NoError(t, err)
	require.False(t, tm.replManager.ticks.Running())
}
