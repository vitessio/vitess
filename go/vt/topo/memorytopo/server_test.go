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

package memorytopo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

func TestMemoryTopo(t *testing.T) {
	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		return NewServer(ctx, test.LocalCellName)
	}, []string{"checkTryLock", "checkShardWithLock"})
}

func TestLockShardContextHasDeadline(t *testing.T) {
	cell := "cell-1"
	ks := "ks"
	shard := "-"
	ts := NewServer(context.Background(), cell)
	_, err := ts.GetOrCreateShard(context.Background(), ks, shard)
	require.NoError(t, err)
	ctx, unlock, err := ts.LockShard(context.Background(), ks, shard, "action")
	require.NoError(t, err)
	defer unlock(&err)
	_, hasDeadline := ctx.Deadline()
	require.True(t, hasDeadline)
}
