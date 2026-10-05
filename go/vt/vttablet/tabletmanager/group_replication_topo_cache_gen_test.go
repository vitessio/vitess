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

package tabletmanager

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// shardReadHookFactory is a memorytopo factory that runs a hook once, right after a read of a shard
// record, before the reader gets the record it read: what a slow reader sees when the record changes
// while its reply is on its way.
type shardReadHookFactory struct {
	*memorytopo.Factory

	mu   sync.Mutex
	hook func()
}

func (f *shardReadHookFactory) setHook(hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = hook
}

// Create is part of the topo.Factory interface.
func (f *shardReadHookFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil {
		return nil, err
	}
	return &shardReadHookConn{Conn: conn, f: f}, nil
}

type shardReadHookConn struct {
	topo.Conn
	f *shardReadHookFactory
}

// Get is part of the topo.Conn interface.
func (c *shardReadHookConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	data, version, err := c.Conn.Get(ctx, filePath)
	if strings.HasSuffix(filePath, "/"+topo.ShardFile) {
		c.f.mu.Lock()
		hook := c.f.hook
		c.f.hook = nil
		c.f.mu.Unlock()
		if hook != nil {
			hook()
		}
	}
	return data, version, err
}

// TestGroupReplicationTopoCacheKeepsWatchedRecord checks that a slow read of the shard record does not
// overwrite a newer record that the shard watch delivered meanwhile: the read's shard fields are older,
// and the fence check would decide on them until the next read. Here VTOrc writes a voter list that drops
// the tablet while the tablet's sync loop reads the shard record; the watch delivers the new list before
// the read returns the old one.
func TestGroupReplicationTopoCacheKeepsWatchedRecord(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1")
	f := &shardReadHookFactory{Factory: mf}
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication}))
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2, 3)
	tm, _ := newGroupReplicationTestTM(t, ts, 1, nil)
	_, err = tm.readShardGroupRecord(ctx, nil)
	require.NoError(t, err)

	f.setHook(func() {
		si, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
			si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "cell1", Uid: 2}, {Cell: "cell1", Uid: 3}}
			return nil
		})
		require.NoError(t, err)
		tm.noteShardFromWatch(si.Shard)
	})
	_, err = tm.readShardGroupRecord(ctx, tm.groupReplicationTopo.lastRecord())
	require.NoError(t, err)

	want := []string{"cell1-0000000002", "cell1-0000000003"}
	aliases := func(voters []*topodatapb.TabletAlias) []string {
		var list []string
		for _, v := range voters {
			list = append(list, topoproto.TabletAliasString(v))
		}
		return list
	}
	assert.Equal(t, want, aliases(tm.groupReplicationTopo.lastRecord().voters), "the record the fence check decides on")
	voters, _ := tm.groupReplicationTopo.lastVoters()
	assert.Equal(t, want, aliases(voters), "the voters a join falls back to")
}
