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

package vstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestWithFlags_ClonesInput(t *testing.T) {
	v := &VStreamClient{}
	flags := &vtgatepb.VStreamFlags{HeartbeatInterval: 1}

	err := WithFlags(flags)(v)
	require.NoError(t, err)

	// mutating the caller-owned struct after validation must not affect the client
	flags.HeartbeatInterval = 0
	assert.EqualValues(t, 1, v.cfg.flags.HeartbeatInterval)
}

func TestWithFlags_RejectsStreamKeyspaceHeartbeats(t *testing.T) {
	v := &VStreamClient{}

	err := WithFlags(&vtgatepb.VStreamFlags{HeartbeatInterval: 1, StreamKeyspaceHeartbeats: true})(v)
	require.ErrorContains(t, err, "StreamKeyspaceHeartbeats is not supported")
}

func TestWithStartingVGtid_Validation(t *testing.T) {
	tests := []struct {
		name    string
		vgtid   *binlogdatapb.VGtid
		wantErr string
	}{
		{name: "nil", vgtid: nil, wantErr: "at least one shard gtid"},
		{name: "empty", vgtid: &binlogdatapb.VGtid{}, wantErr: "at least one shard gtid"},
		{
			name:    "missing keyspace",
			vgtid:   &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{{Shard: "0", Gtid: "MySQL56/1"}}},
			wantErr: "must name a keyspace and shard",
		},
		{
			name:    "empty gtid",
			vgtid:   &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0"}}},
			wantErr: "must be a concrete position",
		},
		{
			name:    "symbolic current",
			vgtid:   &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "current"}}},
			wantErr: "must be a concrete position",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WithStartingVGtid(tt.vgtid)(&VStreamClient{})
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestWithStartingVGtid_ClonesInput(t *testing.T) {
	v := &VStreamClient{}
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}

	err := WithStartingVGtid(vgtid)(v)
	require.NoError(t, err)
	require.NotSame(t, vgtid, v.latestVgtid)

	vgtid.ShardGtids[0].Gtid = "mutated"
	assert.Equal(t, "MySQL56/1", v.latestVgtid.ShardGtids[0].Gtid)
}

func TestWithStateTable_RequiresTableName(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("ks", "")(v)
	require.ErrorContains(t, err, "state table name is required")
}

func TestWithStateTable_RejectsUnknownKeyspace(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("missing", "state")(v)
	require.ErrorContains(t, err, "keyspace missing not found")
}

func TestWithStateTable_RejectsShardedKeyspace(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"sharded": {"-80", "80-"}}}

	err := WithStateTable("sharded", "state")(v)
	require.ErrorContains(t, err, "only unsharded keyspaces are supported")
}

func TestWithStateTable_EscapesIdentifiers(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("ks", "state")(v)
	require.NoError(t, err)

	// the identifiers are stored pre-escaped, since every state query interpolates them
	assert.Equal(t, "`ks`", v.cfg.vgtidStateKeyspace)
	assert.Equal(t, "`state`", v.cfg.vgtidStateTable)
}
