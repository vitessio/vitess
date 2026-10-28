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

package topo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestShardDurabilityPolicy checks the resolver of a shard's durability policy when the keyspace has no
// migration source: the shard's own policy if its record sets one, else its keyspace's, and "" when
// neither sets one or neither record is given. TestShardDurabilityPolicyMigrationSource covers the
// migration source.
func TestShardDurabilityPolicy(t *testing.T) {
	ks := func(durability string) *topodatapb.Keyspace {
		return &topodatapb.Keyspace{DurabilityPolicy: durability}
	}
	assert.Equal(t, "semi_sync", topo.ShardDurabilityPolicy(ks("semi_sync"), nil))
	assert.Equal(t, "semi_sync", topo.ShardDurabilityPolicy(ks("semi_sync"), &topodatapb.Shard{}))
	assert.Equal(t, "group_replication_cross_cell", topo.ShardDurabilityPolicy(ks("semi_sync"), &topodatapb.Shard{DurabilityPolicy: "group_replication_cross_cell"}))
	assert.Equal(t, "group_replication_cross_cell", topo.ShardDurabilityPolicy(ks(""), &topodatapb.Shard{DurabilityPolicy: "group_replication_cross_cell"}))
	assert.Empty(t, topo.ShardDurabilityPolicy(ks(""), &topodatapb.Shard{}))
	assert.Empty(t, topo.ShardDurabilityPolicy(nil, nil))
}

// TestShardDurabilityPolicyMigrationSource checks the resolver while MigrateReplicationMode converts
// a keyspace to Group Replication: the keyspace record names the target policy, and its migration
// source is the policy of every shard that has no policy of its own.
func TestShardDurabilityPolicyMigrationSource(t *testing.T) {
	keyspace := &topodatapb.Keyspace{DurabilityPolicy: "group_replication_cross_cell", MigrationSourceDurabilityPolicy: "semi_sync"}
	assert.Equal(t, "semi_sync", topo.ShardDurabilityPolicy(keyspace, nil))
	assert.Equal(t, "semi_sync", topo.ShardDurabilityPolicy(keyspace, &topodatapb.Shard{}))
	assert.Equal(t, "group_replication_cross_cell", topo.ShardDurabilityPolicy(keyspace, &topodatapb.Shard{DurabilityPolicy: "group_replication_cross_cell"}))

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", keyspace))
	require.NoError(t, ts.CreateShard(ctx, "ks", "-80"))
	require.NoError(t, ts.CreateShard(ctx, "ks", "80-"))
	_, err := ts.UpdateShardFields(ctx, "ks", "-80", func(si *topo.ShardInfo) error {
		si.DurabilityPolicy = "group_replication_cross_cell"
		return nil
	})
	require.NoError(t, err)
	durability, err := ts.GetShardDurability(ctx, "ks", "-80")
	require.NoError(t, err)
	assert.Equal(t, "group_replication_cross_cell", durability)
	durability, err = ts.GetShardDurability(ctx, "ks", "80-")
	require.NoError(t, err)
	assert.Equal(t, "semi_sync", durability, "a shard that is not converted keeps the migration's source policy")
}

// TestGetShardDurability checks that the topology server resolves a shard's policy from its shard
// record and its keyspace record, with "none" when neither sets one.
func TestGetShardDurability(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "-80"))
	require.NoError(t, ts.CreateShard(ctx, "ks", "80-"))
	_, err := ts.UpdateShardFields(ctx, "ks", "-80", func(si *topo.ShardInfo) error {
		si.DurabilityPolicy = "group_replication_cross_cell"
		return nil
	})
	require.NoError(t, err)

	durability, err := ts.GetShardDurability(ctx, "ks", "-80")
	require.NoError(t, err)
	assert.Equal(t, "group_replication_cross_cell", durability)
	durability, err = ts.GetShardDurability(ctx, "ks", "80-")
	require.NoError(t, err)
	assert.Equal(t, "semi_sync", durability)

	si, err := ts.GetShard(ctx, "ks", "-80")
	require.NoError(t, err)
	durability, err = ts.GetShardInfoDurability(ctx, si)
	require.NoError(t, err)
	assert.Equal(t, "group_replication_cross_cell", durability)

	require.NoError(t, ts.CreateKeyspace(ctx, "nopolicy", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "nopolicy", "0"))
	durability, err = ts.GetShardDurability(ctx, "nopolicy", "0")
	require.NoError(t, err)
	assert.Equal(t, "none", durability)
}

// TestShardDurabilityPolicyRoundTrip checks the new fields Shard.durability_policy,
// Keyspace.migration_source_durability_policy and FullStatus.shard_durability_policy_supported through the generated marshalling code: the
// vtprotobuf functions and the reflection-based ones agree, and a message without the new field
// encodes as before, so that readers that do not know it skip it.
func TestShardDurabilityPolicyRoundTrip(t *testing.T) {
	roundTrip := func(t *testing.T, msg, withoutField interface {
		proto.Message
		MarshalVT() ([]byte, error)
		SizeVT() int
	}, decodeVT func([]byte) (proto.Message, error), cloneVT func() proto.Message, empty func() proto.Message,
	) {
		vt, err := msg.MarshalVT()
		require.NoError(t, err)
		assert.Len(t, vt, msg.SizeVT())
		reflected, err := proto.Marshal(msg)
		require.NoError(t, err)

		fromVT := empty()
		require.NoError(t, proto.Unmarshal(vt, fromVT))
		assert.True(t, proto.Equal(msg, fromVT), "vtprotobuf encoding, reflection decoding")
		fromReflected, err := decodeVT(reflected)
		require.NoError(t, err)
		assert.True(t, proto.Equal(msg, fromReflected), "reflection encoding, vtprotobuf decoding")
		assert.True(t, proto.Equal(msg, cloneVT()))

		old, err := withoutField.MarshalVT()
		require.NoError(t, err)
		assert.Equal(t, old, vt[:len(old)], "the new field is encoded after the existing fields")
	}

	t.Run("Shard", func(t *testing.T) {
		shard := &topodatapb.Shard{
			PrimaryAlias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			GroupReplicationVoters:      []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 100}},
			GroupReplicationIncarnation: "17908892198863259",
			DurabilityPolicy:            "group_replication_cross_cell",
		}
		without := shard.CloneVT()
		without.DurabilityPolicy = ""
		roundTrip(t, shard, without, func(b []byte) (proto.Message, error) {
			m := &topodatapb.Shard{}
			return m, m.UnmarshalVT(b)
		}, func() proto.Message { return shard.CloneVT() }, func() proto.Message { return &topodatapb.Shard{} })
	})

	t.Run("Keyspace", func(t *testing.T) {
		keyspace := &topodatapb.Keyspace{
			DurabilityPolicy:                "group_replication_cross_cell",
			SidecarDbName:                   "_vt",
			MigrationSourceDurabilityPolicy: "semi_sync",
		}
		without := keyspace.CloneVT()
		without.MigrationSourceDurabilityPolicy = ""
		roundTrip(t, keyspace, without, func(b []byte) (proto.Message, error) {
			m := &topodatapb.Keyspace{}
			return m, m.UnmarshalVT(b)
		}, func() proto.Message { return keyspace.CloneVT() }, func() proto.Message { return &topodatapb.Keyspace{} })
	})

	t.Run("FullStatus", func(t *testing.T) {
		status := &replicationdatapb.FullStatus{
			ServerUuid:                     "00000000-0000-0000-0000-000000000100",
			GroupReplicationEnabled:        true,
			ShardDurabilityPolicySupported: true,
		}
		without := status.CloneVT()
		without.ShardDurabilityPolicySupported = false
		roundTrip(t, status, without, func(b []byte) (proto.Message, error) {
			m := &replicationdatapb.FullStatus{}
			return m, m.UnmarshalVT(b)
		}, func() proto.Message { return status.CloneVT() }, func() proto.Message { return &replicationdatapb.FullStatus{} })
	})
}
