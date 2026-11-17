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

package reparentutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGroupReplicationWritersWithConcurrentIdentityWrite checks that a voter's vttablet, which
// publishes its identity in the shard record without the shard lock, never makes a writer of the
// group's state that holds the shard lock fail, nor loses either write: PlannedReparentShard's swap
// of a voter, and the bootstrap's writes of its intent, of the incarnation it bootstrapped, of the
// withdrawal of its intent, and of the incarnation of a group that VTOrc adopts. The identity write
// lands either between the writer's read, on which its caller decided, and the writer's
// compare-and-swap, or inside the compare-and-swap, between its read of the record and its versioned
// write, which then fails with BadVersion and is retried on the record that holds the identity. Each
// compare-and-swap compares fields that the identity write does not change.
func TestGroupReplicationWritersWithConcurrentIdentityWrite(t *testing.T) {
	const incarnation, newIncarnation = "1790000001", "1790000002"
	alias := func(cell string, uid uint32) *topodatapb.TabletAlias {
		return &topodatapb.TabletAlias{Cell: cell, Uid: uid}
	}
	voters := []*topodatapb.TabletAlias{alias("zone1", 101), alias("zone2", 200), alias("zone3", 300)}
	swapped := []*topodatapb.TabletAlias{alias("zone1", 101), alias("zone2", 201), alias("zone3", 300)}
	intent := &topodatapb.GroupReplicationBootstrapIntent{
		Target: voters[0], Time: protoutil.TimeToProto(time.Now()), PreviousIncarnation: incarnation, Token: "1790000002-0123456789abcdef",
	}

	tests := []struct {
		name string
		// setup sets the fields of the shard record that the writer reads; the voters are set already.
		setup func(si *topo.ShardInfo)
		// write runs the writer, under the shard lock.
		write func(ctx context.Context, ts *topo.Server) error
		// check checks the writer's change on the final shard record.
		check func(t *testing.T, si *topo.ShardInfo)
	}{{
		name:  "PlannedReparentShard swaps a voter",
		setup: func(si *topo.ShardInfo) { si.GroupReplicationIncarnation = incarnation },
		write: func(ctx context.Context, ts *topo.Server) error {
			return writeGroupSwap(ctx, ts, "ks", "0", &groupSwapPlan{incarnation: incarnation}, voters, swapped)
		},
		check: func(t *testing.T, si *topo.ShardInfo) {
			assert.True(t, votersEqual(swapped, si.GroupReplicationVoters), "voters [%s]", votersString(si.GroupReplicationVoters))
		},
	}, {
		name:  "the bootstrap writes its intent",
		setup: func(si *topo.ShardInfo) { si.GroupReplicationIncarnation = incarnation },
		write: func(ctx context.Context, ts *topo.Server) error {
			_, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", voters[0], incarnation, time.Now())
			return err
		},
		check: func(t *testing.T, si *topo.ShardInfo) {
			assert.True(t, topoproto.TabletAliasEqual(voters[0], si.GroupReplicationBootstrapIntent.GetTarget()), "intent %v", si.GroupReplicationBootstrapIntent)
		},
	}, {
		name: "the bootstrap records the incarnation it bootstrapped",
		setup: func(si *topo.ShardInfo) {
			si.GroupReplicationIncarnation = incarnation
			si.GroupReplicationBootstrapIntent = intent.CloneVT()
		},
		write: func(ctx context.Context, ts *topo.Server) error {
			return RecordGroupReplicationBootstrap(ctx, ts, "ks", "0", intent, newIncarnation)
		},
		check: func(t *testing.T, si *topo.ShardInfo) {
			assert.Equal(t, newIncarnation, si.GroupReplicationIncarnation)
			assert.Nil(t, si.GroupReplicationBootstrapIntent)
		},
	}, {
		name: "the bootstrap withdraws its intent",
		setup: func(si *topo.ShardInfo) {
			si.GroupReplicationIncarnation = incarnation
			si.GroupReplicationBootstrapIntent = intent.CloneVT()
		},
		write: func(ctx context.Context, ts *topo.Server) error {
			withdrawn, err := WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intent)
			if err == nil && !withdrawn {
				return topo.NewError(topo.NoUpdateNeeded, "the intent was not withdrawn")
			}
			return err
		},
		check: func(t *testing.T, si *topo.ShardInfo) {
			assert.Nil(t, si.GroupReplicationBootstrapIntent)
			assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
		},
	}, {
		name:  "VTOrc records the incarnation of a group that nobody recorded",
		setup: func(si *topo.ShardInfo) {},
		write: func(ctx context.Context, ts *topo.Server) error {
			return RecordUnrecordedGroupReplicationIncarnation(ctx, ts, "ks", "0", newIncarnation, time.Now())
		},
		check: func(t *testing.T, si *topo.ShardInfo) {
			assert.Equal(t, newIncarnation, si.GroupReplicationIncarnation)
		},
	}}
	for _, tt := range tests {
		for _, insideCAS := range []bool{false, true} {
			name := tt.name + ", identity written between the decision's read and the compare-and-swap"
			if insideCAS {
				name = tt.name + ", identity written between the read and the write of the compare-and-swap"
			}
			t.Run(name, func(t *testing.T) {
				ctx := t.Context()
				vttabletTS, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
				t.Cleanup(vttabletTS.Close)
				require.NoError(t, vttabletTS.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplicationCrossCell}))
				require.NoError(t, vttabletTS.CreateShard(ctx, "ks", "0"))
				_, err := vttabletTS.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
					si.GroupReplicationVoters = voters
					tt.setup(si)
					return nil
				})
				require.NoError(t, err)
				// The writer writes through the hook; the vttablet writes directly.
				hook := reparenttestutil.NewShardWriteHook(factory)
				writerTS, err := topo.NewWithFactory(hook, "", "")
				require.NoError(t, err)
				t.Cleanup(writerTS.Close)
				identity := policy.NewGroupVoterIdentity(&topodatapb.Tablet{
					Alias: voters[2], Keyspace: "ks", Shard: "0", Hostname: "host300", MysqlHostname: "host300", MysqlPort: 3306,
				}, "00000000-0000-0000-0000-000000000300")
				// publish may run outside of the test's goroutine: it does not stop the test.
				publish := func() {
					_, err := vttabletTS.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
						policy.SetGroupVoterIdentity(si.Shard, identity)
						return nil
					})
					assert.NoError(t, err)
				}

				lockCtx, unlock, err := writerTS.LockShard(ctx, "ks", "0", "test")
				require.NoError(t, err)
				t.Cleanup(func() {
					var err error
					unlock(&err)
				})
				// The caller has read the shard record, and decided on that read.
				if insideCAS {
					hook.Arm(publish)
				} else {
					publish()
				}
				require.NoError(t, tt.write(lockCtx, writerTS))

				si, err := vttabletTS.GetShard(ctx, "ks", "0")
				require.NoError(t, err)
				tt.check(t, si)
				require.Len(t, si.GroupReplicationVoterIdentities, 1)
				assert.True(t, proto.Equal(identity, si.GroupReplicationVoterIdentities[0]), "the identity write must stand")
				if insideCAS {
					// The identity write did land inside the compare-and-swap: its versioned write failed,
					// and its retry succeeded.
					writeErrs := hook.WriteErrors()
					require.Len(t, writeErrs, 2)
					assert.True(t, topo.IsErrType(writeErrs[0], topo.BadVersion), "first write: %v", writeErrs[0])
					assert.NoError(t, writeErrs[1])
				}
			})
		}
	}
}
