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

package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestSetGroupVoterIdentityIgnoresTabletType checks that a voter's published identity does not change
// with what its tablet record holds besides its address: a planned reparent changes the types of the
// voters, and must not make each of them write the shard record again.
func TestSetGroupVoterIdentityIgnoresTabletType(t *testing.T) {
	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}
	tablet := &topodatapb.Tablet{
		Alias: alias, Keyspace: "ks", Shard: "0", Hostname: "host1", PortMap: map[string]int32{"grpc": 15101},
		MysqlHostname: "mysql1", MysqlPort: 3306, Type: topodatapb.TabletType_REPLICA,
	}
	shard := &topodatapb.Shard{GroupReplicationVoters: []*topodatapb.TabletAlias{alias}}
	assert.True(t, SetGroupVoterIdentity(shard, NewGroupVoterIdentity(tablet, "uuid1")))

	primary := tablet.CloneVT()
	primary.Type = topodatapb.TabletType_PRIMARY
	primary.Tags = map[string]string{"k": "v"}
	assert.False(t, SetGroupVoterIdentity(shard, NewGroupVoterIdentity(primary, "uuid1")), "a change of type is no change of identity")

	moved := tablet.CloneVT()
	moved.MysqlPort = 3307
	assert.True(t, SetGroupVoterIdentity(shard, NewGroupVoterIdentity(moved, "uuid1")), "a change of address is")
	assert.Len(t, shard.GroupReplicationVoterIdentities, 1)
}
