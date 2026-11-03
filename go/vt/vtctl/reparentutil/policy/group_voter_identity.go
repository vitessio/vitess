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
	"maps"

	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// NewGroupVoterIdentity returns the identity that a voter publishes in its shard record (see
// Shard.group_replication_voter_identities): the parts of its tablet record that address it, and the
// server_uuid of its MySQL.
func NewGroupVoterIdentity(tablet *topodatapb.Tablet, serverUUID string) *topodatapb.GroupReplicationVoterIdentity {
	return &topodatapb.GroupReplicationVoterIdentity{
		Tablet: &topodatapb.Tablet{
			Alias:         tablet.GetAlias().CloneVT(),
			Keyspace:      tablet.GetKeyspace(),
			Shard:         tablet.GetShard(),
			Hostname:      tablet.GetHostname(),
			PortMap:       maps.Clone(tablet.GetPortMap()),
			MysqlHostname: tablet.GetMysqlHostname(),
			MysqlPort:     tablet.GetMysqlPort(),
		},
		ServerUuid: serverUUID,
	}
}

// GroupVoterIdentities returns the identities that the shard record holds for its listed voters, by
// alias. The entries of tablets that are no longer listed are left out.
func GroupVoterIdentities(shard *topodatapb.Shard) map[string]*topodatapb.GroupReplicationVoterIdentity {
	identities := make(map[string]*topodatapb.GroupReplicationVoterIdentity)
	for _, identity := range shard.GetGroupReplicationVoterIdentities() {
		alias := identity.GetTablet().GetAlias()
		if alias == nil || !IsVoter(shard.GetGroupReplicationVoters(), alias) {
			continue
		}
		identities[topoproto.TabletAliasString(alias)] = identity
	}
	return identities
}

// SetGroupVoterIdentity sets the identity of a listed voter in the shard record, and drops the entries
// of tablets that are no longer listed. It returns whether the record changed. A voter that is not
// listed gets no entry.
func SetGroupVoterIdentity(shard *topodatapb.Shard, identity *topodatapb.GroupReplicationVoterIdentity) bool {
	alias := identity.GetTablet().GetAlias()
	listed := alias != nil && IsVoter(shard.GetGroupReplicationVoters(), alias)
	changed := false
	identities := make([]*topodatapb.GroupReplicationVoterIdentity, 0, len(shard.GetGroupReplicationVoterIdentities())+1)
	found := false
	for _, existing := range shard.GetGroupReplicationVoterIdentities() {
		existingAlias := existing.GetTablet().GetAlias()
		switch {
		case existingAlias == nil || !IsVoter(shard.GetGroupReplicationVoters(), existingAlias):
			changed = true
		case listed && topoproto.TabletAliasEqual(existingAlias, alias):
			if found {
				changed = true
				continue
			}
			found = true
			if !proto.Equal(existing, identity) {
				changed = true
			}
			identities = append(identities, identity)
		default:
			identities = append(identities, existing)
		}
	}
	if listed && !found {
		identities = append(identities, identity)
		changed = true
	}
	if changed {
		shard.GroupReplicationVoterIdentities = identities
	}
	return changed
}
