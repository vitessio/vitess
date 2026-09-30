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
	"github.com/google/uuid"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// ReplicationMode describes how the tablets of a shard replicate from each other and how a
// transaction becomes durable.
type ReplicationMode int

const (
	// ReplicationModeAsync is classic MySQL asynchronous replication, optionally made durable
	// with semi-sync. Vitess (PRS, ERS, VTOrc) decides which tablet is the primary.
	ReplicationModeAsync ReplicationMode = iota
	// ReplicationModeGroupReplication is MySQL Group Replication in single-primary mode. A
	// transaction is durable once a majority of the group has certified it. The group
	// decides which member is writable; Vitess steers that decision (PRS, member weights)
	// and keeps the topology in sync with it.
	ReplicationModeGroupReplication
)

func (m ReplicationMode) String() string {
	switch m {
	case ReplicationModeGroupReplication:
		return "group_replication"
	default:
		return "async"
	}
}

const (
	// DurabilityGroupReplication is the name of the durability policy that uses MySQL Group
	// Replication. Every PRIMARY and REPLICA tablet is a voting member of the shard's group.
	DurabilityGroupReplication = "group_replication"
	// DurabilityGroupReplicationCrossCell is like DurabilityGroupReplication but additionally
	// requires that no single cell holds a majority of the voting members, so that every
	// durable transaction exists in at least two cells.
	DurabilityGroupReplicationCrossCell = "group_replication_cross_cell"

	// MaxGroupReplicationMembers is the maximum number of members MySQL allows in a group.
	MaxGroupReplicationMembers = 9
)

// ReplicationModer is implemented by durability policies that do not use the default
// asynchronous replication mode. It is a separate interface so that durability policies
// registered outside of Vitess keep compiling.
type ReplicationModer interface {
	ReplicationMode() ReplicationMode
}

// GroupReplicationDurabler is implemented by durability policies that use MySQL Group
// Replication.
type GroupReplicationDurabler interface {
	Durabler
	ReplicationModer
	// IsGroupMember returns whether the tablet should be a voting member of its shard's
	// replication group. Tablets that are not members replicate asynchronously from the
	// group's primary.
	IsGroupMember(tablet *topodatapb.Tablet) bool
	// MemberWeight returns the group_replication_member_weight for the tablet. The group
	// prefers members with a higher weight when it elects a primary on its own.
	MemberWeight(tablet *topodatapb.Tablet) int
	// RequiresCrossCellMajority returns whether a majority of the group must span more
	// than one cell.
	RequiresCrossCellMajority() bool
}

// GetReplicationMode returns the replication mode of the durability policy.
func GetReplicationMode(durability Durabler) ReplicationMode {
	if moder, ok := durability.(ReplicationModer); ok {
		return moder.ReplicationMode()
	}
	return ReplicationModeAsync
}

// IsGroupReplication returns whether the durability policy uses MySQL Group Replication.
func IsGroupReplication(durability Durabler) bool {
	return durability != nil && GetReplicationMode(durability) == ReplicationModeGroupReplication
}

// AsGroupReplication returns the durability policy as a GroupReplicationDurabler, if it is one.
func AsGroupReplication(durability Durabler) (GroupReplicationDurabler, bool) {
	if !IsGroupReplication(durability) {
		return nil, false
	}
	grd, ok := durability.(GroupReplicationDurabler)
	return grd, ok
}

// IsGroupMember returns whether the tablet should be a voting member of its shard's group.
func IsGroupMember(durability Durabler, tablet *topodatapb.Tablet) bool {
	grd, ok := AsGroupReplication(durability)
	if !ok || tablet == nil || tablet.Alias == nil {
		return false
	}
	return grd.IsGroupMember(tablet)
}

// groupNameNamespace is the UUIDv5 namespace of Vitess group replication group names.
var groupNameNamespace = uuid.MustParse("6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41")

// GroupName returns the group_replication_group_name of a shard. It is derived from the
// keyspace and shard names, so every tablet of the shard computes the same name without
// coordination, and the shards created by a reshard get new groups.
func GroupName(keyspace, shard string) string {
	return uuid.NewSHA1(groupNameNamespace, []byte(keyspace+"/"+shard)).String()
}

// CellHoldsMajority returns the name of a cell that holds a majority of the given voting
// members, if there is one. A group whose majority lives in one cell loses durable
// transactions if that cell is lost.
func CellHoldsMajority(members []*topodatapb.Tablet) (string, bool) {
	counts := make(map[string]int)
	for _, m := range members {
		if m == nil || m.Alias == nil {
			continue
		}
		counts[m.Alias.Cell]++
	}
	for cell, n := range counts {
		if n > len(members)/2 {
			return cell, true
		}
	}
	return "", false
}

func init() {
	RegisterDurability(DurabilityGroupReplication, func() Durabler {
		return &durabilityGroupReplication{}
	})
	RegisterDurability(DurabilityGroupReplicationCrossCell, func() Durabler {
		return &durabilityGroupReplication{crossCell: true}
	})
}

// durabilityGroupReplication uses MySQL Group Replication in single-primary mode. PRIMARY and
// REPLICA tablets are voting members of the group; every other tablet type replicates
// asynchronously from the group's primary and must never be promoted.
type durabilityGroupReplication struct {
	crossCell bool
}

// PromotionRule implements the Durabler interface.
func (d *durabilityGroupReplication) PromotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

// SemiSyncAckers implements the Durabler interface. The group, not semi-sync, makes
// transactions durable.
func (d *durabilityGroupReplication) SemiSyncAckers(*topodatapb.Tablet) int {
	return 0
}

// IsReplicaSemiSync implements the Durabler interface.
func (d *durabilityGroupReplication) IsReplicaSemiSync(_, _ *topodatapb.Tablet) bool {
	return false
}

// HasSemiSync implements the Durabler interface.
func (d *durabilityGroupReplication) HasSemiSync() bool {
	return false
}

// ReplicationMode implements the ReplicationModer interface.
func (d *durabilityGroupReplication) ReplicationMode() ReplicationMode {
	return ReplicationModeGroupReplication
}

// IsGroupMember implements the GroupReplicationDurabler interface.
func (d *durabilityGroupReplication) IsGroupMember(tablet *topodatapb.Tablet) bool {
	return d.PromotionRule(tablet) != promotionrule.MustNot
}

// MemberWeight implements the GroupReplicationDurabler interface. It maps the promotion rule
// onto group_replication_member_weight so that the group's own elections follow the same
// preferences as Vitess's.
func (d *durabilityGroupReplication) MemberWeight(tablet *topodatapb.Tablet) int {
	return MemberWeightForPromotionRule(d.PromotionRule(tablet))
}

// RequiresCrossCellMajority implements the GroupReplicationDurabler interface.
func (d *durabilityGroupReplication) RequiresCrossCellMajority() bool {
	return d.crossCell
}

// MemberWeightForPromotionRule maps a promotion rule onto group_replication_member_weight,
// which ranges from 0 to 100 and defaults to 50.
func MemberWeightForPromotionRule(rule promotionrule.CandidatePromotionRule) int {
	switch rule {
	case promotionrule.Must:
		return 100
	case promotionrule.Prefer:
		return 75
	case promotionrule.PreferNot:
		return 25
	case promotionrule.MustNot:
		return 0
	default:
		return 50
	}
}
