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
	"math"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo/topoproto"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// GroupIncarnation returns the incarnation of a Group Replication view id, the part before the
// ':'. MySQL keeps the incarnation for as long as the group exists, and every view change only
// increments the part after the ':'. Bootstrapping a group creates a new incarnation, and so does a
// member that ends up alone in a group that it formed on its own.
func GroupIncarnation(viewID string) string {
	incarnation, _, _ := strings.Cut(viewID, ":")
	return incarnation
}

// GroupIncarnationTime returns when the group of the given incarnation was created, as MySQL encodes
// it in the incarnation: the fixed part of a Group Replication view id is the time, in units of 100
// nanoseconds since the Unix epoch, at which the group communication engine installed the group's
// first view (for example 17908892198863259 for a group bootstrapped at 2026-10-01 21:13:39.886
// UTC). It returns false when the incarnation is not such a time, between the years 2000 and 2200:
// it is an implementation detail of MySQL, which callers may only use as an additional check.
func GroupIncarnationTime(incarnation string) (time.Time, bool) {
	units, err := strconv.ParseInt(incarnation, 10, 64)
	if err != nil || units <= 0 || units > math.MaxInt64/100 {
		return time.Time{}, false
	}
	t := time.Unix(0, units*100).UTC()
	if t.Year() < 2000 || t.Year() > 2200 {
		return time.Time{}, false
	}
	return t, true
}

// GroupVoter identifies a voter of a shard's replication group in the membership view of a
// member: by the server_uuid of its MySQL when it is known, else by the MySQL address of its
// tablet record.
type GroupVoter struct {
	Alias *topodatapb.TabletAlias
	// ServerUUID is the server_uuid of the voter's MySQL, if known.
	ServerUUID string
	// MysqlHost and MysqlPort are the MySQL address of the voter's tablet record, if known. A
	// member reports them as MEMBER_HOST and MEMBER_PORT when MySQL's report_host matches the
	// tablet's MySQL hostname.
	MysqlHost string
	MysqlPort int32
}

// Matches returns whether the member of a membership view is the voter's MySQL.
func (v GroupVoter) Matches(m *replicationdatapb.GroupReplicationMember) bool {
	if v.ServerUUID != "" && m.GetMemberUuid() == v.ServerUUID {
		return true
	}
	return v.MysqlHost != "" && v.MysqlPort != 0 && m.GetPort() == v.MysqlPort && strings.EqualFold(m.GetHost(), v.MysqlHost)
}

// LegitimateGroup is the shard's legitimate replication group, as recorded in the shard record:
// the group's incarnation, and its voters. Group Replication decides which member is the primary,
// and Vitess follows it, but only within this group. A member can end up in a group that does not
// hold the shard's acknowledged transactions: a member that left its group, and was made to join
// again while the other members were leaving too, has been seen to form a new group of its own
// (a new incarnation) that has quorum in its own view. Following such a group loses every
// transaction that only the other members have.
//
// A nil LegitimateGroup knows nothing about the shard's group: only MySQL's own view quorum applies.
type LegitimateGroup struct {
	// Incarnation is the recorded incarnation of the shard's group. Empty means unknown, for
	// example for a group created before Vitess recorded incarnations.
	Incarnation string
	// Voters are the listed voters of the shard's group. Empty means that no voter is selected
	// yet; only MySQL's own view quorum applies then.
	Voters []GroupVoter
}

// NewLegitimateGroup returns the shard's legitimate group from its shard record fields. tablets
// maps tablet alias strings to the tablet records of the shard, and serverUUIDs maps tablet alias
// strings to the server_uuid of their MySQL; both may miss entries or be nil.
func NewLegitimateGroup(incarnation string, voters []*topodatapb.TabletAlias, tablets map[string]*topodatapb.Tablet, serverUUIDs map[string]string) *LegitimateGroup {
	g := &LegitimateGroup{Incarnation: incarnation}
	for _, alias := range voters {
		key := topoproto.TabletAliasString(alias)
		voter := GroupVoter{Alias: alias, ServerUUID: serverUUIDs[key]}
		if tablet := tablets[key]; tablet != nil {
			voter.MysqlHost = tablet.MysqlHostname
			voter.MysqlPort = tablet.MysqlPort
		}
		g.Voters = append(g.Voters, voter)
	}
	return g
}

// VoterMajority returns how many listed voters must be ONLINE in a view for the view to hold the
// majority of the shard's voters. It returns 0 when no voter is listed.
func (g *LegitimateGroup) VoterMajority() int {
	if g == nil || len(g.Voters) == 0 {
		return 0
	}
	return len(g.Voters)/2 + 1
}

// OnlineVoters returns how many listed voters are ONLINE in the member's view of its group.
func (g *LegitimateGroup) OnlineVoters(status *replicationdatapb.GroupReplicationStatus) int {
	if g == nil || status == nil {
		return 0
	}
	online := 0
	for _, voter := range g.Voters {
		for _, m := range status.GetMembers() {
			if m.GetState() == mysql.GroupMemberStateOnline && voter.Matches(m) {
				online++
				break
			}
		}
	}
	return online
}

// HasVoterMajority returns whether a majority of the listed voters are ONLINE in the member's view.
// Without listed voters, it is MySQL's own view quorum.
func (g *LegitimateGroup) HasVoterMajority(status *replicationdatapb.GroupReplicationStatus) bool {
	if status == nil {
		return false
	}
	if g == nil || len(g.Voters) == 0 {
		return status.GetHasQuorum()
	}
	return g.OnlineVoters(status) >= g.VoterMajority()
}

// IsForeignIncarnation returns whether the member is active in a group whose incarnation differs
// from the recorded one. Such a member is in a group that Vitess did not create: it must never be
// followed, and it must leave that group.
func (g *LegitimateGroup) IsForeignIncarnation(status *replicationdatapb.GroupReplicationStatus) bool {
	if g == nil || g.Incarnation == "" || !mysql.IsGroupMemberActive(status) {
		return false
	}
	incarnation := GroupIncarnation(status.GetViewId())
	return incarnation != "" && incarnation != g.Incarnation
}

// inIncarnation returns whether the member's view belongs to the recorded incarnation, or the
// incarnation is unknown.
func (g *LegitimateGroup) inIncarnation(status *replicationdatapb.GroupReplicationStatus) bool {
	if g == nil || g.Incarnation == "" {
		return true
	}
	return GroupIncarnation(status.GetViewId()) == g.Incarnation
}

// IsLegitimateMember returns whether the member is an active member of the shard's legitimate
// group: ONLINE or RECOVERING, and not in a foreign incarnation.
func (g *LegitimateGroup) IsLegitimateMember(status *replicationdatapb.GroupReplicationStatus) bool {
	return mysql.IsGroupMemberActive(status) && g.inIncarnation(status)
}

// IsLegitimatePrimary returns whether the member is the primary that Vitess may follow: it is the
// ONLINE primary of a group with quorum in its own view, its view belongs to the recorded
// incarnation (when one is recorded), and a majority of the listed voters are ONLINE in its view
// (MySQL's view quorum alone when no voter is listed).
func (g *LegitimateGroup) IsLegitimatePrimary(status *replicationdatapb.GroupReplicationStatus) bool {
	return mysql.IsGroupPrimary(status) && g.inIncarnation(status) && g.HasVoterMajority(status)
}
