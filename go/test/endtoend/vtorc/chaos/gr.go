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

package chaos

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// GRState is a member's own view of its replication group.
type GRState struct {
	OK          bool   // the query succeeded
	State       string // own MEMBER_STATE (OFFLINE if the plugin is not running)
	Role        string // own MEMBER_ROLE
	PrimaryUUID string // ONLINE PRIMARY in the member's view
	Online      int    // ONLINE members in the member's view
	Members     int    // members in the member's view
	Received    string // RECEIVED_TRANSACTION_SET of the group_replication_applier channel
}

func (g GRState) String() string {
	if !g.OK {
		return "gr=?"
	}
	s := "gr=" + g.State
	if g.Role != "" {
		s += "/" + g.Role
	}
	return s + fmt.Sprintf(" view=%d/%d", g.Online, g.Members)
}

// grState reads the member's view of its group.
func (n *Node) grState() GRState {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return n.grStateCtx(ctx)
}

func (n *Node) grStateCtx(ctx context.Context) GRState {
	rows, err := n.queryCtx(ctx, "select m.MEMBER_ID, m.MEMBER_STATE, m.MEMBER_ROLE, @@global.server_uuid as me from performance_schema.replication_group_members m")
	if err != nil {
		return GRState{}
	}
	g := GRState{OK: true, State: "OFFLINE"}
	for _, r := range rows {
		if r["MEMBER_ID"] == "" {
			continue
		}
		g.Members++
		if r["MEMBER_STATE"] == "ONLINE" {
			g.Online++
			if r["MEMBER_ROLE"] == "PRIMARY" {
				g.PrimaryUUID = r["MEMBER_ID"]
			}
		}
		if r["MEMBER_ID"] == r["me"] {
			g.State = r["MEMBER_STATE"]
			g.Role = r["MEMBER_ROLE"]
		}
	}
	if g.State != "ONLINE" {
		g.Role = ""
	}
	if rows, err := n.queryCtx(ctx, "select RECEIVED_TRANSACTION_SET from performance_schema.replication_connection_status where CHANNEL_NAME = 'group_replication_applier'"); err == nil && len(rows) == 1 {
		g.Received = strings.ReplaceAll(rows[0]["RECEIVED_TRANSACTION_SET"], "\n", "")
	}
	return g
}

// grConvergenceProblems is the Group Replication part of convergenceProblems: every voter that the
// shard record lists is an ONLINE member whose view has all voters ONLINE and p as the primary, and
// no voter is a tablet that a scenario took out for good.
func (c *Chaos) grConvergenceProblems(p *Node) []string {
	var probs []string
	puuid := p.serverUUID()
	voters, err := c.shardVoters()
	if err != nil {
		return []string{fmt.Sprintf("shard record voters: %v", err)}
	}
	for _, n := range voters {
		if c.isGone(n) {
			probs = append(probs, n.Tablet.Alias+" is gone but still a voter")
			continue
		}
		g := n.grState()
		if !g.OK || g.State != "ONLINE" || g.PrimaryUUID != puuid || g.Online != len(voters) {
			probs = append(probs, fmt.Sprintf("%s: %s primary=%s", n.Tablet.Alias, g, aliasOf(c.nodeByUUIDCached(g.PrimaryUUID))))
		}
		// An ONLINE member whose offline_mode was left ON cannot serve: MySQL refuses its
		// vttablet's app connections.
		if off, err := n.scalar("select @@global.offline_mode"); err != nil || off != "0" {
			probs = append(probs, fmt.Sprintf("%s offline_mode=%s err=%v", n.Tablet.Alias, off, err))
		}
	}
	return probs
}

// nodeByUUIDCached maps a server_uuid to a node without querying nodes that are down.
func (c *Chaos) nodeByUUIDCached(uuid string) *Node {
	if uuid == "" {
		return nil
	}
	c.uuidMu.Lock()
	defer c.uuidMu.Unlock()
	if c.uuids == nil {
		c.uuids = map[string]*Node{}
	}
	if n, ok := c.uuids[uuid]; ok {
		return n
	}
	for _, n := range c.Nodes {
		u := n.serverUUID()
		if u != "" {
			c.uuids[u] = n
		}
	}
	return c.uuids[uuid]
}

// grCheckInvariants replaces the semi-sync configuration check in Group Replication mode: the
// final primary is the ONLINE group primary, every voter is an ONLINE member, the shard record
// lists one voter per cell (or wantVoters), none of them gone, and semi-sync is off on the primary.
func (c *Chaos) grCheckInvariants(r *Report, p *Node) {
	deadline := time.Now().Add(60 * time.Second)
	var probs []string
	for {
		probs = c.grConvergenceProblems(p)
		if len(probs) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(probs) > 0 {
		r.violation("GROUP: not every voter is an ONLINE member of %s's group after 60s: %s", p.Tablet.Alias, strings.Join(probs, "; "))
	} else {
		voters, _ := c.shardVoters()
		r.outcome("group: all %d voters ONLINE members, primary %s", len(voters), p.Tablet.Alias)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if si, err := c.Ts.GetShard(ctx, keyspaceName, shardName); err == nil {
		var voters []string
		for _, v := range si.GroupReplicationVoters {
			if n := c.nodeByAlias(v); n != nil {
				voters = append(voters, n.Tablet.Alias)
			}
		}
		r.note("shard record voters: %v", voters)
		want := c.wantVoters
		if want == 0 {
			want = len(cells)
		}
		if len(voters) != want {
			r.violation("GROUP: shard record lists %d voters, want %d", len(voters), want)
		}
	}
	if v := p.variables("rpl_semi_sync_source_enabled")["rpl_semi_sync_source_enabled"]; v == "ON" {
		r.note("primary %s still has rpl_semi_sync_source_enabled=ON", p.Tablet.Alias)
	}
}

// aliasOf returns the alias of the node's tablet, or <none>.
func aliasOf(n *Node) string {
	if n == nil {
		return "<none>"
	}
	return n.Tablet.Alias
}
