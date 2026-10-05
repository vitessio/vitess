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

package inst

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestVoterChangeRefusal checks the rule that VTOrc applies to every change of the voter list: no
// view of the shard's group that lacks a majority of the current voters may hold a majority of the
// new ones.
func TestVoterChangeRefusal(t *testing.T) {
	tablet := func(cell string, uid uint32) *topodatapb.Tablet {
		return &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: cell, Uid: uid}, Type: topodatapb.TabletType_REPLICA}
	}
	uuid := func(tablet *topodatapb.Tablet) string { return fmt.Sprintf("uuid-%d", tablet.Alias.Uid) }
	a, b, c := tablet("zone1", 100), tablet("zone2", 200), tablet("zone3", 300)
	b2, c2 := tablet("zone2", 201), tablet("zone3", 301)
	aliases := func(tablets ...*topodatapb.Tablet) []*topodatapb.TabletAlias {
		var list []*topodatapb.TabletAlias
		for _, tablet := range tablets {
			list = append(list, tablet.Alias)
		}
		return list
	}
	uuids := func(tablets ...*topodatapb.Tablet) []string {
		var list []string
		for _, tablet := range tablets {
			list = append(list, uuid(tablet))
		}
		return list
	}
	// member is a reachable active member whose view holds the given tablets ONLINE.
	member := func(tablet *topodatapb.Tablet, view ...*topodatapb.Tablet) *VoterObservation {
		return &VoterObservation{
			Tablet: tablet, Reachable: true, Active: true, ServerUUID: uuid(tablet),
			ActiveMemberUUIDs: uuids(view...), OnlineMemberUUIDs: uuids(view...),
		}
	}
	// spare is a reachable tablet whose MySQL is not a member.
	spare := func(tablet *topodatapb.Tablet) *VoterObservation {
		return &VoterObservation{Tablet: tablet, Reachable: true, ServerUUID: uuid(tablet)}
	}
	// down is an unreachable tablet, whose MySQL VTOrc last saw with its server_uuid.
	down := func(tablet *topodatapb.Tablet) *VoterObservation {
		return &VoterObservation{Tablet: tablet, ServerUUID: uuid(tablet)}
	}
	tests := []struct {
		name              string
		current, proposed []*topodatapb.TabletAlias
		observations      []*VoterObservation
		wantRefused       bool
	}{{
		name:         "no voter selected yet",
		proposed:     aliases(a),
		observations: []*VoterObservation{member(a, a), down(b), down(c)},
	}, {
		name:         "a view of one of three voters would hold the majority of a list of one",
		current:      aliases(a, b, c),
		proposed:     aliases(a),
		observations: []*VoterObservation{member(a, a), down(b), down(c)},
		wantRefused:  true,
	}, {
		name:         "a view of one of three voters gains no majority of a list of two, whose other voter must join it first",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b),
		observations: []*VoterObservation{member(a, a), spare(b), down(c)},
	}, {
		name:         "a failed voter is replaced with a spare of its cell, its view holds the voter majority",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b, c2),
		observations: []*VoterObservation{member(a, a, b), member(b, a, b), down(c), spare(c2)},
	}, {
		name:         "a view of one of three voters gains no majority with two spares, which must join it first",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b2, c2),
		observations: []*VoterObservation{member(a, a), down(b), down(c), spare(b2), spare(c2)},
	}, {
		name:     "a RECOVERING voter does not count for the current majority, but does for the new one",
		current:  aliases(a, b, c),
		proposed: aliases(a, b),
		observations: []*VoterObservation{
			{Tablet: a, Reachable: true, Active: true, ServerUUID: uuid(a), ActiveMemberUUIDs: uuids(a, b), OnlineMemberUUIDs: uuids(a)},
			{Tablet: b, Reachable: true, Active: true, ServerUUID: uuid(b), ActiveMemberUUIDs: uuids(a, b), OnlineMemberUUIDs: uuids(a)},
			down(c),
		},
		wantRefused: true,
	}, {
		name:         "a view of another incarnation does not count",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b, c2),
		observations: []*VoterObservation{member(a, a, b), member(b, a, b), down(c), {Tablet: c2, Reachable: true, Active: true, Foreign: true, ServerUUID: uuid(c2), ActiveMemberUUIDs: uuids(c2), OnlineMemberUUIDs: uuids(c2)}},
	}, {
		name:         "new voters that VTOrc cannot place in a view could hold the new majority on their own",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b2, c2),
		observations: []*VoterObservation{member(a, a, b, c), member(b, a, b, c), member(c, a, b, c), down(b2), down(c2)},
		wantRefused:  true,
	}, {
		name:         "an unreachable new voter counts in every view",
		current:      aliases(a, b, c),
		proposed:     aliases(a, c2),
		observations: []*VoterObservation{member(a, a), down(b), down(c), down(c2)},
		wantRefused:  true,
	}, {
		name:         "an unreachable new voter that a view reports active counts in that view only",
		current:      aliases(a, b, c),
		proposed:     aliases(a, b, c2),
		observations: []*VoterObservation{member(a, a, b, c2), member(b, a, b, c2), down(c), down(c2)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := voterChangeRefusal(tt.current, tt.proposed, tt.observations)
			if tt.wantRefused {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}
