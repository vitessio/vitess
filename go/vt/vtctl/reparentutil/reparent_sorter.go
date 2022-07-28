/*
Copyright 2021 The Vitess Authors.

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
	"sort"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// reparentSorter sorts tablets by GTID positions and Promotion rules aimed at finding the best
// candidate for intermediate promotion in emergency reparent shard, and the new primary in planned reparent shard
type reparentSorter struct {
	tablets    []*topodatapb.Tablet
	positions  []mysql.Position
	durability Durabler
}

// newReparentSorter creates a new reparentSorter
func newReparentSorter(tablets []*topodatapb.Tablet, positions []mysql.Position, durability Durabler) *reparentSorter {
	return &reparentSorter{
		tablets:    tablets,
		positions:  positions,
		durability: durability,
	}
}

// Len implements the Interface for sorting
func (rs *reparentSorter) Len() int { return len(rs.tablets) }

// Swap implements the Interface for sorting
func (rs *reparentSorter) Swap(i, j int) {
	rs.tablets[i], rs.tablets[j] = rs.tablets[j], rs.tablets[i]
	rs.positions[i], rs.positions[j] = rs.positions[j], rs.positions[i]
}

// Less implements the Interface for sorting
func (rs *reparentSorter) Less(i, j int) bool {
	// Returning "true" in this function means [i] is before [j] in the sorting order,
	// which will lead to [i] be a better candidate for promotion

	// Should not happen
	// fail-safe code
	if rs.tablets[i] == nil {
		return false
	}
	if rs.tablets[j] == nil {
		return true
	}

	if !rs.positions[i].AtLeast(rs.positions[j]) {
		// [i] does not have all GTIDs that [j] does
		return false
	}
	if !rs.positions[j].AtLeast(rs.positions[i]) {
		// [j] does not have all GTIDs that [i] does
		return true
	}

	// at this point, both have the same GTIDs
	// so we check their promotion rules
	jPromotionRule := PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := PromotionRule(rs.durability, rs.tablets[i])
	return !jPromotionRule.BetterThan(iPromotionRule)
}

// sortTabletsForReparent sorts the tablets, given their positions for emergency reparent shard and planned reparent shard.
// Tablets are sorted first by their replication positions, with ties broken by the promotion rules.
func sortTabletsForReparent(tablets []*topodatapb.Tablet, positions []mysql.Position, durability Durabler) error {
	// throw an error internal error in case of unequal number of tablets and positions
	// fail-safe code prevents panic in sorting in case the lengths are unequal
	if len(tablets) != len(positions) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and positions")
	}

	sort.Sort(newReparentSorter(tablets, positions, durability))
	return nil
}
