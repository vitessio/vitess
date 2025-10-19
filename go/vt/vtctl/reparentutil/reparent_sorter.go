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

	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// reparentSorter sorts tablets by GTID positions and Promotion rules aimed at finding the best
// candidate for intermediate promotion in emergency reparent shard, and the new primary in planned reparent shard
type reparentSorter struct {
	tablets          []*topodatapb.Tablet
	positions        []*RelayLogPositions
	innodbBufferPool []int
	durability       policy.Durabler
}

// newReparentSorter creates a new reparentSorter
func newReparentSorter(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, durability policy.Durabler) *reparentSorter {
	return &reparentSorter{
		tablets:          tablets,
		positions:        positions,
		durability:       durability,
		innodbBufferPool: innodbBufferPool,
	}
}

// Len implements the Interface for sorting
func (rs *reparentSorter) Len() int { return len(rs.tablets) }

// Swap implements the Interface for sorting
func (rs *reparentSorter) Swap(i, j int) {
	rs.tablets[i], rs.tablets[j] = rs.tablets[j], rs.tablets[i]
	rs.positions[i], rs.positions[j] = rs.positions[j], rs.positions[i]
	if len(rs.innodbBufferPool) != 0 {
		rs.innodbBufferPool[i], rs.innodbBufferPool[j] = rs.innodbBufferPool[j], rs.innodbBufferPool[i]
	}
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

	// sort by combined positions. if equal, also sort by the executed GTID positions.
	jPositions := rs.positions[j]
	iPositions := rs.positions[i]

	if !iPositions.AtLeast(jPositions) {
		// [i] does not have all GTIDs that [j] does
		return false
	}
	if !jPositions.AtLeast(iPositions) {
		// [j] does not have all GTIDs that [i] does
		return true
	}

	// at this point, both have the same GTIDs
	// so we check their promotion rules
	jPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[i])

	// If the promotion rules are different then we want to sort by the promotion rules.
	if len(rs.innodbBufferPool) != 0 && jPromotionRule == iPromotionRule {
		if rs.innodbBufferPool[i] > rs.innodbBufferPool[j] {
			return true
		}
		if rs.innodbBufferPool[j] > rs.innodbBufferPool[i] {
			return false
		}
	}

	return !jPromotionRule.BetterThan(iPromotionRule)
}

// sortTabletsForReparent sorts the tablets, given their positions for emergency reparent shard and planned reparent shard.
// Tablets are sorted first by their replication positions, with ties broken by the promotion rules.
func sortTabletsForReparent(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, durability policy.Durabler) error {
	// throw an error internal error in case of unequal number of tablets and positions
	// fail-safe code prevents panic in sorting in case the lengths are unequal
	if len(tablets) != len(positions) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and positions")
	}

	sort.Sort(newReparentSorter(tablets, positions, innodbBufferPool, durability))
	return nil
}
