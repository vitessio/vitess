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
	tablets                []*topodatapb.Tablet
	positions              []*RelayLogPositions
	combinedDominatedCount []int
	executedDominatedCount []int
	innodbBufferPool       []int
	durability             policy.Durabler
}

// newReparentSorter creates a new reparentSorter
func newReparentSorter(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, durability policy.Durabler) *reparentSorter {
	return &reparentSorter{
		tablets:   tablets,
		positions: positions,
		combinedDominatedCount: dominatedCountsForSort(tablets, positions, func(moreAdvanced, lessAdvanced *RelayLogPositions) bool {
			return moreAdvanced.Combined.AtLeast(lessAdvanced.Combined) &&
				!lessAdvanced.Combined.AtLeast(moreAdvanced.Combined)
		}),
		executedDominatedCount: dominatedCountsForSort(tablets, positions, func(moreAdvanced, lessAdvanced *RelayLogPositions) bool {
			return moreAdvanced.Combined.Equal(lessAdvanced.Combined) &&
				moreAdvanced.Executed.AtLeast(lessAdvanced.Executed) &&
				!lessAdvanced.Executed.AtLeast(moreAdvanced.Executed)
		}),
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
	rs.combinedDominatedCount[i], rs.combinedDominatedCount[j] = rs.combinedDominatedCount[j], rs.combinedDominatedCount[i]
	rs.executedDominatedCount[i], rs.executedDominatedCount[j] = rs.executedDominatedCount[j], rs.executedDominatedCount[i]
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

	if rs.combinedDominatedCount[i] != rs.combinedDominatedCount[j] {
		return rs.combinedDominatedCount[i] < rs.combinedDominatedCount[j]
	}

	if rs.executedDominatedCount[i] != rs.executedDominatedCount[j] {
		return rs.executedDominatedCount[i] < rs.executedDominatedCount[j]
	}

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

	if jPromotionRule != iPromotionRule {
		return !jPromotionRule.BetterThan(iPromotionRule)
	}

	// All else equal, use the full tablet alias as a stable tiebreaker so
	// that sort order is deterministic across runs, including across cells.
	if rs.tablets[i].Alias.Cell != rs.tablets[j].Alias.Cell {
		return rs.tablets[i].Alias.Cell < rs.tablets[j].Alias.Cell
	}
	return rs.tablets[i].Alias.Uid < rs.tablets[j].Alias.Uid
}

// dominatedCountsForSort returns, for each candidate, how many other candidates
// strictly dominate it under the dominates predicate. The result is only meaningful
// as a sort key: a lower count means more advanced, and the maximal candidates that
// nothing dominates all have count 0.
//
// This count is what lets Less sort safely. dominates is only a partial order, so
// comparing two candidates head-to-head is not transitive — with an incomparable
// third candidate in play, sort.Sort can otherwise seat a dominated candidate above
// the one that dominates it. Counting dominators sidesteps that, because dominates
// itself is transitive: whatever dominates a candidate also dominates everything below
// it, so a dominated candidate always has a strictly higher count than its dominator.
// Ordering by ascending count therefore never places a candidate ahead of one that
// dominates it.
//
// Counts are not unique. Incomparable candidates (e.g. divergent GTID histories) can
// share a count; the caller breaks those ties with its remaining preferences.
func dominatedCountsForSort(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, dominates func(*RelayLogPositions, *RelayLogPositions) bool) []int {
	dominatedCounts := make([]int, len(positions))
	for i := range positions {
		if tablets[i] == nil {
			continue
		}
		for j := range positions {
			if i == j || tablets[j] == nil {
				continue
			}
			if dominates(positions[j], positions[i]) {
				dominatedCounts[i]++
			}
		}
	}
	return dominatedCounts
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
