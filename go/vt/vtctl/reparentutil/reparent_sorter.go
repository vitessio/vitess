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
	"math"
	"sort"

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var unknownVersion = mysqlctl.ServerVersion{Major: math.MaxInt, Minor: math.MaxInt, Patch: math.MaxInt}

// sameFlavorFamily reports whether all the given flavors belong to a single
// replication-compatibility family, so their version numbers can be meaningfully
// compared.
//
// Version ordering is only meaningful within one family. MySQL and Percona share
// a version lineage and are comparable, so a mix of the two is fine; MariaDB has
// its own lineage (10.x/11.x), so comparing it against MySQL/Percona is
// nonsensical (10 > 8 does not mean MariaDB is "newer" in any compatibility
// sense). Flavors whose family could not be determined (FlavorFamilyUnknown) are
// ignored — they are handled by the unknownVersion sentinel, which sorts them
// last while still comparing the known candidates against each other.
func sameFlavorFamily(flavors []mysqlctl.MySQLFlavor) bool {
	var knownFamily mysqlctl.FlavorFamily
	for _, f := range flavors {
		family := f.ReplicationFamily()
		if family == mysqlctl.FlavorFamilyUnknown {
			continue
		}
		if knownFamily == "" {
			knownFamily = family
			continue
		}
		if family != knownFamily {
			return false
		}
	}
	return true
}

// usableMySQLVersions returns mysqlVersions unchanged when it is safe to use them
// as an election tiebreaker, or nil to disable version-aware ordering when the
// candidates span more than one flavor family (see sameFlavorFamily).
//
// mysqlVersions and flavors are parallel slices; a nil/empty mysqlVersions is
// returned unchanged (version ordering already disabled).
func usableMySQLVersions(mysqlVersions []mysqlctl.ServerVersion, flavors []mysqlctl.MySQLFlavor) []mysqlctl.ServerVersion {
	if len(mysqlVersions) == 0 {
		return mysqlVersions
	}
	if !sameFlavorFamily(flavors) {
		return nil
	}
	return mysqlVersions
}

// scopedVersionMap returns versionMap when it is safe to use as an election
// tiebreaker for the given candidate set, or nil to disable version-aware
// ordering when those candidates span more than one flavor family (see
// sameFlavorFamily).
//
// The guard is scoped to the passed candidates specifically — not to every
// tablet in versionMap/flavorMap — so a non-candidate tablet elsewhere in the
// shard (e.g. one dropped for errant GTIDs) cannot disable version comparison for
// the tablets actually being elected among. Used on the ERS election path where
// versions and flavors are keyed by tablet alias.
func scopedVersionMap(candidates []*topodatapb.Tablet, versionMap map[string]mysqlctl.ServerVersion, flavorMap map[string]mysqlctl.MySQLFlavor) map[string]mysqlctl.ServerVersion {
	if len(versionMap) == 0 {
		return versionMap
	}
	flavors := make([]mysqlctl.MySQLFlavor, 0, len(candidates))
	for _, c := range candidates {
		flavors = append(flavors, flavorMap[topoproto.TabletAliasString(c.Alias)])
	}
	if !sameFlavorFamily(flavors) {
		return nil
	}
	return versionMap
}

// SortMode controls the priority order used when sorting reparent candidates.
type SortMode int

const (
	// SortByPosition sorts by: position > promotion rules > version > buffer pool > alias.
	// It is used when the elected tablet is promoted without first catching it up to a
	// source, so replication position must lead to avoid discarding transactions; version
	// only breaks ties among equally-advanced candidates. ERS uses this (it must minimize
	// data loss), as does PRS on the no-clear-primary path (which promotes without catch-up).
	SortByPosition SortMode = iota
	// SortByVersion sorts by: promotion rules > version > position > buffer pool > alias.
	// It is used when the elected tablet is caught up to a known position before promotion
	// (or when no tablet has ever replicated), so replication position is not a data-safety
	// concern and a compatible MySQL version can be preferred. PRS uses this on the graceful
	// and initialization paths.
	SortByVersion
)

// reparentSorter sorts tablets for candidate election during reparent operations.
type reparentSorter struct {
	tablets                []*topodatapb.Tablet
	positions              []*RelayLogPositions
	combinedDominatedCount []int
	executedDominatedCount []int
	innodbBufferPool       []int
	mysqlVersions          []mysqlctl.ServerVersion
	durability             policy.Durabler
	mode                   SortMode
}

func newReparentSorter(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, mysqlVersions []mysqlctl.ServerVersion, durability policy.Durabler, mode SortMode) *reparentSorter {
	return &reparentSorter{
		tablets:   tablets,
		positions: positions,
		combinedDominatedCount: dominatedCountsForSort(tablets, positions, func(moreAdvanced, lessAdvanced *RelayLogPositions) bool {
			return hasDominantPosition(moreAdvanced.Combined, lessAdvanced.Combined)
		}),
		executedDominatedCount: dominatedCountsForSort(tablets, positions, func(moreAdvanced, lessAdvanced *RelayLogPositions) bool {
			return moreAdvanced.Combined.Equal(lessAdvanced.Combined) &&
				hasDominantPosition(moreAdvanced.Executed, lessAdvanced.Executed)
		}),
		durability:       durability,
		innodbBufferPool: innodbBufferPool,
		mysqlVersions:    mysqlVersions,
		mode:             mode,
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
	if len(rs.mysqlVersions) != 0 {
		rs.mysqlVersions[i], rs.mysqlVersions[j] = rs.mysqlVersions[j], rs.mysqlVersions[i]
	}
}

// Less implements the Interface for sorting.
// Returning true means [i] is a better candidate for promotion than [j].
func (rs *reparentSorter) Less(i, j int) bool {
	// Returning "true" in this function means [i] is before [j] in the sorting order,
	// which will lead to [i] be a better candidate for promotion

	// Should not happen
	// fail-safe code
	if rs.tablets[i] == nil || rs.tablets[i].Alias == nil {
		return false
	}
	if rs.tablets[j] == nil || rs.tablets[j].Alias == nil {
		return true
	}

	if rs.mode == SortByVersion {
		return rs.lessVersionFirst(i, j)
	}
	return rs.lessPositionFirst(i, j)
}

// comparePosition orders by replication position using the precomputed dominated
// counts (how many other candidates strictly dominate each one). A lower count is
// more advanced. Combined dominance takes precedence; at an equal count it falls
// through to the Executed-dominated count, which prefers less SQL delay that would
// otherwise slow down the reparent. GTID positions are only partially ordered, so
// counts are not unique: incomparable candidates (disjoint UUIDs) can share a count,
// in which case comparePosition returns 0 and leaves the decision to the next
// tiebreaker. Counting dominators keeps the sort transitive-safe where a naive
// pairwise comparison is not; findMostAdvanced still re-checks the winner after
// sorting.
//
// Returns -1 if [i] is more advanced, +1 if [j] is, and 0 when neither dominates.
func (rs *reparentSorter) comparePosition(i, j int) int {
	if rs.combinedDominatedCount[i] != rs.combinedDominatedCount[j] {
		if rs.combinedDominatedCount[i] < rs.combinedDominatedCount[j] {
			return -1
		}
		return 1
	}
	if rs.executedDominatedCount[i] != rs.executedDominatedCount[j] {
		if rs.executedDominatedCount[i] < rs.executedDominatedCount[j] {
			return -1
		}
		return 1
	}
	return 0
}

// lessPositionFirst sorts by: position > promotion rules > version > buffer pool > alias.
func (rs *reparentSorter) lessPositionFirst(i, j int) bool {
	if v := rs.comparePosition(i, j); v != 0 {
		return v < 0
	}

	jPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[i])

	if jPromotionRule != iPromotionRule {
		return !jPromotionRule.BetterThan(iPromotionRule)
	}

	if v := rs.compareVersion(i, j); v != 0 {
		return v < 0
	}

	if v := rs.compareBufferPool(i, j); v != 0 {
		return v < 0
	}

	return rs.compareAlias(i, j)
}

// lessVersionFirst sorts by: promotion rules > version > position > buffer pool > alias.
func (rs *reparentSorter) lessVersionFirst(i, j int) bool {
	jPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[i])

	if jPromotionRule != iPromotionRule {
		return !jPromotionRule.BetterThan(iPromotionRule)
	}

	if v := rs.compareVersion(i, j); v != 0 {
		return v < 0
	}

	if v := rs.comparePosition(i, j); v != 0 {
		return v < 0
	}

	if v := rs.compareBufferPool(i, j); v != 0 {
		return v < 0
	}

	return rs.compareAlias(i, j)
}

// compareVersion returns -1 if i has a lower version, +1 if j does, 0 if they
// are equivalent for replication. Comparison is by major.minor, with the patch
// component significant only within the pre-8.0.34 MySQL 8.0 series (see
// ServerVersion.CompareForReplication).
func (rs *reparentSorter) compareVersion(i, j int) int {
	if len(rs.mysqlVersions) == 0 {
		return 0
	}
	iVersion := rs.mysqlVersions[i]
	jVersion := rs.mysqlVersions[j]
	return iVersion.CompareForReplication(jVersion)
}

func (rs *reparentSorter) compareBufferPool(i, j int) int {
	if len(rs.innodbBufferPool) == 0 {
		return 0
	}
	if rs.innodbBufferPool[i] > rs.innodbBufferPool[j] {
		return -1
	}
	if rs.innodbBufferPool[j] > rs.innodbBufferPool[i] {
		return 1
	}
	return 0
}

func (rs *reparentSorter) compareAlias(i, j int) bool {
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
		if tablets[i] == nil || tablets[i].Alias == nil {
			continue
		}
		for j := range positions {
			if i == j || tablets[j] == nil || tablets[j].Alias == nil {
				continue
			}
			if dominates(positions[j], positions[i]) {
				dominatedCounts[i]++ // one more candidate strictly dominates i
			}
		}
	}
	return dominatedCounts
}

// hasDominantReparentPosition reports whether moreAdvanced is strictly ahead of
// lessAdvanced under the same two-level order the sorter uses: a strictly greater
// received (Combined) history, or an equal received history with strictly more of it
// applied (Executed). findMostAdvanced uses it as a defense-in-depth check that the
// sort really did place the maximum at index 0 — it should never find a candidate that
// dominates the chosen winner.
func hasDominantReparentPosition(moreAdvanced, lessAdvanced *RelayLogPositions) bool {
	return hasDominantPosition(moreAdvanced.Combined, lessAdvanced.Combined) ||
		(moreAdvanced.Combined.Equal(lessAdvanced.Combined) &&
			hasDominantPosition(moreAdvanced.Executed, lessAdvanced.Executed))
}

// sortTabletsForReparent sorts tablets for candidate election.
// With SortByVersion, the order is: promotion rules > version > position > buffer pool > alias.
// With SortByPosition, the order is: position > promotion rules > version > buffer pool > alias.
func sortTabletsForReparent(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, mysqlVersions []mysqlctl.ServerVersion, durability policy.Durabler, mode SortMode) error {
	if len(tablets) != len(positions) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and positions")
	}
	if len(innodbBufferPool) != 0 && len(innodbBufferPool) != len(tablets) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and innodb buffer pool entries")
	}
	if len(mysqlVersions) != 0 && len(mysqlVersions) != len(tablets) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and mysql versions")
	}

	sort.Sort(newReparentSorter(tablets, positions, innodbBufferPool, mysqlVersions, durability, mode))
	return nil
}
