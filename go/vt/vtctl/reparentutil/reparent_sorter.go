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
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var unknownVersion = mysqlctl.ServerVersion{Major: math.MaxInt, Minor: math.MaxInt, Patch: math.MaxInt}

// usableMySQLVersions returns mysqlVersions unchanged when it is safe to use them
// as an election tiebreaker, or nil to disable version-aware ordering entirely.
//
// Version ordering is only meaningful when every candidate belongs to the same
// replication-compatibility family. MySQL and Percona share a version lineage
// and are comparable, so a mix of the two is fine; MariaDB has its own lineage
// (10.x/11.x), so comparing it against MySQL/Percona is nonsensical (10 > 8 does
// not mean MariaDB is "newer" in any compatibility sense). When candidates span
// more than one family we fall back to the pre-version ordering. Candidates
// whose flavor could not be determined (FlavorUnknown) do not by themselves
// disable ordering — they are handled by the unknownVersion sentinel, which
// sorts them last while still comparing the known candidates against each other.
//
// mysqlVersions and flavors are parallel slices; a nil/empty mysqlVersions is
// returned unchanged (version ordering already disabled).
func usableMySQLVersions(mysqlVersions []mysqlctl.ServerVersion, flavors []mysqlctl.MySQLFlavor) []mysqlctl.ServerVersion {
	if len(mysqlVersions) == 0 {
		return mysqlVersions
	}

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
			// Candidates span multiple flavor families: version comparison is
			// meaningless, so disable version-aware ordering and fall back to
			// position/promotion ordering.
			return nil
		}
	}
	return mysqlVersions
}

// SortMode controls the priority order used when sorting reparent candidates.
type SortMode int

const (
	// SortForERS sorts by: position > promotion rules > version > buffer pool > alias.
	// Position is paramount because ERS must minimize data loss.
	SortForERS SortMode = iota
	// SortForPRS sorts by: promotion rules > version > position > buffer pool > alias.
	// PRS always catches the elected tablet up to the old primary's position, so version
	// compatibility matters more than replication position.
	SortForPRS
)

// reparentSorter sorts tablets for candidate election during reparent operations.
type reparentSorter struct {
	tablets          []*topodatapb.Tablet
	positions        []*RelayLogPositions
	innodbBufferPool []int
	mysqlVersions    []mysqlctl.ServerVersion
	durability       policy.Durabler
	mode             SortMode
}

func newReparentSorter(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, mysqlVersions []mysqlctl.ServerVersion, durability policy.Durabler, mode SortMode) *reparentSorter {
	return &reparentSorter{
		tablets:          tablets,
		positions:        positions,
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
	if rs.tablets[i] == nil {
		return false
	}
	if rs.tablets[j] == nil {
		return true
	}

	if rs.mode == SortForPRS {
		return rs.lessPRS(i, j)
	}
	return rs.lessERS(i, j)
}

// lessERS sorts for ERS: position > promotion rules > version > buffer pool > alias.
// Position is paramount because ERS must minimize data loss.
func (rs *reparentSorter) lessERS(i, j int) bool {
	jPositions := rs.positions[j]
	iPositions := rs.positions[i]

	if !iPositions.AtLeast(jPositions) {
		// [i] is missing GTIDs that [j] has — [j] is more advanced
		return false
	}
	if !jPositions.AtLeast(iPositions) {
		// [j] is missing GTIDs that [i] has — [i] is more advanced
		return true
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

// lessPRS sorts for PRS: promotion rules > version > position > buffer pool > alias.
// PRS always catches the elected tablet up to the old primary's position, so replication
// position is irrelevant for data safety. Version compatibility matters more because
// promoting a newer-version primary breaks replication for older-version replicas.
func (rs *reparentSorter) lessPRS(i, j int) bool {
	jPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[i])

	if jPromotionRule != iPromotionRule {
		return !jPromotionRule.BetterThan(iPromotionRule)
	}

	if v := rs.compareVersion(i, j); v != 0 {
		return v < 0
	}

	jPositions := rs.positions[j]
	iPositions := rs.positions[i]

	if !iPositions.AtLeast(jPositions) {
		// [i] is missing GTIDs that [j] has — [j] is more advanced
		return false
	}
	if !jPositions.AtLeast(iPositions) {
		// [j] is missing GTIDs that [i] has — [i] is more advanced
		return true
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

// sortTabletsForReparent sorts tablets for candidate election.
// With SortForPRS, the order is: promotion rules > version > position > buffer pool > alias.
// With SortForERS, the order is: position > promotion rules > version > buffer pool > alias.
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
