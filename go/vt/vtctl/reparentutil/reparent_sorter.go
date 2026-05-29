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

// reparentSorter sorts tablets by GTID positions, promotion rules, MySQL version, and InnoDB buffer pool size
// aimed at finding the best candidate for intermediate promotion in ERS, and the new primary in PRS.
type reparentSorter struct {
	tablets          []*topodatapb.Tablet
	positions        []*RelayLogPositions
	innodbBufferPool []int
	mysqlVersions    []mysqlctl.ServerVersion
	durability       policy.Durabler
}

// newReparentSorter creates a new reparentSorter
func newReparentSorter(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, mysqlVersions []mysqlctl.ServerVersion, durability policy.Durabler) *reparentSorter {
	return &reparentSorter{
		tablets:          tablets,
		positions:        positions,
		durability:       durability,
		innodbBufferPool: innodbBufferPool,
		mysqlVersions:    mysqlVersions,
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

	// At this point, both have the same GTIDs.
	// Check promotion rules next — they represent explicit operator intent.
	jPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[j])
	iPromotionRule := policy.PromotionRule(rs.durability, rs.tablets[i])

	if jPromotionRule != iPromotionRule {
		return !jPromotionRule.BetterThan(iPromotionRule)
	}

	// Same promotion rule. Prefer lower MySQL release (major.minor) to maintain replication
	// compatibility — replicas must be at the same or higher version than the primary.
	// Patch differences within the same release are ignored.
	if len(rs.mysqlVersions) != 0 {
		iVersion := rs.mysqlVersions[i]
		jVersion := rs.mysqlVersions[j]
		if !iVersion.IsSameRelease(jVersion) {
			iAtLeastJ := iVersion.ReleaseAtLeast(jVersion)
			jAtLeastI := jVersion.ReleaseAtLeast(iVersion)
			if !iAtLeastJ {
				return true
			}
			if !jAtLeastI {
				return false
			}
		}
	}

	if len(rs.innodbBufferPool) != 0 {
		if rs.innodbBufferPool[i] > rs.innodbBufferPool[j] {
			return true
		}
		if rs.innodbBufferPool[j] > rs.innodbBufferPool[i] {
			return false
		}
	}

	// All else equal, use the full tablet alias as a stable tiebreaker so
	// that sort order is deterministic across runs, including across cells.
	if rs.tablets[i].Alias.Cell != rs.tablets[j].Alias.Cell {
		return rs.tablets[i].Alias.Cell < rs.tablets[j].Alias.Cell
	}
	return rs.tablets[i].Alias.Uid < rs.tablets[j].Alias.Uid
}

// sortTabletsForReparent sorts the tablets, given their positions for emergency reparent shard and planned reparent shard.
// Tablets are sorted by: replication position (most advanced preferred), promotion rules (operator intent),
// MySQL version (lowest preferred to maintain replication compatibility), InnoDB buffer pool size, and tablet alias.
func sortTabletsForReparent(tablets []*topodatapb.Tablet, positions []*RelayLogPositions, innodbBufferPool []int, mysqlVersions []mysqlctl.ServerVersion, durability policy.Durabler) error {
	if len(tablets) != len(positions) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and positions")
	}
	if len(innodbBufferPool) != 0 && len(innodbBufferPool) != len(tablets) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and innodb buffer pool entries")
	}
	if len(mysqlVersions) != 0 && len(mysqlVersions) != len(tablets) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and mysql versions")
	}

	sort.Sort(newReparentSorter(tablets, positions, innodbBufferPool, mysqlVersions, durability))
	return nil
}
