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

// ERSIntermediateSourceSorter sorts tablets by GTID positions and Promotion rules aimed at finding the best
// candidate for intermediate promotion in emergency reparent shard
type ERSIntermediateSourceSorter struct {
	tablets   []*topodatapb.Tablet
	positions []mysql.Position
}

// NewERSIntermediateSourceSorter creates a new ERSIntermediateSourceSorter
func NewERSIntermediateSourceSorter(tablets []*topodatapb.Tablet, positions []mysql.Position) *ERSIntermediateSourceSorter {
	return &ERSIntermediateSourceSorter{
		tablets:   tablets,
		positions: positions,
	}
}

// Len implements the Interface for sorting
func (ersISSorter *ERSIntermediateSourceSorter) Len() int { return len(ersISSorter.tablets) }

// Swap implements the Interface for sorting
func (ersISSorter *ERSIntermediateSourceSorter) Swap(i, j int) {
	ersISSorter.tablets[i], ersISSorter.tablets[j] = ersISSorter.tablets[j], ersISSorter.tablets[i]
	ersISSorter.positions[i], ersISSorter.positions[j] = ersISSorter.positions[j], ersISSorter.positions[i]
}

// Less implements the Interface for sorting
func (ersISSorter *ERSIntermediateSourceSorter) Less(i, j int) bool {
	// Returning "true" in this function means [i] is before [j] in the sorting order,
	// which will lead to [i] be a better candidate for promotion

	// Should not happen
	// fail-safe code
	if ersISSorter.tablets[i] == nil {
		return false
	}
	if ersISSorter.tablets[j] == nil {
		return true
	}

	if !ersISSorter.positions[i].AtLeast(ersISSorter.positions[j]) {
		// [i] does not have all GTIDs that [j] does
		return false
	}
	if !ersISSorter.positions[j].AtLeast(ersISSorter.positions[i]) {
		// [j] does not have all GTIDs that [i] does
		return true
	}

	// at this point, both have the same GTIDs
	// so we check their promotion rules
	jPromotionRule := PromotionRule(ersISSorter.tablets[j])
	iPromotionRule := PromotionRule(ersISSorter.tablets[i])
	return !jPromotionRule.BetterThan(iPromotionRule)
}

// sortTabletsForERS sorts the tablets, given their positions for emergency reparent shard
func sortTabletsForERS(tablets []*topodatapb.Tablet, positions []mysql.Position) error {
	// throw an error internal error in case of unequal number of tablets and positions
	// fail-safe code prevents panic in sorting in case the lengths are unequal
	if len(tablets) != len(positions) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unequal number of tablets and positions")
	}

	sort.Sort(NewERSIntermediateSourceSorter(tablets, positions))
	return nil
}
