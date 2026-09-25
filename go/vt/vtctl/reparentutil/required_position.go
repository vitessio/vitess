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

package reparentutil

import (
	"slices"
	"strings"

	"vitess.io/vitess/go/mysql/replication"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// validateRequiredPositionFlavor returns an INVALID_ARGUMENT error if required
// is set but is not a non-empty MySQL56 position. An empty set would silently
// disable the check.
func validateRequiredPositionFlavor(required replication.Position) error {
	if required.GTIDSet == nil {
		return nil
	}

	if !required.MatchesFlavor(replication.Mysql56FlavorID) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "required position must be a MySQL GTID position, got %s", replication.EncodePosition(required))
	}

	if required.IsZero() {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "required position is an empty GTID set")
	}

	return nil
}

// validateRequiredPosition returns an INVALID_ARGUMENT error if required is set
// on a shard that is not MySQL GTID based.
func validateRequiredPosition(required replication.Position, isGTIDBased bool) error {
	if required.GTIDSet == nil {
		return nil
	}

	if !isGTIDBased {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "required position is only supported on MySQL GTID shards")
	}

	return nil
}

// checkRequiredPosition returns a FAILED_PRECONDITION error if no candidate has
// received required. The error names the candidates as noun and lists the most
// advanced received positions. A zero required position disables the check.
func checkRequiredPosition(required replication.Position, candidates map[string]*RelayLogPositions, noun string) error {
	if required.IsZero() {
		return nil
	}

	for _, positions := range candidates {
		if positions.Combined.AtLeast(required) {
			return nil
		}
	}

	best := mostAdvancedReceivedPositions(candidates)
	if len(best) == 0 {
		best = append(best, "none")
	}

	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no %s received required position %s: most advanced received positions: %s", noun, required.String(), strings.Join(best, ", "))
}

// mostAdvancedReceivedPositions returns every Combined position that no other
// candidate exceeds, as alias=position, sorted. Divergent histories have more
// than one.
func mostAdvancedReceivedPositions(candidates map[string]*RelayLogPositions) []string {
	var best []string
	for alias, positions := range candidates {
		position := positions.Combined
		dominated := false
		for _, otherPositions := range candidates {
			other := otherPositions.Combined
			if other.AtLeast(position) && !position.AtLeast(other) {
				dominated = true
				break
			}
		}

		if !dominated {
			best = append(best, alias+"="+formatReceivedPosition(position))
		}
	}

	slices.Sort(best)

	return best
}

// formatReceivedPosition encodes a candidate's received position for an error
// message. A zero position becomes <zero>.
func formatReceivedPosition(position replication.Position) string {
	if position.IsZero() {
		return "<zero>"
	}

	return position.String()
}
