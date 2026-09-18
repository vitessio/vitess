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

// checkRequiredPosition returns nil when a candidate's Combined position includes
// required. Otherwise it returns FAILED_PRECONDITION and names every most advanced
// received position. A zero required position disables the check.
func checkRequiredPosition(required replication.Position, candidates map[string]*RelayLogPositions) error {
	if required.IsZero() {
		return nil
	}

	for _, positions := range candidates {
		if positions.Combined.AtLeast(required) {
			return nil
		}
	}

	// Name every maximum because divergent histories have no single most advanced position.
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
			best = append(best, alias+"="+formatRequiredPosition(position))
		}
	}

	slices.Sort(best)

	if len(best) == 0 {
		best = append(best, "none")
	}

	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no candidate received required position %s: most advanced received positions: %s", replication.EncodePosition(required), strings.Join(best, ", "))
}

// formatRequiredPosition encodes position for an error message. A zero
// position becomes <zero>.
func formatRequiredPosition(position replication.Position) string {
	if position.IsZero() {
		return "<zero>"
	}

	return replication.EncodePosition(position)
}
