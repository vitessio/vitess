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
	"context"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type (
	// EmergencyReparentOptions provides optional parameters to
	// EmergencyReparentShard operations. Options are passed by value, so it is safe
	// for callers to mutate and reuse options structs for multiple calls.
	EmergencyReparentOptions struct {
		newPrimaryAlias     *topodatapb.TabletAlias
		ignoreReplicas      sets.String
		waitReplicasTimeout time.Duration
	}
)

// NewEmergencyReparentOptions creates a new EmergencyReparentOptions which is used in ERS ans PRS
func NewEmergencyReparentOptions(newPrimaryAlias *topodatapb.TabletAlias, ignoreReplicas sets.String, waitReplicasTimeout time.Duration) EmergencyReparentOptions {
	return EmergencyReparentOptions{
		newPrimaryAlias:     newPrimaryAlias,
		ignoreReplicas:      ignoreReplicas,
		waitReplicasTimeout: waitReplicasTimeout,
	}
}

// LockAction implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) LockAction() string {
	return getLockAction(opts.newPrimaryAlias)
}

// GetWaitReplicasTimeout implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) GetWaitReplicasTimeout() time.Duration {
	return opts.waitReplicasTimeout
}

// GetWaitForRelayLogsTimeout implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) GetWaitForRelayLogsTimeout() time.Duration {
	return opts.waitReplicasTimeout
}

// GetIgnoreReplicas implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) GetIgnoreReplicas() sets.String {
	return opts.ignoreReplicas
}

// RestrictValidCandidates implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) RestrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)
	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		// We only allow PRIMARY and REPLICA type of tablets to be considered for replication
		if candidateInfo.Type != topodatapb.TabletType_PRIMARY && candidateInfo.Type != topodatapb.TabletType_REPLICA {
			continue
		}
		restrictedValidCandidates[candidate] = position
	}
	return restrictedValidCandidates, nil
}

// FindPrimaryCandidate implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) FindPrimaryCandidate(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error) {
	// Elect the candidate with the most up-to-date position.
	var winningPrimaryTabletAliasStr string
	var winningPosition mysql.Position
	for alias, position := range validCandidates {
		if winningPosition.IsZero() || position.AtLeast(winningPosition) {
			winningPosition = position
			winningPrimaryTabletAliasStr = alias
		}
	}

	// If we were requested to elect a particular primary, verify it's a valid
	// candidate (non-zero position, no errant GTIDs) and is at least as
	// advanced as the winning position.
	if opts.newPrimaryAlias != nil {
		winningPrimaryTabletAliasStr = topoproto.TabletAliasString(opts.newPrimaryAlias)
		pos, ok := validCandidates[winningPrimaryTabletAliasStr]
		switch {
		case !ok:
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary elect %v has errant GTIDs", winningPrimaryTabletAliasStr)
		case !pos.AtLeast(winningPosition):
			return nil, nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "primary elect %v at position %v is not fully caught up. Winning position: %v", winningPrimaryTabletAliasStr, pos, winningPosition)
		}
	}

	newPrimaryAlias, isFound := tabletMap[winningPrimaryTabletAliasStr]
	if !isFound {
		return nil, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", winningPrimaryTabletAliasStr)
	}
	return newPrimaryAlias.Tablet, tabletMap, nil
}

// PostTabletChangeHook implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) PostTabletChangeHook(*topodatapb.Tablet) {
}

// PromotedReplicaIsIdeal implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) PromotedReplicaIsIdeal(newPrimary, prevPrimary *topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, validCandidates map[string]mysql.Position) bool {
	if opts.newPrimaryAlias != nil {
		// explicit request to promote a specific tablet
		return true
	}
	if prevPrimary != nil {
		// check that the newPrimary has the same cell as the previous primary
		if (newPrimary.Type == topodatapb.TabletType_PRIMARY || newPrimary.Type == topodatapb.TabletType_REPLICA) && newPrimary.Alias.Cell == prevPrimary.Alias.Cell {
			return true
		}
		return false
	}

	if newPrimary.Type == topodatapb.TabletType_PRIMARY || newPrimary.Type == topodatapb.TabletType_REPLICA {
		return true
	}
	return false
}

// GetBetterCandidate implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) GetBetterCandidate(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo) *topodatapb.Tablet {
	if prevPrimary != nil {
		// find one which is of the correct type and matches the cell of the previous primary
		for _, candidate := range validCandidates {
			if (candidate.Type == topodatapb.TabletType_PRIMARY || candidate.Type == topodatapb.TabletType_REPLICA) && prevPrimary.Alias.Cell == candidate.Alias.Cell {
				return candidate
			}
		}
	}
	for _, candidate := range validCandidates {
		if candidate.Type == topodatapb.TabletType_PRIMARY || candidate.Type == topodatapb.TabletType_REPLICA {
			return candidate
		}
	}
	return newPrimary
}

// CheckIfNeedToOverridePromotion implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error {
	return nil
}

// PostERSCompletionHook implements the ReparentFunctions interface
func (opts *EmergencyReparentOptions) PostERSCompletionHook(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) {
}
