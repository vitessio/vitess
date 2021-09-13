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
	// ReparentFunctions is an interface which has all the functions implementation required for re-parenting
	ReparentFunctions interface {
		LockAction() string
		CheckIfFixed() bool
		CheckPrimaryRecoveryType(logutil.Logger) error
		GetWaitReplicasTimeout() time.Duration
		GetWaitForRelayLogsTimeout() time.Duration
		HandleRelayLogFailure(logutil.Logger, error) error
		GetIgnoreReplicas() sets.String
		RestrictValidCandidates(map[string]mysql.Position, map[string]*topo.TabletInfo) (map[string]mysql.Position, error)
		FindPrimaryCandidate(context.Context, logutil.Logger, tmclient.TabletManagerClient, map[string]mysql.Position, map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error)
		PromotedReplicaIsIdeal(*topodatapb.Tablet, *topodatapb.Tablet, map[string]*topo.TabletInfo, map[string]mysql.Position) bool
		PostTabletChangeHook(*topodatapb.Tablet)
		GetBetterCandidate(*topodatapb.Tablet, *topodatapb.Tablet, []*topodatapb.Tablet, map[string]*topo.TabletInfo) *topodatapb.Tablet
		CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error
		PostERSCompletionHook(context.Context, *events.Reparent, logutil.Logger, tmclient.TabletManagerClient)
	}

	// VtctlReparentFunctions is the Vtctl implementation for ReparentFunctions
	VtctlReparentFunctions struct {
		newPrimaryAlias     *topodatapb.TabletAlias
		ignoreReplicas      sets.String
		waitReplicasTimeout time.Duration
	}
)

var (
	_ ReparentFunctions = (*VtctlReparentFunctions)(nil)
)

// NewVtctlReparentFunctions creates a new VtctlReparentFunctions which is used in ERS ans PRS
func NewVtctlReparentFunctions(newPrimaryAlias *topodatapb.TabletAlias, ignoreReplicas sets.String, waitReplicasTimeout time.Duration) *VtctlReparentFunctions {
	return &VtctlReparentFunctions{
		newPrimaryAlias:     newPrimaryAlias,
		ignoreReplicas:      ignoreReplicas,
		waitReplicasTimeout: waitReplicasTimeout,
	}
}

// LockAction implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) LockAction() string {
	return getLockAction(vtctlReparent.newPrimaryAlias)
}

// CheckIfFixed implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckIfFixed() bool {
	// For vtctl command, we know there is no other third party to fix this.
	// If a user has called this command, then we should execute EmergencyReparentShard
	return false
}

// GetWaitReplicasTimeout implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetWaitReplicasTimeout() time.Duration {
	return vtctlReparent.waitReplicasTimeout
}

// GetWaitForRelayLogsTimeout implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetWaitForRelayLogsTimeout() time.Duration {
	return vtctlReparent.waitReplicasTimeout
}

// HandleRelayLogFailure implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) HandleRelayLogFailure(logger logutil.Logger, err error) error {
	// in case of failure in applying relay logs, vtctl should return the error
	// and let the user decide weather they want to ignore the tablets that caused the error in question
	return err
}

// GetIgnoreReplicas implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) GetIgnoreReplicas() sets.String {
	return vtctlReparent.ignoreReplicas
}

// CheckPrimaryRecoveryType implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) CheckPrimaryRecoveryType(logutil.Logger) error {
	return nil
}

// RestrictValidCandidates implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) RestrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
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
func (vtctlReparent *VtctlReparentFunctions) FindPrimaryCandidate(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error) {
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
	if vtctlReparent.newPrimaryAlias != nil {
		winningPrimaryTabletAliasStr = topoproto.TabletAliasString(vtctlReparent.newPrimaryAlias)
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
func (vtctlReparent *VtctlReparentFunctions) PostTabletChangeHook(*topodatapb.Tablet) {
}

// PromotedReplicaIsIdeal implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) PromotedReplicaIsIdeal(newPrimary, prevPrimary *topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, validCandidates map[string]mysql.Position) bool {
	if vtctlReparent.newPrimaryAlias != nil {
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
func (vtctlReparent *VtctlReparentFunctions) GetBetterCandidate(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo) *topodatapb.Tablet {
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
func (vtctlReparent *VtctlReparentFunctions) CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error {
	return nil
}

// PostERSCompletionHook implements the ReparentFunctions interface
func (vtctlReparent *VtctlReparentFunctions) PostERSCompletionHook(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) {
}
