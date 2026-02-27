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

package inst

import (
	"math"
	"slices"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

const (
	detectionAnalysisPriorityShardWideAction = iota
	detectionAnalysisPriorityCritical
	detectionAnalysisPriorityHigh
	detectionAnalysisPriorityMedium
	detectionAnalysisPriorityLow
)

// DetectionAnalysisProblemMeta contains basic metadata describing a problem.
type DetectionAnalysisProblemMeta struct {
	// Analysis is the AnalysisCode representing the type of problem.
	Analysis AnalysisCode

	// Description is a human-readable description of the problem.
	Description string

	// Priority is an integer influencing the priority sorting of problems. A lower
	// number is considered to be higher in the sort, with 0 being the top-priority.
	Priority int
}

// DetectionAnalysisProblem describes how to match, sort and track a problem.
type DetectionAnalysisProblem struct {
	// Meta contains the metadata describing a problem.
	Meta *DetectionAnalysisProblemMeta

	// AfterAnalyses defines problems that must be recovered before this problem.
	AfterAnalyses []AnalysisCode

	// BeforeAnalyses defines problems that must be recovered after this problem.
	BeforeAnalyses []AnalysisCode

	// MatchFunc is a function that returns true when the provided conditions match this problem.
	MatchFunc func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool
}

// RequiresOrderedExecution returns true if the problem must be executed
// sequentially relative to other problems in the same shard.
func (dap *DetectionAnalysisProblem) RequiresOrderedExecution() bool {
	return dap.Meta.Priority == detectionAnalysisPriorityShardWideAction || len(dap.BeforeAnalyses) > 0 || len(dap.AfterAnalyses) > 0
}

// GetPriority returns the priority of a problem as an int.
func (dap *DetectionAnalysisProblem) GetPriority() int {
	if dap.Meta == nil {
		return 0
	}
	return dap.Meta.Priority
}

// GetDetectionAnalysisProblem returns the DetectionAnalysisProblem for the given AnalysisCode.
func GetDetectionAnalysisProblem(code AnalysisCode) *DetectionAnalysisProblem {
	for _, p := range detectionAnalysisProblems {
		if p.Meta.Analysis == code {
			return p
		}
	}
	return nil
}

// HasMatch returns true if a DetectionAnalysisProblem matches the provided states.
func (dap *DetectionAnalysisProblem) HasMatch(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
	if a == nil || ca == nil || dap.MatchFunc == nil {
		return false
	}
	return dap.MatchFunc(a, ca, primary, tablet, isInvalid, isStaleBinlogCoordinates)
}

// detectionAnalysisProblems contains all possible problems to match during detection analysis.
var detectionAnalysisProblems = []*DetectionAnalysisProblem{
	// InvalidPrimary and InvalidReplica
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    InvalidPrimary,
			Description: "VTOrc hasn't been able to reach the primary even once since restart/shutdown",
			Priority:    detectionAnalysisPriorityCritical,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && isInvalid
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    InvalidReplica,
			Description: "VTOrc hasn't been able to reach the replica even once since restart/shutdown",
			Priority:    detectionAnalysisPriorityLow,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return isInvalid
		},
	},

	// PrimaryDiskStalled
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryDiskStalled,
			Description: "Primary has a stalled disk",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		BeforeAnalyses: []AnalysisCode{DeadPrimary, DeadPrimaryAndReplicas, DeadPrimaryAndSomeReplicas, DeadPrimaryWithoutReplicas},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.IsDiskStalled
		},
	},

	// DeadPrimary*
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    DeadPrimaryWithoutReplicas,
			Description: "Primary cannot be reached by vtorc and has no replica",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    DeadPrimary,
			Description: "Primary cannot be reached by vtorc and none of its replicas is replicating",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    DeadPrimaryAndReplicas,
			Description: "Primary cannot be reached by vtorc and none of its replicas is replicating",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    DeadPrimaryAndSomeReplicas,
			Description: "Primary cannot be reached by vtorc; some of its replicas are unreachable and none of its reachable replicas is replicating",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0
		},
	},

	// PrimaryHasPrimary
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryHasPrimary,
			Description: "Primary is replicating from somewhere else",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && !a.IsPrimary
		},
	},

	// MySQL read-only checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryIsReadOnly,
			Description: "Primary is read-only",
			Priority:    detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && a.IsReadOnly
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaIsWritable,
			Description: "Replica is writable",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsReadOnly
		},
	},

	// Semi-sync checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySemiSyncMustBeSet,
			Description: "Primary semi-sync must be set",
			Priority:    detectionAnalysisPriorityMedium,
		},
		AfterAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			if !hasMinSemiSyncAckers(ca.durability, primary, a) {
				return false
			}
			return a.IsClusterPrimary && policy.SemiSyncAckers(ca.durability, tablet) != 0 && !a.SemiSyncPrimaryEnabled
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySemiSyncMustNotBeSet,
			Description: "Primary semi-sync must not be set",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && policy.SemiSyncAckers(ca.durability, tablet) == 0 && a.SemiSyncPrimaryEnabled
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaSemiSyncMustBeSet,
			Description: "Replica semi-sync must be set",
			Priority:    detectionAnalysisPriorityMedium,
		},
		BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && policy.IsReplicaSemiSync(ca.durability, primary, tablet) && !a.SemiSyncReplicaEnabled
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaSemiSyncMustNotBeSet,
			Description: "Replica semi-sync must not be set",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && !policy.IsReplicaSemiSync(ca.durability, primary, tablet) && a.SemiSyncReplicaEnabled
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySemiSyncBlocked,
			Description: "Writes seem to be blocked on semi-sync acks on the primary, even though sufficient replicas are configured to send ACKs",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && a.IsPrimary && a.SemiSyncBlocked && a.CountSemiSyncReplicasEnabled >= a.SemiSyncPrimaryWaitForReplicaCount
		},
	},

	// Primary tablet type checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryCurrentTypeMismatch,
			Description: "Primary tablet's current type is not PRIMARY",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsClusterPrimary && a.CurrentTabletType != topodatapb.TabletType_UNKNOWN && a.CurrentTabletType != topodatapb.TabletType_PRIMARY
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    StaleTopoPrimary,
			Description: "Primary tablet is stale, older than current primary",
			Priority:    detectionAnalysisPriorityHigh,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return isStaleTopoPrimary(a, ca)
		},
	},

	// Errant GTID
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ErrantGTIDDetected,
			Description: "Tablet has errant GTIDs",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && a.ErrantGTID != ""
		},
	},

	// Cluster primary checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ClusterHasNoPrimary,
			Description: "Cluster has no primary",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && ca.primaryAlias == "" && a.ShardPrimaryTermTimestamp.IsZero()
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimaryTabletDeleted,
			Description: "Primary tablet has been deleted",
			Priority:    detectionAnalysisPriorityShardWideAction,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && ca.primaryAlias == "" && !a.ShardPrimaryTermTimestamp.IsZero()
		},
	},

	// Replica connectivity checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    NotConnectedToPrimary,
			Description: "Not connected to the primary",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && a.IsPrimary
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicaMisconfigured,
			Description: "Replica has been misconfigured",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && math.Round(a.HeartbeatInterval*2) != float64(a.ReplicaNetTimeout)
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ConnectedToWrongPrimary,
			Description: "Connected to wrong primary",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && ca.primaryAlias != "" && a.AnalyzedInstancePrimaryAlias != ca.primaryAlias
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    ReplicationStopped,
			Description: "Replication is stopped",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return topo.IsReplicaType(a.TabletType) && !a.IsPrimary && a.ReplicationStopped
		},
	},

	// Unreachable primary checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    UnreachablePrimaryWithLaggingReplicas,
			Description: "Primary cannot be reached by vtorc and all of its replicas are lagging",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    UnreachablePrimary,
			Description: "Primary cannot be reached by vtorc but all of its replicas seem to be replicating; possibly a network/host issue",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == a.CountValidReplicas
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    UnreachablePrimaryWithBrokenReplicas,
			Description: "Primary cannot be reached by vtorc but it has (some, but not all) replicating replicas; possibly a network/host issue",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 && a.CountValidReplicatingReplicas < a.CountValidReplicas
		},
	},

	// Locked semi-sync primary
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    LockedSemiSyncPrimary,
			Description: "Semi sync primary is locked since it doesn't get enough replica acknowledgements",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.SemiSyncPrimaryEnabled && a.SemiSyncPrimaryStatus && a.SemiSyncPrimaryWaitForReplicaCount > 0 && a.SemiSyncPrimaryClients < a.SemiSyncPrimaryWaitForReplicaCount && isStaleBinlogCoordinates
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    LockedSemiSyncPrimaryHypothesis,
			Description: "Semi sync primary seems to be locked, more samplings needed to validate",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.SemiSyncPrimaryEnabled && a.SemiSyncPrimaryStatus && a.SemiSyncPrimaryWaitForReplicaCount > 0 && a.SemiSyncPrimaryClients < a.SemiSyncPrimaryWaitForReplicaCount && !isStaleBinlogCoordinates
		},
	},

	// Primary replica health checks
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySingleReplicaNotReplicating,
			Description: "Primary is reachable but its single replica is not replicating",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    PrimarySingleReplicaDead,
			Description: "Primary is reachable but its single replica is dead",
			Priority:    detectionAnalysisPriorityMedium,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    AllPrimaryReplicasNotReplicating,
			Description: "Primary is reachable but none of its replicas is replicating",
			Priority:    detectionAnalysisPriorityLow,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0
		},
	},
	{
		Meta: &DetectionAnalysisProblemMeta{
			Analysis:    AllPrimaryReplicasNotReplicatingOrDead,
			Description: "Primary is reachable but none of its replicas is replicating",
			Priority:    detectionAnalysisPriorityLow,
		},
		MatchFunc: func(a *DetectionAnalysis, ca *clusterAnalysis, primary, tablet *topodatapb.Tablet, isInvalid, isStaleBinlogCoordinates bool) bool {
			return a.IsPrimary && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0
		},
	},
}

func sortDetectionAnalysisMatchedProblems(allProblems []*DetectionAnalysisProblem) {
	// use slices.SortStableFunc because it keeps the original order of equal elements.
	slices.SortStableFunc(allProblems, compareDetectionAnalysisProblems)
}

// compareDetectionAnalysisProblems compares two DetectionAnalysisProblems using
// the same logic as sortDetectionAnalysisMatchedProblems.
func compareDetectionAnalysisProblems(a, b *DetectionAnalysisProblem) int {
	if a.Meta == nil || b.Meta == nil {
		return 0
	}

	// handle before/after dependencies
	aAnalysis := a.Meta.Analysis
	bAnalysis := b.Meta.Analysis
	if slices.Contains(b.BeforeAnalyses, aAnalysis) || slices.Contains(a.AfterAnalyses, bAnalysis) {
		return 1
	}
	if slices.Contains(a.BeforeAnalyses, bAnalysis) || slices.Contains(b.AfterAnalyses, aAnalysis) {
		return -1
	}

	// effective priority (lower is better).
	aPriority := a.GetPriority()
	bPriority := b.GetPriority()
	switch {
	case aPriority > bPriority:
		return 1
	case aPriority < bPriority:
		return -1
	}

	return 0
}

// sortDetectionAnalyses sorts a slice of DetectionAnalysis by looking up each
// entry's Analysis code in detectionAnalysisProblems and comparing using the
// same priority/dependency logic as sortDetectionAnalysisMatchedProblems.
func sortDetectionAnalyses(analyses []*DetectionAnalysis) {
	slices.SortStableFunc(analyses, func(a, b *DetectionAnalysis) int {
		aProblem := GetDetectionAnalysisProblem(a.Analysis)
		bProblem := GetDetectionAnalysisProblem(b.Analysis)
		if aProblem == nil || bProblem == nil {
			return 0
		}
		return compareDetectionAnalysisProblems(aProblem, bProblem)
	})
}

// GroupDetectionAnalysesByShard groups a slice of DetectionAnalysis by shard key
// (topoproto.KeyspaceShardString) and sorts each group by priority.
func GroupDetectionAnalysesByShard(analyses []*DetectionAnalysis) map[string][]*DetectionAnalysis {
	result := make(map[string][]*DetectionAnalysis)
	for _, a := range analyses {
		key := topoproto.KeyspaceShardString(a.AnalyzedKeyspace, a.AnalyzedShard)
		result[key] = append(result[key], a)
	}
	for _, group := range result {
		sortDetectionAnalyses(group)
	}
	return result
}
