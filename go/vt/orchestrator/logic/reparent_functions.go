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

package logic

import (
	"context"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topotools/events"

	"vitess.io/vitess/go/vt/orchestrator/config"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/topo"
)

// VtOrcReparentFunctions is the VtOrc implementation for ReparentFunctions
type VtOrcReparentFunctions struct {
	analysisEntry        inst.ReplicationAnalysis
	candidateInstanceKey *inst.InstanceKey
	skipProcesses        bool
	topologyRecovery     *TopologyRecovery
	promotedReplica      *inst.Instance
	lostReplicas         [](*inst.Instance)
}

// LockShard implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) LockShard(ctx context.Context) (context.Context, func(*error), error) {
	_, unlock, err := LockShard(ctx, vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: NOT detecting/recovering host, could not obtain shard lock (%v)",
			vtorcReparent.analysisEntry.Analysis, vtorcReparent.analysisEntry.AnalyzedInstanceKey, vtorcReparent.candidateInstanceKey, vtorcReparent.skipProcesses, err)
		return nil, nil, err
	}
	return ctx, unlock, nil
}

// GetTopoServer implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) GetTopoServer() *topo.Server {
	return ts
}

// GetKeyspace implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) GetKeyspace() string {
	tablet, _ := inst.ReadTablet(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	return tablet.Keyspace
}

// GetShard implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) GetShard() string {
	tablet, _ := inst.ReadTablet(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	return tablet.Shard
}

// CheckIfFixed implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) CheckIfFixed() bool {
	// Check if someone else fixed the problem.
	tablet, err := TabletRefresh(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	if err == nil && tablet.Type != topodatapb.TabletType_MASTER {
		// TODO(sougou); use a version that only refreshes the current shard.
		RefreshTablets()
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, "another agent seems to have fixed the problem")
		// TODO(sougou): see if we have to reset the cluster as healthy.
		return true
	}
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("will handle DeadMaster event on %+v", vtorcReparent.analysisEntry.ClusterDetails.ClusterName))
	recoverDeadMasterCounter.Inc(1)
	return false
}

// PreRecoveryProcesses implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) PreRecoveryProcesses(ctx context.Context) error {
	inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, "problem found; will recover")
	if !vtorcReparent.skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", vtorcReparent.topologyRecovery, true); err != nil {
			return vtorcReparent.topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: will recover %+v", vtorcReparent.analysisEntry.AnalyzedInstanceKey))
	return nil
}

// StopReplicationAndBuildStatusMaps implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) StopReplicationAndBuildStatusMaps(context.Context, tmclient.TabletManagerClient, *events.Reparent, logutil.Logger) error {
	err := TabletDemoteMaster(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: TabletDemoteMaster: %v", err))
	return err
}

// GetPrimaryRecoveryType implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) GetPrimaryRecoveryType() MasterRecoveryType {
	vtorcReparent.topologyRecovery.RecoveryType = GetMasterRecoveryType(&vtorcReparent.topologyRecovery.AnalysisEntry)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", vtorcReparent.topologyRecovery.RecoveryType))
	return vtorcReparent.topologyRecovery.RecoveryType
}

// AddError implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) AddError(errorMsg string) error {
	return vtorcReparent.topologyRecovery.AddError(log.Errorf(errorMsg))
}

// FindPrimaryCandidates implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) FindPrimaryCandidates(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	postponedAll := false
	promotedReplicaIsIdeal := func(promoted *inst.Instance, hasBestPromotionRule bool) bool {
		if promoted == nil {
			return false
		}
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", promoted.Key))
		if vtorcReparent.candidateInstanceKey != nil { //explicit request to promote a specific server
			return promoted.Key.Equals(vtorcReparent.candidateInstanceKey)
		}
		if promoted.DataCenter == vtorcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter &&
			promoted.PhysicalEnvironment == vtorcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment {
			if promoted.PromotionRule == inst.MustPromoteRule || promoted.PromotionRule == inst.PreferPromoteRule ||
				(hasBestPromotionRule && promoted.PromotionRule != inst.MustNotPromoteRule) {
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", promoted.Key))
				postponedAll = true
				return true
			}
		}
		return false
	}

	AuditTopologyRecovery(vtorcReparent.topologyRecovery, "RecoverDeadMaster: regrouping replicas via GTID")
	lostReplicas, _, cannotReplicateReplicas, promotedReplica, err := inst.RegroupReplicasGTID(&vtorcReparent.analysisEntry.AnalyzedInstanceKey, true, nil, &vtorcReparent.topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
	vtorcReparent.topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	for _, replica := range lostReplicas {
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMasterHost(&replica.Key)
			}
			return nil
		}
		vtorcReparent.topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detach %+v lost replicas", len(lostReplicas)))
	}

	func() error {
		// TODO(sougou): Commented out: this downtime feels a little aggressive.
		//inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&vtorcReparent.analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: %d postponed functions", vtorcReparent.topologyRecovery.PostponedFunctionsContainer.Len()))

	if promotedReplica != nil && !postponedAll {
		promotedReplica, err = replacePromotedReplicaWithCandidate(vtorcReparent.topologyRecovery, &vtorcReparent.analysisEntry.AnalyzedInstanceKey, promotedReplica, vtorcReparent.candidateInstanceKey)
		vtorcReparent.topologyRecovery.AddError(err)
	}

	vtorcReparent.promotedReplica = promotedReplica
	vtorcReparent.lostReplicas = lostReplicas
	return nil
}
