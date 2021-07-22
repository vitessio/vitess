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
