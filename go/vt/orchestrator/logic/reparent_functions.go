package logic

import (
	"context"
	"fmt"

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
