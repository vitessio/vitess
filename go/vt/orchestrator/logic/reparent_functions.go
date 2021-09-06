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

	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/mysql"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/vt/orchestrator/attributes"
	"vitess.io/vitess/go/vt/orchestrator/kv"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"

	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topotools/events"

	"vitess.io/vitess/go/vt/orchestrator/config"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/topo"
)

var _ reparentutil.ReparentFunctions = (*VtOrcReparentFunctions)(nil)

// VtOrcReparentFunctions is the VtOrc implementation for ReparentFunctions
type VtOrcReparentFunctions struct {
	analysisEntry        inst.ReplicationAnalysis
	candidateInstanceKey *inst.InstanceKey
	skipProcesses        bool
	topologyRecovery     *TopologyRecovery
	promotedReplica      *inst.Instance
	recoveryAttempted    bool
	hasBestPromotionRule bool
}

func NewVtorcReparentFunctions(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, skipProcesses bool, topologyRecovery *TopologyRecovery) *VtOrcReparentFunctions {
	return &VtOrcReparentFunctions{
		analysisEntry:        analysisEntry,
		candidateInstanceKey: candidateInstanceKey,
		skipProcesses:        skipProcesses,
		topologyRecovery:     topologyRecovery,
	}
}

// LockShard implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) LockShard(ctx context.Context) (context.Context, func(*error), error) {
	ctx, unlock, err := LockShard(ctx, vtorcReparent.analysisEntry.AnalyzedInstanceKey)
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
	if err == nil && tablet.Type != topodatapb.TabletType_PRIMARY {
		// TODO(sougou); use a version that only refreshes the current shard.
		RefreshTablets()
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, "another agent seems to have fixed the problem")
		// TODO(sougou): see if we have to reset the cluster as healthy.
		return true
	}
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("will handle DeadMaster event on %+v", vtorcReparent.analysisEntry.ClusterDetails.ClusterName))
	recoverDeadPrimaryCounter.Inc(1)
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

// TODO : Discuss correct way
func (vtorcReparent *VtOrcReparentFunctions) GetWaitReplicasTimeout() time.Duration {
	return 1 * time.Second
}

// TODO : Discuss correct way
func (vtorcReparent *VtOrcReparentFunctions) GetWaitForRelayLogsTimeout() time.Duration {
	return 1 * time.Second
}

// TODO : Discuss correct way
func (vtorcReparent *VtOrcReparentFunctions) HandleRelayLogFailure(err error) error {
	log.Infof("failed to apply all relay logs - %v", err)
	return nil
}

func (vtorcReparent *VtOrcReparentFunctions) GetIgnoreReplicas() sets.String {
	return nil
}

// CheckPrimaryRecoveryType implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) CheckPrimaryRecoveryType() error {
	vtorcReparent.topologyRecovery.RecoveryType = GetPrimaryRecoveryType(&vtorcReparent.topologyRecovery.AnalysisEntry)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", vtorcReparent.topologyRecovery.RecoveryType))
	if vtorcReparent.topologyRecovery.RecoveryType != PrimaryRecoveryGTID {
		return vtorcReparent.topologyRecovery.AddError(log.Errorf("RecoveryType unknown/unsupported"))
	}
	return nil
}

// RestrictValidCandidates implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) RestrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	// we do not restrict the valid candidates for VtOrc for 2 reasons -
	// any candidate that can no longer replicate from the new primary is detached later on
	// we already restrict which candidate can become the primary via inst.IsBannedFromBeingCandidateReplica when we choose the candidate
	return validCandidates, nil
}

// FindPrimaryCandidates implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) FindPrimaryCandidates(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (*topodatapb.Tablet, map[string]*topo.TabletInfo, error) {
	//promotedReplicaIsIdeal := func(promoted *inst.Instance, hasBestPromotionRule bool) bool {
	//	if promoted == nil {
	//		return false
	//	}
	//	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", promoted.Key))
	//	if vtorcReparent.candidateInstanceKey != nil { //explicit request to promote a specific server
	//		return promoted.Key.Equals(vtorcReparent.candidateInstanceKey)
	//	}
	//	if promoted.DataCenter == vtorcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter &&
	//		promoted.PhysicalEnvironment == vtorcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment {
	//		if promoted.PromotionRule == inst.MustPromoteRule || promoted.PromotionRule == inst.PreferPromoteRule ||
	//			(hasBestPromotionRule && promoted.PromotionRule != inst.MustNotPromoteRule) {
	//			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", promoted.Key))
	//			vtorcReparent.postponedAll = true
	//			return true
	//		}
	//	}
	//	return false
	//}

	AuditTopologyRecovery(vtorcReparent.topologyRecovery, "RecoverDeadMaster: regrouping replicas via GTID")
	lostReplicas, promotedReplica, hasBestPromotionRule, err := ChooseCandidate(tmc, &vtorcReparent.analysisEntry.AnalyzedInstanceKey, validCandidates, tabletMap)
	vtorcReparent.topologyRecovery.AddError(err)
	vtorcReparent.hasBestPromotionRule = hasBestPromotionRule
	if err != nil {
		return nil, nil, err
	}
	newPrimary, err := inst.ReadTablet(promotedReplica.Key)
	if err != nil {
		return nil, nil, err
	}

	for _, replica := range lostReplicas {
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterPrimaryFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaPrimaryHost(&replica.Key)
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

	vtorcReparent.promotedReplica = promotedReplica
	vtorcReparent.topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	vtorcReparent.recoveryAttempted = true

	tabletMapWithoutLostReplicas := map[string]*topo.TabletInfo{}

	for alias, info := range tabletMap {
		instance := getInstanceFromTablet(info.Tablet)
		isLost := false
		for _, replica := range lostReplicas {
			if instance.Key.Equals(&replica.Key) {
				isLost = true
				break
			}
		}
		if !isLost {
			tabletMapWithoutLostReplicas[alias] = info
		}
	}

	return newPrimary, tabletMapWithoutLostReplicas, nil
}

// ChooseCandidate will choose a candidate replica of a given instance, and take its siblings using GTID
func ChooseCandidate(
	tmc tmclient.TabletManagerClient,
	masterKey *inst.InstanceKey,
	validCandidates map[string]mysql.Position,
	tabletMap map[string]*topo.TabletInfo,
) (
	lostReplicas [](*inst.Instance),
	candidateReplica *inst.Instance,
	hasBestPromotionRule bool,
	err error,
) {
	var emptyReplicas [](*inst.Instance)
	var unmovedReplicas [](*inst.Instance)
	//var movedReplicas [](*inst.Instance)

	dataCenterHint := ""
	if master, _, _ := inst.ReadInstance(masterKey); master != nil {
		dataCenterHint = master.DataCenter
	}

	var replicas [](*inst.Instance)

	for candidate := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return emptyReplicas, candidateReplica, false, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		candidateInstance, _, err := inst.ReadInstance(&inst.InstanceKey{
			Hostname: candidateInfo.MysqlHostname,
			Port:     int(candidateInfo.MysqlPort),
		})
		if err != nil {
			log.Errorf("%v", err)
			return emptyReplicas, candidateReplica, false, err
		}
		replicas = append(replicas, candidateInstance)
	}

	inst.SortInstancesDataCenterHint(replicas, dataCenterHint)
	for _, replica := range replicas {
		log.Debugf("- sorted replica: %+v %+v", replica.Key, replica.ExecBinlogCoordinates)
	}

	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := inst.ChooseCandidateReplica(replicas)
	if err != nil {
		return emptyReplicas, candidateReplica, false, err
	}
	if candidateReplica == nil {
		return emptyReplicas, candidateReplica, false, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "could not find a candidate replica for ERS")
	}
	mostUpToDateReplica := replicas[0]
	if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateReplica.ExecBinlogCoordinates) {
		log.Warningf("GetCandidateReplica: chosen replica: %+v is behind most-up-to-date replica: %+v", candidateReplica.Key, mostUpToDateReplica.Key)
	}

	log.Debugf("GetCandidateReplica: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateReplica.Key, len(aheadReplicas), len(equalReplicas), len(laterReplicas), len(cannotReplicateReplicas))

	replicasToMove := append(equalReplicas, laterReplicas...)
	hasBestPromotionRule = true
	for _, replica := range replicasToMove {
		if replica.PromotionRule.BetterThan(candidateReplica.PromotionRule) {
			hasBestPromotionRule = false
		}
	}

	//now := time.Now().UnixNano()
	//if err := inst.SwitchMaster(candidateReplica.Key, *masterKey); err != nil {
	//	return emptyReplicas, candidateReplica, err
	//}
	//
	//candidateReplicaTablet, err := inst.ReadTablet(candidateReplica.Key)
	//if err != nil {
	//	return emptyReplicas, candidateReplica, err
	//}
	//
	//moveGTIDFunc := func() error {
	//	log.Debugf("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))
	//
	//	for _, instance := range replicasToMove {
	//		tablet, err := inst.ReadTablet(instance.Key)
	//		if err != nil {
	//			goto Cleanup
	//		}
	//
	//		if maintenanceToken, merr := inst.BeginMaintenance(&instance.Key, inst.GetMaintenanceOwner(), fmt.Sprintf("move below %+v", candidateReplica.Key)); merr != nil {
	//			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", instance.Key, merr)
	//			goto Cleanup
	//		} else {
	//			defer inst.EndMaintenance(maintenanceToken)
	//		}
	//
	//		err = tmc.SetReplicationSource(context.Background(), tablet, candidateReplicaTablet.Alias, now, "", false)
	//		if err != nil {
	//			unmovedReplicas = append(unmovedReplicas, instance)
	//			err = vterrors.Wrapf(err, "tablet %v SetMaster failed: %v", tablet.Alias, err)
	//			goto Cleanup
	//		}
	//		movedReplicas = append(movedReplicas, instance)
	//
	//	Cleanup:
	//		tmc.StartReplication(context.Background(), tablet)
	//		return err
	//	}
	//
	unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
	unmovedReplicas = append(unmovedReplicas, cannotReplicateReplicas...)
	//	if err != nil {
	//		log.Errore(err)
	//	}
	//	return nil
	//}
	//
	//if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
	//	postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	//} else {
	//	err = moveGTIDFunc()
	//}
	//
	//inst.StartReplication(&candidateReplica.Key)
	//
	//log.Debugf("RegroupReplicasGTID: done")
	//inst.AuditOperation("regroup-replicas-gtid", masterKey, fmt.Sprintf("regrouped replicas of %+v via GTID; promoted %+v", *masterKey, candidateReplica.Key))
	return unmovedReplicas, candidateReplica, hasBestPromotionRule, err
}

// PromotedReplicaIsIdeal implements the ReparentFunctions interface
func (vtOrcReparent *VtOrcReparentFunctions) PromotedReplicaIsIdeal(newPrimary, oldPrimary *topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo, validCandidates map[string]mysql.Position) bool {
	AuditTopologyRecovery(vtOrcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", newPrimary.Alias))
	newPrimaryKey := &inst.InstanceKey{
		Hostname: newPrimary.MysqlHostname,
		Port:     int(newPrimary.MysqlPort),
	}
	newPrimaryInst, _, _ := inst.ReadInstance(newPrimaryKey)
	if vtOrcReparent.candidateInstanceKey != nil { //explicit request to promote a specific server
		return newPrimaryKey.Equals(vtOrcReparent.candidateInstanceKey)
	}
	if newPrimaryInst.DataCenter == vtOrcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter &&
		newPrimaryInst.PhysicalEnvironment == vtOrcReparent.topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment {
		if newPrimaryInst.PromotionRule == inst.MustPromoteRule || newPrimaryInst.PromotionRule == inst.PreferPromoteRule ||
			(vtOrcReparent.hasBestPromotionRule && newPrimaryInst.PromotionRule != inst.MustNotPromoteRule) {
			AuditTopologyRecovery(vtOrcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", newPrimaryInst.Key))
			return true
		}
	}
	return false
}

// PostReplicationChangeHook implements the ReparentFunctions interface
func (vtOrcReparent *VtOrcReparentFunctions) PostTabletChangeHook(tablet *topodatapb.Tablet) {
	instanceKey := &inst.InstanceKey{
		Hostname: tablet.MysqlHostname,
		Port:     int(tablet.MysqlPort),
	}
	inst.ReadTopologyInstance(instanceKey)
	TabletRefresh(*instanceKey)
}

func (vtorcReparent *VtOrcReparentFunctions) GetBetterCandidate(newPrimary, prevPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet, tabletMap map[string]*topo.TabletInfo) *topodatapb.Tablet {
	if vtorcReparent.candidateInstanceKey != nil {
		candidateTablet, _ := inst.ReadTablet(*vtorcReparent.candidateInstanceKey)
		// return the requested candidate as long as it is valid
		for _, validCandidate := range validCandidates {
			if topoproto.TabletAliasEqual(validCandidate.Alias, candidateTablet.Alias) {
				return validCandidate
			}
		}
	}
	replacementCandidate := getReplacementForPromotedReplica(vtorcReparent.topologyRecovery, newPrimary, prevPrimary, validCandidates)
	vtorcReparent.promotedReplica = getInstanceFromTablet(replacementCandidate)

	return replacementCandidate
}

func getReplacementForPromotedReplica(topologyRecovery *TopologyRecovery, newPrimary, oldPrimary *topodatapb.Tablet, validCandidates []*topodatapb.Tablet) *topodatapb.Tablet {
	var preferredCandidates []*topodatapb.Tablet
	var neutralReplicas []*topodatapb.Tablet
	for _, candidate := range validCandidates {
		promotionRule := inst.PromotionRule(candidate)
		if promotionRule == inst.MustPromoteRule || promotionRule == inst.PreferPromoteRule {
			preferredCandidates = append(preferredCandidates, candidate)
		}
		if promotionRule == inst.NeutralPromoteRule {
			neutralReplicas = append(neutralReplicas, candidate)
		}
	}

	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the primary
	// There's many options. We may wish to replace the server we promoted with a better one.
	AuditTopologyRecovery(topologyRecovery, "checking if should replace promoted replica with a better candidate")
	AuditTopologyRecovery(topologyRecovery, "+ checking if promoted replica is the ideal candidate")
	if oldPrimary != nil {
		for _, candidateReplica := range preferredCandidates {
			if topoproto.TabletAliasEqual(newPrimary.Alias, candidateReplica.Alias) &&
				newPrimary.Alias.Cell == oldPrimary.Alias.Cell {
				// Seems like we promoted a candidate in the same cell as dead IM! Ideal! We're happy!
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is the ideal candidate", newPrimary.Alias))
				return newPrimary
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV

	// Try a candidate replica that is in same DC & env as the dead instance
	AuditTopologyRecovery(topologyRecovery, "+ searching for an ideal candidate")
	if oldPrimary != nil {
		for _, candidateReplica := range preferredCandidates {
			if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(candidateReplica), getInstanceFromTablet(newPrimary)) &&
				candidateReplica.Alias.Cell == oldPrimary.Alias.Cell {
				// This would make a great candidate
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("orchestrator picks %+v as candidate replacement, based on being in same cell as failed instance", candidateReplica.Alias))
				return candidateReplica
			}
		}
	}

	// We cannot find a candidate in same DC and ENV as dead primary
	AuditTopologyRecovery(topologyRecovery, "+ checking if promoted replica is an OK candidate")
	for _, candidateReplica := range preferredCandidates {
		if topoproto.TabletAliasEqual(newPrimary.Alias, candidateReplica.Alias) {
			// Seems like we promoted a candidate replica (though not in same DC and ENV as dead primary)
			if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, getInstanceFromTablet(candidateReplica)); satisfied {
				// Good enough. No further action required.
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", newPrimary.Alias))
				return newPrimary
			} else {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Alias, reason))
			}
		}
	}

	// Still nothing?
	// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
	AuditTopologyRecovery(topologyRecovery, "+ searching for a candidate")
	for _, candidateReplica := range preferredCandidates {
		if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(candidateReplica), getInstanceFromTablet(newPrimary)) &&
			newPrimary.Alias.Cell == candidateReplica.Alias.Cell {
			// OK, better than nothing
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", newPrimary.Alias, candidateReplica.Alias))
			return candidateReplica
		}
	}

	// Still nothing?
	// Try a candidate replica (our promoted replica is not an "is_candidate")
	AuditTopologyRecovery(topologyRecovery, "+ searching for a candidate")
	for _, candidateReplica := range preferredCandidates {
		if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(candidateReplica), getInstanceFromTablet(newPrimary)) {
			if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, getInstanceFromTablet(candidateReplica)); satisfied {
				// OK, better than nothing
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement", newPrimary.Alias, candidateReplica.Alias))
				return candidateReplica
			} else {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Alias, reason))
			}
		}
	}

	keepSearchingHint := ""
	if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, getInstanceFromTablet(newPrimary)); !satisfied {
		keepSearchingHint = fmt.Sprintf("Will keep searching; %s", reason)
	} else if inst.PromotionRule(newPrimary) == inst.PreferNotPromoteRule {
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", newPrimary.Alias)
	}
	if keepSearchingHint != "" {
		AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
		// find neutral instance in same dv&env as dead primary
		if oldPrimary != nil {
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as dead master")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(neutralReplica), getInstanceFromTablet(newPrimary)) &&
					oldPrimary.Alias.Cell == neutralReplica.Alias.Cell {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as dead master", newPrimary.Alias, neutralReplica.Alias))
					return neutralReplica
				}
			}
		}

		// find neutral instance in same dv&env as promoted replica
		AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica")
		for _, neutralReplica := range neutralReplicas {
			if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(neutralReplica), getInstanceFromTablet(newPrimary)) &&
				newPrimary.Alias.Cell == neutralReplica.Alias.Cell {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", newPrimary.Alias, neutralReplica.Alias))
				return neutralReplica
			}
		}

		AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace a prefer_not")
		for _, neutralReplica := range neutralReplicas {
			if canTakeOverPromotedServerAsPrimary(getInstanceFromTablet(neutralReplica), getInstanceFromTablet(newPrimary)) {
				if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, getInstanceFromTablet(neutralReplica)); satisfied {
					// OK, better than nothing
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on promoted instance having prefer_not promotion rule", newPrimary.Alias, neutralReplica.Alias))
					return neutralReplica
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", neutralReplica.Alias, reason))
				}
			}
		}
	}

	return newPrimary
}

func getInstanceFromTablet(tablet *topodatapb.Tablet) *inst.Instance {
	instance, _, _ := inst.ReadInstance(&inst.InstanceKey{
		Hostname: tablet.MysqlHostname,
		Port:     int(tablet.MysqlPort),
	})
	return instance
}

// CheckIfNeedToOverridePrimary implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) CheckIfNeedToOverridePromotion(newPrimary *topodatapb.Tablet) error {
	// TODO : use fixing code outside
	//if vtorcReparent.promotedReplica == nil {
	//	err := TabletUndoDemoteMaster(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
	//	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: TabletUndoDemoteMaster: %v", err))
	//	message := "Failure: no replica promoted."
	//	AuditTopologyRecovery(vtorcReparent.topologyRecovery, message)
	//	inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, message)
	//	return err
	//}

	// TODO: Move out to post-change code
	message := fmt.Sprintf("promoted replica: %+v", vtorcReparent.promotedReplica.Key)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, message)
	inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, message)

	newPrimaryInstance := getInstanceFromTablet(newPrimary)
	overrideMasterPromotion := func() error {
		// Scenarios where we might cancel the promotion.
		if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&vtorcReparent.analysisEntry, newPrimaryInstance); !satisfied {
			return fmt.Errorf("RecoverDeadMaster: failed %+v promotion; %s", newPrimaryInstance.Key, reason)
		}
		if config.Config.FailPrimaryPromotionOnLagMinutes > 0 &&
			time.Duration(newPrimaryInstance.ReplicationLagSeconds.Int64)*time.Second >= time.Duration(config.Config.FailPrimaryPromotionOnLagMinutes)*time.Minute {
			// candidate replica lags too much
			return fmt.Errorf("RecoverDeadMaster: failed promotion. FailPrimaryPromotionOnLagMinutes is set to %d (minutes) and promoted replica %+v 's lag is %d (seconds)", config.Config.FailPrimaryPromotionOnLagMinutes, newPrimaryInstance.Key, newPrimaryInstance.ReplicationLagSeconds.Int64)
		}
		if config.Config.FailPrimaryPromotionIfSQLThreadNotUpToDate && !newPrimaryInstance.SQLThreadUpToDate() {
			return fmt.Errorf("RecoverDeadMaster: failed promotion. FailPrimaryPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", newPrimaryInstance.Key)
		}
		if config.Config.DelayPrimaryPromotionIfSQLThreadNotUpToDate && !newPrimaryInstance.SQLThreadUpToDate() {
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: waiting for SQL thread on %+v", newPrimaryInstance.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&newPrimaryInstance.Key, 0, 0); err != nil {
				return fmt.Errorf("DelayMasterPromotionIfSQLThreadNotUpToDate error: %+v", err)
			}
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: SQL thread caught up on %+v", newPrimaryInstance.Key))
		}
		// All seems well. No override done.
		return nil
	}
	if err := overrideMasterPromotion(); err != nil {
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, err.Error())
		vtorcReparent.promotedReplica = nil
		return err
	}
	return nil
}

// PostERSCompletionHook implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) PostERSCompletionHook(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) {
	// And this is the end; whether successful or not, we're done.
	resolveRecovery(vtorcReparent.topologyRecovery, vtorcReparent.promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back.
	if vtorcReparent.promotedReplica != nil {
		// Success!
		recoverDeadPrimarySuccessCounter.Inc(1)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: successfully promoted %+v", vtorcReparent.promotedReplica.Key))
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: promoted server coordinates: %+v", vtorcReparent.promotedReplica.SelfBinlogCoordinates))

		kvPairs := inst.GetClusterPrimaryKVPairs(vtorcReparent.analysisEntry.ClusterDetails.ClusterAlias, &vtorcReparent.promotedReplica.Key)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		for _, kvPair := range kvPairs {
			err := kv.PutKVPair(kvPair)
			log.Errore(err)
		}
		{
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("Distributing KV %+v", kvPairs))
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if config.Config.PrimaryFailoverDetachReplicaPrimaryHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, "- RecoverDeadPrimary: detaching master host on promoted master")
				inst.DetachReplicaPrimaryHost(&vtorcReparent.promotedReplica.Key)
				return nil
			}
			vtorcReparent.topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detaching promoted master host %+v", vtorcReparent.promotedReplica.Key))
		}
		func() error {
			before := vtorcReparent.analysisEntry.AnalyzedInstanceKey.StringCode()
			after := vtorcReparent.promotedReplica.Key.StringCode()
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: updating cluster_alias: %v -> %v", before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := vtorcReparent.analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(vtorcReparent.promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(vtorcReparent.analysisEntry.ClusterDetails.ClusterDomain, vtorcReparent.promotedReplica.Key.StringCode())

		if !vtorcReparent.skipProcesses {
			// Execute post master-failover processes
			executeProcesses(config.Config.PostPrimaryFailoverProcesses, "PostPrimaryFailoverProcesses", vtorcReparent.topologyRecovery, false)
		}
	} else {
		recoverDeadPrimaryFailureCounter.Inc(1)
	}
}
