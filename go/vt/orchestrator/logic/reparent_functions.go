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

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"

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
	lostReplicas         [](*inst.Instance)
	recoveryAttempted    bool
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

func (vtorcReparent *VtOrcReparentFunctions) GetWaitReplicasTimeout() time.Duration {
	return time.Duration(config.Config.LockShardTimeoutSeconds) * time.Second
}

func (vtorcReparent *VtOrcReparentFunctions) GetIgnoreReplicas() sets.String {
	return nil
}

// CheckPrimaryRecoveryType implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) CheckPrimaryRecoveryType() error {
	vtorcReparent.topologyRecovery.RecoveryType = GetMasterRecoveryType(&vtorcReparent.topologyRecovery.AnalysisEntry)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", vtorcReparent.topologyRecovery.RecoveryType))
	if vtorcReparent.topologyRecovery.RecoveryType != MasterRecoveryGTID {
		return vtorcReparent.topologyRecovery.AddError(log.Errorf("RecoveryType unknown/unsupported"))
	}
	return nil
}

// RestrictValidCandidates implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) RestrictValidCandidates(validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) (map[string]mysql.Position, error) {
	restrictedValidCandidates := make(map[string]mysql.Position)

	for candidate, position := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}

		candidateInstance, _, err := inst.ReadInstance(&inst.InstanceKey{
			Hostname: candidateInfo.MysqlHostname,
			Port:     int(candidateInfo.MysqlPort),
		})

		if err != nil {
			return nil, err
		}

		if candidateInstance.PromotionRule == inst.MustNotPromoteRule {
			continue
		}

		restrictedValidCandidates[candidate] = position
	}

	return restrictedValidCandidates, nil
}

// FindPrimaryCandidates implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) FindPrimaryCandidates(ctx context.Context, logger logutil.Logger, tmc tmclient.TabletManagerClient, validCandidates map[string]mysql.Position, tabletMap map[string]*topo.TabletInfo) error {
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
	lostReplicas, promotedReplica, err := ChooseCandidate(tmc, &vtorcReparent.analysisEntry.AnalyzedInstanceKey, &vtorcReparent.topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal, validCandidates, tabletMap)
	vtorcReparent.topologyRecovery.AddError(err)
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
	vtorcReparent.recoveryAttempted = true
	return nil
}

// ChooseCandidate will choose a candidate replica of a given instance, and take its siblings using GTID
func ChooseCandidate(
	tmc tmclient.TabletManagerClient,
	masterKey *inst.InstanceKey,
	postponedFunctionsContainer *inst.PostponedFunctionsContainer,
	postponeAllMatchOperations func(*inst.Instance, bool) bool,
	validCandidates map[string]mysql.Position,
	tabletMap map[string]*topo.TabletInfo,
) (
	lostReplicas [](*inst.Instance),
	candidateReplica *inst.Instance,
	err error,
) {
	var emptyReplicas [](*inst.Instance)
	var unmovedReplicas [](*inst.Instance)
	var movedReplicas [](*inst.Instance)

	dataCenterHint := ""
	if master, _, _ := inst.ReadInstance(masterKey); master != nil {
		dataCenterHint = master.DataCenter
	}

	var replicas [](*inst.Instance)
	log.Errorf("started Manan's new function")

	for candidate := range validCandidates {
		candidateInfo, ok := tabletMap[candidate]
		if !ok {
			return emptyReplicas, candidateReplica, vterrors.Errorf(vtrpc.Code_INTERNAL, "candidate %v not found in the tablet map; this an impossible situation", candidate)
		}
		candidateInstance, _, err := inst.ReadInstance(&inst.InstanceKey{
			Hostname: candidateInfo.MysqlHostname,
			Port:     int(candidateInfo.MysqlPort),
		})
		if err != nil {
			log.Errorf("%v", err)
			return emptyReplicas, candidateReplica, err
		}
		replicas = append(replicas, candidateInstance)
	}

	inst.SortInstancesDataCenterHint(replicas, dataCenterHint)
	for _, replica := range replicas {
		log.Debugf("- sorted replica: %+v %+v", replica.Key, replica.ExecBinlogCoordinates)
	}

	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := inst.ChooseCandidateReplica(replicas)
	if err != nil {
		return emptyReplicas, candidateReplica, err
	}
	if candidateReplica != nil {
		mostUpToDateReplica := replicas[0]
		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateReplica.ExecBinlogCoordinates) {
			log.Warningf("GetCandidateReplica: chosen replica: %+v is behind most-up-to-date replica: %+v", candidateReplica.Key, mostUpToDateReplica.Key)
		}
	}
	log.Debugf("GetCandidateReplica: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateReplica.Key, len(aheadReplicas), len(equalReplicas), len(laterReplicas), len(cannotReplicateReplicas))

	replicasToMove := append(equalReplicas, laterReplicas...)
	hasBestPromotionRule := true
	if candidateReplica != nil {
		for _, replica := range replicasToMove {
			if replica.PromotionRule.BetterThan(candidateReplica.PromotionRule) {
				hasBestPromotionRule = false
			}
		}
	}

	// TODO: Use the set replication source functionality instead of the moveReplicasViaGTID
	//now := time.Now().UnixNano()
	if err := inst.SwitchMaster(candidateReplica.Key, *masterKey); err != nil {
		return emptyReplicas, candidateReplica, err
	}

	//candidateReplicaTablet, err := inst.ReadTablet(candidateReplica.Key)
	//if err != nil {
	//	return emptyReplicas, candidateReplica, err
	//}

	//moveGTIDFunc := func() error {
	//	log.Debugf("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))
	//
	//	for _, instance := range replicasToMove {
	//		tablet, err := inst.ReadTablet(instance.Key)
	//		if err != nil {
	//			return err
	//		}
	//
	//		err = tmc.SetReplicationSource(context.Background(), tablet, candidateReplicaTablet.Alias, now, "", false)
	//		if err != nil {
	//			unmovedReplicas = append(unmovedReplicas, instance)
	//			err = vterrors.Wrapf(err, "tablet %v SetMaster failed: %v", tablet.Alias, err)
	//			return err
	//		}
	//		movedReplicas = append(movedReplicas, instance)
	//	}
	//
	//	unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
	//	unmovedReplicas = append(unmovedReplicas, cannotReplicateReplicas...)
	//	return log.Errore(err)
	//}
	moveGTIDFunc := func() error {
		log.Debugf("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))

		movedReplicas, unmovedReplicas, err, _ = inst.MoveReplicasViaGTID(replicasToMove, candidateReplica, postponedFunctionsContainer)
		unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
		return log.Errore(err)
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
		postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	} else {
		err = moveGTIDFunc()
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
		postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	} else {
		err = moveGTIDFunc()
	}

	inst.StartReplication(&candidateReplica.Key)

	log.Debugf("RegroupReplicasGTID: done")
	inst.AuditOperation("regroup-replicas-gtid", masterKey, fmt.Sprintf("regrouped replicas of %+v via GTID; promoted %+v", *masterKey, candidateReplica.Key))
	return unmovedReplicas, candidateReplica, err
}

// CheckIfNeedToOverridePrimary implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) CheckIfNeedToOverridePrimary() error {
	if vtorcReparent.promotedReplica == nil {
		err := TabletUndoDemoteMaster(vtorcReparent.analysisEntry.AnalyzedInstanceKey)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: TabletUndoDemoteMaster: %v", err))
		message := "Failure: no replica promoted."
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, message)
		inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, message)
		return err
	}

	message := fmt.Sprintf("promoted replica: %+v", vtorcReparent.promotedReplica.Key)
	AuditTopologyRecovery(vtorcReparent.topologyRecovery, message)
	inst.AuditOperation("recover-dead-master", &vtorcReparent.analysisEntry.AnalyzedInstanceKey, message)
	vtorcReparent.topologyRecovery.LostReplicas.AddInstances(vtorcReparent.lostReplicas)

	var err error
	overrideMasterPromotion := func() (*inst.Instance, error) {
		if vtorcReparent.promotedReplica == nil {
			// No promotion; nothing to override.
			return vtorcReparent.promotedReplica, err
		}
		// Scenarios where we might cancel the promotion.
		if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&vtorcReparent.analysisEntry, vtorcReparent.promotedReplica); !satisfied {
			return nil, fmt.Errorf("RecoverDeadMaster: failed %+v promotion; %s", vtorcReparent.promotedReplica.Key, reason)
		}
		if config.Config.FailMasterPromotionOnLagMinutes > 0 &&
			time.Duration(vtorcReparent.promotedReplica.ReplicationLagSeconds.Int64)*time.Second >= time.Duration(config.Config.FailMasterPromotionOnLagMinutes)*time.Minute {
			// candidate replica lags too much
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionOnLagMinutes is set to %d (minutes) and promoted replica %+v 's lag is %d (seconds)", config.Config.FailMasterPromotionOnLagMinutes, vtorcReparent.promotedReplica.Key, vtorcReparent.promotedReplica.ReplicationLagSeconds.Int64)
		}
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !vtorcReparent.promotedReplica.SQLThreadUpToDate() {
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", vtorcReparent.promotedReplica.Key)
		}
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate && !vtorcReparent.promotedReplica.SQLThreadUpToDate() {
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: waiting for SQL thread on %+v", vtorcReparent.promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&vtorcReparent.promotedReplica.Key, 0, 0); err != nil {
				return nil, fmt.Errorf("DelayMasterPromotionIfSQLThreadNotUpToDate error: %+v", err)
			}
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: SQL thread caught up on %+v", vtorcReparent.promotedReplica.Key))
		}
		// All seems well. No override done.
		return vtorcReparent.promotedReplica, err
	}
	if vtorcReparent.promotedReplica, err = overrideMasterPromotion(); err != nil {
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, err.Error())
	}
	return nil
}

// StartReplication implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) StartReplication(ctx context.Context, ev *events.Reparent, logger logutil.Logger, tmc tmclient.TabletManagerClient) error {
	// And this is the end; whether successful or not, we're done.
	resolveRecovery(vtorcReparent.topologyRecovery, vtorcReparent.promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back.
	if vtorcReparent.promotedReplica != nil {
		// Success!
		recoverDeadMasterSuccessCounter.Inc(1)
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("RecoverDeadMaster: successfully promoted %+v", vtorcReparent.promotedReplica.Key))
		AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: promoted server coordinates: %+v", vtorcReparent.promotedReplica.SelfBinlogCoordinates))

		AuditTopologyRecovery(vtorcReparent.topologyRecovery, "- RecoverDeadMaster: will apply MySQL changes to promoted master")
		{
			_, err := inst.ResetReplicationOperation(&vtorcReparent.promotedReplica.Key)
			if err != nil {
				// Ugly, but this is important. Let's give it another try
				_, err = inst.ResetReplicationOperation(&vtorcReparent.promotedReplica.Key)
			}
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying RESET SLAVE ALL on promoted master: success=%t", (err == nil)))
			if err != nil {
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: NOTE that %+v is promoted even though SHOW SLAVE STATUS may still show it has a master", vtorcReparent.promotedReplica.Key))
			}
		}
		{
			count := inst.MasterSemiSync(vtorcReparent.promotedReplica.Key)
			err := inst.SetSemiSyncMaster(&vtorcReparent.promotedReplica.Key, count > 0)
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying semi-sync %v: success=%t", count > 0, (err == nil)))

			// Dont' allow writes if semi-sync settings fail.
			if err == nil {
				_, err := inst.SetReadOnly(&vtorcReparent.promotedReplica.Key, false)
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=0 on promoted master: success=%t", (err == nil)))
			}
		}
		// Let's attempt, though we won't necessarily succeed, to set old master as read-only
		go func() {
			_, err := inst.SetReadOnly(&vtorcReparent.analysisEntry.AnalyzedInstanceKey, true)
			AuditTopologyRecovery(vtorcReparent.topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=1 on demoted master: success=%t", (err == nil)))
		}()

		kvPairs := inst.GetClusterMasterKVPairs(vtorcReparent.analysisEntry.ClusterDetails.ClusterAlias, &vtorcReparent.promotedReplica.Key)
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
		if config.Config.MasterFailoverDetachReplicaMasterHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(vtorcReparent.topologyRecovery, "- RecoverDeadMaster: detaching master host on promoted master")
				inst.DetachReplicaMasterHost(&vtorcReparent.promotedReplica.Key)
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
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", vtorcReparent.topologyRecovery, false)
		}
	} else {
		recoverDeadMasterFailureCounter.Inc(1)
	}
	return nil
}

// GetNewPrimary implements the ReparentFunctions interface
func (vtorcReparent *VtOrcReparentFunctions) GetNewPrimary() *topodatapb.Tablet {
	tablet, _ := inst.ReadTablet(vtorcReparent.promotedReplica.Key)
	return tablet
}

func (vtorcReparent *VtOrcReparentFunctions) SetMaps(tabletMap map[string]*topo.TabletInfo, statusMap map[string]*replicationdatapb.StopReplicationStatus, primaryStatusMap map[string]*replicationdatapb.PrimaryStatus) {
}
