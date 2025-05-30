/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/util"
)

const (
	CheckAndRecoverGenericProblemRecoveryName        string = "CheckAndRecoverGenericProblem"
	RecoverDeadPrimaryRecoveryName                   string = "RecoverDeadPrimary"
	RecoverPrimaryTabletDeletedRecoveryName          string = "RecoverPrimaryTabletDeleted"
	RecoverPrimaryHasPrimaryRecoveryName             string = "RecoverPrimaryHasPrimary"
	CheckAndRecoverLockedSemiSyncPrimaryRecoveryName string = "CheckAndRecoverLockedSemiSyncPrimary"
	ElectNewPrimaryRecoveryName                      string = "ElectNewPrimary"
	FixPrimaryRecoveryName                           string = "FixPrimary"
	FixReplicaRecoveryName                           string = "FixReplica"
	RecoverErrantGTIDDetectedName                    string = "RecoverErrantGTIDDetected"
)

var (
	countPendingRecoveries = stats.NewGauge("PendingRecoveries", "Count of the number of pending recoveries")

	// detectedProblems is used to track the number of detected problems.
	//
	// When an issue is active it will be set to 1, when it is no longer active
	// it will be reset back to 0.
	detectedProblems = stats.NewGaugesWithMultiLabels("DetectedProblems", "Count of the different detected problems", []string{
		"Analysis",
		"TabletAlias",
		"Keyspace",
		"Shard",
	})

	// shardsLockCounter is a count of in-flight shard locks. Use atomics to read/update.
	shardsLockCounter int64

	// recoveriesCounterLabels are labels for grouping the counter based stats for recoveries.
	recoveriesCounterLabels = []string{"RecoveryType", "Keyspace", "Shard"}

	// recoveriesCounter counts the number of recoveries that VTOrc has performed
	recoveriesCounter = stats.NewCountersWithMultiLabels("RecoveriesCount", "Count of the different recoveries performed", recoveriesCounterLabels)

	// recoveriesSuccessfulCounter counts the number of successful recoveries that VTOrc has performed
	recoveriesSuccessfulCounter = stats.NewCountersWithMultiLabels("SuccessfulRecoveries", "Count of the different successful recoveries performed", recoveriesCounterLabels)

	// recoveriesFailureCounter counts the number of failed recoveries that VTOrc has performed
	recoveriesFailureCounter = stats.NewCountersWithMultiLabels("FailedRecoveries", "Count of the different failed recoveries performed", recoveriesCounterLabels)

	// shardLockTimings measures the timing of LockShard operations.
	shardLockTimingsActions = []string{"Lock", "Unlock"}
	shardLockTimings        = stats.NewTimings("ShardLockTimings", "Timings of global shard locks", "Action", shardLockTimingsActions...)
)

// recoveryFunction is the code of the recovery function to be used
// this is returned from getCheckAndRecoverFunctionCode to compare the functions returned
// Each recoveryFunction is one to one mapped to a corresponding recovery.
type recoveryFunction int

const (
	noRecoveryFunc recoveryFunction = iota
	recoverGenericProblemFunc
	recoverDeadPrimaryFunc
	recoverPrimaryTabletDeletedFunc
	recoverPrimaryHasPrimaryFunc
	recoverLockedSemiSyncPrimaryFunc
	electNewPrimaryFunc
	fixPrimaryFunc
	fixReplicaFunc
	recoverErrantGTIDDetectedFunc
)

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	ID                     int64
	AnalysisEntry          inst.ReplicationAnalysis
	SuccessorAlias         string
	IsSuccessful           bool
	AllErrors              []string
	RecoveryStartTimestamp string
	RecoveryEndTimestamp   string
	DetectionID            int64
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.AllErrors = []string{}
	return topologyRecovery
}

func (topologyRecovery *TopologyRecovery) AddError(err error) error {
	if err != nil {
		topologyRecovery.AllErrors = append(topologyRecovery.AllErrors, err.Error())
	}
	return err
}

func (topologyRecovery *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		_ = topologyRecovery.AddError(err)
	}
}

type TopologyRecoveryStep struct {
	ID         int64
	RecoveryID int64
	AuditAt    string
	Message    string
}

func NewTopologyRecoveryStep(id int64, message string) *TopologyRecoveryStep {
	return &TopologyRecoveryStep{
		RecoveryID: id,
		Message:    message,
	}
}

func init() {
	// ShardLocksActive is a stats representation of shardsLockCounter.
	stats.NewGaugeFunc("ShardLocksActive", "Number of actively-held shard locks", func() int64 {
		return atomic.LoadInt64(&shardsLockCounter)
	})
	go initializeTopologyRecoveryPostConfiguration()
}

func initializeTopologyRecoveryPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()
}

func getLockAction(analysedInstance string, code inst.AnalysisCode) string {
	return fmt.Sprintf("VTOrc Recovery for %v on %v", code, analysedInstance)
}

// LockShard locks the keyspace-shard preventing others from performing conflicting actions.
func LockShard(ctx context.Context, keyspace, shard, lockAction string) (context.Context, func(*error), error) {
	if keyspace == "" {
		return nil, nil, errors.New("can't lock shard: keyspace is unspecified")
	}
	if shard == "" {
		return nil, nil, errors.New("can't lock shard: shard name is unspecified")
	}
	val := atomic.LoadInt32(&hasReceivedSIGTERM)
	if val > 0 {
		return nil, nil, errors.New("can't lock shard: SIGTERM received")
	}

	startTime := time.Now()
	defer func() {
		lockTime := time.Since(startTime)
		shardLockTimings.Add("Lock", lockTime)
	}()

	atomic.AddInt64(&shardsLockCounter, 1)
	ctx, unlock, err := ts.TryLockShard(ctx, keyspace, shard, lockAction)
	if err != nil {
		atomic.AddInt64(&shardsLockCounter, -1)
		return nil, nil, err
	}
	return ctx, func(e *error) {
		startTime := time.Now()
		defer func() {
			atomic.AddInt64(&shardsLockCounter, -1)
			shardLockTimings.Add("Unlock", time.Since(startTime))
		}()
		unlock(e)
	}, nil
}

// AuditTopologyRecovery audits a single step in a topology recovery process.
func AuditTopologyRecovery(topologyRecovery *TopologyRecovery, message string) error {
	log.Infof("topology_recovery: %s", message)
	if topologyRecovery == nil {
		return nil
	}

	recoveryStep := NewTopologyRecoveryStep(topologyRecovery.ID, message)
	return writeTopologyRecoveryStep(recoveryStep)
}

func resolveRecovery(topologyRecovery *TopologyRecovery, successorInstance *inst.Instance) error {
	if successorInstance != nil {
		topologyRecovery.SuccessorAlias = successorInstance.InstanceAlias
		topologyRecovery.IsSuccessful = true
	}
	return writeResolveRecovery(topologyRecovery)
}

// recoverPrimaryHasPrimary resets the replication on the primary instance
func recoverPrimaryHasPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimaryHasPrimary.", analysisEntry.AnalyzedInstanceAlias)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, will fix incorrect primaryship on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	// Read the tablet information from the database to find the shard and keyspace of the tablet
	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		logger.Errorf("Failed to read instance %s, aborting recovery", analysisEntry.AnalyzedInstanceAlias)
		return false, nil, err
	}

	// Reset replication on current primary.
	err = resetReplicationParameters(ctx, analyzedTablet)
	return true, topologyRecovery, err
}

// runEmergencyReparentOp runs a recovery for which we have to run ERS. Here waitForAllTablets is a boolean telling ERS whether it should wait for all the tablets
// or is it okay to skip 1.
func runEmergencyReparentOp(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, recoveryName string, waitForAllTablets bool, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	// Read the tablet information from the database to find the shard and keyspace of the tablet
	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		logger.Errorf("Failed to read instance %s, aborting recovery", analysisEntry.AnalyzedInstanceAlias)
		return false, nil, err
	}

	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %v.", analysisEntry.AnalyzedInstanceAlias, recoveryName)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, %v %+v", analysisEntry.Analysis, recoveryName, analysisEntry.AnalyzedInstanceAlias)
	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, promotedReplica)
	}()

	ev, err := reparentutil.NewEmergencyReparenter(ts, tmc, logutil.NewCallbackLogger(func(event *logutilpb.Event) {
		level := event.GetLevel()
		value := event.GetValue()
		// we only log the warnings and errors explicitly, everything gets logged as an information message anyways in auditing topology recovery
		switch level {
		case logutilpb.Level_WARNING:
			logger.Warningf("ERS - %s", value)
		case logutilpb.Level_ERROR:
			logger.Errorf("ERS - %s", value)
		default:
			logger.Infof("ERS - %s", value)
		}
		_ = AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		tablet.Keyspace,
		tablet.Shard,
		reparentutil.EmergencyReparentOptions{
			IgnoreReplicas:            nil,
			WaitReplicasTimeout:       config.GetWaitReplicasTimeout(),
			PreventCrossCellPromotion: config.GetPreventCrossCellFailover(),
			WaitAllTablets:            waitForAllTablets,
		},
	)
	if err != nil {
		logger.Errorf("Error running ERS - %v", err)
	}

	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(topoproto.TabletAliasString(ev.NewPrimary.Alias))
	}
	postErsCompletion(topologyRecovery, analysisEntry, recoveryName, promotedReplica)
	return true, topologyRecovery, err
}

// recoverDeadPrimary checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func recoverDeadPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return runEmergencyReparentOp(ctx, analysisEntry, "RecoverDeadPrimary", false, logger)
}

// recoverPrimaryTabletDeleted tries to run a recovery for the case where the primary tablet has been deleted.
func recoverPrimaryTabletDeleted(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return runEmergencyReparentOp(ctx, analysisEntry, "PrimaryTabletDeleted", true, logger)
}

func postErsCompletion(topologyRecovery *TopologyRecovery, analysisEntry *inst.ReplicationAnalysis, recoveryName string, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.InstanceAlias)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		_ = inst.AuditOperation(recoveryName, analysisEntry.AnalyzedInstanceAlias, message)
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%v: successfully promoted %+v", recoveryName, promotedReplica.InstanceAlias))
	}
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverLockedSemiSyncPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	logger.Warning("No actions in checkAndRecoverLockedSemiSyncPrimary")
	return false, nil, nil
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (bool, *TopologyRecovery, error) {
	logger.Warning("No actions in checkAndRecoverGenericProblem")
	return false, nil, nil
}

// getCheckAndRecoverFunctionCode gets the recovery function code to use for the given analysis.
func getCheckAndRecoverFunctionCode(analysisCode inst.AnalysisCode, tabletAlias string) recoveryFunction {
	switch analysisCode {
	// primary
	case inst.DeadPrimary, inst.DeadPrimaryAndSomeReplicas, inst.PrimaryDiskStalled, inst.PrimarySemiSyncBlocked:
		// If ERS is disabled, we have no way of repairing the cluster.
		if !config.ERSEnabled() {
			log.Infof("VTOrc not configured to run ERS, skipping recovering %v", analysisCode)
			return noRecoveryFunc
		}
		return recoverDeadPrimaryFunc
	case inst.PrimaryTabletDeleted:
		// If ERS is disabled, we have no way of repairing the cluster.
		if !config.ERSEnabled() {
			log.Infof("VTOrc not configured to run ERS, skipping recovering %v", analysisCode)
			return noRecoveryFunc
		}
		return recoverPrimaryTabletDeletedFunc
	case inst.ErrantGTIDDetected:
		if !config.ConvertTabletWithErrantGTIDs() {
			log.Infof("VTOrc not configured to do anything on detecting errant GTIDs, skipping recovering %v", analysisCode)
			return noRecoveryFunc
		}
		return recoverErrantGTIDDetectedFunc
	case inst.PrimaryHasPrimary:
		return recoverPrimaryHasPrimaryFunc
	case inst.LockedSemiSyncPrimary:
		return recoverLockedSemiSyncPrimaryFunc
	case inst.ClusterHasNoPrimary:
		return electNewPrimaryFunc
	case inst.PrimaryIsReadOnly, inst.PrimarySemiSyncMustBeSet, inst.PrimarySemiSyncMustNotBeSet, inst.PrimaryCurrentTypeMismatch:
		return fixPrimaryFunc
	// replica
	case inst.NotConnectedToPrimary, inst.ConnectedToWrongPrimary, inst.ReplicationStopped, inst.ReplicaIsWritable,
		inst.ReplicaSemiSyncMustBeSet, inst.ReplicaSemiSyncMustNotBeSet, inst.ReplicaMisconfigured:
		return fixReplicaFunc
	// primary, non actionable
	case inst.DeadPrimaryAndReplicas:
		return recoverGenericProblemFunc
	case inst.UnreachablePrimary:
		return recoverGenericProblemFunc
	case inst.UnreachablePrimaryWithLaggingReplicas:
		return recoverGenericProblemFunc
	case inst.AllPrimaryReplicasNotReplicating:
		return recoverGenericProblemFunc
	case inst.AllPrimaryReplicasNotReplicatingOrDead:
		return recoverGenericProblemFunc
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllPrimaryReplicasStale:
	//   return recoverGenericProblemFunc

	return noRecoveryFunc
}

// hasActionableRecovery tells if a recoveryFunction has an actionable recovery or not
func hasActionableRecovery(recoveryFunctionCode recoveryFunction) bool {
	switch recoveryFunctionCode {
	case noRecoveryFunc:
		return false
	case recoverGenericProblemFunc:
		return false
	case recoverDeadPrimaryFunc:
		return true
	case recoverPrimaryTabletDeletedFunc:
		return true
	case recoverPrimaryHasPrimaryFunc:
		return true
	case recoverLockedSemiSyncPrimaryFunc:
		return true
	case electNewPrimaryFunc:
		return true
	case fixPrimaryFunc:
		return true
	case fixReplicaFunc:
		return true
	case recoverErrantGTIDDetectedFunc:
		return true
	default:
		return false
	}
}

// getCheckAndRecoverFunction gets the recovery function for the given code.
func getCheckAndRecoverFunction(recoveryFunctionCode recoveryFunction) (
	checkAndRecoverFunction func(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
) {
	switch recoveryFunctionCode {
	case noRecoveryFunc:
		return nil
	case recoverGenericProblemFunc:
		return checkAndRecoverGenericProblem
	case recoverDeadPrimaryFunc:
		return recoverDeadPrimary
	case recoverPrimaryTabletDeletedFunc:
		return recoverPrimaryTabletDeleted
	case recoverPrimaryHasPrimaryFunc:
		return recoverPrimaryHasPrimary
	case recoverLockedSemiSyncPrimaryFunc:
		return checkAndRecoverLockedSemiSyncPrimary
	case electNewPrimaryFunc:
		return electNewPrimary
	case fixPrimaryFunc:
		return fixPrimary
	case fixReplicaFunc:
		return fixReplica
	case recoverErrantGTIDDetectedFunc:
		return recoverErrantGTIDDetected
	default:
		return nil
	}
}

// getRecoverFunctionName gets the recovery function name for the given code.
// This name is used for metrics
func getRecoverFunctionName(recoveryFunctionCode recoveryFunction) string {
	switch recoveryFunctionCode {
	case noRecoveryFunc:
		return ""
	case recoverGenericProblemFunc:
		return CheckAndRecoverGenericProblemRecoveryName
	case recoverDeadPrimaryFunc:
		return RecoverDeadPrimaryRecoveryName
	case recoverPrimaryTabletDeletedFunc:
		return RecoverPrimaryTabletDeletedRecoveryName
	case recoverPrimaryHasPrimaryFunc:
		return RecoverPrimaryHasPrimaryRecoveryName
	case recoverLockedSemiSyncPrimaryFunc:
		return CheckAndRecoverLockedSemiSyncPrimaryRecoveryName
	case electNewPrimaryFunc:
		return ElectNewPrimaryRecoveryName
	case fixPrimaryFunc:
		return FixPrimaryRecoveryName
	case fixReplicaFunc:
		return FixReplicaRecoveryName
	case recoverErrantGTIDDetectedFunc:
		return RecoverErrantGTIDDetectedName
	default:
		return ""
	}
}

// isClusterWideRecovery returns whether the given recovery is a cluster-wide recovery or not
func isClusterWideRecovery(recoveryFunctionCode recoveryFunction) bool {
	switch recoveryFunctionCode {
	case recoverDeadPrimaryFunc, electNewPrimaryFunc, recoverPrimaryTabletDeletedFunc:
		return true
	default:
		return false
	}
}

// analysisEntriesHaveSameRecovery tells whether the two analysis entries have the same recovery function or not
func analysisEntriesHaveSameRecovery(prevAnalysis, newAnalysis *inst.ReplicationAnalysis) bool {
	prevRecoveryFunctionCode := getCheckAndRecoverFunctionCode(prevAnalysis.Analysis, prevAnalysis.AnalyzedInstanceAlias)
	newRecoveryFunctionCode := getCheckAndRecoverFunctionCode(newAnalysis.Analysis, newAnalysis.AnalyzedInstanceAlias)
	return prevRecoveryFunctionCode == newRecoveryFunctionCode
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronously
func executeCheckAndRecoverFunction(analysisEntry *inst.ReplicationAnalysis) (err error) {
	countPendingRecoveries.Add(1)
	defer countPendingRecoveries.Add(-1)

	logger := log.NewPrefixedLogger(fmt.Sprintf("Recovery for %s on %s/%s", analysisEntry.Analysis, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard))
	logger.Info("Starting checkAndRecover")

	checkAndRecoverFunctionCode := getCheckAndRecoverFunctionCode(analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	isActionableRecovery := hasActionableRecovery(checkAndRecoverFunctionCode)
	analysisEntry.IsActionableRecovery = isActionableRecovery

	if checkAndRecoverFunctionCode == noRecoveryFunc {
		logger.Warning("No recovery strategies for problem, aborting recovery")
		// Unhandled problem type
		if analysisEntry.Analysis != inst.NoProblem {
			if util.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceAlias) {
				logger.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: tablet: %+v",
					analysisEntry.AnalyzedInstanceAlias)
			}
		}

		return nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceAlias) {
		logger.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias, isActionableRecovery)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.
	// Record the failure detected in the logs.
	err = InsertRecoveryDetection(analysisEntry)
	if err != nil {
		logger.Errorf("executeCheckAndRecoverFunction: error inserting recovery detection record, aborting recovery: %+v", err)
		return err
	}

	// Check for recovery being disabled globally
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this
		logger.Errorf("Unable to determine if recovery is disabled globally, still attempting to recover: %v", err)
	} else if recoveryDisabledGlobally {
		logger.Infof("CheckAndRecover: Tablet: %+v: NOT Recovering host (disabled globally)",
			analysisEntry.AnalyzedInstanceAlias)

		return err
	}

	// We lock the shard here and then refresh the tablets information
	ctx, unlock, err := LockShard(context.Background(), analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard,
		getLockAction(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis),
	)
	if err != nil {
		logger.Errorf("Failed to lock shard, aborting recovery: %v", err)
		return err
	}
	defer unlock(&err)

	// Check if the recovery is already fixed or not. We need this because vtorc works on ephemeral data to find the failure scenarios.
	// That data might be old, because of a cluster operation that was run through vtctld or some other vtorc. So before we do any
	// changes, we should be checking that this failure is indeed needed to be fixed. We do this after locking the shard to be sure
	// that the data that we use now is up-to-date.
	if isActionableRecovery {
		logger.Infof("executeCheckAndRecoverFunction: Proceeding with %v recovery on %v validation after acquiring shard lock.", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
		// The first step we have to do is refresh the keyspace and shard information
		// This is required to know if the durability policies have changed or not
		// If they have, then recoveries like ReplicaSemiSyncMustNotBeSet, etc won't be valid anymore.
		// Similarly, a new primary could have been elected in the mean-time that can cause
		// a change in the recovery we run.
		err = RefreshKeyspaceAndShard(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
		if err != nil {
			logger.Errorf("Failed to refresh keyspace and shard, aborting recovery: %v", err)
			return err
		}
		// If we are about to run a cluster-wide recovery, it is imperative to first refresh all the tablets
		// of a shard because a new tablet could have been promoted, and we need to have this visibility before we
		// run a cluster operation of our own.
		if isClusterWideRecovery(checkAndRecoverFunctionCode) {
			var tabletsToIgnore []string
			if checkAndRecoverFunctionCode == recoverDeadPrimaryFunc {
				tabletsToIgnore = append(tabletsToIgnore, analysisEntry.AnalyzedInstanceAlias)
			}
			// We ignore the dead primary tablet because it is going to be unreachable. If all the other tablets aren't able to reach this tablet either,
			// we can proceed with the dead primary recovery. We don't need to refresh the information for this dead tablet.
			logger.Info("Force refreshing all shard tablets")
			forceRefreshAllTabletsInShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, tabletsToIgnore)
		} else {
			// If we are not running a cluster-wide recovery, then it is only concerned with the specific tablet
			// on which the failure occurred and the primary instance of the shard.
			// For example, ConnectedToWrongPrimary analysis only cares for whom the current primary tablet is
			// and the host-port set on the tablet in question.
			// So, we only need to refresh the tablet info records (to know if the primary tablet has changed),
			// and the replication data of the new primary and this tablet.
			logger.Info("Refreshing shard tablet info")
			refreshTabletInfoOfShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			logger.Info("Discovering analysis instance")
			DiscoverInstance(analysisEntry.AnalyzedInstanceAlias, true)
			logger.Info("Getting shard primary")
			primaryTablet, err := shardPrimary(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			if err != nil {
				logger.Errorf("executeCheckAndRecoverFunction: Tablet: %+v: error while finding the shard primary: %v",
					analysisEntry.AnalyzedInstanceAlias, err)
				return err
			}
			primaryTabletAlias := topoproto.TabletAliasString(primaryTablet.Alias)
			// We can skip the refresh if we know the tablet we are looking at is the primary tablet.
			// This would be the case for PrimaryHasPrimary recovery. We don't need to refresh the same tablet twice.
			if analysisEntry.AnalyzedInstanceAlias != primaryTabletAlias {
				logger.Info("Discovering primary instance")
				DiscoverInstance(primaryTabletAlias, true)
			}
		}
		alreadyFixed, err := checkIfAlreadyFixed(analysisEntry)
		if err != nil {
			logger.Errorf("executeCheckAndRecoverFunction: Tablet: %+v: error while trying to find if the problem is already fixed: %v",
				analysisEntry.AnalyzedInstanceAlias, err)
			return err
		}
		if alreadyFixed {
			logger.Infof("Analysis: %v on tablet %v - No longer valid, some other agent must have fixed the problem.", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
			return nil
		}
	}

	// Actually attempt recovery:
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceAlias) {
		logger.Infof("executeCheckAndRecoverFunction: proceeding with recovery on %+v; isRecoverable?: %+v", analysisEntry.AnalyzedInstanceAlias, isActionableRecovery)
	}
	recoveryAttempted, topologyRecovery, err := getCheckAndRecoverFunction(checkAndRecoverFunctionCode)(ctx, analysisEntry, logger)
	if !recoveryAttempted {
		logger.Errorf("Recovery not attempted: %+v", err)
		return err
	}
	recoveryName := getRecoverFunctionName(checkAndRecoverFunctionCode)
	recoveryLabels := []string{recoveryName, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard}
	recoveriesCounter.Add(recoveryLabels, 1)
	if err != nil {
		logger.Errorf("Failed to recover: %+v", err)
		recoveriesFailureCounter.Add(recoveryLabels, 1)
	} else {
		logger.Info("Recovery succeeded")
		recoveriesSuccessfulCounter.Add(recoveryLabels, 1)
	}
	if topologyRecovery == nil {
		logger.Error("Topology recovery is nil - recovery might have failed")
		return err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		logger.Infof("Topology recovery: %+v", string(b))
	} else {
		logger.Infof("Topology recovery: %+v", topologyRecovery)
	}
	// If we ran a cluster wide recovery and actually attempted it, then we know that the replication state for all the tablets in this cluster
	// would have changed. So we can go ahead and pre-emptively refresh them.
	// For this refresh we don't use the same context that we used for the recovery, since that context might have expired or could expire soon
	// Instead we pass the background context. The call forceRefreshAllTabletsInShard handles adding a timeout to it for us.
	if isClusterWideRecovery(checkAndRecoverFunctionCode) {
		logger.Info("Forcing refresh of all tablets post recovery")
		forceRefreshAllTabletsInShard(context.Background(), analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, nil)
	} else {
		// For all other recoveries, we would have changed the replication status of the analyzed tablet
		// so it doesn't hurt to re-read the information of this tablet, otherwise we'll requeue the same recovery
		// that we just completed because we would be using stale data.
		logger.Info("Force discovering problem instance %s post recovery", analysisEntry.AnalyzedInstanceAlias)
		DiscoverInstance(analysisEntry.AnalyzedInstanceAlias, true)
	}
	return err
}

// checkIfAlreadyFixed checks whether the problem that the analysis entry represents has already been fixed by another agent or not
func checkIfAlreadyFixed(analysisEntry *inst.ReplicationAnalysis) (bool, error) {
	// Run a replication analysis again. We will check if the problem persisted
	analysisEntries, err := inst.GetReplicationAnalysis(analysisEntry.ClusterDetails.Keyspace, analysisEntry.ClusterDetails.Shard, &inst.ReplicationAnalysisHints{})
	if err != nil {
		return false, err
	}

	for _, entry := range analysisEntries {
		// If there is a analysis which has the same recovery required, then we should proceed with the recovery
		if entry.AnalyzedInstanceAlias == analysisEntry.AnalyzedInstanceAlias && analysisEntriesHaveSameRecovery(analysisEntry, entry) {
			return false, nil
		}
	}

	// We didn't find a replication analysis matching the original failure, which means that some other agent probably fixed it.
	return true, nil
}

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover() {
	// Allow the analysis to run even if we don't want to recover
	replicationAnalysis, err := inst.GetReplicationAnalysis("", "", &inst.ReplicationAnalysisHints{AuditAnalysis: true})
	if err != nil {
		log.Error(err)
		return
	}

	// Regardless of if the problem is solved or not we want to monitor active
	// issues, we use a map of labels and set a counter to `1` for each problem
	// then we reset any counter that is not present in the current analysis.
	active := make(map[string]struct{})
	for _, e := range replicationAnalysis {
		if e.Analysis != inst.NoProblem {
			names := [...]string{
				string(e.Analysis),
				e.AnalyzedInstanceAlias,
				e.AnalyzedKeyspace,
				e.AnalyzedShard,
			}

			key := detectedProblems.GetLabelName(names[:]...)
			active[key] = struct{}{}
			detectedProblems.Set(names[:], 1)
		}
	}

	// Reset any non-active problems.
	for key := range detectedProblems.Counts() {
		if _, ok := active[key]; !ok {
			detectedProblems.ResetKey(key)
		}
	}

	// intentionally iterating entries in random order
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]

		go func() {
			if err := executeCheckAndRecoverFunction(analysisEntry); err != nil {
				log.Error(err)
			}
		}()
	}
}

func postPrsCompletion(topologyRecovery *TopologyRecovery, analysisEntry *inst.ReplicationAnalysis, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.InstanceAlias)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		_ = inst.AuditOperation(string(analysisEntry.Analysis), analysisEntry.AnalyzedInstanceAlias, message)
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%+v: successfully promoted %+v", analysisEntry.Analysis, promotedReplica.InstanceAlias))
	}
}

// electNewPrimary elects a new primary while none were present before.
func electNewPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil || err != nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another electNewPrimary.", analysisEntry.AnalyzedInstanceAlias)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, will elect a new primary for %v:%v", analysisEntry.Analysis, analysisEntry.ClusterDetails.Keyspace, analysisEntry.ClusterDetails.Shard)

	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, promotedReplica)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		logger.Errorf("Failed to read instance %s, aborting recovery", analysisEntry.AnalyzedInstanceAlias)
		return false, topologyRecovery, err
	}
	_ = AuditTopologyRecovery(topologyRecovery, "starting PlannedReparentShard for electing new primary.")

	ev, err := reparentutil.NewPlannedReparenter(ts, tmc, logutil.NewCallbackLogger(func(event *logutilpb.Event) {
		level := event.GetLevel()
		value := event.GetValue()
		// we only log the warnings and errors explicitly, everything gets logged as an information message anyways in auditing topology recovery
		switch level {
		case logutilpb.Level_WARNING:
			logger.Warningf("PRS - %s", value)
		case logutilpb.Level_ERROR:
			logger.Errorf("PRS - %s", value)
		}
		_ = AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		analyzedTablet.Keyspace,
		analyzedTablet.Shard,
		reparentutil.PlannedReparentOptions{
			WaitReplicasTimeout: config.GetWaitReplicasTimeout(),
			TolerableReplLag:    config.GetTolerableReplicationLag(),
		},
	)

	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(topoproto.TabletAliasString(ev.NewPrimary.Alias))
	}
	postPrsCompletion(topologyRecovery, analysisEntry, promotedReplica)
	return true, topologyRecovery, err
}

// fixPrimary sets the primary as read-write.
func fixPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimary.", analysisEntry.AnalyzedInstanceAlias)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, will fix primary to read-write %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		logger.Errorf("Failed to read instance %s, aborting recovery", analysisEntry.AnalyzedInstanceAlias)
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet.Keyspace)
	if err != nil {
		logger.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	if err := tabletUndoDemotePrimary(ctx, analyzedTablet, policy.SemiSyncAckers(durabilityPolicy, analyzedTablet) > 0); err != nil {
		return true, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// fixReplica sets the replica as read-only and points it at the current primary.
func fixReplica(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixReplica.", analysisEntry.AnalyzedInstanceAlias)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, will fix replica %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		logger.Errorf("Failed to read instance %s, aborting recovery", analysisEntry.AnalyzedInstanceAlias)
		return false, topologyRecovery, err
	}

	primaryTablet, err := shardPrimary(analyzedTablet.Keyspace, analyzedTablet.Shard)
	if err != nil {
		logger.Info("Could not compute primary for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet.Keyspace)
	if err != nil {
		logger.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	err = setReadOnly(ctx, analyzedTablet)
	if err != nil {
		logger.Info("Could not set the tablet %v to readonly - %v", analysisEntry.AnalyzedInstanceAlias, err)
		return true, topologyRecovery, err
	}

	err = setReplicationSource(ctx, analyzedTablet, primaryTablet, policy.IsReplicaSemiSync(durabilityPolicy, primaryTablet, analyzedTablet), float64(analysisEntry.ReplicaNetTimeout)/2)
	return true, topologyRecovery, err
}

// recoverErrantGTIDDetected changes the tablet type of a replica tablet that has errant GTIDs.
func recoverErrantGTIDDetected(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another recoverErrantGTIDDetected.", analysisEntry.AnalyzedInstanceAlias)
		logger.Warning(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	logger.Infof("Analysis: %v, will fix tablet %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}

	primaryTablet, err := shardPrimary(analyzedTablet.Keyspace, analyzedTablet.Shard)
	if err != nil {
		logger.Info("Could not compute primary for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet.Keyspace)
	if err != nil {
		logger.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	err = changeTabletType(ctx, analyzedTablet, topodatapb.TabletType_DRAINED, policy.IsReplicaSemiSync(durabilityPolicy, primaryTablet, analyzedTablet))
	return true, topologyRecovery, err
}
