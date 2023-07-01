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
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/util"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type RecoveryType string

const (
	CheckAndRecoverGenericProblemRecoveryName        string = "CheckAndRecoverGenericProblem"
	RecoverDeadPrimaryRecoveryName                   string = "RecoverDeadPrimary"
	RecoverPrimaryHasPrimaryRecoveryName             string = "RecoverPrimaryHasPrimary"
	CheckAndRecoverLockedSemiSyncPrimaryRecoveryName string = "CheckAndRecoverLockedSemiSyncPrimary"
	ElectNewPrimaryRecoveryName                      string = "ElectNewPrimary"
	FixPrimaryRecoveryName                           string = "FixPrimary"
	FixReplicaRecoveryName                           string = "FixReplica"
)

var (
	actionableRecoveriesNames = []string{
		RecoverDeadPrimaryRecoveryName,
		RecoverPrimaryHasPrimaryRecoveryName,
		ElectNewPrimaryRecoveryName,
		FixPrimaryRecoveryName,
		FixReplicaRecoveryName,
	}

	countPendingRecoveries = stats.NewGauge("PendingRecoveries", "Count of the number of pending recoveries")

	// recoveriesCounter counts the number of recoveries that VTOrc has performed
	recoveriesCounter = stats.NewCountersWithSingleLabel("RecoveriesCount", "Count of the different recoveries performed", "RecoveryType", actionableRecoveriesNames...)

	// recoveriesSuccessfulCounter counts the number of successful recoveries that VTOrc has performed
	recoveriesSuccessfulCounter = stats.NewCountersWithSingleLabel("SuccessfulRecoveries", "Count of the different successful recoveries performed", "RecoveryType", actionableRecoveriesNames...)

	// recoveriesFailureCounter counts the number of failed recoveries that VTOrc has performed
	recoveriesFailureCounter = stats.NewCountersWithSingleLabel("FailedRecoveries", "Count of the different failed recoveries performed", "RecoveryType", actionableRecoveriesNames...)
)

// recoveryFunction is the code of the recovery function to be used
// this is returned from getCheckAndRecoverFunctionCode to compare the functions returned
// Each recoveryFunction is one to one mapped to a corresponding recovery.
type recoveryFunction int

const (
	noRecoveryFunc recoveryFunction = iota
	recoverGenericProblemFunc
	recoverDeadPrimaryFunc
	recoverPrimaryHasPrimaryFunc
	recoverLockedSemiSyncPrimaryFunc
	electNewPrimaryFunc
	fixPrimaryFunc
	fixReplicaFunc
)

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	ID                     int64
	UID                    string
	AnalysisEntry          inst.ReplicationAnalysis
	SuccessorHostname      string
	SuccessorPort          int
	SuccessorAlias         string
	IsActive               bool
	IsSuccessful           bool
	AllErrors              []string
	RecoveryStartTimestamp string
	RecoveryEndTimestamp   string
	ProcessingNodeHostname string
	ProcessingNodeToken    string
	Acknowledged           bool
	AcknowledgedAt         string
	AcknowledgedBy         string
	AcknowledgedComment    string
	LastDetectionID        int64
	RelatedRecoveryID      int64
	Type                   RecoveryType
	RecoveryType           PrimaryRecoveryType
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = util.PrettyUniqueToken()
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.RecoveryType = NotPrimaryRecovery
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
	ID          int64
	RecoveryUID string
	AuditAt     string
	Message     string
}

func NewTopologyRecoveryStep(uid string, message string) *TopologyRecoveryStep {
	return &TopologyRecoveryStep{
		RecoveryUID: uid,
		Message:     message,
	}
}

type PrimaryRecoveryType string

const (
	NotPrimaryRecovery          PrimaryRecoveryType = "NotPrimaryRecovery"
	PrimaryRecoveryGTID         PrimaryRecoveryType = "PrimaryRecoveryGTID"
	PrimaryRecoveryBinlogServer PrimaryRecoveryType = "PrimaryRecoveryBinlogServer"
	PrimaryRecoveryUnknown      PrimaryRecoveryType = "PrimaryRecoveryUnknown"
)

var emergencyReadTopologyInstanceMap *cache.Cache
var emergencyRestartReplicaTopologyInstanceMap *cache.Cache
var emergencyOperationGracefulPeriodMap *cache.Cache

func init() {
	go initializeTopologyRecoveryPostConfiguration()
}

func initializeTopologyRecoveryPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	emergencyReadTopologyInstanceMap = cache.New(time.Second, time.Millisecond*250)
	emergencyRestartReplicaTopologyInstanceMap = cache.New(time.Second*30, time.Second)
	emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
}

// AuditTopologyRecovery audits a single step in a topology recovery process.
func AuditTopologyRecovery(topologyRecovery *TopologyRecovery, message string) error {
	log.Infof("topology_recovery: %s", message)
	if topologyRecovery == nil {
		return nil
	}

	recoveryStep := NewTopologyRecoveryStep(topologyRecovery.UID, message)
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
func recoverPrimaryHasPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimaryHasPrimary.", analysisEntry.AnalyzedInstanceAlias))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix incorrect primaryship on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	// Reset replication on current primary.
	err = inst.ResetReplicationParameters(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// recoverDeadPrimary checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func recoverDeadPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	if !analysisEntry.ClusterDetails.HasAutomatedPrimaryRecovery {
		return false, nil, nil
	}

	// Read the tablet information from the database to find the shard and keyspace of the tablet
	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, nil, err
	}

	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry, true, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadPrimary.", analysisEntry.AnalyzedInstanceAlias))
		return false, nil, err
	}
	log.Infof("Analysis: %v, deadprimary %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, promotedReplica)
	}()

	ev, err := reparentutil.NewEmergencyReparenter(ts, tmclient.NewTabletManagerClient(), logutil.NewCallbackLogger(func(event *logutilpb.Event) {
		level := event.GetLevel()
		value := event.GetValue()
		// we only log the warnings and errors explicitly, everything gets logged as an information message anyways in auditing topology recovery
		switch level {
		case logutilpb.Level_WARNING:
			log.Warningf("ERS - %s", value)
		case logutilpb.Level_ERROR:
			log.Errorf("ERS - %s", value)
		default:
			log.Infof("ERS - %s", value)
		}
		_ = AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		tablet.Keyspace,
		tablet.Shard,
		reparentutil.EmergencyReparentOptions{
			IgnoreReplicas:            nil,
			WaitReplicasTimeout:       time.Duration(config.Config.WaitReplicasTimeoutSeconds) * time.Second,
			PreventCrossCellPromotion: config.Config.PreventCrossDataCenterPrimaryFailover,
		},
	)
	if err != nil {
		log.Errorf("Error running ERS - %v", err)
	}

	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(topoproto.TabletAliasString(ev.NewPrimary.Alias))
	}
	postErsCompletion(topologyRecovery, analysisEntry, promotedReplica)
	return true, topologyRecovery, err
}

func postErsCompletion(topologyRecovery *TopologyRecovery, analysisEntry *inst.ReplicationAnalysis, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.InstanceAlias)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		_ = inst.AuditOperation("recover-dead-primary", analysisEntry.AnalyzedInstanceAlias, message)
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadPrimary: successfully promoted %+v", promotedReplica.InstanceAlias))
	}
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverLockedSemiSyncPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return false, nil, nil
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
func emergentlyReadTopologyInstance(tabletAlias string, analysisCode inst.AnalysisCode) (instance *inst.Instance) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(tabletAlias, true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return nil
	}
	instance, _ = inst.ReadTopologyInstance(tabletAlias)
	_ = inst.AuditOperation("emergently-read-topology-instance", tabletAlias, string(analysisCode))
	return instance
}

// Force reading of replicas of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its replicas.
func emergentlyReadTopologyInstanceReplicas(primaryHost string, primaryPort int, analysisCode inst.AnalysisCode) {
	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryHost, primaryPort)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyReadTopologyInstance(replica.InstanceAlias, analysisCode)
	}
}

// emergentlyRestartReplicationOnTopologyInstance forces a RestartReplication on a given instance.
func emergentlyRestartReplicationOnTopologyInstance(tabletAlias string, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(tabletAlias, true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted on this specific replica
		return
	}
	go inst.ExecuteOnTopology(func() {
		_ = restartReplication(tabletAlias)
		_ = inst.AuditOperation("emergently-restart-replication-topology-instance", tabletAlias, string(analysisCode))
	})
}

func beginEmergencyOperationGracefulPeriod(tabletAlias string) {
	emergencyOperationGracefulPeriodMap.Set(tabletAlias, true, cache.DefaultExpiration)
}

func isInEmergencyOperationGracefulPeriod(tabletAlias string) bool {
	_, found := emergencyOperationGracefulPeriodMap.Get(tabletAlias)
	return found
}

// emergentlyRestartReplicationOnTopologyInstanceReplicas forces a stop slave + start slave on
// replicas of a given instance, in an attempt to cause them to re-evaluate their replication state.
// This can be useful in scenarios where the primary has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the primary is bad.
func emergentlyRestartReplicationOnTopologyInstanceReplicas(primaryHost string, primaryPort int, tabletAlias string, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(tabletAlias, true, cache.DefaultExpiration); existsInCacheError != nil {
		// While each replica's RestartReplication() is throttled on its own, it's also wasteful to
		// iterate all replicas all the time. This is the reason why we do grand-throttle check.
		return
	}
	beginEmergencyOperationGracefulPeriod(tabletAlias)

	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryHost, primaryPort)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyRestartReplicationOnTopologyInstance(replica.InstanceAlias, analysisCode)
	}
}

func emergentlyRecordStaleBinlogCoordinates(tabletAlias string, binlogCoordinates *inst.BinlogCoordinates) {
	err := inst.RecordStaleInstanceBinlogCoordinates(tabletAlias, binlogCoordinates)
	if err != nil {
		log.Error(err)
	}
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes.
func checkAndExecuteFailureDetectionProcesses(analysisEntry *inst.ReplicationAnalysis) (detectionRegistrationSuccess bool, processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(analysisEntry); !ok {
		if util.ClearToLog("checkAndExecuteFailureDetectionProcesses", analysisEntry.AnalyzedInstanceAlias) {
			log.Infof("checkAndExecuteFailureDetectionProcesses: could not register %+v detection on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
		}
		return false, false, nil
	}
	log.Infof("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	return true, false, nil
}

// getCheckAndRecoverFunctionCode gets the recovery function code to use for the given analysis.
func getCheckAndRecoverFunctionCode(analysisCode inst.AnalysisCode, tabletAlias string) recoveryFunction {
	switch analysisCode {
	// primary
	case inst.DeadPrimary, inst.DeadPrimaryAndSomeReplicas:
		// If ERS is disabled, we have no way of repairing the cluster.
		if !config.ERSEnabled() {
			return noRecoveryFunc
		}
		if isInEmergencyOperationGracefulPeriod(tabletAlias) {
			return recoverGenericProblemFunc
		}
		return recoverDeadPrimaryFunc
	case inst.PrimaryHasPrimary:
		return recoverPrimaryHasPrimaryFunc
	case inst.LockedSemiSyncPrimary:
		if isInEmergencyOperationGracefulPeriod(tabletAlias) {
			return recoverGenericProblemFunc
		}
		return recoverLockedSemiSyncPrimaryFunc
	case inst.ClusterHasNoPrimary:
		return electNewPrimaryFunc
	case inst.PrimaryIsReadOnly, inst.PrimarySemiSyncMustBeSet, inst.PrimarySemiSyncMustNotBeSet:
		return fixPrimaryFunc
	// replica
	case inst.NotConnectedToPrimary, inst.ConnectedToWrongPrimary, inst.ReplicationStopped, inst.ReplicaIsWritable,
		inst.ReplicaSemiSyncMustBeSet, inst.ReplicaSemiSyncMustNotBeSet:
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
	default:
		return false
	}
}

// getCheckAndRecoverFunction gets the recovery function for the given code.
func getCheckAndRecoverFunction(recoveryFunctionCode recoveryFunction) (
	checkAndRecoverFunction func(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
) {
	switch recoveryFunctionCode {
	case noRecoveryFunc:
		return nil
	case recoverGenericProblemFunc:
		return checkAndRecoverGenericProblem
	case recoverDeadPrimaryFunc:
		return recoverDeadPrimary
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
	default:
		return ""
	}
}

// isClusterWideRecovery returns whether the given recovery is a cluster-wide recovery or not
func isClusterWideRecovery(recoveryFunctionCode recoveryFunction) bool {
	switch recoveryFunctionCode {
	case recoverDeadPrimaryFunc, electNewPrimaryFunc:
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

func runEmergentOperations(analysisEntry *inst.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case inst.DeadPrimaryAndReplicas:
		go emergentlyReadTopologyInstance(analysisEntry.AnalyzedInstancePrimaryAlias, analysisEntry.Analysis)
	case inst.UnreachablePrimary:
		go emergentlyReadTopologyInstance(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(analysisEntry.AnalyzedInstanceHostname, analysisEntry.AnalyzedInstancePort, analysisEntry.Analysis)
	case inst.UnreachablePrimaryWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(analysisEntry.AnalyzedInstanceHostname, analysisEntry.AnalyzedInstancePort, analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis)
	case inst.LockedSemiSyncPrimaryHypothesis:
		go emergentlyReadTopologyInstance(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis)
		go emergentlyRecordStaleBinlogCoordinates(analysisEntry.AnalyzedInstanceAlias, &analysisEntry.AnalyzedInstanceBinlogCoordinates)
	case inst.AllPrimaryReplicasNotReplicating:
		go emergentlyReadTopologyInstance(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis)
	case inst.AllPrimaryReplicasNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis)
	}
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func executeCheckAndRecoverFunction(analysisEntry *inst.ReplicationAnalysis) (err error) {
	countPendingRecoveries.Add(1)
	defer countPendingRecoveries.Add(-1)

	checkAndRecoverFunctionCode := getCheckAndRecoverFunctionCode(analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	isActionableRecovery := hasActionableRecovery(checkAndRecoverFunctionCode)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(analysisEntry)

	if checkAndRecoverFunctionCode == noRecoveryFunc {
		// Unhandled problem type
		if analysisEntry.Analysis != inst.NoProblem {
			if util.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceAlias) {
				log.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: %+v; tablet: %+v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
			}
		}

		return nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceAlias) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias, isActionableRecovery)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.

	// Initiate detection:
	_, _, err = checkAndExecuteFailureDetectionProcesses(analysisEntry)
	if err != nil {
		log.Errorf("executeCheckAndRecoverFunction: error on failure detection: %+v", err)
		return err
	}
	// We don't mind whether detection really executed the processes or not
	// (it may have been silenced due to previous detection). We only care there's no error.

	// We're about to embark on recovery shortly...

	// Check for recovery being disabled globally
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this
		log.Errorf("Unable to determine if recovery is disabled globally: %v", err)
	} else if recoveryDisabledGlobally {
		log.Infof("CheckAndRecover: Analysis: %+v, Tablet: %+v: NOT Recovering host (disabled globally)",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)

		return err
	}

	// We lock the shard here and then refresh the tablets information
	ctx, unlock, err := LockShard(context.Background(), analysisEntry.AnalyzedInstanceAlias, getLockAction(analysisEntry.AnalyzedInstanceAlias, analysisEntry.Analysis))
	if err != nil {
		return err
	}
	defer unlock(&err)

	// Check if the recovery is already fixed or not. We need this because vtorc works on ephemeral data to find the failure scenarios.
	// That data might be old, because of a cluster operation that was run through vtctld or some other vtorc. So before we do any
	// changes, we should be checking that this failure is indeed needed to be fixed. We do this after locking the shard to be sure
	// that the data that we use now is up-to-date.
	if isActionableRecovery {
		log.Errorf("executeCheckAndRecoverFunction: Proceeding with %v recovery on %v validation after acquiring shard lock.", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
		// The first step we have to do is refresh the keyspace information
		// This is required to know if the durability policies have changed or not
		// If they have, then recoveries like ReplicaSemiSyncMustNotBeSet, etc won't be valid anymore
		err := RefreshKeyspace(analysisEntry.AnalyzedKeyspace)
		if err != nil {
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
			forceRefreshAllTabletsInShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, tabletsToIgnore)
		} else {
			// If we are not running a cluster-wide recovery, then it is only concerned with the specific tablet
			// on which the failure occurred and the primary instance of the shard.
			// For example, ConnectedToWrongPrimary analysis only cares for whom the current primary tablet is
			// and the host-port set on the tablet in question.
			// So, we only need to refresh the tablet info records (to know if the primary tablet has changed),
			// and the replication data of the new primary and this tablet.
			refreshTabletInfoOfShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			DiscoverInstance(analysisEntry.AnalyzedInstanceAlias, true)
			primaryTablet, err := shardPrimary(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			if err != nil {
				log.Errorf("executeCheckAndRecoverFunction: Analysis: %+v, Tablet: %+v: error while finding the shard primary: %v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias, err)
				return err
			}
			primaryTabletAlias := topoproto.TabletAliasString(primaryTablet.Alias)
			// We can skip the refresh if we know the tablet we are looking at is the primary tablet.
			// This would be the case for PrimaryHasPrimary recovery. We don't need to refresh the same tablet twice.
			if analysisEntry.AnalyzedInstanceAlias != primaryTabletAlias {
				DiscoverInstance(primaryTabletAlias, true)
			}
		}
		alreadyFixed, err := checkIfAlreadyFixed(analysisEntry)
		if err != nil {
			log.Errorf("executeCheckAndRecoverFunction: Analysis: %+v, Tablet: %+v: error while trying to find if the problem is already fixed: %v",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias, err)
			return err
		}
		if alreadyFixed {
			log.Infof("Analysis: %v on tablet %v - No longer valid, some other agent must have fixed the problem.", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
			return nil
		}
	}

	// Actually attempt recovery:
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceAlias) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias, isActionableRecovery)
	}
	recoveryAttempted, topologyRecovery, err := getCheckAndRecoverFunction(checkAndRecoverFunctionCode)(ctx, analysisEntry)
	if !recoveryAttempted {
		return err
	}
	recoveryName := getRecoverFunctionName(checkAndRecoverFunctionCode)
	recoveriesCounter.Add(recoveryName, 1)
	if err != nil {
		recoveriesFailureCounter.Add(recoveryName, 1)
	} else {
		recoveriesSuccessfulCounter.Add(recoveryName, 1)
	}
	if topologyRecovery == nil {
		return err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", topologyRecovery)
	}
	// If we ran a cluster wide recovery and actually attempted it, then we know that the replication state for all the tablets in this cluster
	// would have changed. So we can go ahead and pre-emptively refresh them.
	// For this refresh we don't use the same context that we used for the recovery, since that context might have expired or could expire soon
	// Instead we pass the background context. The call forceRefreshAllTabletsInShard handles adding a timeout to it for us.
	if isClusterWideRecovery(checkAndRecoverFunctionCode) {
		forceRefreshAllTabletsInShard(context.Background(), analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, nil)
	} else {
		// For all other recoveries, we would have changed the replication status of the analyzed tablet
		// so it doesn't hurt to re-read the information of this tablet, otherwise we'll requeue the same recovery
		// that we just completed because we would be using stale data.
		DiscoverInstance(analysisEntry.AnalyzedInstanceAlias, true)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
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
	// intentionally iterating entries in random order
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]

		go func() {
			err = executeCheckAndRecoverFunction(analysisEntry)
			if err != nil {
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
func electNewPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry, false /*failIfFailedInstanceInActiveRecovery*/, true /*failIfClusterInActiveRecovery*/)
	if topologyRecovery == nil || err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another electNewPrimary.", analysisEntry.AnalyzedInstanceAlias))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will elect a new primary for %v:%v", analysisEntry.Analysis, analysisEntry.ClusterDetails.Keyspace, analysisEntry.ClusterDetails.Shard)

	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, promotedReplica)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	_ = AuditTopologyRecovery(topologyRecovery, "starting PlannedReparentShard for electing new primary.")

	ev, err := reparentutil.NewPlannedReparenter(ts, tmclient.NewTabletManagerClient(), logutil.NewCallbackLogger(func(event *logutilpb.Event) {
		level := event.GetLevel()
		value := event.GetValue()
		// we only log the warnings and errors explicitly, everything gets logged as an information message anyways in auditing topology recovery
		switch level {
		case logutilpb.Level_WARNING:
			log.Warningf("PRS - %s", value)
		case logutilpb.Level_ERROR:
			log.Errorf("PRS - %s", value)
		}
		_ = AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		analyzedTablet.Keyspace,
		analyzedTablet.Shard,
		reparentutil.PlannedReparentOptions{
			WaitReplicasTimeout: time.Duration(config.Config.WaitReplicasTimeoutSeconds) * time.Second,
		},
	)

	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(topoproto.TabletAliasString(ev.NewPrimary.Alias))
	}
	postPrsCompletion(topologyRecovery, analysisEntry, promotedReplica)
	return true, topologyRecovery, err
}

// fixPrimary sets the primary as read-write.
func fixPrimary(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimary.", analysisEntry.AnalyzedInstanceAlias))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix primary to read-write %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet.Keyspace)
	if err != nil {
		log.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	if err := tabletUndoDemotePrimary(ctx, analyzedTablet, reparentutil.SemiSyncAckers(durabilityPolicy, analyzedTablet) > 0); err != nil {
		return true, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// fixReplica sets the replica as read-only and points it at the current primary.
func fixReplica(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixReplica.", analysisEntry.AnalyzedInstanceAlias))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix replica %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceAlias)
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
		log.Info("Could not compute primary for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet.Keyspace)
	if err != nil {
		log.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	err = setReadOnly(ctx, analyzedTablet)
	if err != nil {
		log.Info("Could not set the tablet %v to readonly - %v", analysisEntry.AnalyzedInstanceAlias, err)
		return true, topologyRecovery, err
	}

	err = setReplicationSource(ctx, analyzedTablet, primaryTablet, reparentutil.IsReplicaSemiSync(durabilityPolicy, primaryTablet, analyzedTablet))
	return true, topologyRecovery, err
}
