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

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtorc/attributes"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/util"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type RecoveryType string

const (
	PrimaryRecovery             RecoveryType = "PrimaryRecovery"
	CoPrimaryRecovery           RecoveryType = "CoPrimaryRecovery"
	IntermediatePrimaryRecovery RecoveryType = "IntermediatePrimaryRecovery"

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

type RecoveryAcknowledgement struct {
	CreatedAt time.Time
	Owner     string
	Comment   string

	Key           inst.InstanceKey
	ClusterName   string
	ID            int64
	UID           string
	AllRecoveries bool
}

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryID   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	ID                        int64
	UID                       string
	AnalysisEntry             inst.ReplicationAnalysis
	SuccessorKey              *inst.InstanceKey
	SuccessorAlias            string
	IsActive                  bool
	IsSuccessful              bool
	LostReplicas              inst.InstanceKeyMap
	ParticipatingInstanceKeys inst.InstanceKeyMap
	AllErrors                 []string
	RecoveryStartTimestamp    string
	RecoveryEndTimestamp      string
	ProcessingNodeHostname    string
	ProcessingNodeToken       string
	Acknowledged              bool
	AcknowledgedAt            string
	AcknowledgedBy            string
	AcknowledgedComment       string
	LastDetectionID           int64
	RelatedRecoveryID         int64
	Type                      RecoveryType
	RecoveryType              PrimaryRecoveryType
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = util.PrettyUniqueToken()
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostReplicas = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
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
		topologyRecovery.SuccessorKey = &successorInstance.Key
		topologyRecovery.SuccessorAlias = successorInstance.InstanceAlias
		topologyRecovery.IsSuccessful = true
	}
	return writeResolveRecovery(topologyRecovery)
}

// recoverPrimaryHasPrimary resets the replication on the primary instance
func recoverPrimaryHasPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimaryHasPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix incorrect primaryship %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	// Reset replication on current primary.
	err = inst.ResetReplicationParameters(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// recoverDeadPrimary checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func recoverDeadPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedPrimaryRecovery) {
		return false, nil, nil
	}

	// Read the tablet information from the database to find the shard and keyspace of the tablet
	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)

	var candidateTabletAlias *topodatapb.TabletAlias
	if candidateInstanceKey != nil {
		candidateTablet, err := inst.ReadTablet(*candidateInstanceKey)
		if err != nil {
			return false, nil, err
		}
		candidateTabletAlias = candidateTablet.Alias
	}
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, deadprimary %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
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
		}
		_ = AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		tablet.Keyspace,
		tablet.Shard,
		reparentutil.EmergencyReparentOptions{
			NewPrimaryAlias:           candidateTabletAlias,
			IgnoreReplicas:            nil,
			WaitReplicasTimeout:       time.Duration(config.Config.WaitReplicasTimeoutSeconds) * time.Second,
			PreventCrossCellPromotion: config.Config.PreventCrossDataCenterPrimaryFailover,
		},
	)

	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(&inst.InstanceKey{
			Hostname: ev.NewPrimary.MysqlHostname,
			Port:     int(ev.NewPrimary.MysqlPort),
		})
	}
	postErsCompletion(topologyRecovery, analysisEntry, skipProcesses, promotedReplica)
	return true, topologyRecovery, err
}

func postErsCompletion(topologyRecovery *TopologyRecovery, analysisEntry inst.ReplicationAnalysis, skipProcesses bool, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		_ = inst.AuditOperation("recover-dead-primary", &analysisEntry.AnalyzedInstanceKey, message)
	}
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadPrimary: successfully promoted %+v", promotedReplica.Key))

		_ = attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())
	}
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverLockedSemiSyncPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return false, nil, nil
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) (instance *inst.Instance) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return nil
	}
	instance, _ = inst.ReadTopologyInstance(instanceKey)
	_ = inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	return instance
}

// Force reading of replicas of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its replicas.
func emergentlyReadTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyReadTopologyInstance(&replica.Key, analysisCode)
	}
}

// emergentlyRestartReplicationOnTopologyInstance forces a RestartReplication on a given instance.
func emergentlyRestartReplicationOnTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted on this specific replica
		return
	}
	go inst.ExecuteOnTopology(func() {
		_ = restartReplication(instanceKey)
		_ = inst.AuditOperation("emergently-restart-replication-topology-instance", instanceKey, string(analysisCode))
	})
}

func beginEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) {
	emergencyOperationGracefulPeriodMap.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
}

func isInEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) bool {
	_, found := emergencyOperationGracefulPeriodMap.Get(instanceKey.StringCode())
	return found
}

// emergentlyRestartReplicationOnTopologyInstanceReplicas forces a stop slave + start slave on
// replicas of a given instance, in an attempt to cause them to re-evaluate their replication state.
// This can be useful in scenarios where the primary has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the primary is bad.
func emergentlyRestartReplicationOnTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// While each replica's RestartReplication() is throttled on its own, it's also wasteful to
		// iterate all replicas all the time. This is the reason why we do grand-throttle check.
		return
	}
	beginEmergencyOperationGracefulPeriod(instanceKey)

	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		replicaKey := &replica.Key
		go emergentlyRestartReplicationOnTopologyInstance(replicaKey, analysisCode)
	}
}

func emergentlyRecordStaleBinlogCoordinates(instanceKey *inst.InstanceKey, binlogCoordinates *inst.BinlogCoordinates) {
	err := inst.RecordStaleInstanceBinlogCoordinates(instanceKey, binlogCoordinates)
	if err != nil {
		log.Error(err)
	}
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes.
func checkAndExecuteFailureDetectionProcesses(analysisEntry inst.ReplicationAnalysis, skipProcesses bool) (detectionRegistrationSuccess bool, processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		if util.ClearToLog("checkAndExecuteFailureDetectionProcesses", analysisEntry.AnalyzedInstanceKey.StringCode()) {
			log.Infof("checkAndExecuteFailureDetectionProcesses: could not register %+v detection on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
		}
		return false, false, nil
	}
	log.Infof("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	return true, false, nil
}

// getCheckAndRecoverFunctionCode gets the recovery function code to use for the given analysis.
func getCheckAndRecoverFunctionCode(analysisCode inst.AnalysisCode, analyzedInstanceKey *inst.InstanceKey) recoveryFunction {
	switch analysisCode {
	// primary
	case inst.DeadPrimary, inst.DeadPrimaryAndSomeReplicas:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return recoverGenericProblemFunc
		}
		return recoverDeadPrimaryFunc
	case inst.PrimaryHasPrimary:
		return recoverPrimaryHasPrimaryFunc
	case inst.LockedSemiSyncPrimary:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
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
	checkAndRecoverFunction func(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
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
func analysisEntriesHaveSameRecovery(prevAnalysis, newAnalysis inst.ReplicationAnalysis) bool {
	prevRecoveryFunctionCode := getCheckAndRecoverFunctionCode(prevAnalysis.Analysis, &prevAnalysis.AnalyzedInstanceKey)
	newRecoveryFunctionCode := getCheckAndRecoverFunctionCode(newAnalysis.Analysis, &newAnalysis.AnalyzedInstanceKey)
	return prevRecoveryFunctionCode == newRecoveryFunctionCode
}

func runEmergentOperations(analysisEntry *inst.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case inst.DeadPrimaryAndReplicas:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstancePrimaryKey, analysisEntry.Analysis)
	case inst.UnreachablePrimary:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.UnreachablePrimaryWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.LockedSemiSyncPrimaryHypothesis:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyRecordStaleBinlogCoordinates(&analysisEntry.AnalyzedInstanceKey, &analysisEntry.AnalyzedInstanceBinlogCoordinates)
	case inst.AllPrimaryReplicasNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllPrimaryReplicasNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	}
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	countPendingRecoveries.Add(1)
	defer countPendingRecoveries.Add(-1)

	checkAndRecoverFunctionCode := getCheckAndRecoverFunctionCode(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
	isActionableRecovery := hasActionableRecovery(checkAndRecoverFunctionCode)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(&analysisEntry)

	if checkAndRecoverFunctionCode == noRecoveryFunc {
		// Unhandled problem type
		if analysisEntry.Analysis != inst.NoProblem {
			if util.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceKey.StringCode()) {
				log.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: %+v; key: %+v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
			}
		}

		return false, nil, nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.

	// Initiate detection:
	_, _, err = checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses)
	if err != nil {
		log.Errorf("executeCheckAndRecoverFunction: error on failure detection: %+v", err)
		return false, nil, err
	}
	// We don't mind whether detection really executed the processes or not
	// (it may have been silenced due to previous detection). We only care there's no error.

	// We're about to embark on recovery shortly...

	// Check for recovery being disabled globally
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this
		log.Errorf("Unable to determine if recovery is disabled globally: %v", err)
	} else if recoveryDisabledGlobally {
		if !forceInstanceRecovery {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT Recovering host (disabled globally)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)

			return false, nil, err
		}
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: recoveries disabled globally but forcing this recovery",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
	}

	// We lock the shard here and then refresh the tablets information
	ctx, unlock, err := LockShard(context.Background(), analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, nil, err
	}
	defer unlock(&err)

	// Check if the recovery is already fixed or not. We need this because vtorc works on ephemeral data to find the failure scenarios.
	// That data might be old, because of a cluster operation that was run through vtctld or some other vtorc. So before we do any
	// changes, we should be checking that this failure is indeed needed to be fixed. We do this after locking the shard to be sure
	// that the data that we use now is up-to-date.
	if isActionableRecovery {
		// The first step we have to do is refresh the keyspace information
		// This is required to know if the durability policies have changed or not
		// If they have, then recoveries like ReplicaSemiSyncMustNotBeSet, etc won't be valid anymore
		err := RefreshKeyspace(analysisEntry.AnalyzedKeyspace)
		if err != nil {
			return false, nil, err
		}
		// If we are about to run a cluster-wide recovery, it is imperative to first refresh all the tablets
		// of a shard because a new tablet could have been promoted, and we need to have this visibility before we
		// run a cluster operation of our own.
		if isClusterWideRecovery(checkAndRecoverFunctionCode) {
			forceRefreshAllTabletsInShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
		} else {
			// If we are not running a cluster-wide recovery, then it is only concerned with the specific tablet
			// on which the failure occurred and the primary instance of the shard.
			// For example, ConnectedToWrongPrimary analysis only cares for whom the current primary tablet is
			// and the host-port set on the tablet in question.
			// So, we only need to refresh the tablet info records (to know if the primary tablet has changed),
			// and the replication data of the new primary and this tablet.
			refreshTabletInfoOfShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			DiscoverInstance(analysisEntry.AnalyzedInstanceKey, true)
			primaryTablet, err := shardPrimary(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
			if err != nil {
				log.Errorf("executeCheckAndRecoverFunction: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+"skipProcesses: %v: error while finding the shard primary: %v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
				return false, nil, err
			}
			primaryInstanceKey := inst.InstanceKey{
				Hostname: primaryTablet.MysqlHostname,
				Port:     int(primaryTablet.MysqlPort),
			}
			// We can skip the refresh if we know the tablet we are looking at is the primary tablet.
			// This would be the case for PrimaryHasPrimary recovery. We don't need to refresh the same tablet twice.
			if !analysisEntry.AnalyzedInstanceKey.Equals(&primaryInstanceKey) {
				DiscoverInstance(primaryInstanceKey, true)
			}
		}
		alreadyFixed, err := checkIfAlreadyFixed(analysisEntry)
		if err != nil {
			log.Errorf("executeCheckAndRecoverFunction: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+"skipProcesses: %v: error while trying to find if the problem is already fixed: %v",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
			return false, nil, err
		}
		if alreadyFixed {
			log.Infof("Analysis: %v - No longer valid, some other agent must have fixed the problem.", analysisEntry.Analysis)
			return false, nil, nil
		}
	}

	// Actually attempt recovery:
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}
	recoveryAttempted, topologyRecovery, err = getCheckAndRecoverFunction(checkAndRecoverFunctionCode)(ctx, analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	recoveryName := getRecoverFunctionName(checkAndRecoverFunctionCode)
	recoveriesCounter.Add(recoveryName, 1)
	if err != nil {
		recoveriesFailureCounter.Add(recoveryName, 1)
	} else {
		recoveriesSuccessfulCounter.Add(recoveryName, 1)
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", topologyRecovery)
	}
	// If we ran a cluster wide recovery and actually attemped it, then we know that the replication state for all the tablets in this cluster
	// would have changed. So we can go ahead and pre-emptively refresh them.
	// For this refresh we don't use the same context that we used for the recovery, since that context might have expired or could expire soon
	// Instead we pass the background context. The call forceRefreshAllTabletsInShard handles adding a timeout to it for us.
	if isClusterWideRecovery(checkAndRecoverFunctionCode) {
		forceRefreshAllTabletsInShard(context.Background(), analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	} else {
		// For all other recoveries, we would have changed the replication status of the analyzed tablet
		// so it doesn't hurt to re-read the information of this tablet, otherwise we'll requeue the same recovery
		// that we just completed because we would be using stale data.
		DiscoverInstance(analysisEntry.AnalyzedInstanceKey, true)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
	}
	return recoveryAttempted, topologyRecovery, err
}

// checkIfAlreadyFixed checks whether the problem that the analysis entry represents has already been fixed by another agent or not
func checkIfAlreadyFixed(analysisEntry inst.ReplicationAnalysis) (bool, error) {
	// Run a replication analysis again. We will check if the problem persisted
	analysisEntries, err := inst.GetReplicationAnalysis(analysisEntry.ClusterDetails.ClusterName, &inst.ReplicationAnalysisHints{})
	if err != nil {
		return false, err
	}

	for _, entry := range analysisEntries {
		// If there is a analysis which has the same recovery required, then we should proceed with the recovery
		if entry.AnalyzedInstanceKey.Equals(&analysisEntry.AnalyzedInstanceKey) && analysisEntriesHaveSameRecovery(analysisEntry, entry) {
			return false, nil
		}
	}

	// We didn't find a replication analysis matching the original failure, which means that some other agent probably fixed it.
	return true, nil
}

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *inst.InstanceKey, err error) {
	// Allow the analysis to run even if we don't want to recover
	replicationAnalysis, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{IncludeDowntimed: true, AuditAnalysis: true})
	if err != nil {
		log.Error(err)
		return false, nil, err
	}
	// intentionally iterating entries in random order
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]
		if specificInstance != nil {
			// We are looking for a specific instance; if this is not the one, skip!
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.SkippableDueToDowntime && specificInstance == nil {
			// Only recover a downtimed server if explicitly requested
			continue
		}

		if specificInstance != nil {
			// force mode. Keep it synchronuous
			var topologyRecovery *TopologyRecovery
			recoveryAttempted, topologyRecovery, err = executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
			if err != nil {
				log.Error(err)
			}
			if topologyRecovery != nil {
				promotedReplicaKey = topologyRecovery.SuccessorKey
			}
		} else {
			go func() {
				_, _, err := executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
				if err != nil {
					log.Error(err)
				}
			}()
		}
	}
	return recoveryAttempted, promotedReplicaKey, err
}

func postPrsCompletion(topologyRecovery *TopologyRecovery, analysisEntry inst.ReplicationAnalysis, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		_ = inst.AuditOperation(string(analysisEntry.Analysis), &analysisEntry.AnalyzedInstanceKey, message)
	}
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%+v: successfully promoted %+v", analysisEntry.Analysis, promotedReplica.Key))
		_ = attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())
	}
}

// electNewPrimary elects a new primary while none were present before.
func electNewPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false /*failIfFailedInstanceInActiveRecovery*/, true /*failIfClusterInActiveRecovery*/)
	if topologyRecovery == nil || err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another electNewPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will elect a new primary: %v", analysisEntry.Analysis, analysisEntry.ClusterDetails.ClusterName)

	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, promotedReplica)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)
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
		promotedReplica, _, _ = inst.ReadInstance(&inst.InstanceKey{
			Hostname: ev.NewPrimary.MysqlHostname,
			Port:     int(ev.NewPrimary.MysqlPort),
		})
	}
	postPrsCompletion(topologyRecovery, analysisEntry, promotedReplica)
	return true, topologyRecovery, err
}

// fixPrimary sets the primary as read-write.
func fixPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix primary to read-write %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet)
	if err != nil {
		log.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	if err := tabletUndoDemotePrimary(ctx, analyzedTablet, inst.SemiSyncAckers(durabilityPolicy, analyzedTablet) > 0); err != nil {
		return true, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// fixReplica sets the replica as read-only and points it at the current primary.
func fixReplica(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixReplica.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix replica %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		_ = resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}

	primaryTablet, err := shardPrimary(analyzedTablet.Keyspace, analyzedTablet.Shard)
	if err != nil {
		log.Info("Could not compute primary for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(analyzedTablet)
	if err != nil {
		log.Info("Could not read the durability policy for %v/%v", analyzedTablet.Keyspace, analyzedTablet.Shard)
		return false, topologyRecovery, err
	}

	err = setReadOnly(ctx, analyzedTablet)
	if err != nil {
		log.Info("Could not set the tablet %v to readonly - %v", topoproto.TabletAliasString(analyzedTablet.Alias), err)
		return true, topologyRecovery, err
	}

	err = setReplicationSource(ctx, analyzedTablet, primaryTablet, inst.IsReplicaSemiSync(durabilityPolicy, primaryTablet, analyzedTablet))
	return true, topologyRecovery, err
}
