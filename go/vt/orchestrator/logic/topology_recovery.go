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
	goos "os"
	"strings"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/orchestrator/attributes"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	ometrics "vitess.io/vitess/go/vt/orchestrator/metrics"
	"vitess.io/vitess/go/vt/orchestrator/os"
	"vitess.io/vitess/go/vt/orchestrator/process"
	"vitess.io/vitess/go/vt/orchestrator/util"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var countPendingRecoveries int64

type RecoveryType string

const (
	PrimaryRecovery             RecoveryType = "PrimaryRecovery"
	CoPrimaryRecovery           RecoveryType = "CoPrimaryRecovery"
	IntermediatePrimaryRecovery RecoveryType = "IntermediatePrimaryRecovery"
)

// recoveryFunction is the code of the recovery function to be used
// this is returned from getCheckAndRecoverFunction to compare the functions returned
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

func NewRecoveryAcknowledgement(owner string, comment string) *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     owner,
		Comment:   comment,
	}
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
		topologyRecovery.AddError(err)
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

// InstancesByCountReplicas sorts instances by umber of replicas, descending
type InstancesByCountReplicas [](*inst.Instance)

func (instancesByCountReplicas InstancesByCountReplicas) Len() int {
	return len(instancesByCountReplicas)
}
func (instancesByCountReplicas InstancesByCountReplicas) Swap(i, j int) {
	instancesByCountReplicas[i], instancesByCountReplicas[j] = instancesByCountReplicas[j], instancesByCountReplicas[i]
}
func (instancesByCountReplicas InstancesByCountReplicas) Less(i, j int) bool {
	if len(instancesByCountReplicas[i].Replicas) == len(instancesByCountReplicas[j].Replicas) {
		// Secondary sorting: prefer more advanced replicas
		return !instancesByCountReplicas[i].ExecBinlogCoordinates.SmallerThan(&instancesByCountReplicas[j].ExecBinlogCoordinates)
	}
	return len(instancesByCountReplicas[i].Replicas) < len(instancesByCountReplicas[j].Replicas)
}

var recoverDeadIntermediatePrimaryCounter = metrics.NewCounter()
var recoverDeadIntermediatePrimarySuccessCounter = metrics.NewCounter()
var recoverDeadIntermediatePrimaryFailureCounter = metrics.NewCounter()
var recoverDeadCoPrimaryCounter = metrics.NewCounter()
var recoverDeadCoPrimarySuccessCounter = metrics.NewCounter()
var recoverDeadCoPrimaryFailureCounter = metrics.NewCounter()
var countPendingRecoveriesGauge = metrics.NewGauge()

func init() {
	metrics.Register("recover.dead_intermediate_primary.start", recoverDeadIntermediatePrimaryCounter)
	metrics.Register("recover.dead_intermediate_primary.success", recoverDeadIntermediatePrimarySuccessCounter)
	metrics.Register("recover.dead_intermediate_primary.fail", recoverDeadIntermediatePrimaryFailureCounter)
	metrics.Register("recover.dead_co_primary.start", recoverDeadCoPrimaryCounter)
	metrics.Register("recover.dead_co_primary.success", recoverDeadCoPrimarySuccessCounter)
	metrics.Register("recover.dead_co_primary.fail", recoverDeadCoPrimaryFailureCounter)
	metrics.Register("recover.pending", countPendingRecoveriesGauge)

	go initializeTopologyRecoveryPostConfiguration()

	ometrics.OnMetricsTick(func() {
		countPendingRecoveriesGauge.Update(getCountPendingRecoveries())
	})
}

func getCountPendingRecoveries() int64 {
	return atomic.LoadInt64(&countPendingRecoveries)
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

// prepareCommand replaces agreed-upon placeholders with analysis data
func prepareCommand(command string, topologyRecovery *TopologyRecovery) (result string, async bool) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	command = strings.TrimSpace(command)
	if strings.HasSuffix(command, "&") {
		command = strings.TrimRight(command, "&")
		async = true
	}
	command = strings.Replace(command, "{failureType}", string(analysisEntry.Analysis), -1)
	command = strings.Replace(command, "{instanceType}", string(analysisEntry.GetAnalysisInstanceType()), -1)
	command = strings.Replace(command, "{isPrimary}", fmt.Sprintf("%t", analysisEntry.IsPrimary), -1)
	command = strings.Replace(command, "{isCoPrimary}", fmt.Sprintf("%t", analysisEntry.IsCoPrimary), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{command}", analysisEntry.CommandHint, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{failureClusterDomain}", analysisEntry.ClusterDetails.ClusterDomain, -1)
	command = strings.Replace(command, "{countReplicas}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoPrimaryRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedPrimaryRecovery), -1)
	command = strings.Replace(command, "{autoIntermediatePrimaryRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediatePrimaryRecovery), -1)
	command = strings.Replace(command, "{orchestratorHost}", process.ThisHostname, -1)
	command = strings.Replace(command, "{recoveryUID}", topologyRecovery.UID, -1)

	command = strings.Replace(command, "{isSuccessful}", fmt.Sprint(topologyRecovery.SuccessorKey != nil), -1)
	if topologyRecovery.SuccessorKey != nil {
		command = strings.Replace(command, "{successorHost}", topologyRecovery.SuccessorKey.Hostname, -1)
		command = strings.Replace(command, "{successorPort}", fmt.Sprintf("%d", topologyRecovery.SuccessorKey.Port), -1)
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		command = strings.Replace(command, "{successorAlias}", topologyRecovery.SuccessorAlias, -1)
	}

	command = strings.Replace(command, "{lostReplicas}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{countLostReplicas}", fmt.Sprintf("%d", len(topologyRecovery.LostReplicas)), -1)
	command = strings.Replace(command, "{replicaHosts}", analysisEntry.Replicas.ToCommaDelimitedList(), -1)

	return command, async
}

// applyEnvironmentVariables sets the relevant environment variables for a recovery
func applyEnvironmentVariables(topologyRecovery *TopologyRecovery) []string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	env := goos.Environ()
	env = append(env, fmt.Sprintf("ORC_FAILURE_TYPE=%s", string(analysisEntry.Analysis)))
	env = append(env, fmt.Sprintf("ORC_INSTANCE_TYPE=%s", string(analysisEntry.GetAnalysisInstanceType())))
	env = append(env, fmt.Sprintf("ORC_IS_PRIMARY=%t", analysisEntry.IsPrimary))
	env = append(env, fmt.Sprintf("ORC_IS_CO_PRIMARY=%t", analysisEntry.IsCoPrimary))
	env = append(env, fmt.Sprintf("ORC_FAILURE_DESCRIPTION=%s", analysisEntry.Description))
	env = append(env, fmt.Sprintf("ORC_COMMAND=%s", analysisEntry.CommandHint))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", analysisEntry.AnalyzedInstanceKey.Hostname))
	env = append(env, fmt.Sprintf("ORC_FAILED_PORT=%d", analysisEntry.AnalyzedInstanceKey.Port))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER=%s", analysisEntry.ClusterDetails.ClusterName))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_ALIAS=%s", analysisEntry.ClusterDetails.ClusterAlias))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_DOMAIN=%s", analysisEntry.ClusterDetails.ClusterDomain))
	env = append(env, fmt.Sprintf("ORC_COUNT_REPLICAS=%d", analysisEntry.CountReplicas))
	env = append(env, fmt.Sprintf("ORC_IS_DOWNTIMED=%v", analysisEntry.IsDowntimed))
	env = append(env, fmt.Sprintf("ORC_AUTO_PRIMARY_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedPrimaryRecovery))
	env = append(env, fmt.Sprintf("ORC_AUTO_INTERMEDIATE_PRIMARY_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedIntermediatePrimaryRecovery))
	env = append(env, fmt.Sprintf("ORC_ORCHESTRATOR_HOST=%s", process.ThisHostname))
	env = append(env, fmt.Sprintf("ORC_IS_SUCCESSFUL=%v", (topologyRecovery.SuccessorKey != nil)))
	env = append(env, fmt.Sprintf("ORC_LOST_REPLICAS=%s", topologyRecovery.LostReplicas.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_REPLICA_HOSTS=%s", analysisEntry.Replicas.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_RECOVERY_UID=%s", topologyRecovery.UID))

	if topologyRecovery.SuccessorKey != nil {
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_HOST=%s", topologyRecovery.SuccessorKey.Hostname))
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_PORT=%d", topologyRecovery.SuccessorKey.Port))
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_ALIAS=%s", topologyRecovery.SuccessorAlias))
	}

	return env
}

func executeProcess(command string, env []string, topologyRecovery *TopologyRecovery, fullDescription string) (err error) {
	// Log the command to be run and record how long it takes as this may be useful
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %s: %s", fullDescription, command))
	start := time.Now()
	var info string
	if err = os.CommandRun(command, env); err == nil {
		info = fmt.Sprintf("Completed %s in %v", fullDescription, time.Since(start))
	} else {
		info = fmt.Sprintf("Execution of %s failed in %v with error: %v", fullDescription, time.Since(start), err)
		log.Errorf(info)
	}
	AuditTopologyRecovery(topologyRecovery, info)
	return err
}

// executeProcesses executes a list of processes
func executeProcesses(processes []string, description string, topologyRecovery *TopologyRecovery, failOnError bool) (err error) {
	if len(processes) == 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("No %s hooks to run", description))
		return nil
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %d %s hooks", len(processes), description))
	for i, command := range processes {
		command, async := prepareCommand(command, topologyRecovery)
		env := applyEnvironmentVariables(topologyRecovery)

		fullDescription := fmt.Sprintf("%s hook %d of %d", description, i+1, len(processes))
		if async {
			fullDescription = fmt.Sprintf("%s (async)", fullDescription)
		}
		if async {
			// Ignore errors
			go executeProcess(command, env, topologyRecovery, fullDescription)
		} else {
			if cmdErr := executeProcess(command, env, topologyRecovery, fullDescription); cmdErr != nil {
				if failOnError {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Not running further %s hooks", description))
					return cmdErr
				}
				if err == nil {
					// Keep first error encountered
					err = cmdErr
				}
			}
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("done running %s hooks", description))
	return err
}

func PrimaryFailoverGeographicConstraintSatisfied(analysisEntry *inst.ReplicationAnalysis, suggestedInstance *inst.Instance) (satisfied bool, dissatisfiedReason string) {
	if config.Config.PreventCrossDataCenterPrimaryFailover {
		if suggestedInstance.DataCenter != analysisEntry.AnalyzedInstanceDataCenter {
			return false, fmt.Sprintf("PreventCrossDataCenterPrimaryFailover: will not promote server in %s when failed server in %s", suggestedInstance.DataCenter, analysisEntry.AnalyzedInstanceDataCenter)
		}
	}
	if config.Config.PreventCrossRegionPrimaryFailover {
		if suggestedInstance.Region != analysisEntry.AnalyzedInstanceRegion {
			return false, fmt.Sprintf("PreventCrossRegionPrimaryFailover: will not promote server in %s when failed server in %s", suggestedInstance.Region, analysisEntry.AnalyzedInstanceRegion)
		}
	}
	return true, ""
}

// SuggestReplacementForPromotedReplica returns a server to take over the already
// promoted replica, if such server is found and makes an improvement over the promoted replica.
func SuggestReplacementForPromotedReplica(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (replacement *inst.Instance, actionRequired bool, err error) {
	candidateReplicas, _ := inst.ReadClusterCandidateInstances(promotedReplica.ClusterName)
	candidateReplicas = inst.RemoveInstance(candidateReplicas, deadInstanceKey)
	deadInstance, _, err := inst.ReadInstance(deadInstanceKey)
	if err != nil {
		deadInstance = nil
	}
	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the primary
	// There's many options. We may wish to replace the server we promoted with a better one.
	AuditTopologyRecovery(topologyRecovery, "checking if should replace promoted replica with a better candidate")
	if candidateInstanceKey == nil {
		AuditTopologyRecovery(topologyRecovery, "+ checking if promoted replica is the ideal candidate")
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if promotedReplica.Key.Equals(&candidateReplica.Key) &&
					promotedReplica.DataCenter == deadInstance.DataCenter &&
					promotedReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					// Seems like we promoted a candidate in the same DC & ENV as dead IM! Ideal! We're happy!
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is the ideal candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				}
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the dead instance
		AuditTopologyRecovery(topologyRecovery, "+ searching for an ideal candidate")
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if canTakeOverPromotedServerAsPrimary(candidateReplica, promotedReplica) &&
					candidateReplica.DataCenter == deadInstance.DataCenter &&
					candidateReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					// This would make a great candidate
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as failed instance", *deadInstanceKey, candidateReplica.Key))
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		// We cannot find a candidate in same DC and ENV as dead primary
		AuditTopologyRecovery(topologyRecovery, "+ checking if promoted replica is an OK candidate")
		for _, candidateReplica := range candidateReplicas {
			if promotedReplica.Key.Equals(&candidateReplica.Key) {
				// Seems like we promoted a candidate replica (though not in same DC and ENV as dead primary)
				satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica)
				if satisfied {
					// Good enough. No further action required.
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				}
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Key, reason))
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, "+ searching for a candidate")
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsPrimary(candidateReplica, promotedReplica) &&
				promotedReplica.DataCenter == candidateReplica.DataCenter &&
				promotedReplica.PhysicalEnvironment == candidateReplica.PhysicalEnvironment {
				// OK, better than nothing
				candidateInstanceKey = &candidateReplica.Key
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, candidateReplica.Key))
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, "+ searching for a candidate")
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsPrimary(candidateReplica, promotedReplica) {
				if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
					// OK, better than nothing
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement", promotedReplica.Key, candidateReplica.Key))
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Key, reason))
				}
			}
		}
	}

	keepSearchingHint := ""
	if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, promotedReplica); !satisfied {
		keepSearchingHint = fmt.Sprintf("Will keep searching; %s", reason)
	} else if promotedReplica.PromotionRule == promotionrule.PreferNot {
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", promotedReplica.Key)
	}
	if keepSearchingHint != "" {
		AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		neutralReplicas, _ := inst.ReadClusterNeutralPromotionRuleInstances(promotedReplica.ClusterName)

		if candidateInstanceKey == nil {
			// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
			// find neutral instance in same dv&env as dead primary
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as dead primary")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsPrimary(neutralReplica, promotedReplica) &&
					deadInstance.DataCenter == neutralReplica.DataCenter &&
					deadInstance.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as dead primary", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			// find neutral instance in same dv&env as promoted replica
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsPrimary(neutralReplica, promotedReplica) &&
					promotedReplica.DataCenter == neutralReplica.DataCenter &&
					promotedReplica.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace a prefer_not")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsPrimary(neutralReplica, promotedReplica) {
					if satisfied, reason := PrimaryFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, neutralReplica); satisfied {
						// OK, better than nothing
						candidateInstanceKey = &neutralReplica.Key
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on promoted instance having prefer_not promotion rule", promotedReplica.Key, neutralReplica.Key))
					} else {
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", neutralReplica.Key, reason))
					}
				}
			}
		}
	}

	// So do we have a candidate?
	if candidateInstanceKey == nil {
		// Found nothing. Stick with promoted replica
		AuditTopologyRecovery(topologyRecovery, "+ found no server to promote on top promoted replica")
		return promotedReplica, false, nil
	}
	if promotedReplica.Key.Equals(candidateInstanceKey) {
		// Sanity. It IS the candidate, nothing to promote...
		AuditTopologyRecovery(topologyRecovery, "+ sanity check: found our very own server to promote; doing nothing")
		return promotedReplica, false, nil
	}
	replacement, _, err = inst.ReadInstance(candidateInstanceKey)
	return replacement, true, err
}

// recoverPrimaryHasPrimary resets the replication on the primary instance
func recoverPrimaryHasPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimaryHasPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix incorrect primaryship %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, nil)
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
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, deadprimary %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, promotedReplica)
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
		AuditTopologyRecovery(topologyRecovery, value)
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

	// We should refresh the tablet information again to update our information.
	RefreshTablets(true /* forceRefresh */)
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
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-primary", &analysisEntry.AnalyzedInstanceKey, message)
	}
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadPrimary: successfully promoted %+v", promotedReplica.Key))

		if config.Config.PrimaryFailoverDetachReplicaPrimaryHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(topologyRecovery, "- RecoverDeadPrimary: detaching primary host on promoted primary")
				inst.DetachReplicaPrimaryHost(&promotedReplica.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadPrimary, detaching promoted primary host %+v", promotedReplica.Key))
		}
		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadPrimary: updating cluster_alias: %v -> %v", before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())

		if !skipProcesses {
			// Execute post primary-failover processes
			executeProcesses(config.Config.PostPrimaryFailoverProcesses, "PostPrimaryFailoverProcesses", topologyRecovery, false)
		}
	}
}

func isGenerallyValidAsWouldBePrimary(replica *inst.Instance, requireLogReplicationUpdates bool) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if requireLogReplicationUpdates && !replica.LogReplicationUpdatesEnabled {
		return false
	}
	if replica.IsBinlogServer() {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(replica) {
		return false
	}

	return true
}

func canTakeOverPromotedServerAsPrimary(wantToTakeOver *inst.Instance, toBeTakenOver *inst.Instance) bool {
	if !isGenerallyValidAsWouldBePrimary(wantToTakeOver, true) {
		return false
	}
	if !wantToTakeOver.SourceKey.Equals(&toBeTakenOver.Key) {
		return false
	}
	if canReplicate, _ := toBeTakenOver.CanReplicateFrom(wantToTakeOver); !canReplicate {
		return false
	}
	return true
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
func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) (instance *inst.Instance, err error) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return nil, nil
	}
	instance, err = inst.ReadTopologyInstance(instanceKey)
	inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	return instance, err
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
		inst.RestartReplicationQuick(instanceKey)
		inst.AuditOperation("emergently-restart-replication-topology-instance", instanceKey, string(analysisCode))
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
	// Execute on-detection processes
	if skipProcesses {
		return true, false, nil
	}
	err = executeProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", NewTopologyRecovery(analysisEntry), true)
	return true, true, err
}

// getCheckAndRecoverFunction gets the recovery function to use for the given analysis.
// It also returns a recoveryFunction which is supposed to be unique for each function that we return.
// It is used for checking the equality of the returned function.
func getCheckAndRecoverFunction(analysisCode inst.AnalysisCode, analyzedInstanceKey *inst.InstanceKey) (
	checkAndRecoverFunction func(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
	recoverFunctionCode recoveryFunction,
	isActionableRecovery bool,
) {
	switch analysisCode {
	// primary
	case inst.DeadPrimary, inst.DeadPrimaryAndSomeReplicas:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
		}
		return recoverDeadPrimary, recoverDeadPrimaryFunc, true
	case inst.PrimaryHasPrimary:
		return recoverPrimaryHasPrimary, recoverPrimaryHasPrimaryFunc, true
	case inst.LockedSemiSyncPrimary:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
		}
		return checkAndRecoverLockedSemiSyncPrimary, recoverLockedSemiSyncPrimaryFunc, true
	case inst.ClusterHasNoPrimary:
		return electNewPrimary, electNewPrimaryFunc, true
	case inst.PrimaryIsReadOnly, inst.PrimarySemiSyncMustBeSet, inst.PrimarySemiSyncMustNotBeSet:
		return fixPrimary, fixPrimaryFunc, true
	// replica
	case inst.NotConnectedToPrimary, inst.ConnectedToWrongPrimary, inst.ReplicationStopped, inst.ReplicaIsWritable,
		inst.ReplicaSemiSyncMustBeSet, inst.ReplicaSemiSyncMustNotBeSet:
		return fixReplica, fixReplicaFunc, true
	// primary, non actionable
	case inst.DeadPrimaryAndReplicas:
		return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
	case inst.UnreachablePrimary:
		return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
	case inst.UnreachablePrimaryWithLaggingReplicas:
		return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
	case inst.AllPrimaryReplicasNotReplicating:
		return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
	case inst.AllPrimaryReplicasNotReplicatingOrDead:
		return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllPrimaryReplicasStale:
	//   return checkAndRecoverGenericProblem, recoverGenericProblemFunc, false

	return nil, noRecoveryFunc, false
}

// analysisEntriesHaveSameRecovery tells whether the two analysis entries have the same recovery function or not
func analysisEntriesHaveSameRecovery(prevAnalysis, newAnalysis inst.ReplicationAnalysis) bool {
	_, prevRecoveryFunctionCode, _ := getCheckAndRecoverFunction(prevAnalysis.Analysis, &prevAnalysis.AnalyzedInstanceKey)
	_, newRecoveryFunctionCode, _ := getCheckAndRecoverFunction(newAnalysis.Analysis, &newAnalysis.AnalyzedInstanceKey)
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
	atomic.AddInt64(&countPendingRecoveries, 1)
	defer atomic.AddInt64(&countPendingRecoveries, -1)

	checkAndRecoverFunction, _, isActionableRecovery := getCheckAndRecoverFunction(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(&analysisEntry)

	if checkAndRecoverFunction == nil {
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
		err := RefreshKeyspace(analysisEntry.AnalyzedKeyspace)
		if err != nil {
			return false, nil, err
		}
		// TODO (@GuptaManan100): Refresh only the shard tablet information instead of all the tablets
		RefreshTablets(true /* forceRefresh */)
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
	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(ctx, analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", topologyRecovery)
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			// Execute general unsuccessful post failover processes
			executeProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			// Execute general post failover processes
			inst.EndDowntime(topologyRecovery.SuccessorKey)
			executeProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
	}
	return recoveryAttempted, topologyRecovery, err
}

// checkIfAlreadyFixed checks whether the problem that the analysis entry represents has already been fixed by another agent or not
func checkIfAlreadyFixed(analysisEntry inst.ReplicationAnalysis) (bool, error) {
	// Run a replication analysis again. We will check if the problem persisted
	// TODO (@GuptaManan100): Use specific cluster name to filter the scope of replication analysis.
	// Can't do this now since SuggestedClusterAlias, ClusterName, ClusterAlias aren't consistent
	// and passing any one causes issues in some failures
	analysisEntries, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{})
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
	if *config.RuntimeCLIFlags.Noop {
		log.Infof("--noop provided; will not execute processes")
		skipProcesses = true
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

func forceAnalysisEntry(clusterName string, analysisCode inst.AnalysisCode, commandHint string, failedInstanceKey *inst.InstanceKey) (analysisEntry inst.ReplicationAnalysis, err error) {
	clusterInfo, err := inst.ReadClusterInfo(clusterName)
	if err != nil {
		return analysisEntry, err
	}

	clusterAnalysisEntries, err := inst.GetReplicationAnalysis(clusterInfo.ClusterName, &inst.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		return analysisEntry, err
	}

	for _, entry := range clusterAnalysisEntries {
		if entry.AnalyzedInstanceKey.Equals(failedInstanceKey) {
			analysisEntry = entry
		}
	}
	analysisEntry.Analysis = analysisCode // we force this analysis
	analysisEntry.CommandHint = commandHint
	analysisEntry.ClusterDetails = *clusterInfo
	analysisEntry.AnalyzedInstanceKey = *failedInstanceKey

	return analysisEntry, nil
}

// ForceExecuteRecovery can be called to issue a recovery process even if analysis says there is no recovery case.
// The caller of this function injects the type of analysis it wishes the function to assume.
// By calling this function one takes responsibility for one's actions.
func ForceExecuteRecovery(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
}

// ForcePrimaryFailover *trusts* primary of given cluster is dead and initiates a failover
func ForcePrimaryFailover(clusterName string) (topologyRecovery *TopologyRecovery, err error) {
	clusterPrimaries, err := inst.ReadClusterPrimary(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v", clusterName)
	}
	if len(clusterPrimaries) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v", clusterName)
	}
	clusterPrimary := clusterPrimaries[0]

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadPrimary, inst.ForcePrimaryFailoverCommandHint, &clusterPrimary.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, nil, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

// ForcePrimaryTakeover *trusts* primary of given cluster is dead and fails over to designated instance,
// which has to be its direct child.
func ForcePrimaryTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	clusterPrimaries, err := inst.ReadClusterWriteablePrimary(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v", clusterName)
	}
	if len(clusterPrimaries) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v", clusterName)
	}
	clusterPrimary := clusterPrimaries[0]

	if !destination.SourceKey.Equals(&clusterPrimary.Key) {
		return nil, fmt.Errorf("you may only promote a direct child of the primary %+v. The primary of %+v is %+v", clusterPrimary.Key, destination.Key, destination.SourceKey)
	}
	log.Infof("will demote %+v and promote %+v instead", clusterPrimary.Key, destination.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadPrimary, inst.ForcePrimaryTakeoverCommandHint, &clusterPrimary.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &destination.Key, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

// GracefulPrimaryTakeover will demote primary of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the primary, then wait
// for the designated replica to catch up with last position.
// It will point old primary at the newly promoted primary at the correct coordinates.
// All of this is accomplished via PlannedReparentShard operation. It is an idempotent operation, look at its documentation for more detail
func GracefulPrimaryTakeover(clusterName string, designatedKey *inst.InstanceKey) (topologyRecovery *TopologyRecovery, err error) {
	clusterPrimaries, err := inst.ReadClusterPrimary(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v; error: %+v", clusterName, err)
	}
	if len(clusterPrimaries) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster primary for %+v. Found %+v potential primarys", clusterName, len(clusterPrimaries))
	}
	clusterPrimary := clusterPrimaries[0]

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.GraceFulPrimaryTakeover, inst.GracefulPrimaryTakeoverCommandHint, &clusterPrimary.Key)
	if err != nil {
		return nil, err
	}
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false /*failIfFailedInstanceInActiveRecovery*/, false /*failIfClusterInActiveRecovery*/)
	if err != nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not register recovery on %+v. Unable to issue PlannedReparentShard. err=%v", analysisEntry.AnalyzedInstanceKey, err))
		return topologyRecovery, err
	}
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not register recovery on %+v. Unable to issue PlannedReparentShard. Recovery is nil", analysisEntry.AnalyzedInstanceKey))
		return nil, nil
	}
	// recovery is now registered. From now on this process owns the recovery and other processes will notbe able to run
	// a recovery for some time.
	// Let's audit anything that happens from this point on, including any early return
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("registered recovery on %+v. Recovery: %+v", analysisEntry.AnalyzedInstanceKey, topologyRecovery))
	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, promotedReplica)
	}()

	primaryTablet, err := inst.ReadTablet(clusterPrimary.Key)
	if err != nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("rerror reading primary tablet %+v: %+v", clusterPrimary.Key, err))
		return topologyRecovery, err
	}
	if designatedKey != nil && !designatedKey.IsValid() {
		// An empty or invalid key is as good as no key
		designatedKey = nil
	}
	var designatedTabletAlias *topodatapb.TabletAlias
	if designatedKey != nil {
		designatedTablet, err := inst.ReadTablet(*designatedKey)
		if err != nil {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("rerror reading designated tablet %+v: %+v", *designatedKey, err))
			return topologyRecovery, err
		}
		designatedTabletAlias = designatedTablet.Alias
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("started PlannedReparentShard, new primary will be %s.", topoproto.TabletAliasString(designatedTabletAlias)))
	} else {
		AuditTopologyRecovery(topologyRecovery, "started PlannedReparentShard with automatic primary selection.")
	}

	// check for the constraint failure for cross cell promotion
	if designatedTabletAlias != nil && designatedTabletAlias.Cell != primaryTablet.Alias.Cell && config.Config.PreventCrossDataCenterPrimaryFailover {
		errorMessage := fmt.Sprintf("GracefulPrimaryTakeover: constraint failure - %s and %s are in different cells", topoproto.TabletAliasString(designatedTabletAlias), topoproto.TabletAliasString(primaryTablet.Alias))
		AuditTopologyRecovery(topologyRecovery, errorMessage)
		return topologyRecovery, fmt.Errorf(errorMessage)
	}

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
		AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(context.Background(),
		primaryTablet.Keyspace,
		primaryTablet.Shard,
		reparentutil.PlannedReparentOptions{
			NewPrimaryAlias:     designatedTabletAlias,
			WaitReplicasTimeout: time.Duration(config.Config.WaitReplicasTimeoutSeconds) * time.Second,
		},
	)

	// here we need to forcefully refresh all the tablets otherwise old information is used and failover scenarios are spawned off which are not required
	// For example, if we do not refresh the tablets forcefully and the new primary is found in the cache then its source key is not updated and this spawns off
	// PrimaryHasPrimary analysis which runs ERS
	RefreshTablets(true /* forceRefresh */)
	if ev != nil && ev.NewPrimary != nil {
		promotedReplica, _, _ = inst.ReadInstance(&inst.InstanceKey{
			Hostname: ev.NewPrimary.MysqlHostname,
			Port:     int(ev.NewPrimary.MysqlPort),
		})
	}
	postPrsCompletion(topologyRecovery, analysisEntry, promotedReplica)
	return topologyRecovery, err
}

func postPrsCompletion(topologyRecovery *TopologyRecovery, analysisEntry inst.ReplicationAnalysis, promotedReplica *inst.Instance) {
	if promotedReplica != nil {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation(string(analysisEntry.Analysis), &analysisEntry.AnalyzedInstanceKey, message)
	}
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%+v: successfully promoted %+v", analysisEntry.Analysis, promotedReplica.Key))

		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- %+v: updating cluster_alias: %v -> %v", analysisEntry.Analysis, before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())
	}
}

// electNewPrimary elects a new primary while none were present before.
func electNewPrimary(ctx context.Context, analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false /*failIfFailedInstanceInActiveRecovery*/, true /*failIfClusterInActiveRecovery*/)
	if topologyRecovery == nil || err != nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another electNewPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will elect a new primary: %v", analysisEntry.Analysis, analysisEntry.SuggestedClusterAlias)

	var promotedReplica *inst.Instance
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, promotedReplica)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}
	AuditTopologyRecovery(topologyRecovery, "starting PlannedReparentShard for electing new primary.")

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
		AuditTopologyRecovery(topologyRecovery, value)
	})).ReparentShard(ctx,
		analyzedTablet.Keyspace,
		analyzedTablet.Shard,
		reparentutil.PlannedReparentOptions{
			WaitReplicasTimeout: time.Duration(config.Config.WaitReplicasTimeoutSeconds) * time.Second,
		},
	)

	// here we need to forcefully refresh all the tablets otherwise old information is used and failover scenarios are spawned off which are not required
	// For example, if we do not refresh the tablets forcefully and the new primary is found in the cache then its source key is not updated and this spawns off
	// PrimaryHasPrimary analysis which runs ERS
	RefreshTablets(true /* forceRefresh */)
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
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixPrimary.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix primary to read-write %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, nil)
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
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixReplica.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix replica %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// This has to be done in the end; whether successful or not, we should mark that the recovery is done.
	// So that after the active period passes, we are able to run other recoveries.
	defer func() {
		resolveRecovery(topologyRecovery, nil)
	}()

	analyzedTablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}

	primaryTablet, err := shardPrimary(ctx, analyzedTablet.Keyspace, analyzedTablet.Shard)
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
