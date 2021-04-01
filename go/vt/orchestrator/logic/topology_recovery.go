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
	"encoding/json"
	"fmt"
	"math/rand"
	goos "os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/attributes"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/kv"
	ometrics "vitess.io/vitess/go/vt/orchestrator/metrics"
	"vitess.io/vitess/go/vt/orchestrator/os"
	"vitess.io/vitess/go/vt/orchestrator/process"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"
	"vitess.io/vitess/go/vt/orchestrator/util"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var countPendingRecoveries int64

type RecoveryType string

const (
	MasterRecovery             RecoveryType = "MasterRecovery"
	CoMasterRecovery           RecoveryType = "CoMasterRecovery"
	IntermediateMasterRecovery RecoveryType = "IntermediateMasterRecovery"
)

type RecoveryAcknowledgement struct {
	CreatedAt time.Time
	Owner     string
	Comment   string

	Key           inst.InstanceKey
	ClusterName   string
	Id            int64
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

func NewInternalAcknowledgement() *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     "orchestrator",
		Comment:   "internal",
	}
}

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	Id                        int64
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
	LastDetectionId           int64
	RelatedRecoveryId         int64
	Type                      RecoveryType
	RecoveryType              MasterRecoveryType
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = util.PrettyUniqueToken()
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostReplicas = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.RecoveryType = NotMasterRecovery
	return topologyRecovery
}

func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type TopologyRecoveryStep struct {
	Id          int64
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

type MasterRecoveryType string

const (
	NotMasterRecovery          MasterRecoveryType = "NotMasterRecovery"
	MasterRecoveryGTID         MasterRecoveryType = "MasterRecoveryGTID"
	MasterRecoveryPseudoGTID   MasterRecoveryType = "MasterRecoveryPseudoGTID"
	MasterRecoveryBinlogServer MasterRecoveryType = "MasterRecoveryBinlogServer"
)

var emergencyReadTopologyInstanceMap *cache.Cache
var emergencyRestartReplicaTopologyInstanceMap *cache.Cache
var emergencyOperationGracefulPeriodMap *cache.Cache

// InstancesByCountReplicas sorts instances by umber of replicas, descending
type InstancesByCountReplicas [](*inst.Instance)

func (this InstancesByCountReplicas) Len() int      { return len(this) }
func (this InstancesByCountReplicas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountReplicas) Less(i, j int) bool {
	if len(this[i].Replicas) == len(this[j].Replicas) {
		// Secondary sorting: prefer more advanced replicas
		return !this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
	}
	return len(this[i].Replicas) < len(this[j].Replicas)
}

var recoverDeadMasterCounter = metrics.NewCounter()
var recoverDeadMasterSuccessCounter = metrics.NewCounter()
var recoverDeadMasterFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterFailureCounter = metrics.NewCounter()
var recoverDeadCoMasterCounter = metrics.NewCounter()
var recoverDeadCoMasterSuccessCounter = metrics.NewCounter()
var recoverDeadCoMasterFailureCounter = metrics.NewCounter()
var countPendingRecoveriesGauge = metrics.NewGauge()

func init() {
	metrics.Register("recover.dead_master.start", recoverDeadMasterCounter)
	metrics.Register("recover.dead_master.success", recoverDeadMasterSuccessCounter)
	metrics.Register("recover.dead_master.fail", recoverDeadMasterFailureCounter)
	metrics.Register("recover.dead_intermediate_master.start", recoverDeadIntermediateMasterCounter)
	metrics.Register("recover.dead_intermediate_master.success", recoverDeadIntermediateMasterSuccessCounter)
	metrics.Register("recover.dead_intermediate_master.fail", recoverDeadIntermediateMasterFailureCounter)
	metrics.Register("recover.dead_co_master.start", recoverDeadCoMasterCounter)
	metrics.Register("recover.dead_co_master.success", recoverDeadCoMasterSuccessCounter)
	metrics.Register("recover.dead_co_master.fail", recoverDeadCoMasterFailureCounter)
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
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("write-recovery-step", recoveryStep)
		return err
	} else {
		return writeTopologyRecoveryStep(recoveryStep)
	}
}

func resolveRecovery(topologyRecovery *TopologyRecovery, successorInstance *inst.Instance) error {
	if successorInstance != nil {
		topologyRecovery.SuccessorKey = &successorInstance.Key
		topologyRecovery.SuccessorAlias = successorInstance.InstanceAlias
		topologyRecovery.IsSuccessful = true
	}
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("resolve-recovery", topologyRecovery)
		return err
	} else {
		return writeResolveRecovery(topologyRecovery)
	}
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
	command = strings.Replace(command, "{isMaster}", fmt.Sprintf("%t", analysisEntry.IsMaster), -1)
	command = strings.Replace(command, "{isCoMaster}", fmt.Sprintf("%t", analysisEntry.IsCoMaster), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{command}", analysisEntry.CommandHint, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{failureClusterDomain}", analysisEntry.ClusterDetails.ClusterDomain, -1)
	command = strings.Replace(command, "{countSlaves}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{countReplicas}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedMasterRecovery), -1)
	command = strings.Replace(command, "{autoIntermediateMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery), -1)
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

	command = strings.Replace(command, "{lostSlaves}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{lostReplicas}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{countLostReplicas}", fmt.Sprintf("%d", len(topologyRecovery.LostReplicas)), -1)
	command = strings.Replace(command, "{slaveHosts}", analysisEntry.Replicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{replicaHosts}", analysisEntry.Replicas.ToCommaDelimitedList(), -1)

	return command, async
}

// applyEnvironmentVariables sets the relevant environment variables for a recovery
func applyEnvironmentVariables(topologyRecovery *TopologyRecovery) []string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	env := goos.Environ()
	env = append(env, fmt.Sprintf("ORC_FAILURE_TYPE=%s", string(analysisEntry.Analysis)))
	env = append(env, fmt.Sprintf("ORC_INSTANCE_TYPE=%s", string(analysisEntry.GetAnalysisInstanceType())))
	env = append(env, fmt.Sprintf("ORC_IS_MASTER=%t", analysisEntry.IsMaster))
	env = append(env, fmt.Sprintf("ORC_IS_CO_MASTER=%t", analysisEntry.IsCoMaster))
	env = append(env, fmt.Sprintf("ORC_FAILURE_DESCRIPTION=%s", analysisEntry.Description))
	env = append(env, fmt.Sprintf("ORC_COMMAND=%s", analysisEntry.CommandHint))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", analysisEntry.AnalyzedInstanceKey.Hostname))
	env = append(env, fmt.Sprintf("ORC_FAILED_PORT=%d", analysisEntry.AnalyzedInstanceKey.Port))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER=%s", analysisEntry.ClusterDetails.ClusterName))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_ALIAS=%s", analysisEntry.ClusterDetails.ClusterAlias))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_DOMAIN=%s", analysisEntry.ClusterDetails.ClusterDomain))
	env = append(env, fmt.Sprintf("ORC_COUNT_REPLICAS=%d", analysisEntry.CountReplicas))
	env = append(env, fmt.Sprintf("ORC_IS_DOWNTIMED=%v", analysisEntry.IsDowntimed))
	env = append(env, fmt.Sprintf("ORC_AUTO_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedMasterRecovery))
	env = append(env, fmt.Sprintf("ORC_AUTO_INTERMEDIATE_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery))
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

func recoverDeadMasterInBinlogServerTopology(topologyRecovery *TopologyRecovery) (promotedReplica *inst.Instance, err error) {
	failedMasterKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *inst.Instance

	_, promotedBinlogServer, err = inst.RegroupReplicasBinlogServers(failedMasterKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	promotedBinlogServer, err = inst.StopReplication(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Find candidate replica
	promotedReplica, err = inst.GetCandidateReplicaOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Align it with binlog server coordinates
	promotedReplica, err = inst.StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StartReplicationUntilMasterCoordinates(&promotedReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Detach, flush binary logs forward
	promotedReplica, err = inst.ResetReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogsTo(&promotedReplica.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogs(&promotedReplica.Key, 1)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.PurgeBinaryLogsToLatest(&promotedReplica.Key, false)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Reconnect binlog servers to promoted replica (now master):
	promotedBinlogServer, err = inst.SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedBinlogServer, err = inst.Repoint(&promotedBinlogServer.Key, &promotedReplica.Key, inst.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		// Move binlog server replicas up to replicate from master.
		// This can only be done once a BLS has skipped to the next binlog
		// We postpone this operation. The master is already promoted and we're happy.
		binlogServerReplicas, err := inst.ReadBinlogServerReplicaInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerReplica := range binlogServerReplicas {
			binlogServerReplica := binlogServerReplica
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerReplica, err := inst.StopReplication(&binlogServerReplica.Key)
				if err != nil {
					return err
				}
				// Make sure the BLS has the "next binlog" -- the one the master flushed & purged to. Otherwise the BLS
				// will request a binlog the master does not have
				if binlogServerReplica.ExecBinlogCoordinates.SmallerThan(&promotedBinlogServer.ExecBinlogCoordinates) {
					binlogServerReplica, err = inst.StartReplicationUntilMasterCoordinates(&binlogServerReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = inst.Repoint(&binlogServerReplica.Key, &promotedReplica.Key, inst.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("recoverDeadMasterInBinlogServerTopology, moving binlog server %+v", binlogServerReplica.Key))
		}
	}()

	return promotedReplica, err
}

func GetMasterRecoveryType(analysisEntry *inst.ReplicationAnalysis) (masterRecoveryType MasterRecoveryType) {
	masterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		masterRecoveryType = MasterRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		masterRecoveryType = MasterRecoveryBinlogServer
	}
	return masterRecoveryType
}

// recoverDeadMaster recovers a dead master, complete logic inside
func recoverDeadMaster(topologyRecovery *TopologyRecovery, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	var cannotReplicateReplicas [](*inst.Instance)
	postponedAll := false

	inst.AuditOperation("recover-dead-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return false, nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: will recover %+v", *failedInstanceKey))

	err = TabletDemoteMaster(*failedInstanceKey)
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: TabletDemoteMaster: %v", err))

	topologyRecovery.RecoveryType = GetMasterRecoveryType(analysisEntry)
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", topologyRecovery.RecoveryType))

	promotedReplicaIsIdeal := func(promoted *inst.Instance, hasBestPromotionRule bool) bool {
		if promoted == nil {
			return false
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", promoted.Key))
		if candidateInstanceKey != nil { //explicit request to promote a specific server
			return promoted.Key.Equals(candidateInstanceKey)
		}
		if promoted.DataCenter == topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter &&
			promoted.PhysicalEnvironment == topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment {
			if promoted.PromotionRule == inst.MustPromoteRule || promoted.PromotionRule == inst.PreferPromoteRule ||
				(hasBestPromotionRule && promoted.PromotionRule != inst.MustNotPromoteRule) {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", promoted.Key))
				postponedAll = true
				return true
			}
		}
		return false
	}
	switch topologyRecovery.RecoveryType {
	case MasterRecoveryGTID:
		{
			AuditTopologyRecovery(topologyRecovery, "RecoverDeadMaster: regrouping replicas via GTID")
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MasterRecoveryPseudoGTID:
		{
			AuditTopologyRecovery(topologyRecovery, "RecoverDeadMaster: regrouping replicas via Pseudo-GTID")
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MasterRecoveryBinlogServer:
		{
			AuditTopologyRecovery(topologyRecovery, "RecoverDeadMaster: recovering via binlog servers")
			promotedReplica, err = recoverDeadMasterInBinlogServerTopology(topologyRecovery)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	for _, replica := range lostReplicas {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMasterHost(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detach %+v lost replicas", len(lostReplicas)))
	}

	func() error {
		// TODO(sougou): Commented out: this downtime feels a little aggressive.
		//inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))

	if promotedReplica != nil && !postponedAll {
		promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, &analysisEntry.AnalyzedInstanceKey, promotedReplica, candidateInstanceKey)
		topologyRecovery.AddError(err)
	}

	if promotedReplica == nil {
		err := TabletUndoDemoteMaster(*failedInstanceKey)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: TabletUndoDemoteMaster: %v", err))
		message := "Failure: no replica promoted."
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-master", failedInstanceKey, message)
		return true, promotedReplica, lostReplicas, err
	}

	message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
	AuditTopologyRecovery(topologyRecovery, message)
	inst.AuditOperation("recover-dead-master", failedInstanceKey, message)
	return true, promotedReplica, lostReplicas, err
}

func MasterFailoverGeographicConstraintSatisfied(analysisEntry *inst.ReplicationAnalysis, suggestedInstance *inst.Instance) (satisfied bool, dissatisfiedReason string) {
	if config.Config.PreventCrossDataCenterMasterFailover {
		if suggestedInstance.DataCenter != analysisEntry.AnalyzedInstanceDataCenter {
			return false, fmt.Sprintf("PreventCrossDataCenterMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.DataCenter, analysisEntry.AnalyzedInstanceDataCenter)
		}
	}
	if config.Config.PreventCrossRegionMasterFailover {
		if suggestedInstance.Region != analysisEntry.AnalyzedInstanceRegion {
			return false, fmt.Sprintf("PreventCrossRegionMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.Region, analysisEntry.AnalyzedInstanceRegion)
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
	// Maybe we promoted a server in a different DC than the master
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
				if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) &&
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
		// We cannot find a candidate in same DC and ENV as dead master
		AuditTopologyRecovery(topologyRecovery, "+ checking if promoted replica is an OK candidate")
		for _, candidateReplica := range candidateReplicas {
			if promotedReplica.Key.Equals(&candidateReplica.Key) {
				// Seems like we promoted a candidate replica (though not in same DC and ENV as dead master)
				if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
					// Good enough. No further action required.
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Key, reason))
				}
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, "+ searching for a candidate")
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) &&
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
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) {
				if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
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
	if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, promotedReplica); !satisfied {
		keepSearchingHint = fmt.Sprintf("Will keep searching; %s", reason)
	} else if promotedReplica.PromotionRule == inst.PreferNotPromoteRule {
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", promotedReplica.Key)
	}
	if keepSearchingHint != "" {
		AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		neutralReplicas, _ := inst.ReadClusterNeutralPromotionRuleInstances(promotedReplica.ClusterName)

		if candidateInstanceKey == nil {
			// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
			// find neutral instance in same dv&env as dead master
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as dead master")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
					deadInstance.DataCenter == neutralReplica.DataCenter &&
					deadInstance.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as dead master", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			// find neutral instance in same dv&env as promoted replica
			AuditTopologyRecovery(topologyRecovery, "+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica")
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
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
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) {
					if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, neutralReplica); satisfied {
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

// replacePromotedReplicaWithCandidate is called after a master (or co-master)
// died and was replaced by some promotedReplica.
// But, is there an even better replica to promote?
// if candidateInstanceKey is given, then it is forced to be promoted over the promotedReplica
// Otherwise, search for the best to promote!
func replacePromotedReplicaWithCandidate(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (*inst.Instance, error) {
	candidateInstance, actionRequired, err := SuggestReplacementForPromotedReplica(topologyRecovery, deadInstanceKey, promotedReplica, candidateInstanceKey)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	if !actionRequired {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v requires no further action", promotedReplica.Key))
		return promotedReplica, nil
	}

	// Try and promote suggested candidate, if applicable and possible
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedReplica.Key, candidateInstance.Key))

	if candidateInstance.MasterKey.Equals(&promotedReplica.Key) {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: suggested candidate %+v is replica of promoted instance %+v. Will try and take its master", candidateInstance.Key, promotedReplica.Key))
		candidateInstance, err = inst.TakeMaster(&candidateInstance.Key, topologyRecovery.Type == CoMasterRecovery)
		if err != nil {
			return promotedReplica, log.Errore(err)
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("success promoting %+v over %+v", candidateInstance.Key, promotedReplica.Key))

		// As followup to taking over, let's relocate all the rest of the replicas under the candidate instance
		relocateReplicasFunc := func() error {
			log.Debugf("replace-promoted-replica-with-candidate: relocating replicas of %+v below %+v", promotedReplica.Key, candidateInstance.Key)

			relocatedReplicas, _, err, _ := inst.RelocateReplicas(&promotedReplica.Key, &candidateInstance.Key, "")
			log.Debugf("replace-promoted-replica-with-candidate: + relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key))
			return log.Errore(err)
		}
		postponedFunctionsContainer := &topologyRecovery.PostponedFunctionsContainer
		if postponedFunctionsContainer != nil {
			postponedFunctionsContainer.AddPostponedFunction(relocateReplicasFunc, fmt.Sprintf("replace-promoted-replica-with-candidate: relocate replicas of %+v", promotedReplica.Key))
		} else {
			_ = relocateReplicasFunc()
			// We do not propagate the error. It is logged, but otherwise should not fail the entire failover operation
		}
		return candidateInstance, nil
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not manage to promoted suggested candidate %+v", candidateInstance.Key))
	return promotedReplica, nil
}

// checkAndRecoverDeadMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, deadmaster %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)

	// That's it! We must do recovery!
	// TODO(sougou): This function gets called by GracefulMasterTakeover which may
	// need to obtain shard lock before getting here.
	unlock, err := LockShard(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: NOT detecting/recovering host, could not obtain shard lock (%v)",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
		return false, nil, err
	}
	defer unlock(&err)

	// Check if someone else fixed the problem.
	tablet, err := TabletRefresh(analysisEntry.AnalyzedInstanceKey)
	if err == nil && tablet.Type != topodatapb.TabletType_MASTER {
		// TODO(sougou); use a version that only refreshes the current shard.
		RefreshTablets()
		AuditTopologyRecovery(topologyRecovery, "another agent seems to have fixed the problem")
		// TODO(sougou): see if we have to reset the cluster as healthy.
		return false, topologyRecovery, nil
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("will handle DeadMaster event on %+v", analysisEntry.ClusterDetails.ClusterName))
	recoverDeadMasterCounter.Inc(1)
	recoveryAttempted, promotedReplica, lostReplicas, err := recoverDeadMaster(topologyRecovery, candidateInstanceKey, skipProcesses)
	if err != nil {
		AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	if !recoveryAttempted {
		return false, topologyRecovery, err
	}

	overrideMasterPromotion := func() (*inst.Instance, error) {
		if promotedReplica == nil {
			// No promotion; nothing to override.
			return promotedReplica, err
		}
		// Scenarios where we might cancel the promotion.
		if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&analysisEntry, promotedReplica); !satisfied {
			return nil, fmt.Errorf("RecoverDeadMaster: failed %+v promotion; %s", promotedReplica.Key, reason)
		}
		if config.Config.FailMasterPromotionOnLagMinutes > 0 &&
			time.Duration(promotedReplica.ReplicationLagSeconds.Int64)*time.Second >= time.Duration(config.Config.FailMasterPromotionOnLagMinutes)*time.Minute {
			// candidate replica lags too much
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionOnLagMinutes is set to %d (minutes) and promoted replica %+v 's lag is %d (seconds)", config.Config.FailMasterPromotionOnLagMinutes, promotedReplica.Key, promotedReplica.ReplicationLagSeconds.Int64)
		}
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: waiting for SQL thread on %+v", promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return nil, fmt.Errorf("DelayMasterPromotionIfSQLThreadNotUpToDate error: %+v", err)
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: SQL thread caught up on %+v", promotedReplica.Key))
		}
		// All seems well. No override done.
		return promotedReplica, err
	}
	if promotedReplica, err = overrideMasterPromotion(); err != nil {
		AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	// And this is the end; whether successful or not, we're done.
	resolveRecovery(topologyRecovery, promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		recoverDeadMasterSuccessCounter.Inc(1)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: successfully promoted %+v", promotedReplica.Key))
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: promoted server coordinates: %+v", promotedReplica.SelfBinlogCoordinates))

		AuditTopologyRecovery(topologyRecovery, "- RecoverDeadMaster: will apply MySQL changes to promoted master")
		{
			_, err := inst.ResetReplicationOperation(&promotedReplica.Key)
			if err != nil {
				// Ugly, but this is important. Let's give it another try
				_, err = inst.ResetReplicationOperation(&promotedReplica.Key)
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying RESET SLAVE ALL on promoted master: success=%t", (err == nil)))
			if err != nil {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: NOTE that %+v is promoted even though SHOW SLAVE STATUS may still show it has a master", promotedReplica.Key))
			}
		}
		{
			count := inst.MasterSemiSync(promotedReplica.Key)
			err := inst.SetSemiSyncMaster(&promotedReplica.Key, count > 0)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying semi-sync %v: success=%t", count > 0, (err == nil)))

			// Dont' allow writes if semi-sync settings fail.
			if err == nil {
				_, err := inst.SetReadOnly(&promotedReplica.Key, false)
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=0 on promoted master: success=%t", (err == nil)))
			}
		}
		// Let's attempt, though we won't necessarily succeed, to set old master as read-only
		go func() {
			_, err := inst.SetReadOnly(&analysisEntry.AnalyzedInstanceKey, true)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=1 on demoted master: success=%t", (err == nil)))
		}()

		kvPairs := inst.GetClusterMasterKVPairs(analysisEntry.ClusterDetails.ClusterAlias, &promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		if orcraft.IsRaftEnabled() {
			for _, kvPair := range kvPairs {
				_, err := orcraft.PublishCommand("put-key-value", kvPair)
				log.Errore(err)
			}
			// since we'll be affecting 3rd party tools here, we _prefer_ to mitigate re-applying
			// of the put-key-value event upon startup. We _recommend_ a snapshot in the near future.
			go orcraft.PublishCommand("async-snapshot", "")
		} else {
			for _, kvPair := range kvPairs {
				err := kv.PutKVPair(kvPair)
				log.Errore(err)
			}
		}
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Distributing KV %+v", kvPairs))
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if config.Config.MasterFailoverDetachReplicaMasterHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(topologyRecovery, "- RecoverDeadMaster: detaching master host on promoted master")
				inst.DetachReplicaMasterHost(&promotedReplica.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detaching promoted master host %+v", promotedReplica.Key))
		}
		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: updating cluster_alias: %v -> %v", before, after))
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
			// Execute post master-failover processes
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadMasterFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

// isGenerallyValidAsCandidateSiblingOfIntermediateMaster sees that basic server configuration and state are valid
func isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling *inst.Instance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogReplicationUpdatesEnabled {
		return false
	}
	if !sibling.ReplicaRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMaster checks to see that the given sibling is capable to take over instance's replicas
func isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *inst.Instance, sibling *inst.Instance) bool {
	if sibling.Key.Equals(&intermediateMasterInstance.Key) {
		// same instance
		return false
	}
	if !isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling) {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMasterInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsBinlogServer() != intermediateMasterInstance.IsBinlogServer() {
		// When both are binlog servers, failover is trivial.
		// When failed IM is binlog server, its sibling is still valid, but we catually prefer to just repoint the replica up -- simplest!
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMasterInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

func isGenerallyValidAsWouldBeMaster(replica *inst.Instance, requireLogReplicationUpdates bool) bool {
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

func canTakeOverPromotedServerAsMaster(wantToTakeOver *inst.Instance, toBeTakenOver *inst.Instance) bool {
	if !isGenerallyValidAsWouldBeMaster(wantToTakeOver, true) {
		return false
	}
	if !wantToTakeOver.MasterKey.Equals(&toBeTakenOver.Key) {
		return false
	}
	if canReplicate, _ := toBeTakenOver.CanReplicateFrom(wantToTakeOver); !canReplicate {
		return false
	}
	return true
}

// GetCandidateSiblingOfIntermediateMaster chooses the best sibling of a dead intermediate master
// to whom the IM's replicas can be moved.
func GetCandidateSiblingOfIntermediateMaster(topologyRecovery *TopologyRecovery, intermediateMasterInstance *inst.Instance) (*inst.Instance, error) {

	siblings, err := inst.ReadReplicaInstances(&intermediateMasterInstance.MasterKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMasterInstance.Key)
	}

	sort.Sort(sort.Reverse(InstancesByCountReplicas(siblings)))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("searching for the best candidate sibling of dead intermediate master %+v", intermediateMasterInstance.Key))
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as the ideal candidate", sibling.Key))
			return sibling, nil
		}
	}
	// No candidate in same DC & env, let's search for a candidate anywhere
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) && sibling.IsCandidate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [candidate sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Go for some valid in the same DC & ENV
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [same dc & environment]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Just whatever is valid.
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [any sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMasterInstance.Key)
}

// RecoverDeadIntermediateMaster performs intermediate master recovery; complete logic inside
func RecoverDeadIntermediateMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	topologyRecovery.Type = IntermediateMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMasterInstance, _, err := inst.ReadInstance(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find possible candidate
	candidateSiblingOfIntermediateMaster, _ := GetCandidateSiblingOfIntermediateMaster(topologyRecovery, intermediateMasterInstance)
	relocateReplicasToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMaster == nil {
			return
		}
		// We have a candidate
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will attempt a candidate intermediate master: %+v", candidateSiblingOfIntermediateMaster.Key))
		relocatedReplicas, candidateSibling, err, errs := inst.RelocateReplicas(failedInstanceKey, &candidateSiblingOfIntermediateMaster.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMaster.Key)

		if len(relocatedReplicas) == 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: failed to move any replica to candidate intermediate master (%+v)", candidateSibling.Key))
			return
		}
		if err != nil || len(errs) > 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: move to candidate intermediate master (%+v) did not complete: err: %+v, errs: %+v", candidateSibling.Key, err, errs))
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated %d replicas under candidate sibling: %+v; %d errors: %+v", len(relocatedReplicas), candidateSibling.Key, len(errs), errs))
		}
	}
	// Plan A: find a replacement intermediate master in same Data Center
	if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter == intermediateMasterInstance.DataCenter {
		relocateReplicasToCandidateSibling()
	}
	if !recoveryResolved {
		AuditTopologyRecovery(topologyRecovery, "- RecoverDeadIntermediateMaster: will next attempt regrouping of replicas")
		// Plan B: regroup (we wish to reduce cross-DC replication streams)
		lostReplicas, _, _, _, regroupPromotedReplica, regroupError := inst.RegroupReplicas(failedInstanceKey, true, nil, nil)
		if regroupError != nil {
			topologyRecovery.AddError(regroupError)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regroup failed on: %+v", regroupError))
		}
		if regroupPromotedReplica != nil {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regrouped under %+v, with %d lost replicas", regroupPromotedReplica.Key, len(lostReplicas)))
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedReplica.Key)
			if len(lostReplicas) == 0 && regroupError == nil {
				// Seems like the regroup worked flawlessly. The local replica took over all of its siblings.
				// We can consider this host to be the successor.
				successorInstance = regroupPromotedReplica
			}
		}
		// Plan C: try replacement intermediate master in other DC...
		if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter != intermediateMasterInstance.DataCenter {
			AuditTopologyRecovery(topologyRecovery, "- RecoverDeadIntermediateMaster: will next attempt relocating to another DC server")
			relocateReplicasToCandidateSibling()
		}
	}
	if !recoveryResolved {
		// Do we still have leftovers? some replicas couldn't move? Couldn't regroup? Only left with regroup's resulting leader?
		// nothing moved?
		// We don't care much if regroup made it or not. We prefer that it made it, in which case we only need to relocate up
		// one replica, but the operation is still valid if regroup partially/completely failed. We just promote anything
		// not regrouped.
		// So, match up all that's left, plan D
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt to relocate up from %+v", *failedInstanceKey))

		relocatedReplicas, masterInstance, _, errs := inst.RelocateReplicas(failedInstanceKey, &analysisEntry.AnalyzedInstanceMasterKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceMasterKey)

		if len(relocatedReplicas) > 0 {
			recoveryResolved = true
			if successorInstance == nil {
				// There could have been a local replica taking over its siblings. We'd like to consider that one as successor.
				successorInstance = masterInstance
			}
			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated replicas under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMaster failed to match up any replica from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	resolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadIntermediateMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadIntermediateMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadIntermediateMasterCounter.Inc(1)
	promotedReplica, err := RecoverDeadIntermediateMaster(topologyRecovery, skipProcesses)
	if promotedReplica != nil {
		// success
		recoverDeadIntermediateMasterSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadCoMaster recovers a dead co-master, complete logic inside
func RecoverDeadCoMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = CoMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMasterKey := &analysisEntry.AnalyzedInstanceMasterKey
	otherCoMaster, found, _ := inst.ReadInstance(otherCoMasterKey)
	if otherCoMaster == nil || !found {
		return nil, lostReplicas, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not read info for co-master %+v of %+v", *otherCoMasterKey, *failedInstanceKey))
	}
	inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: will recover %+v", *failedInstanceKey))

	var coMasterRecoveryType MasterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMasterRecoveryType = MasterRecoveryGTID
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: coMasterRecoveryType=%+v", coMasterRecoveryType))

	var cannotReplicateReplicas [](*inst.Instance)
	switch coMasterRecoveryType {
	case MasterRecoveryGTID:
		{
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	case MasterRecoveryPseudoGTID:
		{
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	mustPromoteOtherCoMaster := config.Config.CoMasterRecoveryMustPromoteOtherCoMaster
	if !otherCoMaster.ReadOnly {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: other co-master %+v is writeable hence has to be promoted", otherCoMaster.Key))
		mustPromoteOtherCoMaster = true
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster? %+v", mustPromoteOtherCoMaster))

	if promotedReplica != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
		if mustPromoteOtherCoMaster {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster. Verifying that %+v is/can be promoted", *otherCoMasterKey))
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, otherCoMasterKey)
		} else {
			// We are allowed to promote any server
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, nil)
		}
		topologyRecovery.AddError(err)
	}
	if promotedReplica != nil {
		if mustPromoteOtherCoMaster && !promotedReplica.Key.Equals(otherCoMasterKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not manage to promote other-co-master %+v; was only able to promote %+v; mustPromoteOtherCoMaster is true (either CoMasterRecoveryMustPromoteOtherCoMaster is true, or co-master is writeable), therefore failing", *otherCoMasterKey, promotedReplica.Key))
			promotedReplica = nil
		}
	}
	if promotedReplica != nil {
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting to ensure the SQL thread catches up on %+v", promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return promotedReplica, lostReplicas, err
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("SQL thread caught up on %+v", promotedReplica.Key))
		}
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
	}

	// OK, we may have someone promoted. Either this was the other co-master or another replica.
	// Noting down that we DO NOT attempt to set a new co-master topology. We are good with remaining with a single master.
	// I tried solving the "let's promote a replica and create a new co-master setup" but this turns so complex due to various factors.
	// I see this as risky and not worth the questionable benefit.
	// Maybe future me is a smarter person and finds a simple solution. Unlikely. I'm getting dumber.
	//
	// ...
	// Now that we're convinved, take a look at what we can be left with:
	// Say we started with M1<->M2<-S1, with M2 failing, and we promoted S1.
	// We now have M1->S1 (because S1 is promoted), S1->M2 (because that's what it remembers), M2->M1 (because that's what it remembers)
	// !! This is an evil 3-node circle that must be broken.
	// config.Config.ApplyMySQLPromotionAfterMasterFailover, if true, will cause it to break, because we would RESET SLAVE on S1
	// but we want to make sure the circle is broken no matter what.
	// So in the case we promoted not-the-other-co-master, we issue a detach-replica-master-host, which is a reversible operation
	if promotedReplica != nil && !promotedReplica.Key.Equals(otherCoMasterKey) {
		_, err = inst.DetachReplicaMasterHost(&promotedReplica.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadCoMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMasterHost(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadCoMaster, detaching %+v replicas", len(lostReplicas)))
	}

	func() error {
		inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	return promotedReplica, lostReplicas, err
}

// checkAndRecoverDeadCoMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadCoMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadCoMasterCounter.Inc(1)
	promotedReplica, lostReplicas, err := RecoverDeadCoMaster(topologyRecovery, skipProcesses)
	resolveRecovery(topologyRecovery, promotedReplica)
	if promotedReplica == nil {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "Failure: no replica promoted.")
	} else {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, fmt.Sprintf("promoted: %+v", promotedReplica.Key))
	}
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	if promotedReplica != nil {
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return false, nil, log.Errorf("Promoted replica %+v: sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		// success
		recoverDeadCoMasterSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMasterFailover {
			AuditTopologyRecovery(topologyRecovery, "- RecoverDeadMaster: will apply MySQL changes to promoted master")
			inst.SetReadOnly(&promotedReplica.Key, false)
		}
		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverLockedSemiSyncMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {

	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, true, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverLockedSemiSyncMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	return false, nil, nil
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
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
// This can be useful in scenarios where the master has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the master is bad.
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
	log.Errore(err)
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

func getCheckAndRecoverFunction(analysisCode inst.AnalysisCode, analyzedInstanceKey *inst.InstanceKey) (
	checkAndRecoverFunction func(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
	isActionableRecovery bool,
) {
	switch analysisCode {
	// master
	case inst.DeadMaster, inst.DeadMasterAndSomeReplicas:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverDeadMaster, true
		}
	case inst.LockedSemiSyncMaster:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverLockedSemiSyncMaster, true
		}
	// topo
	case inst.ClusterHasNoMaster:
		return electNewMaster, true
	case inst.MasterHasMaster:
		return fixClusterAndMaster, true
	case inst.MasterIsReadOnly, inst.MasterSemiSyncMustBeSet, inst.MasterSemiSyncMustNotBeSet:
		return fixMaster, true
	case inst.NotConnectedToMaster, inst.ConnectedToWrongMaster, inst.ReplicationStopped, inst.ReplicaIsWritable,
		inst.ReplicaSemiSyncMustBeSet, inst.ReplicaSemiSyncMustNotBeSet:
		return fixReplica, false
	// intermediate master
	case inst.DeadIntermediateMaster:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterAndSomeReplicas:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterWithSingleReplicaFailingToConnect:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.AllIntermediateMasterReplicasFailingToConnectOrDead:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterAndReplicas:
		return checkAndRecoverGenericProblem, false
	// co-master
	case inst.DeadCoMaster:
		return checkAndRecoverDeadCoMaster, true
	case inst.DeadCoMasterAndSomeReplicas:
		return checkAndRecoverDeadCoMaster, true
	// master, non actionable
	case inst.DeadMasterAndReplicas:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMaster:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMasterWithLaggingReplicas:
		return checkAndRecoverGenericProblem, false
	case inst.AllMasterReplicasNotReplicating:
		return checkAndRecoverGenericProblem, false
	case inst.AllMasterReplicasNotReplicatingOrDead:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableIntermediateMasterWithLaggingReplicas:
		return checkAndRecoverGenericProblem, false
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllMasterReplicasStale:
	//   return checkAndRecoverGenericProblem, false

	return nil, false
}

func runEmergentOperations(analysisEntry *inst.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case inst.DeadMasterAndReplicas:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	case inst.UnreachableMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.UnreachableMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.LockedSemiSyncMasterHypothesis:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyRecordStaleBinlogCoordinates(&analysisEntry.AnalyzedInstanceKey, &analysisEntry.AnalyzedInstanceBinlogCoordinates)
	case inst.UnreachableIntermediateMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMasterReplicasNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMasterReplicasNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.FirstTierReplicaFailingToConnectToMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	}
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	atomic.AddInt64(&countPendingRecoveries, 1)
	defer atomic.AddInt64(&countPendingRecoveries, -1)

	checkAndRecoverFunction, isActionableRecovery := getCheckAndRecoverFunction(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
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

	if orcraft.IsRaftEnabled() {
		// with raft, all nodes can (and should) run analysis,
		// but only the leader proceeds to execute detection hooks and then to failover.
		if !orcraft.IsLeader() {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT detecting/recovering host (raft non-leader)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
			return false, nil, err
		}
	}

	// Initiate detection:
	registrationSuccess, _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses)
	if registrationSuccess {
		if orcraft.IsRaftEnabled() {
			_, err := orcraft.PublishCommand("register-failure-detection", analysisEntry)
			log.Errore(err)
		}
	}
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

	// Actually attempt recovery:
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}
	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
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

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *inst.InstanceKey, err error) {
	// Allow the analysis to run even if we don't want to recover
	replicationAnalysis, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{IncludeDowntimed: true, AuditAnalysis: true})
	if err != nil {
		return false, nil, log.Errore(err)
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
			log.Errore(err)
			if topologyRecovery != nil {
				promotedReplicaKey = topologyRecovery.SuccessorKey
			}
		} else {
			go func() {
				_, _, err := executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
				log.Errore(err)
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

// ForceMasterFailover *trusts* master of given cluster is dead and initiates a failover
func ForceMasterFailover(clusterName string) (topologyRecovery *TopologyRecovery, err error) {
	clusterMasters, err := inst.ReadClusterMaster(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	if len(clusterMasters) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	clusterMaster := clusterMasters[0]

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.ForceMasterFailoverCommandHint, &clusterMaster.Key)
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

// ForceMasterTakeover *trusts* master of given cluster is dead and fails over to designated instance,
// which has to be its direct child.
func ForceMasterTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	clusterMasters, err := inst.ReadClusterWriteableMaster(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	if len(clusterMasters) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	clusterMaster := clusterMasters[0]

	if !destination.MasterKey.Equals(&clusterMaster.Key) {
		return nil, fmt.Errorf("You may only promote a direct child of the master %+v. The master of %+v is %+v.", clusterMaster.Key, destination.Key, destination.MasterKey)
	}
	log.Infof("Will demote %+v and promote %+v instead", clusterMaster.Key, destination.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.ForceMasterTakeoverCommandHint, &clusterMaster.Key)
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

func getGracefulMasterTakeoverDesignatedInstance(clusterMasterKey *inst.InstanceKey, designatedKey *inst.InstanceKey, clusterMasterDirectReplicas [](*inst.Instance), auto bool) (designatedInstance *inst.Instance, err error) {
	if designatedKey == nil {
		// User did not specify a replica to promote
		if len(clusterMasterDirectReplicas) == 1 {
			// Single replica. That's the one we'll promote
			return clusterMasterDirectReplicas[0], nil
		}
		// More than one replica.
		if !auto {
			return nil, fmt.Errorf("GracefulMasterTakeover: target instance not indicated, auto=false, and master %+v has %+v replicas. orchestrator cannot choose where to failover to. Aborting", *clusterMasterKey, len(clusterMasterDirectReplicas))
		}
		log.Debugf("GracefulMasterTakeover: request takeover for master %+v, no designated replica indicated. orchestrator will attempt to auto deduce replica.", *clusterMasterKey)
		designatedInstance, _, _, _, _, err = inst.GetCandidateReplica(clusterMasterKey, false)
		if err != nil || designatedInstance == nil {
			return nil, fmt.Errorf("GracefulMasterTakeover: no target instance indicated, failed to auto-detect candidate replica for master %+v. Aborting", *clusterMasterKey)
		}
		log.Debugf("GracefulMasterTakeover: candidateReplica=%+v", designatedInstance.Key)
		if _, err := inst.StartReplication(&designatedInstance.Key); err != nil {
			return nil, fmt.Errorf("GracefulMasterTakeover:cannot start replication on designated replica %+v. Aborting", designatedKey)
		}
		log.Infof("GracefulMasterTakeover: designated master deduced to be %+v", designatedInstance.Key)
		return designatedInstance, nil
	}

	// Verify designated instance is a direct replica of master
	for _, directReplica := range clusterMasterDirectReplicas {
		if directReplica.Key.Equals(designatedKey) {
			designatedInstance = directReplica
		}
	}
	if designatedInstance == nil {
		return nil, fmt.Errorf("GracefulMasterTakeover: indicated designated instance %+v must be directly replicating from the master %+v", *designatedKey, *clusterMasterKey)
	}
	log.Infof("GracefulMasterTakeover: designated master instructed to be %+v", designatedInstance.Key)
	return designatedInstance, nil
}

// GracefulMasterTakeover will demote master of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the master, then wait
// for the designated replica to catch up with last position.
// It will point old master at the newly promoted master at the correct coordinates.
func GracefulMasterTakeover(clusterName string, designatedKey *inst.InstanceKey, auto bool) (topologyRecovery *TopologyRecovery, promotedMasterCoordinates *inst.BinlogCoordinates, err error) {
	clusterMasters, err := inst.ReadClusterMaster(clusterName)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v; error: %+v", clusterName, err)
	}
	if len(clusterMasters) != 1 {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v. Found %+v potential masters", clusterName, len(clusterMasters))
	}
	clusterMaster := clusterMasters[0]

	clusterMasterDirectReplicas, err := inst.ReadReplicaInstances(&clusterMaster.Key)
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if len(clusterMasterDirectReplicas) == 0 {
		return nil, nil, fmt.Errorf("Master %+v doesn't seem to have replicas", clusterMaster.Key)
	}

	if designatedKey != nil && !designatedKey.IsValid() {
		// An empty or invalid key is as good as no key
		designatedKey = nil
	}
	designatedInstance, err := getGracefulMasterTakeoverDesignatedInstance(&clusterMaster.Key, designatedKey, clusterMasterDirectReplicas, auto)
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if inst.IsBannedFromBeingCandidateReplica(designatedInstance) {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: designated instance %+v cannot be promoted due to promotion rule or it is explicitly ignored in PromotionIgnoreHostnameFilters configuration", designatedInstance.Key)
	}

	masterOfDesignatedInstance, err := inst.GetInstanceMaster(designatedInstance)
	if err != nil {
		return nil, nil, err
	}
	if !masterOfDesignatedInstance.Key.Equals(&clusterMaster.Key) {
		return nil, nil, fmt.Errorf("Sanity check failure. It seems like the designated instance %+v does not replicate from the master %+v (designated instance's master key is %+v). This error is strange. Panicking", designatedInstance.Key, clusterMaster.Key, designatedInstance.MasterKey)
	}
	if !designatedInstance.HasReasonableMaintenanceReplicationLag() {
		return nil, nil, fmt.Errorf("Desginated instance %+v seems to be lagging to much for thie operation. Aborting.", designatedInstance.Key)
	}

	if len(clusterMasterDirectReplicas) > 1 {
		log.Infof("GracefulMasterTakeover: Will let %+v take over its siblings", designatedInstance.Key)
		relocatedReplicas, _, err, _ := inst.RelocateReplicas(&clusterMaster.Key, &designatedInstance.Key, "")
		if len(relocatedReplicas) != len(clusterMasterDirectReplicas)-1 {
			// We are unable to make designated instance master of all its siblings
			relocatedReplicasKeyMap := inst.NewInstanceKeyMap()
			relocatedReplicasKeyMap.AddInstances(relocatedReplicas)
			// Let's see which replicas have not been relocated
			for _, directReplica := range clusterMasterDirectReplicas {
				if relocatedReplicasKeyMap.HasKey(directReplica.Key) {
					// relocated, good
					continue
				}
				if directReplica.Key.Equals(&designatedInstance.Key) {
					// obviously we skip this one
					continue
				}
				if directReplica.IsDowntimed {
					// obviously we skip this one
					log.Warningf("GracefulMasterTakeover: unable to relocate %+v below designated %+v, but since it is downtimed (downtime reason: %s) I will proceed", directReplica.Key, designatedInstance.Key, directReplica.DowntimeReason)
					continue
				}
				return nil, nil, fmt.Errorf("Desginated instance %+v cannot take over all of its siblings. Error: %+v", designatedInstance.Key, err)
			}
		}
	}
	log.Infof("GracefulMasterTakeover: Will demote %+v and promote %+v instead", clusterMaster.Key, designatedInstance.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.GracefulMasterTakeoverCommandHint, &clusterMaster.Key)
	if err != nil {
		return nil, nil, err
	}
	preGracefulTakeoverTopologyRecovery := &TopologyRecovery{
		SuccessorKey:  &designatedInstance.Key,
		AnalysisEntry: analysisEntry,
	}
	if err := executeProcesses(config.Config.PreGracefulTakeoverProcesses, "PreGracefulTakeoverProcesses", preGracefulTakeoverTopologyRecovery, true); err != nil {
		return nil, nil, fmt.Errorf("Failed running PreGracefulTakeoverProcesses: %+v", err)
	}
	demotedMasterSelfBinlogCoordinates := &clusterMaster.SelfBinlogCoordinates
	log.Infof("GracefulMasterTakeover: Will wait for %+v to reach master coordinates %+v", designatedInstance.Key, *demotedMasterSelfBinlogCoordinates)
	if designatedInstance, _, err = inst.WaitForExecBinlogCoordinatesToReach(&designatedInstance.Key, demotedMasterSelfBinlogCoordinates, time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds)*time.Second); err != nil {
		return nil, nil, err
	}
	promotedMasterCoordinates = &designatedInstance.SelfBinlogCoordinates

	log.Infof("GracefulMasterTakeover: attempting recovery")
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &designatedInstance.Key, false)
	if err != nil {
		log.Errorf("GracefulMasterTakeover: noting an error, and for now proceeding: %+v", err)
	}
	if !recoveryAttempted {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: recovery attempted but with no results. This should not happen")
	}
	var gtidHint inst.OperationGTIDHint = inst.GTIDHintNeutral
	if topologyRecovery.RecoveryType == MasterRecoveryGTID {
		gtidHint = inst.GTIDHintForce
	}
	clusterMaster, err = inst.ChangeMasterTo(&clusterMaster.Key, &designatedInstance.Key, promotedMasterCoordinates, false, gtidHint)
	if !clusterMaster.SelfBinlogCoordinates.Equals(demotedMasterSelfBinlogCoordinates) {
		log.Errorf("GracefulMasterTakeover: sanity problem. Demoted master's coordinates changed from %+v to %+v while supposed to have been frozen", *demotedMasterSelfBinlogCoordinates, clusterMaster.SelfBinlogCoordinates)
	}
	_, startReplicationErr := inst.StartReplication(&clusterMaster.Key)
	if err == nil {
		err = startReplicationErr
	}

	if designatedInstance.AllowTLS {
		_, enableSSLErr := inst.EnableMasterSSL(&clusterMaster.Key)
		if err == nil {
			err = enableSSLErr
		}
	}
	executeProcesses(config.Config.PostGracefulTakeoverProcesses, "PostGracefulTakeoverProcesses", topologyRecovery, false)

	return topologyRecovery, promotedMasterCoordinates, err
}

// electNewMaster elects a new master while none were present before.
// TODO(sougou): this should be mreged with recoverDeadMaster
func electNewMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another electNewMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will elect a new master: %v", analysisEntry.Analysis, analysisEntry.SuggestedClusterAlias)

	unlock, err := LockShard(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: NOT detecting/recovering host, could not obtain shard lock (%v)",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
		return false, topologyRecovery, err
	}
	defer unlock(&err)

	// TODO(sougou): check if another Orc succeeded before fixing anything.

	replicas, err := inst.ReadClusterAliasInstances(analysisEntry.SuggestedClusterAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	// TODO(sougou): this is not reliable, because of the timeout.
	replicas = inst.StopReplicasNicely(replicas, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	if len(replicas) == 0 {
		return false, topologyRecovery, fmt.Errorf("no instances in cluster %v", analysisEntry.SuggestedClusterAlias)
	}

	// Find an initial candidate
	var candidate *inst.Instance
	for _, replica := range replicas {
		// TODO(sougou): this needs to do more. see inst.chooseCandidateReplica
		if !inst.IsBannedFromBeingCandidateReplica(replica) {
			candidate = replica
			break
		}
	}
	if candidate == nil {
		err := fmt.Errorf("no candidate qualifies to be a master")
		AuditTopologyRecovery(topologyRecovery, err.Error())
		return true, topologyRecovery, err
	}

	// Compare the current candidate with the rest to see if other instances can be
	// moved under. If not, see if the other intance can become a candidate instead.
	for _, replica := range replicas {
		if replica == candidate {
			continue
		}
		if err := inst.CheckMoveViaGTID(replica, candidate); err != nil {
			if err := inst.CheckMoveViaGTID(candidate, replica); err != nil {
				return false, topologyRecovery, fmt.Errorf("instances are not compatible: %+v %+v: %v", candidate, replica, err)
			} else {
				// Make sure the new candidate meets the requirements.
				if !inst.IsBannedFromBeingCandidateReplica(replica) {
					candidate = replica
				}
			}
		}
	}

	if _, err := inst.ChangeTabletType(candidate.Key, topodatapb.TabletType_MASTER); err != nil {
		return true, topologyRecovery, err
	}
	// TODO(sougou): parallelize
	for _, replica := range replicas {
		if replica.Key == candidate.Key {
			continue
		}
		if _, err := inst.MoveBelowGTID(&replica.Key, &candidate.Key); err != nil {
			return false, topologyRecovery, err
		}
	}
	count := inst.MasterSemiSync(candidate.Key)
	err = inst.SetSemiSyncMaster(&candidate.Key, count > 0)
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- electNewMaster: applying semi-sync %v: success=%t", count > 0, (err == nil)))
	if err != nil {
		return false, topologyRecovery, err
	}
	_, err = inst.SetReadOnly(&candidate.Key, false)
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- electNewMaster: set read-only false: success=%t", (err == nil)))
	if err != nil {
		return false, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// fixClusterAndMaster performs a traditional vitess PlannedReparentShard.
func fixClusterAndMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixClusterAndMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix incorrect mastership %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)

	// Reset replication on current master. This will prevent the comaster code-path.
	// TODO(sougou): this should probably done while holding a lock.
	_, err = inst.ResetReplicationOperation(&analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		return false, topologyRecovery, err
	}

	altAnalysis, err := forceAnalysisEntry(analysisEntry.ClusterDetails.ClusterName, inst.DeadMaster, "", &analysisEntry.AnalyzedInstanceMasterKey)
	if err != nil {
		return false, topologyRecovery, err
	}
	recoveryAttempted, topologyRecovery, err = ForceExecuteRecovery(altAnalysis, &analysisEntry.AnalyzedInstanceKey, false)
	if err != nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if _, err := TabletRefresh(analysisEntry.AnalyzedInstanceKey); err != nil {
		log.Errore(err)
	}
	return recoveryAttempted, topologyRecovery, err
}

// fixMaster sets the master as read-write.
func fixMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix master to read-write %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)

	unlock, err := LockShard(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: NOT detecting/recovering host, could not obtain shard lock (%v)",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
		return false, topologyRecovery, err
	}
	defer unlock(&err)

	// TODO(sougou): this code pattern has reached DRY limits. Reuse.
	count := inst.MasterSemiSync(analysisEntry.AnalyzedInstanceKey)
	err = inst.SetSemiSyncMaster(&analysisEntry.AnalyzedInstanceKey, count > 0)
	//AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- fixMaster: applying semi-sync %v: success=%t", count > 0, (err == nil)))
	if err != nil {
		return false, topologyRecovery, err
	}

	if err := TabletUndoDemoteMaster(analysisEntry.AnalyzedInstanceKey); err != nil {
		return false, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}

// fixReplica sets the replica as read-only and points it at the current master.
func fixReplica(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, false, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another fixReplica.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	log.Infof("Analysis: %v, will fix replica %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)

	unlock, err := LockShard(analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: NOT detecting/recovering host, could not obtain shard lock (%v)",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses, err)
		return false, topologyRecovery, err
	}
	defer unlock(&err)

	if _, err := inst.SetReadOnly(&analysisEntry.AnalyzedInstanceKey, true); err != nil {
		return false, topologyRecovery, err
	}

	masterKey, err := ShardMaster(&analysisEntry.AnalyzedInstanceKey)
	if err != nil {
		log.Info("Could not compute master for %+v", analysisEntry.AnalyzedInstanceKey)
		return false, topologyRecovery, err
	}
	if _, err := inst.MoveBelowGTID(&analysisEntry.AnalyzedInstanceKey, masterKey); err != nil {
		return false, topologyRecovery, err
	}
	return true, topologyRecovery, nil
}
