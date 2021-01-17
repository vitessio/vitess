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

package inst

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type AnalysisCode string
type StructureAnalysisCode string

const (
	NoProblem                                               AnalysisCode = "NoProblem"
	ClusterHasNoMaster                                      AnalysisCode = "ClusterHasNoMaster"
	DeadMasterWithoutReplicas                               AnalysisCode = "DeadMasterWithoutReplicas"
	DeadMaster                                              AnalysisCode = "DeadMaster"
	DeadMasterAndReplicas                                   AnalysisCode = "DeadMasterAndReplicas"
	DeadMasterAndSomeReplicas                               AnalysisCode = "DeadMasterAndSomeReplicas"
	MasterHasMaster                                         AnalysisCode = "MasterHasMaster"
	MasterIsReadOnly                                        AnalysisCode = "MasterIsReadOnly"
	MasterSemiSyncMustBeSet                                 AnalysisCode = "MasterSemiSyncMustBeSet"
	MasterSemiSyncMustNotBeSet                              AnalysisCode = "MasterSemiSyncMustNotBeSet"
	ReplicaIsWritable                                       AnalysisCode = "ReplicaIsWritable"
	NotConnectedToMaster                                    AnalysisCode = "NotConnectedToMaster"
	ConnectedToWrongMaster                                  AnalysisCode = "ConnectedToWrongMaster"
	ReplicationStopped                                      AnalysisCode = "ReplicationStopped"
	ReplicaSemiSyncMustBeSet                                AnalysisCode = "ReplicaSemiSyncMustBeSet"
	ReplicaSemiSyncMustNotBeSet                             AnalysisCode = "ReplicaSemiSyncMustNotBeSet"
	UnreachableMasterWithLaggingReplicas                    AnalysisCode = "UnreachableMasterWithLaggingReplicas"
	UnreachableMaster                                       AnalysisCode = "UnreachableMaster"
	MasterSingleReplicaNotReplicating                       AnalysisCode = "MasterSingleReplicaNotReplicating"
	MasterSingleReplicaDead                                 AnalysisCode = "MasterSingleReplicaDead"
	AllMasterReplicasNotReplicating                         AnalysisCode = "AllMasterReplicasNotReplicating"
	AllMasterReplicasNotReplicatingOrDead                   AnalysisCode = "AllMasterReplicasNotReplicatingOrDead"
	LockedSemiSyncMasterHypothesis                          AnalysisCode = "LockedSemiSyncMasterHypothesis"
	LockedSemiSyncMaster                                    AnalysisCode = "LockedSemiSyncMaster"
	MasterWithoutReplicas                                   AnalysisCode = "MasterWithoutReplicas"
	DeadCoMaster                                            AnalysisCode = "DeadCoMaster"
	DeadCoMasterAndSomeReplicas                             AnalysisCode = "DeadCoMasterAndSomeReplicas"
	UnreachableCoMaster                                     AnalysisCode = "UnreachableCoMaster"
	AllCoMasterReplicasNotReplicating                       AnalysisCode = "AllCoMasterReplicasNotReplicating"
	DeadIntermediateMaster                                  AnalysisCode = "DeadIntermediateMaster"
	DeadIntermediateMasterWithSingleReplica                 AnalysisCode = "DeadIntermediateMasterWithSingleReplica"
	DeadIntermediateMasterWithSingleReplicaFailingToConnect AnalysisCode = "DeadIntermediateMasterWithSingleReplicaFailingToConnect"
	DeadIntermediateMasterAndSomeReplicas                   AnalysisCode = "DeadIntermediateMasterAndSomeReplicas"
	DeadIntermediateMasterAndReplicas                       AnalysisCode = "DeadIntermediateMasterAndReplicas"
	UnreachableIntermediateMasterWithLaggingReplicas        AnalysisCode = "UnreachableIntermediateMasterWithLaggingReplicas"
	UnreachableIntermediateMaster                           AnalysisCode = "UnreachableIntermediateMaster"
	AllIntermediateMasterReplicasFailingToConnectOrDead     AnalysisCode = "AllIntermediateMasterReplicasFailingToConnectOrDead"
	AllIntermediateMasterReplicasNotReplicating             AnalysisCode = "AllIntermediateMasterReplicasNotReplicating"
	FirstTierReplicaFailingToConnectToMaster                AnalysisCode = "FirstTierReplicaFailingToConnectToMaster"
	BinlogServerFailingToConnectToMaster                    AnalysisCode = "BinlogServerFailingToConnectToMaster"
)

const (
	StatementAndMixedLoggingReplicasStructureWarning     StructureAnalysisCode = "StatementAndMixedLoggingReplicasStructureWarning"
	StatementAndRowLoggingReplicasStructureWarning       StructureAnalysisCode = "StatementAndRowLoggingReplicasStructureWarning"
	MixedAndRowLoggingReplicasStructureWarning           StructureAnalysisCode = "MixedAndRowLoggingReplicasStructureWarning"
	MultipleMajorVersionsLoggingReplicasStructureWarning StructureAnalysisCode = "MultipleMajorVersionsLoggingReplicasStructureWarning"
	NoLoggingReplicasStructureWarning                    StructureAnalysisCode = "NoLoggingReplicasStructureWarning"
	DifferentGTIDModesStructureWarning                   StructureAnalysisCode = "DifferentGTIDModesStructureWarning"
	ErrantGTIDStructureWarning                           StructureAnalysisCode = "ErrantGTIDStructureWarning"
	NoFailoverSupportStructureWarning                    StructureAnalysisCode = "NoFailoverSupportStructureWarning"
	NoWriteableMasterStructureWarning                    StructureAnalysisCode = "NoWriteableMasterStructureWarning"
	NotEnoughValidSemiSyncReplicasStructureWarning       StructureAnalysisCode = "NotEnoughValidSemiSyncReplicasStructureWarning"
)

type InstanceAnalysis struct {
	key      *InstanceKey
	analysis AnalysisCode
}

func NewInstanceAnalysis(instanceKey *InstanceKey, analysis AnalysisCode) *InstanceAnalysis {
	return &InstanceAnalysis{
		key:      instanceKey,
		analysis: analysis,
	}
}

func (instanceAnalysis *InstanceAnalysis) String() string {
	return fmt.Sprintf("%s/%s", instanceAnalysis.key.StringCode(), string(instanceAnalysis.analysis))
}

// PeerAnalysisMap indicates the number of peers agreeing on an analysis.
// Key of this map is a InstanceAnalysis.String()
type PeerAnalysisMap map[string]int

type ReplicationAnalysisHints struct {
	IncludeDowntimed bool
	IncludeNoProblem bool
	AuditAnalysis    bool
}

const (
	ForceMasterFailoverCommandHint    string = "force-master-failover"
	ForceMasterTakeoverCommandHint    string = "force-master-takeover"
	GracefulMasterTakeoverCommandHint string = "graceful-master-takeover"
)

type AnalysisInstanceType string

const (
	AnalysisInstanceTypeMaster             AnalysisInstanceType = "master"
	AnalysisInstanceTypeCoMaster           AnalysisInstanceType = "co-master"
	AnalysisInstanceTypeIntermediateMaster AnalysisInstanceType = "intermediate-master"
)

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceKey                       InstanceKey
	AnalyzedInstanceMasterKey                 InstanceKey
	TabletType                                topodatapb.TabletType
	MasterTimeStamp                           time.Time
	SuggestedClusterAlias                     string
	ClusterDetails                            ClusterInfo
	AnalyzedInstanceDataCenter                string
	AnalyzedInstanceRegion                    string
	AnalyzedInstancePhysicalEnvironment       string
	AnalyzedInstanceBinlogCoordinates         BinlogCoordinates
	IsMaster                                  bool
	IsClusterMaster                           bool
	IsCoMaster                                bool
	LastCheckValid                            bool
	LastCheckPartialSuccess                   bool
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountReplicasFailingToConnectToMaster     uint
	CountDowntimedReplicas                    uint
	ReplicationDepth                          uint
	Replicas                                  InstanceKeyMap
	SlaveHosts                                InstanceKeyMap // for backwards compatibility. Equals `Replicas`
	IsFailingToConnectToMaster                bool
	ReplicationStopped                        bool
	Analysis                                  AnalysisCode
	Description                               string
	StructureAnalysis                         []StructureAnalysisCode
	IsDowntimed                               bool
	IsReplicasDowntimed                       bool // as good as downtimed because all replicas are downtimed AND analysis is all about the replicas (e.e. AllMasterReplicasNotReplicating)
	DowntimeEndTimestamp                      string
	DowntimeRemainingSeconds                  int
	IsBinlogServer                            bool
	PseudoGTIDImmediateTopology               bool
	OracleGTIDImmediateTopology               bool
	MariaDBGTIDImmediateTopology              bool
	BinlogServerImmediateTopology             bool
	SemiSyncMasterEnabled                     bool
	SemiSyncMasterStatus                      bool
	SemiSyncMasterWaitForReplicaCount         uint
	SemiSyncMasterClients                     uint
	SemiSyncReplicaEnabled                    bool
	CountSemiSyncReplicasEnabled              uint
	CountLoggingReplicas                      uint
	CountStatementBasedLoggingReplicas        uint
	CountMixedBasedLoggingReplicas            uint
	CountRowBasedLoggingReplicas              uint
	CountDistinctMajorVersionsLoggingReplicas uint
	CountDelayedReplicas                      uint
	CountLaggingReplicas                      uint
	IsActionableRecovery                      bool
	ProcessingNodeHostname                    string
	ProcessingNodeToken                       string
	CountAdditionalAgreeingNodes              int
	StartActivePeriod                         string
	SkippableDueToDowntime                    bool
	GTIDMode                                  string
	MinReplicaGTIDMode                        string
	MaxReplicaGTIDMode                        string
	MaxReplicaGTIDErrant                      string
	CommandHint                               string
	IsReadOnly                                bool
}

type AnalysisMap map[string](*ReplicationAnalysis)

type ReplicationAnalysisChangelog struct {
	AnalyzedInstanceKey InstanceKey
	Changelog           []string
}

func (this *ReplicationAnalysis) MarshalJSON() ([]byte, error) {
	i := struct {
		ReplicationAnalysis
	}{}
	i.ReplicationAnalysis = *this
	// backwards compatibility
	i.SlaveHosts = i.Replicas

	return json.Marshal(i)
}

// ReadReplicaHostsFromString parses and reads replica keys from comma delimited string
func (this *ReplicationAnalysis) ReadReplicaHostsFromString(replicaHostsString string) error {
	this.Replicas = *NewInstanceKeyMap()
	return this.Replicas.ReadCommaDelimitedList(replicaHostsString)
}

// AnalysisString returns a human friendly description of all analysis issues
func (this *ReplicationAnalysis) AnalysisString() string {
	result := []string{}
	if this.Analysis != NoProblem {
		result = append(result, string(this.Analysis))
	}
	for _, structureAnalysis := range this.StructureAnalysis {
		result = append(result, string(structureAnalysis))
	}
	return strings.Join(result, ", ")
}

// Get a string description of the analyzed instance type (master? co-master? intermediate-master?)
func (this *ReplicationAnalysis) GetAnalysisInstanceType() AnalysisInstanceType {
	if this.IsCoMaster {
		return AnalysisInstanceTypeCoMaster
	}
	if this.IsMaster {
		return AnalysisInstanceTypeMaster
	}
	return AnalysisInstanceTypeIntermediateMaster
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.Config.InstancePollSeconds + 1
}
