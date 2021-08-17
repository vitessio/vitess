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
	NoProblem                                                AnalysisCode = "NoProblem"
	ClusterHasNoPrimary                                      AnalysisCode = "ClusterHasNoMaster"
	DeadPrimaryWithoutReplicas                               AnalysisCode = "DeadMasterWithoutReplicas"
	DeadPrimary                                              AnalysisCode = "DeadMaster"
	DeadPrimaryAndReplicas                                   AnalysisCode = "DeadMasterAndReplicas"
	DeadPrimaryAndSomeReplicas                               AnalysisCode = "DeadMasterAndSomeReplicas"
	PrimaryHasPrimary                                        AnalysisCode = "MasterHasMaster"
	PrimaryIsReadOnly                                        AnalysisCode = "MasterIsReadOnly"
	PrimarySemiSyncMustBeSet                                 AnalysisCode = "MasterSemiSyncMustBeSet"
	PrimarySemiSyncMustNotBeSet                              AnalysisCode = "MasterSemiSyncMustNotBeSet"
	ReplicaIsWritable                                        AnalysisCode = "ReplicaIsWritable"
	NotConnectedToPrimary                                    AnalysisCode = "NotConnectedToMaster"
	ConnectedToWrongPrimary                                  AnalysisCode = "ConnectedToWrongMaster"
	ReplicationStopped                                       AnalysisCode = "ReplicationStopped"
	ReplicaSemiSyncMustBeSet                                 AnalysisCode = "ReplicaSemiSyncMustBeSet"
	ReplicaSemiSyncMustNotBeSet                              AnalysisCode = "ReplicaSemiSyncMustNotBeSet"
	UnreachablePrimaryWithLaggingReplicas                    AnalysisCode = "UnreachableMasterWithLaggingReplicas"
	UnreachablePrimary                                       AnalysisCode = "UnreachableMaster"
	PrimarySingleReplicaNotReplicating                       AnalysisCode = "MasterSingleReplicaNotReplicating"
	PrimarySingleReplicaDead                                 AnalysisCode = "MasterSingleReplicaDead"
	AllPrimaryReplicasNotReplicating                         AnalysisCode = "AllMasterReplicasNotReplicating"
	AllPrimaryReplicasNotReplicatingOrDead                   AnalysisCode = "AllMasterReplicasNotReplicatingOrDead"
	LockedSemiSyncPrimaryHypothesis                          AnalysisCode = "LockedSemiSyncMasterHypothesis"
	LockedSemiSyncPrimary                                    AnalysisCode = "LockedSemiSyncMaster"
	PrimaryWithoutReplicas                                   AnalysisCode = "MasterWithoutReplicas"
	DeadCoPrimary                                            AnalysisCode = "DeadCoMaster"
	DeadCoPrimaryAndSomeReplicas                             AnalysisCode = "DeadCoMasterAndSomeReplicas"
	UnreachableCoPrimary                                     AnalysisCode = "UnreachableCoMaster"
	AllCoPrimaryReplicasNotReplicating                       AnalysisCode = "AllCoMasterReplicasNotReplicating"
	DeadIntermediatePrimary                                  AnalysisCode = "DeadIntermediateMaster"
	DeadIntermediatePrimaryWithSingleReplica                 AnalysisCode = "DeadIntermediateMasterWithSingleReplica"
	DeadIntermediatePrimaryWithSingleReplicaFailingToConnect AnalysisCode = "DeadIntermediateMasterWithSingleReplicaFailingToConnect"
	DeadIntermediatePrimaryAndSomeReplicas                   AnalysisCode = "DeadIntermediateMasterAndSomeReplicas"
	DeadIntermediatePrimaryAndReplicas                       AnalysisCode = "DeadIntermediateMasterAndReplicas"
	UnreachableIntermediatePrimaryWithLaggingReplicas        AnalysisCode = "UnreachableIntermediateMasterWithLaggingReplicas"
	UnreachableIntermediatePrimary                           AnalysisCode = "UnreachableIntermediateMaster"
	AllIntermediatePrimaryReplicasFailingToConnectOrDead     AnalysisCode = "AllIntermediateMasterReplicasFailingToConnectOrDead"
	AllIntermediatePrimaryReplicasNotReplicating             AnalysisCode = "AllIntermediateMasterReplicasNotReplicating"
	FirstTierReplicaFailingToConnectToPrimary                AnalysisCode = "FirstTierReplicaFailingToConnectToMaster"
	BinlogServerFailingToConnectToPrimary                    AnalysisCode = "BinlogServerFailingToConnectToMaster"
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
	NoWriteablePrimaryStructureWarning                   StructureAnalysisCode = "NoWriteableMasterStructureWarning"
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
	ForcePrimaryFailoverCommandHint    string = "force-primary-failover"
	ForcePrimaryTakeoverCommandHint    string = "force-primary-takeover"
	GracefulPrimaryTakeoverCommandHint string = "graceful-primary-takeover"
)

type AnalysisInstanceType string

const (
	AnalysisInstanceTypePrimary             AnalysisInstanceType = "master"
	AnalysisInstanceTypeCoPrimary           AnalysisInstanceType = "co-master"
	AnalysisInstanceTypeIntermediatePrimary AnalysisInstanceType = "intermediate-master"
)

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceKey                       InstanceKey
	AnalyzedInstancePrimaryKey                InstanceKey
	TabletType                                topodatapb.TabletType
	PrimaryTimeStamp                          time.Time
	SuggestedClusterAlias                     string
	ClusterDetails                            ClusterInfo
	AnalyzedInstanceDataCenter                string
	AnalyzedInstanceRegion                    string
	AnalyzedInstancePhysicalEnvironment       string
	AnalyzedInstanceBinlogCoordinates         BinlogCoordinates
	IsPrimary                                 bool
	IsClusterPrimary                          bool
	IsCoPrimary                               bool
	LastCheckValid                            bool
	LastCheckPartialSuccess                   bool
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountReplicasFailingToConnectToPrimary    uint
	CountDowntimedReplicas                    uint
	ReplicationDepth                          uint
	Replicas                                  InstanceKeyMap
	IsFailingToConnectToPrimary               bool
	ReplicationStopped                        bool
	Analysis                                  AnalysisCode
	Description                               string
	StructureAnalysis                         []StructureAnalysisCode
	IsDowntimed                               bool
	IsReplicasDowntimed                       bool // as good as downtimed because all replicas are downtimed AND analysis is all about the replicas (e.e. AllPrimaryReplicasNotReplicating)
	DowntimeEndTimestamp                      string
	DowntimeRemainingSeconds                  int
	IsBinlogServer                            bool
	OracleGTIDImmediateTopology               bool
	MariaDBGTIDImmediateTopology              bool
	BinlogServerImmediateTopology             bool
	SemiSyncPrimaryEnabled                    bool
	SemiSyncPrimaryStatus                     bool
	SemiSyncPrimaryWaitForReplicaCount        uint
	SemiSyncPrimaryClients                    uint
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

// Get a string description of the analyzed instance type (primary? co-primary? intermediate-primary?)
func (this *ReplicationAnalysis) GetAnalysisInstanceType() AnalysisInstanceType {
	if this.IsCoPrimary {
		return AnalysisInstanceTypeCoPrimary
	}
	if this.IsPrimary {
		return AnalysisInstanceTypePrimary
	}
	return AnalysisInstanceTypeIntermediatePrimary
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.Config.InstancePollSeconds + 1
}
