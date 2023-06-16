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
	"strings"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/config"
)

type AnalysisCode string
type StructureAnalysisCode string

const (
	NoProblem                              AnalysisCode = "NoProblem"
	ClusterHasNoPrimary                    AnalysisCode = "ClusterHasNoPrimary"
	DeadPrimaryWithoutReplicas             AnalysisCode = "DeadPrimaryWithoutReplicas"
	DeadPrimary                            AnalysisCode = "DeadPrimary"
	DeadPrimaryAndReplicas                 AnalysisCode = "DeadPrimaryAndReplicas"
	DeadPrimaryAndSomeReplicas             AnalysisCode = "DeadPrimaryAndSomeReplicas"
	PrimaryHasPrimary                      AnalysisCode = "PrimaryHasPrimary"
	PrimaryIsReadOnly                      AnalysisCode = "PrimaryIsReadOnly"
	PrimarySemiSyncMustBeSet               AnalysisCode = "PrimarySemiSyncMustBeSet"
	PrimarySemiSyncMustNotBeSet            AnalysisCode = "PrimarySemiSyncMustNotBeSet"
	ReplicaIsWritable                      AnalysisCode = "ReplicaIsWritable"
	NotConnectedToPrimary                  AnalysisCode = "NotConnectedToPrimary"
	ConnectedToWrongPrimary                AnalysisCode = "ConnectedToWrongPrimary"
	ReplicationStopped                     AnalysisCode = "ReplicationStopped"
	ReplicaSemiSyncMustBeSet               AnalysisCode = "ReplicaSemiSyncMustBeSet"
	ReplicaSemiSyncMustNotBeSet            AnalysisCode = "ReplicaSemiSyncMustNotBeSet"
	UnreachablePrimaryWithLaggingReplicas  AnalysisCode = "UnreachablePrimaryWithLaggingReplicas"
	UnreachablePrimary                     AnalysisCode = "UnreachablePrimary"
	PrimarySingleReplicaNotReplicating     AnalysisCode = "PrimarySingleReplicaNotReplicating"
	PrimarySingleReplicaDead               AnalysisCode = "PrimarySingleReplicaDead"
	AllPrimaryReplicasNotReplicating       AnalysisCode = "AllPrimaryReplicasNotReplicating"
	AllPrimaryReplicasNotReplicatingOrDead AnalysisCode = "AllPrimaryReplicasNotReplicatingOrDead"
	LockedSemiSyncPrimaryHypothesis        AnalysisCode = "LockedSemiSyncPrimaryHypothesis"
	LockedSemiSyncPrimary                  AnalysisCode = "LockedSemiSyncPrimary"
	PrimaryWithoutReplicas                 AnalysisCode = "PrimaryWithoutReplicas"
	BinlogServerFailingToConnectToPrimary  AnalysisCode = "BinlogServerFailingToConnectToPrimary"
	GraceFulPrimaryTakeover                AnalysisCode = "GracefulPrimaryTakeover"
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
	NoWriteablePrimaryStructureWarning                   StructureAnalysisCode = "NoWriteablePrimaryStructureWarning"
	NotEnoughValidSemiSyncReplicasStructureWarning       StructureAnalysisCode = "NotEnoughValidSemiSyncReplicasStructureWarning"
)

// PeerAnalysisMap indicates the number of peers agreeing on an analysis.
// Key of this map is a InstanceAnalysis.String()
type PeerAnalysisMap map[string]int

type ReplicationAnalysisHints struct {
	AuditAnalysis bool
}

type AnalysisInstanceType string

const (
	AnalysisInstanceTypePrimary             AnalysisInstanceType = "primary"
	AnalysisInstanceTypeCoPrimary           AnalysisInstanceType = "co-primary"
	AnalysisInstanceTypeIntermediatePrimary AnalysisInstanceType = "intermediate-primary"
)

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceHostname                  string
	AnalyzedInstancePort                      int
	AnalyzedInstanceAlias                     string
	AnalyzedInstancePrimaryAlias              string
	TabletType                                topodatapb.TabletType
	PrimaryTimeStamp                          time.Time
	ClusterDetails                            ClusterInfo
	AnalyzedInstanceDataCenter                string
	AnalyzedInstanceRegion                    string
	AnalyzedKeyspace                          string
	AnalyzedShard                             string
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
	ReplicationDepth                          uint
	IsFailingToConnectToPrimary               bool
	ReplicationStopped                        bool
	Analysis                                  AnalysisCode
	Description                               string
	StructureAnalysis                         []StructureAnalysisCode
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
	StartActivePeriod                         string
	GTIDMode                                  string
	MinReplicaGTIDMode                        string
	MaxReplicaGTIDMode                        string
	MaxReplicaGTIDErrant                      string
	IsReadOnly                                bool
}

func (replicationAnalysis *ReplicationAnalysis) MarshalJSON() ([]byte, error) {
	i := struct {
		ReplicationAnalysis
	}{}
	i.ReplicationAnalysis = *replicationAnalysis

	return json.Marshal(i)
}

// AnalysisString returns a human friendly description of all analysis issues
func (replicationAnalysis *ReplicationAnalysis) AnalysisString() string {
	result := []string{}
	if replicationAnalysis.Analysis != NoProblem {
		result = append(result, string(replicationAnalysis.Analysis))
	}
	for _, structureAnalysis := range replicationAnalysis.StructureAnalysis {
		result = append(result, string(structureAnalysis))
	}
	return strings.Join(result, ", ")
}

// Get a string description of the analyzed instance type (primary? co-primary? intermediate-primary?)
func (replicationAnalysis *ReplicationAnalysis) GetAnalysisInstanceType() AnalysisInstanceType {
	if replicationAnalysis.IsCoPrimary {
		return AnalysisInstanceTypeCoPrimary
	}

	if replicationAnalysis.IsPrimary {
		return AnalysisInstanceTypePrimary
	}
	return AnalysisInstanceTypeIntermediatePrimary
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.Config.InstancePollSeconds + 1
}
