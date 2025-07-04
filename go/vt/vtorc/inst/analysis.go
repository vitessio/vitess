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
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/config"
)

type AnalysisCode string

const (
	NoProblem                              AnalysisCode = "NoProblem"
	ClusterHasNoPrimary                    AnalysisCode = "ClusterHasNoPrimary"
	PrimaryTabletDeleted                   AnalysisCode = "PrimaryTabletDeleted"
	InvalidPrimary                         AnalysisCode = "InvalidPrimary"
	InvalidReplica                         AnalysisCode = "InvalidReplica"
	DeadPrimaryWithoutReplicas             AnalysisCode = "DeadPrimaryWithoutReplicas"
	DeadPrimary                            AnalysisCode = "DeadPrimary"
	DeadPrimaryAndReplicas                 AnalysisCode = "DeadPrimaryAndReplicas"
	DeadPrimaryAndSomeReplicas             AnalysisCode = "DeadPrimaryAndSomeReplicas"
	PrimaryHasPrimary                      AnalysisCode = "PrimaryHasPrimary"
	PrimaryIsReadOnly                      AnalysisCode = "PrimaryIsReadOnly"
	PrimaryCurrentTypeMismatch             AnalysisCode = "PrimaryCurrentTypeMismatch"
	PrimarySemiSyncMustBeSet               AnalysisCode = "PrimarySemiSyncMustBeSet"
	PrimarySemiSyncMustNotBeSet            AnalysisCode = "PrimarySemiSyncMustNotBeSet"
	ReplicaIsWritable                      AnalysisCode = "ReplicaIsWritable"
	NotConnectedToPrimary                  AnalysisCode = "NotConnectedToPrimary"
	ConnectedToWrongPrimary                AnalysisCode = "ConnectedToWrongPrimary"
	ReplicationStopped                     AnalysisCode = "ReplicationStopped"
	ReplicaSemiSyncMustBeSet               AnalysisCode = "ReplicaSemiSyncMustBeSet"
	ReplicaSemiSyncMustNotBeSet            AnalysisCode = "ReplicaSemiSyncMustNotBeSet"
	ReplicaMisconfigured                   AnalysisCode = "ReplicaMisconfigured"
	UnreachablePrimaryWithLaggingReplicas  AnalysisCode = "UnreachablePrimaryWithLaggingReplicas"
	UnreachablePrimary                     AnalysisCode = "UnreachablePrimary"
	PrimarySingleReplicaNotReplicating     AnalysisCode = "PrimarySingleReplicaNotReplicating"
	PrimarySingleReplicaDead               AnalysisCode = "PrimarySingleReplicaDead"
	AllPrimaryReplicasNotReplicating       AnalysisCode = "AllPrimaryReplicasNotReplicating"
	AllPrimaryReplicasNotReplicatingOrDead AnalysisCode = "AllPrimaryReplicasNotReplicatingOrDead"
	LockedSemiSyncPrimaryHypothesis        AnalysisCode = "LockedSemiSyncPrimaryHypothesis"
	LockedSemiSyncPrimary                  AnalysisCode = "LockedSemiSyncPrimary"
	PrimarySemiSyncBlocked                 AnalysisCode = "PrimarySemiSyncBlocked"
	ErrantGTIDDetected                     AnalysisCode = "ErrantGTIDDetected"
	PrimaryDiskStalled                     AnalysisCode = "PrimaryDiskStalled"
)

type StructureAnalysisCode string

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

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceAlias        string
	AnalyzedInstancePrimaryAlias string
	TabletType                   topodatapb.TabletType
	CurrentTabletType            topodatapb.TabletType
	PrimaryTimeStamp             time.Time
	AnalyzedKeyspace             string
	AnalyzedShard                string
	// ShardPrimaryTermTimestamp is the primary term start time stored in the shard record.
	ShardPrimaryTermTimestamp                 time.Time
	AnalyzedInstanceBinlogCoordinates         BinlogCoordinates
	IsPrimary                                 bool
	IsClusterPrimary                          bool
	LastCheckValid                            bool
	LastCheckPartialSuccess                   bool
	CountReplicas                             uint
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	ReplicationStopped                        bool
	ErrantGTID                                string
	ReplicaNetTimeout                         int32
	HeartbeatInterval                         float64
	Analysis                                  AnalysisCode
	Description                               string
	StructureAnalysis                         []StructureAnalysisCode
	OracleGTIDImmediateTopology               bool
	BinlogServerImmediateTopology             bool
	SemiSyncPrimaryEnabled                    bool
	SemiSyncPrimaryStatus                     bool
	SemiSyncPrimaryWaitForReplicaCount        uint
	SemiSyncPrimaryClients                    uint
	SemiSyncReplicaEnabled                    bool
	SemiSyncBlocked                           bool
	CountSemiSyncReplicasEnabled              uint
	CountLoggingReplicas                      uint
	CountStatementBasedLoggingReplicas        uint
	CountMixedBasedLoggingReplicas            uint
	CountRowBasedLoggingReplicas              uint
	CountDistinctMajorVersionsLoggingReplicas uint
	CountDelayedReplicas                      uint
	CountLaggingReplicas                      uint
	IsActionableRecovery                      bool
	RecoveryId                                int64
	GTIDMode                                  string
	MinReplicaGTIDMode                        string
	MaxReplicaGTIDMode                        string
	MaxReplicaGTIDErrant                      string
	IsReadOnly                                bool
	IsDiskStalled                             bool
}

func (replicationAnalysis *ReplicationAnalysis) MarshalJSON() ([]byte, error) {
	i := struct {
		ReplicationAnalysis
	}{}
	i.ReplicationAnalysis = *replicationAnalysis

	return json.Marshal(i)
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.GetInstancePollSeconds()
}
