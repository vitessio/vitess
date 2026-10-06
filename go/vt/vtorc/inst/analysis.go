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
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
)

type AnalysisCode string

const (
	NoProblem                              AnalysisCode = "NoProblem"
	ClusterHasNoPrimary                    AnalysisCode = "ClusterHasNoPrimary"
	PrimaryTabletDeleted                   AnalysisCode = "PrimaryTabletDeleted"
	IncapacitatedPrimary                   AnalysisCode = "IncapacitatedPrimary"
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
	UnreachablePrimaryWithBrokenReplicas   AnalysisCode = "UnreachablePrimaryWithBrokenReplicas"
	PrimarySingleReplicaNotReplicating     AnalysisCode = "PrimarySingleReplicaNotReplicating"
	PrimarySingleReplicaDead               AnalysisCode = "PrimarySingleReplicaDead"
	AllPrimaryReplicasNotReplicating       AnalysisCode = "AllPrimaryReplicasNotReplicating"
	AllPrimaryReplicasNotReplicatingOrDead AnalysisCode = "AllPrimaryReplicasNotReplicatingOrDead"
	LockedSemiSyncPrimaryHypothesis        AnalysisCode = "LockedSemiSyncPrimaryHypothesis"
	PrimarySemiSyncBlocked                 AnalysisCode = "PrimarySemiSyncBlocked"
	ErrantGTIDDetected                     AnalysisCode = "ErrantGTIDDetected"
	PrimaryDiskStalled                     AnalysisCode = "PrimaryDiskStalled"
	PrimaryTabletUnreachableByQuorum       AnalysisCode = "PrimaryTabletUnreachableByQuorum"

	// GroupPrimaryNotInTopo describes a tablet whose MySQL is the ONLINE primary of its shard's
	// replication group, with quorum, while the tablet is not the topology primary. The tablet
	// normally promotes itself; VTOrc promotes it when it does not.
	GroupPrimaryNotInTopo AnalysisCode = "GroupPrimaryNotInTopo"
	// GroupMemberNotOnline describes a tablet that is a voter of its shard's replication group
	// (Shard.group_replication_voters), but whose MySQL is not an active member, while the group
	// is active on other tablets.
	GroupMemberNotOnline AnalysisCode = "GroupMemberNotOnline"
	// GroupNotBootstrapped describes a shard whose durability policy uses Group Replication, but
	// on which no tablet is an active member of the group, while all voters are reachable.
	GroupNotBootstrapped AnalysisCode = "GroupNotBootstrapped"
	// GroupBootstrapNotRecorded describes a tablet that is the target of its shard's bootstrap
	// intent (Shard.group_replication_bootstrap_intent), whose MySQL is the primary of a group of
	// another incarnation than the shard record lists: the bootstrap happened, but its reply was
	// lost before the incarnation was recorded. VTOrc adopts the group.
	GroupBootstrapNotRecorded AnalysisCode = "GroupBootstrapNotRecorded"
	// GroupVotersOutOfDate describes a shard whose voters VTOrc changes (see PlanGroupVoters): no
	// voter is listed yet (InitialVoters), a voter failed and a spare of its cell takes its seat
	// (SwapVoter), a cell with an eligible tablet has no voter (GrowVoter), or a voter whose tablet
	// record was deleted has no spare and leaves the list (RemoveVoter). It is reported on a single
	// tablet of the shard.
	GroupVotersOutOfDate AnalysisCode = "GroupVotersOutOfDate"
	// GroupPrimaryNotVoter describes the tablet whose MySQL is the primary of its shard's legitimate
	// replication group while it is not a voter. It does not serve; VTOrc moves the group primary to
	// an ONLINE voter of its view.
	GroupPrimaryNotVoter AnalysisCode = "GroupPrimaryNotVoter"
	// GroupVotersBelowTarget describes a shard whose group has fewer voters than cells with an
	// eligible tablet, or fewer than policy.MinGroupReplicationCells, and that VTOrc cannot grow now:
	// for example, its tablets that may be voters are in too few cells, so that VTOrc writes no initial
	// voter list and bootstraps no group. It is reported on a single tablet of the shard, and has no
	// recovery.
	GroupVotersBelowTarget AnalysisCode = "GroupVotersBelowTarget"
	// GroupVoterUnreplaceable describes a shard with a voter that failed (unreachable for
	// --group-replication-voter-replacement-grace-period, and active in no view of the group) whose
	// cell has no valid spare. It is reported on a single tablet of the shard, and has no recovery.
	GroupVoterUnreplaceable AnalysisCode = "GroupVoterUnreplaceable"
	// GroupVoterRecordDeleted describes a shard with a voter whose tablet record was deleted while its
	// MySQL is still active in the group, or still in the view of the group primary while its cell has
	// no spare: VTOrc only replaces or removes such a voter once it left the group. It is reported on a
	// single tablet of the shard, and has no recovery.
	GroupVoterRecordDeleted AnalysisCode = "GroupVoterRecordDeleted"
	// GroupQuorumLost describes a shard whose group has active members, none of which has quorum.
	// The group cannot commit. VTOrc does not act; forcing a new membership is an operator decision.
	GroupQuorumLost AnalysisCode = "GroupQuorumLost"
	// GroupCellMajority describes a shard with the group_replication_cross_cell durability policy
	// whose ONLINE group members are in the majority in a single cell.
	GroupCellMajority AnalysisCode = "GroupCellMajority"

	// StaleTopoPrimary describes when a tablet still has the type PRIMARY in the topology when a newer primary
	// has been elected. VTOrc should demote this primary to a replica.
	StaleTopoPrimary AnalysisCode = "StaleTopoPrimary"
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

type DetectionAnalysisHints struct {
	AuditAnalysis bool
}

// DetectionAnalysis represents an analysis of a detected problem.
type DetectionAnalysis struct {
	AnalyzedInstanceAlias        *topodatapb.TabletAlias
	AnalyzedInstancePrimaryAlias *topodatapb.TabletAlias

	// TabletType is the tablet's type as seen in the topology.
	TabletType topodatapb.TabletType

	// IsTabletShutdown is true when the analyzed tablet's record carries a TabletShutdownTime,
	// i.e. its vttablet was gracefully shut down (an intentional operator action) rather than
	// crashing. The quorum-confirmed ERS path fails closed when this is set so an intentionally
	// shut down primary is never failed over.
	IsTabletShutdown bool

	// CurrentTabletType is the type this tablet is currently running as.
	CurrentTabletType topodatapb.TabletType

	PrimaryTimeStamp                          time.Time
	AnalyzedKeyspace                          string
	AnalyzedShard                             string
	AnalyzedCell                              string
	AnalyzedKeyspaceEmergencyReparentDisabled bool
	AnalyzedShardEmergencyReparentDisabled    bool
	// ShardPrimaryTermTimestamp is the primary term start time stored in the shard record.
	ShardPrimaryTermTimestamp         time.Time
	AnalyzedInstanceBinlogCoordinates BinlogCoordinates
	IsPrimary                         bool
	IsClusterPrimary                  bool
	LastCheckValid                    bool
	PrimaryHealthUnhealthy            bool
	LastCheckPartialSuccess           bool
	CountReplicas                     uint
	// ShardEligibleObservers is the number of REPLICA/RDONLY tablets in the shard (from topo),
	// i.e. the population eligible to vote in the shard-peer health quorum. It is the expected
	// observer count fed to the quorum gate, derived independently of the primary's instance data
	// so it is available even when VTOrc has never reached the primary (the cold-start case).
	ShardEligibleObservers                    int
	CountValidReplicas                        uint
	CountValidReplicatingReplicas             uint
	CountValidSemiSyncReplicatingReplicas     uint
	ReplicationStopped                        bool
	ErrantGTID                                string
	ReplicaNetTimeout                         int32
	HeartbeatInterval                         float64
	Analysis                                  AnalysisCode
	AnalysisMatchedProblems                   []*DetectionAnalysisProblemMeta
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

	// AnalyzedServerUUID is the server_uuid of the analyzed tablet's MySQL, as last seen.
	AnalyzedServerUUID string
	// GroupReplicationPluginActive, GroupMemberState, GroupMemberRole, GroupHasQuorum and
	// GroupOnlineMembers are the MySQL Group Replication state of the analyzed tablet, as last seen.
	GroupReplicationPluginActive bool
	GroupMemberState             string
	GroupMemberRole              string
	GroupHasQuorum               bool
	GroupOnlineMembers           uint
	// IsGroupMemberActive is true when the analyzed tablet's MySQL is an ONLINE or RECOVERING
	// member of a replication group. Such a tablet replicates through the group and not through
	// the default replication channel.
	IsGroupMemberActive bool
	// IsGroupPrimary is true when the analyzed tablet's MySQL is the ONLINE primary of a group
	// that has quorum.
	IsGroupPrimary bool
	// GroupStartInProgress is true when a START GROUP_REPLICATION runs on the analyzed tablet's
	// MySQL, as last seen: it reports OFFLINE, but its membership is changing.
	GroupStartInProgress bool
	// ShardGroupActiveMembers is the number of tablets of the shard that VTOrc reached on its
	// last check and whose MySQL is an active group member.
	ShardGroupActiveMembers uint
	// ShardGroupQuorumMembers is the number of those active members that have quorum.
	ShardGroupQuorumMembers uint
	// ShardGroupPrimaryAlias is the tablet whose MySQL is the shard's group primary, if VTOrc
	// reached it.
	ShardGroupPrimaryAlias *topodatapb.TabletAlias
	// ShardGroupPrimaryUUID is the server_uuid of the group primary, as reported by the reachable
	// members that have quorum.
	ShardGroupPrimaryUUID string
	// ShardGroupVotingMembers is the number of voters of the shard's group, as recorded in the
	// shard record.
	ShardGroupVotingMembers uint
	// ShardGroupUnreachableVotingMembers is the number of voters that VTOrc could not reach on its
	// last check.
	ShardGroupUnreachableVotingMembers uint
	// ShardGroupCellMajority is the cell that holds a majority of the shard's ONLINE members, if any.
	ShardGroupCellMajority string
	// ShardGroupVoters are the voters of the shard's group, as recorded in the shard record.
	ShardGroupVoters []*topodatapb.TabletAlias
	// ShardGroupDesiredVoters are the voters that the shard's durability policy selects, when
	// VTOrc may change them.
	ShardGroupDesiredVoters []*topodatapb.TabletAlias
	// ShardGroupLegitimateActiveMembers is the number of reachable tablets of the shard whose MySQL
	// is an active member of the shard's legitimate group (the incarnation recorded in the shard
	// record, if any) with quorum in its view.
	ShardGroupLegitimateActiveMembers uint
	// IsGroupMemberForeign is true when the analyzed tablet's MySQL is active in a group of
	// another incarnation than the one the shard record lists.
	IsGroupMemberForeign bool
	// GroupViewIncarnation is the incarnation of the analyzed tablet's MySQL's view of its group,
	// and ShardGroupIncarnation the incarnation that the shard record lists.
	GroupViewIncarnation  string
	ShardGroupIncarnation string
	// IsGroupBootstrapIntentTarget is true when the analyzed tablet is the target of the shard's
	// current bootstrap intent.
	IsGroupBootstrapIntentTarget bool
	// IsLegitimateGroupPrimary is true when the analyzed tablet's MySQL is the primary of the
	// shard's legitimate group: the recorded incarnation, and a majority of the listed voters
	// ONLINE in its view (see policy.LegitimateGroup).
	IsLegitimateGroupPrimary bool
	// IsGroupVoter is true when the analyzed tablet is a voter of its shard's group. Tablets that
	// are not voters replicate asynchronously from the primary.
	IsGroupVoter bool
	// shardGroupAnyActive is true when any tablet of the shard, reachable or not, last reported
	// an active member, or a START GROUP_REPLICATION in progress.
	shardGroupAnyActive bool
	// shardGroupAnyMember is true when any tablet of the shard, reachable or not, last reported an
	// active member: shardGroupAnyActive without the STARTs in progress.
	shardGroupAnyMember bool
	// shardReachableNonMemberPrimary is true when VTOrc reached a PRIMARY tablet of the shard whose
	// MySQL is not an active group member.
	shardReachableNonMemberPrimary bool
	// groupVoterAnalysis is the shard-wide voter analysis of the analyzed tablet's shard, when it is
	// reported on the analyzed tablet (see PlanGroupVoters), and GroupVoterReason says why.
	groupVoterAnalysis AnalysisCode
	GroupVoterReason   string

	QuorumDetail *QuorumResult `json:",omitempty"`
}

// hasMinSemiSyncAckers returns true if there are a minimum number of semi-sync ackers enabled and replicating.
// True is always returned if the durability policy does not require semi-sync ackers (eg: "none"). This gives
// a useful signal if it is safe to enable semi-sync without risk of stalling ongoing PRIMARY writes.
func hasMinSemiSyncAckers(durabler policy.Durabler, primary *topodatapb.Tablet, analysis *DetectionAnalysis) bool {
	if durabler == nil || analysis == nil {
		return false
	}
	return int(analysis.CountValidSemiSyncReplicatingReplicas) >= durabler.SemiSyncAckers(primary)
}

func (detectionAnalysis *DetectionAnalysis) MarshalJSON() ([]byte, error) {
	i := struct {
		DetectionAnalysis
	}{
		DetectionAnalysis: *detectionAnalysis,
	}

	return json.Marshal(i)
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.GetInstancePollSeconds()
}
