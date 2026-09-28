/*
Copyright 2026 The Vitess Authors.
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSortDetectionAnalysisMatchedProblems(t *testing.T) {
	worstPriority := 10
	testCases := []struct {
		name               string
		in                 []*DetectionAnalysisProblem
		postSortByAnalysis []AnalysisCode
	}{
		{
			name: "default",
			in: []*DetectionAnalysisProblem{
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    InvalidReplica,
						Description: "should be 2nd-last, not a shardWideAction, low priority",
						Priority:    detectionAnalysisPriorityLow,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    InvalidReplica,
						Description: "should be last, not a shardWideAction, worst priority",
						Priority:    worstPriority,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    PrimaryIsReadOnly,
						Description: "should be after DeadPrimary, high priority",
						Priority:    detectionAnalysisPriorityHigh,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    PrimarySemiSyncMustBeSet,
						Description: "should be after ReplicaSemiSyncMustBeSet, has an after dependency",
						Priority:    detectionAnalysisPriorityMedium,
					},
					AfterAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    ReplicaSemiSyncMustBeSet,
						Description: "should be before PrimarySemiSyncMustBeSet, has a before dependency",
						Priority:    detectionAnalysisPriorityMedium,
					},
					BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    DeadPrimary,
						Description: "should be 1st, shard-wide action priority",
						Priority:    detectionAnalysisPriorityShardWideAction,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    ReplicaSemiSyncMustNotBeSet,
						Description: "should be after PrimarySemiSyncMustNotBeSet, has an after dependency",
						Priority:    detectionAnalysisPriorityMedium,
					},
					AfterAnalyses: []AnalysisCode{PrimarySemiSyncMustNotBeSet},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis:    PrimarySemiSyncMustNotBeSet,
						Description: "should be before ReplicaSemiSyncMustNotBeSet, has a before dependency",
						Priority:    detectionAnalysisPriorityMedium,
					},
					BeforeAnalyses: []AnalysisCode{ReplicaSemiSyncMustNotBeSet},
				},
			},
			postSortByAnalysis: []AnalysisCode{
				DeadPrimary,
				PrimaryIsReadOnly,
				ReplicaSemiSyncMustBeSet,
				PrimarySemiSyncMustBeSet,
				PrimarySemiSyncMustNotBeSet,
				ReplicaSemiSyncMustNotBeSet,
				InvalidReplica,
				InvalidReplica,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sorted := testCase.in
			sortDetectionAnalysisMatchedProblems(sorted)

			require.Len(t, sorted, len(testCase.postSortByAnalysis))
			for i, analysis := range testCase.postSortByAnalysis {
				assert.Equal(t, analysis, sorted[i].Meta.Analysis)
			}

			// confirm last problem has the worstPriority
			require.Equal(t, worstPriority, sorted[len(sorted)-1].Meta.Priority)
		})
	}
}

func TestRequiresOrderedExecution(t *testing.T) {
	tests := []struct {
		name     string
		problem  *DetectionAnalysisProblem
		expected bool
	}{
		{
			name: "shard-wide action priority",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityShardWideAction},
			},
			expected: true,
		},
		{
			name: "critical priority",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityCritical},
			},
			expected: false,
		},
		{
			name: "has BeforeAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
				BeforeAnalyses: []AnalysisCode{DeadPrimary},
			},
			expected: true,
		},
		{
			name: "has AfterAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta:          &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
				AfterAnalyses: []AnalysisCode{DeadPrimary},
			},
			expected: true,
		},
		{
			name: "referenced by another problem's BeforeAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustNotBeSet, Priority: detectionAnalysisPriorityMedium},
			},
			expected: true,
		},
		{
			name: "referenced by another problem's AfterAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustNotBeSet, Priority: detectionAnalysisPriorityMedium},
			},
			expected: true,
		},
		{
			// ReplicationStopped declares BeforeAnalyses: [PrimarySemiSyncBlocked],
			// so it requires ordered execution.
			name:     "ReplicationStopped has BeforeAnalyses dependency",
			problem:  GetDetectionAnalysisProblem(ReplicationStopped),
			expected: true,
		},
		{
			name: "independent problem",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityLow},
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.problem.RequiresOrderedExecution())
		})
	}
}

func TestGetDetectionAnalysisProblem(t *testing.T) {
	problem := GetDetectionAnalysisProblem(DeadPrimary)
	require.NotNil(t, problem)
	assert.Equal(t, DeadPrimary, problem.Meta.Analysis)

	problem = GetDetectionAnalysisProblem("NonExistentCode")
	assert.Nil(t, problem)
}

func TestCompareDetectionAnalysisProblems(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *DetectionAnalysisProblem
		expected int
	}{
		{
			name: "shard-wide action beats non-shard-wide",
			a: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityShardWideAction},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityHigh},
			},
			expected: -1,
		},
		{
			name: "higher priority wins",
			a: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityHigh},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityLow},
			},
			expected: -1,
		},
		{
			name: "equal priority",
			a: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
			},
			expected: 0,
		},
		{
			name: "before dependency - MustBeSet",
			a: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustBeSet},
				BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustBeSet},
			},
			expected: -1,
		},
		{
			name: "before dependency - MustNotBeSet",
			a: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustNotBeSet},
				BeforeAnalyses: []AnalysisCode{ReplicaSemiSyncMustNotBeSet},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustNotBeSet},
			},
			expected: -1,
		},
		{
			name: "after dependency - MustNotBeSet",
			a: &DetectionAnalysisProblem{
				Meta:          &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustNotBeSet},
				AfterAnalyses: []AnalysisCode{PrimarySemiSyncMustNotBeSet},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustNotBeSet},
			},
			expected: 1,
		},
		{
			name: "before dependency - ReplicationStopped before PrimarySemiSyncBlocked",
			a: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{Analysis: ReplicationStopped},
				BeforeAnalyses: []AnalysisCode{PrimarySemiSyncBlocked},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncBlocked},
			},
			expected: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareDetectionAnalysisProblems(tt.a, tt.b))
		})
	}
}

func TestGroupDetectionAnalysesByShard(t *testing.T) {
	analyses := []*DetectionAnalysis{
		{
			Analysis:         ReplicationStopped,
			AnalyzedKeyspace: "ks1",
			AnalyzedShard:    "0",
			TabletType:       topodatapb.TabletType_REPLICA,
		},
		{
			Analysis:         DeadPrimary,
			AnalyzedKeyspace: "ks1",
			AnalyzedShard:    "0",
			TabletType:       topodatapb.TabletType_PRIMARY,
		},
		{
			Analysis:         PrimaryIsReadOnly,
			AnalyzedKeyspace: "ks2",
			AnalyzedShard:    "0",
			TabletType:       topodatapb.TabletType_PRIMARY,
		},
	}

	result := GroupDetectionAnalysesByShard(analyses)

	require.Len(t, result, 2)

	// ks1/0 should have 2 entries, sorted with DeadPrimary first
	ks1 := result["ks1/0"]
	require.Len(t, ks1, 2)
	assert.Equal(t, DeadPrimary, ks1[0].Analysis)
	assert.Equal(t, ReplicationStopped, ks1[1].Analysis)

	// ks2/0 should have 1 entry
	ks2 := result["ks2/0"]
	require.Len(t, ks2, 1)
	assert.Equal(t, PrimaryIsReadOnly, ks2[0].Analysis)
}

func TestPrimaryTabletUnreachableByQuorumMatch(t *testing.T) {
	now := time.Now()
	primary := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}

	resetShardPeerHealth()
	RecordShardPeerHealth(&topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, topodatapb.TabletType_REPLICA, "ks", "0",
		[]*replicationdatapb.ShardPeerHealth{{TabletAlias: primary, ConsecutivePingFailures: 5, LastAttemptedPing: protoutil.TimeToProto(now)}}, now)
	RecordShardPeerHealth(&topodatapb.TabletAlias{Cell: "zone1", Uid: 102}, topodatapb.TabletType_REPLICA, "ks", "0",
		[]*replicationdatapb.ShardPeerHealth{{TabletAlias: primary, ConsecutivePingFailures: 5, LastAttemptedPing: protoutil.TimeToProto(now)}}, now)

	a := &DetectionAnalysis{
		IsClusterPrimary:       true,
		LastCheckValid:         false,
		AnalyzedInstanceAlias:  primary,
		AnalyzedKeyspace:       "ks",
		AnalyzedShard:          "0",
		ShardEligibleObservers: 2, // both replicas report down -> a majority of the shard
	}
	problem := GetDetectionAnalysisProblem(PrimaryTabletUnreachableByQuorum)
	require.NotNil(t, problem)
	assert.False(t, problem.MatchFunc(a, &clusterAnalysis{}, nil, &topodatapb.Tablet{Alias: primary}, false, false), "feature is disabled by default")
	assert.True(t, matchPrimaryTabletUnreachableByQuorum(a, now))

	// When the matcher fires, it records the structured quorum detail for the audit.
	require.NotNil(t, a.QuorumDetail, "matcher must record the quorum detail when it fires")
	assert.True(t, a.QuorumDetail.Down)
	assert.Equal(t, 2, a.QuorumDetail.DownVotes)

	// Minority protection through the matcher: the same two fresh down votes must NOT fire ERS
	// when the shard is known (via ShardEligibleObservers from topo) to have more eligible observers
	// that are not reporting down — a minority view cannot drive a failover.
	minority := &DetectionAnalysis{
		IsClusterPrimary:       true,
		LastCheckValid:         false,
		AnalyzedInstanceAlias:  primary,
		AnalyzedKeyspace:       "ks",
		AnalyzedShard:          "0",
		ShardEligibleObservers: 5, // 2 of 5 reporting down is not a majority of the shard
	}
	assert.False(t, matchPrimaryTabletUnreachableByQuorum(minority, now))
	assert.Nil(t, minority.QuorumDetail, "the matcher must not record a quorum detail when it does not fire")

	// If VTOrc can still reach the primary, no match.
	a.LastCheckValid = true
	assert.False(t, problem.MatchFunc(a, &clusterAnalysis{}, nil, &topodatapb.Tablet{Alias: primary}, false, false))

	// The non-firing path must not record a quorum detail. Use a fresh analysis because the
	// earlier firing already set QuorumDetail on `a`.
	notFired := &DetectionAnalysis{IsClusterPrimary: true, LastCheckValid: true, AnalyzedInstanceAlias: primary, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
	assert.False(t, matchPrimaryTabletUnreachableByQuorum(notFired, now))
	assert.Nil(t, notFired.QuorumDetail)

	// An intentionally shut down primary must not fire ERS even though the same fresh quorum reports
	// its vttablet down: a graceful shutdown stamps TabletShutdownTime and is an operator action.
	shutdown := &DetectionAnalysis{
		IsClusterPrimary:       true,
		LastCheckValid:         false,
		IsTabletShutdown:       true,
		AnalyzedInstanceAlias:  primary,
		AnalyzedKeyspace:       "ks",
		AnalyzedShard:          "0",
		ShardEligibleObservers: 2,
	}
	assert.False(t, matchPrimaryTabletUnreachableByQuorum(shutdown, now), "intentionally shut down primary must not be failed over")
	assert.Nil(t, shutdown.QuorumDetail, "no quorum detail recorded when the matcher fails closed on shutdown")
}
