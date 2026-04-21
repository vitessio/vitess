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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			// Per-tablet sort uses priority only (no dependency ordering).
			// Equal-priority items preserve original order (stable sort).
			name: "priority only",
			in: []*DetectionAnalysisProblem{
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis: InvalidReplica,
						Priority: detectionAnalysisPriorityLow,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis: InvalidReplica,
						Priority: worstPriority,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis: PrimaryIsReadOnly,
						Priority: detectionAnalysisPriorityHigh,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis: PrimarySemiSyncMustBeSet,
						Priority: detectionAnalysisPriorityMedium,
					},
				},
				{
					Meta: &DetectionAnalysisProblemMeta{
						Analysis: DeadPrimary,
						Priority: detectionAnalysisPriorityShardWideAction,
					},
				},
			},
			postSortByAnalysis: []AnalysisCode{
				DeadPrimary,
				PrimaryIsReadOnly,
				PrimarySemiSyncMustBeSet,
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
			name: "has BeforeAnalysesFunc",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
				BeforeAnalysesFunc: func(_ *DetectionAnalysis, _ []*DetectionAnalysis) []AnalysisCode {
					return []AnalysisCode{DeadPrimary}
				},
			},
			expected: true,
		},
		{
			name: "has AfterAnalysesFunc",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityMedium},
				AfterAnalysesFunc: func(_ *DetectionAnalysis, _ []*DetectionAnalysis) []AnalysisCode {
					return []AnalysisCode{DeadPrimary}
				},
			},
			expected: true,
		},
		{
			name: "referenced by another problem's BeforeAnalysesFunc",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustNotBeSet, Priority: detectionAnalysisPriorityMedium},
			},
			expected: true,
		},
		{
			name: "referenced by another problem's AfterAnalysesFunc",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustNotBeSet, Priority: detectionAnalysisPriorityMedium},
			},
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
			assert.Equal(t, tt.expected, tt.problem.RequiresOrderedExecution(nil, nil))
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

func TestCompareDetectionAnalyses(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *DetectionAnalysis
		expected int
	}{
		{
			name:     "shard-wide action beats non-shard-wide",
			a:        &DetectionAnalysis{Analysis: DeadPrimary},
			b:        &DetectionAnalysis{Analysis: PrimaryIsReadOnly},
			expected: -1,
		},
		{
			name:     "higher priority wins",
			a:        &DetectionAnalysis{Analysis: PrimaryIsReadOnly},
			b:        &DetectionAnalysis{Analysis: ReplicationStopped},
			expected: -1,
		},
		{
			name:     "equal priority",
			a:        &DetectionAnalysis{Analysis: ReplicationStopped},
			b:        &DetectionAnalysis{Analysis: NotConnectedToPrimary},
			expected: 0,
		},
		{
			// ReplicaSemiSyncMustBeSet declares BeforeAnalysesFunc → [PrimarySemiSyncMustBeSet]
			name:     "before dependency - ReplicaSemiSyncMustBeSet before PrimarySemiSyncMustBeSet",
			a:        &DetectionAnalysis{Analysis: ReplicaSemiSyncMustBeSet},
			b:        &DetectionAnalysis{Analysis: PrimarySemiSyncMustBeSet},
			expected: -1,
		},
		{
			// PrimarySemiSyncMustBeSet declares AfterAnalysesFunc → [ReplicaSemiSyncMustBeSet]
			name:     "after dependency - PrimarySemiSyncMustBeSet after ReplicaSemiSyncMustBeSet",
			a:        &DetectionAnalysis{Analysis: PrimarySemiSyncMustBeSet},
			b:        &DetectionAnalysis{Analysis: ReplicaSemiSyncMustBeSet},
			expected: 1,
		},
		{
			// ReplicationStopped with SemiSyncReplicaEnabled declares
			// BeforeAnalysesFunc → [PrimarySemiSyncBlocked] when PrimarySemiSyncBlocked is present.
			name:     "acker ReplicationStopped before PrimarySemiSyncBlocked",
			a:        &DetectionAnalysis{Analysis: ReplicationStopped, SemiSyncReplicaEnabled: true},
			b:        &DetectionAnalysis{Analysis: PrimarySemiSyncBlocked},
			expected: -1,
		},
		{
			// Non-acker ReplicationStopped should NOT declare the dependency.
			name:     "non-acker ReplicationStopped vs PrimarySemiSyncBlocked uses priority",
			a:        &DetectionAnalysis{Analysis: ReplicationStopped, SemiSyncReplicaEnabled: false},
			b:        &DetectionAnalysis{Analysis: PrimarySemiSyncBlocked},
			expected: 1, // PrimarySemiSyncBlocked is ShardWideAction (priority 0), ReplicationStopped is Medium (priority 3)
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardAnalyses := []*DetectionAnalysis{tt.a, tt.b}
			assert.Equal(t, tt.expected, compareDetectionAnalyses(tt.a, tt.b, shardAnalyses))
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
