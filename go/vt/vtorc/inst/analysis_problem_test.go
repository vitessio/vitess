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
						Description: "should be after DeadPrimary, not a shardWideAction, high priority",
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
						Analysis:           DeadPrimary,
						Description:        "should be 1st, HasShardWideAction is always critical priority",
						HasShardWideAction: true,
					},
				},
			},
			postSortByAnalysis: []AnalysisCode{
				DeadPrimary,
				PrimaryIsReadOnly,
				ReplicaSemiSyncMustBeSet,
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
			name: "critical priority",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Priority: detectionAnalysisPriorityCritical},
			},
			expected: true,
		},
		{
			name: "HasShardWideAction",
			problem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{HasShardWideAction: true},
			},
			expected: true,
		},
		{
			name: "has BeforeAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{},
				BeforeAnalyses: []AnalysisCode{DeadPrimary},
			},
			expected: true,
		},
		{
			name: "has AfterAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta:          &DetectionAnalysisProblemMeta{},
				AfterAnalyses: []AnalysisCode{DeadPrimary},
			},
			expected: true,
		},
		{
			name: "has both BeforeAnalyses and AfterAnalyses",
			problem: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{},
				BeforeAnalyses: []AnalysisCode{ReplicaSemiSyncMustBeSet},
				AfterAnalyses:  []AnalysisCode{PrimarySemiSyncMustBeSet},
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
			name: "shard-wide beats non-shard-wide",
			a: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{HasShardWideAction: true},
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
			name: "before dependency",
			a: &DetectionAnalysisProblem{
				Meta:           &DetectionAnalysisProblemMeta{Analysis: ReplicaSemiSyncMustBeSet},
				BeforeAnalyses: []AnalysisCode{PrimarySemiSyncMustBeSet},
			},
			b: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{Analysis: PrimarySemiSyncMustBeSet},
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

func TestSortDetectionAnalyses(t *testing.T) {
	analyses := []*DetectionAnalysis{
		{Analysis: ReplicationStopped, AnalyzedInstanceAlias: "replica1"},
		{Analysis: DeadPrimary, AnalyzedInstanceAlias: "primary"},
		{Analysis: PrimaryIsReadOnly, AnalyzedInstanceAlias: "primary2"},
	}
	sortDetectionAnalyses(analyses)

	// DeadPrimary (shard-wide) should be first
	assert.Equal(t, DeadPrimary, analyses[0].Analysis)
	// PrimaryIsReadOnly (high) before ReplicationStopped (medium)
	assert.Equal(t, PrimaryIsReadOnly, analyses[1].Analysis)
	assert.Equal(t, ReplicationStopped, analyses[2].Analysis)
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
