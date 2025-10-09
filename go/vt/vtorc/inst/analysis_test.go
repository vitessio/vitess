/*
Copyright 2025 The Vitess Authors.

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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

func TestHasMinSemiSyncAckers(t *testing.T) {
	durablerNone, _ := policy.GetDurabilityPolicy("none")
	durablerCrossCell, _ := policy.GetDurabilityPolicy("cross_cell")
	tablet := &topodatapb.Tablet{Keyspace: t.Name(), Shard: "-"}

	testCases := []struct {
		name     string
		durabler policy.Durabler
		analysis *DetectionAnalysis
		expect   bool
	}{
		{
			name: "durability policy none",
			analysis: &DetectionAnalysis{
				CountValidSemiSyncReplicatingReplicas: 0,
			},
			durabler: durablerNone,
			expect:   true,
		},
		{
			name:     "durability policy cross_cell without min ackers",
			durabler: durablerCrossCell,
			analysis: &DetectionAnalysis{
				CountValidSemiSyncReplicatingReplicas: 0,
			},
			expect: false,
		},
		{
			name:     "durability policy cross_cell with min ackers",
			durabler: durablerCrossCell,
			analysis: &DetectionAnalysis{
				CountValidSemiSyncReplicatingReplicas: uint(durablerCrossCell.SemiSyncAckers(tablet)),
			},
			expect: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expect, HasMinSemiSyncAckers(testCase.durabler, tablet, testCase.analysis))
		})
	}
}
