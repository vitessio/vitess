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

package registry

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestNoOpStrategy_Lifecycle tests the Start and Stop methods of NoOpStrategy.
func TestNoOpStrategy_Lifecycle(t *testing.T) {
	strategy := &NoOpStrategy{}

	// Test that Start and Stop can be called without errors
	// These are no-ops but should not panic
	strategy.Start()
	strategy.Stop()

	// Test multiple Start/Stop calls (should be safe)
	strategy.Start()
	strategy.Start()
	strategy.Stop()
	strategy.Stop()

	// Verify Evaluate still works after Start/Stop
	decision := strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT 1"}, 0, QueryAttributes{WorkloadName: "test", Priority: 100})
	require.False(t, decision.Throttle, "NoOpStrategy should never throttle")
}

func TestNoOpStrategy_Evaluate(t *testing.T) {
	tests := []struct {
		name           string
		giveTabletType topodatapb.TabletType
		giveSQL        string
		expectedResult ThrottleDecision
	}{
		{
			name:           "no throttling for tablet type primary",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveSQL:        "SELECT * FROM users",
			expectedResult: ThrottleDecision{
				Throttle: false,
				Message:  "NoOpStrategy: no throttling applied",
			},
		},
		{
			name:           "no throttling for tablet type replica",
			giveTabletType: topodatapb.TabletType_REPLICA,
			giveSQL:        "INSERT INTO logs VALUES (1)",
			expectedResult: ThrottleDecision{
				Throttle: false,
				Message:  "NoOpStrategy: no throttling applied",
			},
		},
		{
			name:           "no throttling for tablet type rdonly",
			giveTabletType: topodatapb.TabletType_RDONLY,
			giveSQL:        "UPDATE stats SET count = count + 1",
			expectedResult: ThrottleDecision{
				Throttle: false,
				Message:  "NoOpStrategy: no throttling applied",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &NoOpStrategy{}

			result := strategy.Evaluate(context.Background(), tt.giveTabletType, &sqlparser.ParsedQuery{Query: tt.giveSQL}, 0, QueryAttributes{WorkloadName: "test", Priority: 100})
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
