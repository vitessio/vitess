package registry

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
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
	decision := strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, "SELECT 1", 0, nil)
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

			result := strategy.Evaluate(context.Background(), tt.giveTabletType, tt.giveSQL, 0, nil)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
