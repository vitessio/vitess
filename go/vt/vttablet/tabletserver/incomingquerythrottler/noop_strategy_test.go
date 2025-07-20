package incomingquerythrottler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestNoOpStrategy_ThrottleIfNeeded(t *testing.T) {
	tests := []struct {
		name           string
		giveTabletType topodatapb.TabletType
	}{
		{
			name:           "no erorr for tablet type primary",
			giveTabletType: topodatapb.TabletType_PRIMARY,
		},
		{
			name:           "no erorr for tablet type replica",
			giveTabletType: topodatapb.TabletType_REPLICA,
		},
		{
			name:           "no erorr for tablet type rdonly",
			giveTabletType: topodatapb.TabletType_RDONLY,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &NoOpStrategy{}

			gotErr := strategy.ThrottleIfNeeded(context.Background(), tt.giveTabletType, "Sample SQL", 0, nil)
			require.NoError(t, gotErr)
		})
	}
}

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

	// Verify ThrottleIfNeeded still works after Start/Stop
	err := strategy.ThrottleIfNeeded(context.Background(), topodatapb.TabletType_PRIMARY, "SELECT 1", 0, nil)
	require.NoError(t, err)
}
