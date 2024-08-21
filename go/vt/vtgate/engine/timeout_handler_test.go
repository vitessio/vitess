package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// TestTimeoutHandler tests timeout handler primitive.
func TestTimeoutHandler(t *testing.T) {
	tests := []struct {
		name      string
		sleepTime time.Duration
		timeout   int
		wantErr   string
	}{
		{
			name:      "Timeout without failure",
			sleepTime: 100 * time.Millisecond,
			timeout:   1000,
			wantErr:   "",
		}, {
			name:      "Timeout with failure",
			sleepTime: 2 * time.Second,
			timeout:   100,
			wantErr:   "VT15001: Query execution was interrupted, maximum statement execution time exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := createTimeoutHandlerForTesting(tt.timeout, tt.sleepTime).TryExecute(context.Background(), &noopVCursor{}, nil, false)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			err = createTimeoutHandlerForTesting(tt.timeout, tt.sleepTime).TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(result *sqltypes.Result) error {
				return nil
			})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// createTimeoutHandlerForTesting creates a TimeoutHandler for testing that has a fakePrimitive as an input.
func createTimeoutHandlerForTesting(timeout int, sleepTime time.Duration) *TimeoutHandler {
	return NewTimeoutHandler(&fakePrimitive{
		results:   nil,
		sleepTime: sleepTime,
	}, timeout)
}
