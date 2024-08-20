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
		name    string
		input   *TimeoutHandler
		vc      VCursor
		wantErr string
	}{
		{
			name: "No timeout",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 100 * time.Millisecond,
			}, 0),
			vc:      &noopVCursor{},
			wantErr: "",
		}, {
			name: "Timeout without failure",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 100 * time.Millisecond,
			}, 1000),
			vc:      &noopVCursor{},
			wantErr: "",
		}, {
			name: "Timeout in session",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 2 * time.Second,
			}, 0),
			vc: &noopVCursor{
				queryTimeout: 100,
			},
			wantErr: "VT15001: Query execution was interrupted, maximum statement execution time exceeded",
		}, {
			name: "Timeout in comments",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 2 * time.Second,
			}, 100),
			vc:      &noopVCursor{},
			wantErr: "VT15001: Query execution was interrupted, maximum statement execution time exceeded",
		}, {
			name: "Timeout in both",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 2 * time.Second,
			}, 100),
			vc: &noopVCursor{
				queryTimeout: 4000,
			},
			wantErr: "VT15001: Query execution was interrupted, maximum statement execution time exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.input.TryExecute(context.Background(), tt.vc, nil, false)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			err = tt.input.TryStreamExecute(context.Background(), tt.vc, nil, false, func(result *sqltypes.Result) error {
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
