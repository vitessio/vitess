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
		wantErr string
	}{
		{
			name: "Timeout without failure",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 100 * time.Millisecond,
			}, 1000),
			wantErr: "",
		}, {
			name: "Timeout with failure",
			input: NewTimeoutHandler(&fakePrimitive{
				results:   nil,
				sleepTime: 2 * time.Second,
			}, 100),
			wantErr: "VT15001: Query execution was interrupted, maximum statement execution time exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.input.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			err = tt.input.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(result *sqltypes.Result) error {
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
