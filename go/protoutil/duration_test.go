package protoutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestDurationFromProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        *vttime.Duration
		expected  time.Duration
		isOk      bool
		shouldErr bool
	}{
		{
			name:      "success",
			in:        &vttime.Duration{Seconds: 1000},
			expected:  time.Second * 1000,
			isOk:      true,
			shouldErr: false,
		},
		{
			name:      "nil value",
			in:        nil,
			expected:  0,
			isOk:      false,
			shouldErr: false,
		},
		{
			name: "error",
			in: &vttime.Duration{
				// This is the max allowed seconds for a durationpb, plus 1.
				Seconds: int64(10000*365.25*24*60*60) + 1,
			},
			expected:  0,
			isOk:      true,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, ok, err := DurationFromProto(tt.in)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Equal(t, tt.isOk, ok, "expected (_, ok, _) = DurationFromProto; to be ok = %v", tt.isOk)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.isOk, ok, "expected (_, ok, _) = DurationFromProto; to be ok = %v", tt.isOk)
		})
	}
}
