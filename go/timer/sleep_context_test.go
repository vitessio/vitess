package timer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSleepContext(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	err := SleepContext(ctx, 10*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, time.Since(start) > 10*time.Millisecond, time.Since(start))
	assert.True(t, time.Since(start) < 100*time.Millisecond, time.Since(start))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	start = time.Now()
	err = SleepContext(ctx, 100*time.Millisecond)
	require.Error(t, err)
	assert.True(t, time.Since(start) > 10*time.Millisecond, time.Since(start))
	assert.True(t, time.Since(start) < 100*time.Millisecond, time.Since(start))
}
