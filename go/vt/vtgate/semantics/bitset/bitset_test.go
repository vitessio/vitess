package bitset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingletons(t *testing.T) {
	for i := 0; i < 40; i++ {
		bs := Single(i)

		require.Equal(t, 1, bs.Popcount())
		require.Equal(t, i, bs.SingleBit())

		var called bool
		bs.ForEach(func(offset int) {
			require.False(t, called)
			require.Equal(t, i, offset)
			called = true
		})
		require.True(t, called)
	}
}
