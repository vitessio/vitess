package bf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloom(t *testing.T) {
	bf := New(0.1)
	bf.EnsureCapacity(500)

	exist := bf.Insert(123)
	require.False(t, exist)

	exist = bf.Exist(123)
	require.True(t, exist)

	exist = bf.Exist(456)
	require.False(t, exist)

	bf = New(0.01)
	require.Equal(t, 512, bf.Capacity)
	require.Equal(t, 0.01, bf.FalsePositiveRate)

	bf.Insert(123)
	exist = bf.Exist(123)
	require.True(t, exist)

	bf.Insert(256)
	exist = bf.Exist(256)
	require.True(t, exist)

	bf.Reset()

	exist = bf.Exist(123)
	require.False(t, exist)

	exist = bf.Exist(256)
	require.False(t, exist)
}
