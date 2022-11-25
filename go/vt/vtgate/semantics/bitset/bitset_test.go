package bitset

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingletons(t *testing.T) {
	for i := 0; i < 40; i++ {
		bs := Single(i)

		t.Logf("%d: %#v", i, []byte(bs))

		require.Equal(t, 1, bs.Popcount())
		require.Equal(t, i, bs.Single())

		var called bool
		bs.ForEach(func(offset int) {
			require.False(t, called)
			require.Equal(t, i, offset)
			called = true
		})
		require.True(t, called)
	}
}

func Test1(t *testing.T) {
	var bytes []byte
	for i := 8; i < 16; i++ {
		bytes = binary.BigEndian.AppendUint16(bytes, 1<<i)
	}
	t.Logf("%#v", string(bytes))
}
