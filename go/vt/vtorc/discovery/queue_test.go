package discovery

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	now := time.Now()
	q := NewQueue()
	q.nowFunc = func() time.Time { return now }
	require.Zero(t, q.QueueLen())

	// Push
	q.Push(t.Name())
	require.Equal(t, 2, q.QueueLen())

	// Consume
	require.Equal(t, t.Name(), q.Consume())
	require.Zero(t, q.QueueLen())
}

func BenchmarkQueues(b *testing.B) {
	tests := []struct {
		name string
		q    Queue
	}{
		{"LegacyQueue", CreateQueue("test")},
		{"QueueV2", NewQueue()},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.q.Push(strconv.Itoa(i))
				test.q.QueueLen()
				test.q.Release(test.q.Consume())
			}
		})
	}
}
