/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	q := NewQueue()
	require.Zero(t, q.QueueLen())

	// Push
	q.Push(t.Name())
	require.Equal(t, 1, q.QueueLen())
	_, found := q.enqueued[t.Name()]
	require.True(t, found)

	// Push duplicate
	q.Push(t.Name())
	require.Equal(t, 1, q.QueueLen())

	// Consume
	require.Equal(t, t.Name(), q.Consume())
	require.Equal(t, 1, q.QueueLen())
	_, found = q.enqueued[t.Name()]
	require.True(t, found)

	// Release
	q.Release(t.Name())
	require.Zero(t, q.QueueLen())
	_, found = q.enqueued[t.Name()]
	require.False(t, found)
}

type testQueue interface {
	QueueLen() int
	Push(string)
	Consume() string
	Release(string)
}

func BenchmarkQueues(b *testing.B) {
	tests := []struct {
		name  string
		queue testQueue
	}{
		{"Current", NewQueue()},
	}
	for _, test := range tests {
		q := test.queue
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := 0; i < 1000; i++ {
					q.Push(b.Name() + strconv.Itoa(i))
				}
				q.QueueLen()
				for i := 0; i < 1000; i++ {
					q.Release(q.Consume())
				}
			}
		})
	}
}
