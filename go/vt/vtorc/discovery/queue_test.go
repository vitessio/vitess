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
