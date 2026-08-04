/*
Copyright 2019 The Vitess Authors.

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

package buffer

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkWaitForFailoverEnd_Idle(b *testing.B) {
	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()

	ctx := context.Background()
	// Pre-create the shard buffer so we're benchmarking the steady-state path.
	buf.getOrCreateBuffer(keyspace, shard)

	b.ResetTimer()
	for b.Loop() {
		buf.WaitForFailoverEnd(ctx, keyspace, shard, nil, nil)
	}
}

func BenchmarkWaitForFailoverEnd_IdleParallel(b *testing.B) {
	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()

	ctx := context.Background()
	buf.getOrCreateBuffer(keyspace, shard)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf.WaitForFailoverEnd(ctx, keyspace, shard, nil, nil)
		}
	})
}

func BenchmarkWaitForFailoverEnd_MultiShard(b *testing.B) {
	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()

	ctx := context.Background()
	const numShards = 8
	shards := make([]string, numShards)
	for i := range numShards {
		shards[i] = fmt.Sprintf("-%02x", i)
		buf.getOrCreateBuffer(keyspace, shards[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			buf.WaitForFailoverEnd(ctx, keyspace, shards[i%numShards], nil, nil)
			i++
		}
	})
}
