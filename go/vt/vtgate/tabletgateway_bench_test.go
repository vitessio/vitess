/*
Copyright 2026 The Vitess Authors.

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

package vtgate

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

func BenchmarkGetStatsAggregator(b *testing.B) {
	for _, numTargets := range []int{1, 64} {
		b.Run(fmt.Sprintf("targets=%d", numTargets), func(b *testing.B) {
			ctx := b.Context()
			tg := NewTabletGateway(ctx, discovery.NewFakeHealthCheck(nil), &econtext.FakeTopoServer{}, "cell")
			b.Cleanup(func() { _ = tg.Close(ctx) })

			targets := make([]*querypb.Target, numTargets)
			for i := range targets {
				targets[i] = &querypb.Target{Keyspace: "ks", Shard: fmt.Sprintf("%02x-", i), TabletType: topodatapb.TabletType_PRIMARY}
				tg.getStatsAggregator(targets[i])
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if tg.getStatsAggregator(targets[i%numTargets]) == nil {
						b.Error("nil aggregator")
						return
					}
					i++
				}
			})
		})
	}
}
