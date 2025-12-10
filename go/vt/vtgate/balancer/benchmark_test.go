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

package balancer

import (
	"testing"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func BenchmarkBalancers(b *testing.B) {
	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	tablets := []*discovery.TabletHealth{
		{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "local",
					Uid:  100,
				},
				Keyspace: "keyspace",
				Shard:    "0",
			},
		},
		{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "local",
					Uid:  101,
				},
				Keyspace: "keyspace",
				Shard:    "0",
			},
		},
		{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "external",
					Uid:  200,
				},
				Keyspace: "keyspace",
				Shard:    "0",
			},
		},
	}

	opts := PickOpts{SessionUUID: "session-uuid-123"}

	benchmarks := []struct {
		name string
		mode Mode
	}{
		{"Session", ModeSession},
		{"Random", ModeRandom},
		{"PreferCell", ModePreferCell},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			balancer, err := NewTabletBalancer(bm.mode, "local", []string{"local", "external"})
			if err != nil {
				b.Fatalf("failed to create balancer: %v", err)
			}

			for b.Loop() {
				balancer.Pick(target, tablets, opts)
			}
		})
	}
}
