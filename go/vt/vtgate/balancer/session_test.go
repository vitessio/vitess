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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo/fakesrvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func newSessionBalancer(t *testing.T) (*SessionBalancer, chan *discovery.TabletHealth) {
	ctx := t.Context()

	ch := make(chan *discovery.TabletHealth, 10)
	hc := discovery.NewFakeHealthCheck(ch)
	b, _ := NewSessionBalancer(ctx, "local", &fakesrvtopo.FakeSrvTopo{}, hc)
	sb := b.(*SessionBalancer)

	return sb, ch
}

func TestNewSessionBalancer(t *testing.T) {
	b, _ := newSessionBalancer(t)

	require.Equal(t, "local", b.localCell)
	require.NotNil(t, b.hc)
	require.NotNil(t, b.localTablets)
	require.NotNil(t, b.externalTablets)
	require.NotNil(t, b.tablets)
}

func TestPickNoTablets(t *testing.T) {
	b, _ := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	opts := buildOpts("a")
	result := b.Pick(target, nil, opts)
	require.Nil(t, result)
}

func TestPickLocalOnly(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	localTablet1 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	localTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet1
	hcChan <- localTablet2

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Pick for a specific session UUID
	opts := buildOpts("a")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	// Pick again with same session hash, should return same tablet
	picked2 := b.Pick(target, nil, opts)
	require.Equal(t, picked1, picked2, fmt.Sprintf("expected %s, got %s", tabletAlias(picked1), tabletAlias(picked2)))

	// Pick with different session hash, empirically know that it should return tablet2
	opts = buildOpts("b")
	picked3 := b.Pick(target, nil, opts)
	require.NotNil(t, picked3)
	require.NotEqual(t, picked2, picked3, fmt.Sprintf("expected different tablets, got %s for both", tabletAlias(picked3)))
}

func TestPickPreferLocal(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	localTablet1 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	localTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	externalTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  200,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "external",
		},
		Serving: true,
	}

	hcChan <- localTablet1
	hcChan <- localTablet2
	hcChan <- externalTablet

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Pick should prefer local cell
	opts := buildOpts("a")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)
	require.Equal(t, "local", picked1.Target.Cell)
}

func TestPickNoLocal(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	externalTablet1 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  200,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "external",
		},
		Serving: true,
	}

	externalTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  201,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "external",
		},
		Serving: true,
	}

	hcChan <- externalTablet1
	hcChan <- externalTablet2

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Pick should return external cell since there are no local cells
	opts := buildOpts("a")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)
	require.Equal(t, "external", picked1.Target.Cell)
}

func TestTabletNotServing(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
	}

	localTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	externalTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  200,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "external",
		},
		Serving: true,
	}

	hcChan <- localTablet
	hcChan <- externalTablet

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	opts := buildOpts("a")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	// Local tablet goes out of serving
	localTablet.Serving = false
	hcChan <- localTablet

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Should not pick the local tablet again
	picked2 := b.Pick(target, nil, opts)
	require.NotEqual(t, picked1, picked2)
}

func TestNewLocalTablet(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
	}

	localTablet1 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet1

	time.Sleep(10 * time.Millisecond)

	opts := buildOpts("b")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	localTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet2

	time.Sleep(10 * time.Millisecond)

	picked2 := b.Pick(target, nil, opts)
	require.NotNil(t, picked2)
	require.NotEqual(t, picked1, picked2, fmt.Sprintf("expected different tablets, got %s", tabletAlias(picked2)))
}

func TestNewExternalTablet(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
	}

	externalTablet1 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- externalTablet1

	time.Sleep(10 * time.Millisecond)

	opts := buildOpts("a")
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	externalTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- externalTablet2

	time.Sleep(10 * time.Millisecond)

	picked2 := b.Pick(target, nil, opts)
	require.NotNil(t, picked2)
	require.NotEqual(t, picked1, picked2, fmt.Sprintf("expected different tablets, got %s", tabletAlias(picked2)))
}

func TestPickNoOpts(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	localTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Test with empty opts
	result := b.Pick(target, nil, PickOpts{})
	require.Nil(t, result)
}

func TestPickInvalidTablets(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	localTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	localTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet
	hcChan <- localTablet2

	// Give a moment for the worker to process the tablets
	time.Sleep(10 * time.Millisecond)

	// Get a tablet regularly
	opts := buildOpts("a")
	tablet := b.Pick(target, nil, opts)
	require.NotNil(t, tablet)

	// Mark returned tablet as invalid, should return other tablet
	opts.InvalidTablets = map[string]bool{topoproto.TabletAliasString(tablet.Tablet.Alias): true}
	tablet2 := b.Pick(target, nil, opts)
	require.NotEqual(t, tablet, tablet2)

	// Mark both as invalid, should return nil
	opts.InvalidTablets[topoproto.TabletAliasString(tablet2.Tablet.Alias)] = true
	tablet3 := b.Pick(target, nil, opts)
	require.Nil(t, tablet3)
}

func TestTabletTypesToWatch(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	// Valid tablet type
	localTablet := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	// Valid tablet type
	localTablet2 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_RDONLY,
			Cell:       "local",
		},
		Serving: true,
	}

	// Invalid tablet type
	localTablet3 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_PRIMARY,
			Cell:       "local",
		},
		Serving: true,
	}

	// Invalid tablet type
	localTablet4 := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  101,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_BACKUP,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- localTablet
	hcChan <- localTablet2
	hcChan <- localTablet3
	hcChan <- localTablet4

	// Give a moment for the worker to process the tablets
	time.Sleep(100 * time.Millisecond)

	b.mu.RLock()
	defer b.mu.RUnlock()

	require.Len(t, b.localTablets, 2)
	require.Len(t, b.externalTablets, 0)

	for _, target := range b.localTablets {
		for _, tablet := range target {
			require.Contains(t, tabletTypesToWatch, tablet.Target.TabletType)
		}
	}
}

func TestLocalTabletTargetChanges(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	replica := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "local",
		},
		Serving: true,
	}

	primary := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "local",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_PRIMARY,
			Cell:       "local",
		},
		Serving: true,
	}

	hcChan <- replica

	// Give a moment for the worker to process the tablets
	time.Sleep(100 * time.Millisecond)

	require.Len(t, b.localTablets, 1, b.print())
	require.Len(t, b.localTablets[discovery.KeyFromTarget(replica.Target)], 1, b.print())

	require.Len(t, b.externalTablets, 0, b.print())

	// Reparent happens, tablet is now a primary
	hcChan <- primary

	// Give a moment for the worker to process the tablets
	time.Sleep(100 * time.Millisecond)

	require.Len(t, b.localTablets, 1, b.print())
	require.Len(t, b.localTablets[discovery.KeyFromTarget(replica.Target)], 0, b.print())

	require.Len(t, b.externalTablets, 0, b.print())
}

func TestExternalTabletTargetChanges(t *testing.T) {
	b, hcChan := newSessionBalancer(t)

	replica := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_REPLICA,
			Cell:       "external",
		},
		Serving: true,
	}

	primary := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "external",
				Uid:  100,
			},
			Keyspace: "keyspace",
			Shard:    "0",
		},
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "0",
			TabletType: topodatapb.TabletType_PRIMARY,
			Cell:       "external",
		},
		Serving: true,
	}

	hcChan <- replica

	// Give a moment for the worker to process the tablets
	time.Sleep(100 * time.Millisecond)

	require.Len(t, b.externalTablets, 1, b.print())
	require.Len(t, b.externalTablets[discovery.KeyFromTarget(replica.Target)], 1, b.print())

	require.Len(t, b.localTablets, 0, b.print())

	// Reparent happens, tablet is now a primary
	hcChan <- primary

	// Give a moment for the worker to process the tablets
	time.Sleep(100 * time.Millisecond)

	require.Len(t, b.externalTablets, 1, b.print())
	require.Len(t, b.externalTablets[discovery.KeyFromTarget(replica.Target)], 0, b.print())

	require.Len(t, b.localTablets, 0, b.print())
}

func buildOpts(uuid string) PickOpts {
	return PickOpts{SessionUUID: uuid}
}
