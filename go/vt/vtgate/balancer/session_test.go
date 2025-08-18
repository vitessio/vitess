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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newSessionBalancer(t *testing.T) (*SessionBalancer, chan *discovery.TabletHealth) {
	ctx := t.Context()

	ch := make(chan *discovery.TabletHealth, 10)
	hc := discovery.NewFakeHealthCheck(ch)
	b := NewSessionBalancer(ctx, "local", hc)
	sb := b.(*SessionBalancer)

	return sb, ch
}

func TestNewSessionBalancer(t *testing.T) {
	b, _ := newSessionBalancer(t)

	require.Equal(t, "local", b.localCell)
	require.NotNil(t, b.hc)
	require.NotNil(t, b.localRings)
	require.Len(t, b.localRings, 0)
	require.NotNil(t, b.externalRings)
	require.Len(t, b.externalRings, 0)
}

func TestPickNoTablets(t *testing.T) {
	b, _ := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	opts := sessionHash(12345)
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

	// Pick for a specific session hash
	opts := sessionHash(12345)
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	// Pick again with same session hash, should return same tablet
	picked2 := b.Pick(target, nil, opts)
	require.Equal(t, picked1, picked2, fmt.Sprintf("expected %s, got %s", tabletAlias(picked1), tabletAlias(picked2)))

	// Pick with different session hash, empirically know that it should return tablet2
	opts = sessionHash(5018141287610575993)
	picked3 := b.Pick(target, nil, opts)
	require.NotNil(t, picked3)
	require.NotEqual(t, picked2, picked3, fmt.Sprintf("expected different tablets, got %s", tabletAlias(picked3)))
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
	opts := sessionHash(5018141287610575993)
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
	opts := sessionHash(12345)
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

	opts := sessionHash(5018141287610575993)
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

	opts := sessionHash(5018141287610575993)
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

	opts := sessionHash(5018141287610575993)
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

func TestDebugHandler(t *testing.T) {
	ctx := t.Context()

	ch := make(chan *discovery.TabletHealth, 10)
	hc := discovery.NewFakeHealthCheck(ch)
	b := NewSessionBalancer(ctx, "local", hc)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/debug", nil)

	b.DebugHandler(w, r)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestPickNoSessionHash(t *testing.T) {
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

	// Test with nil opts
	result := b.Pick(target, nil, nil)
	require.Nil(t, result)

	// Test with opts but nil session hash
	optsNoHash := &PickOpts{sessionHash: nil}
	result = b.Pick(target, nil, optsNoHash)
	require.Nil(t, result)
}

func sessionHash(i uint64) *PickOpts {
	return &PickOpts{sessionHash: &i}
}
