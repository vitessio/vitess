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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

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
	b, hcChan := newSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	result := b.Pick(target, nil, nil)
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

	// Pick for a specific session hash
	opts := &PickOpts{sessionHash: 12345}
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)

	// Pick again with same session hash, should return same tablet
	picked2 := b.Pick(target, nil, opts)
	require.Equal(t, picked1, picked2)

	// Pick with different session hash, empirically know that it should return different tablet
	opts = &PickOpts{sessionHash: 67890}
	picked3 := b.Pick(target, nil, opts)
	require.NotNil(t, picked3)
	require.NotEqual(t, picked2, picked3)
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

	// Pick should prefer local cell
	opts := &PickOpts{sessionHash: 12345}
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

	// Pick should return external cell since there are no local cells
	opts := &PickOpts{sessionHash: 12345}
	picked1 := b.Pick(target, nil, opts)
	require.NotNil(t, picked1)
	require.Equal(t, "external", picked1.Target.Cell)
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
