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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func createSessionBalancer(t *testing.T) TabletBalancer {
	t.Helper()

	return newSessionBalancer("local")
}

func TestSessionPickNoTablets(t *testing.T) {
	b := createSessionBalancer(t)

	target := &querypb.Target{
		Keyspace:   "keyspace",
		Shard:      "0",
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local",
	}

	result := b.Pick(target, nil, WithSessionUUID("a"))
	require.Nil(t, result)
}

func TestSessionPickLocalOnly(t *testing.T) {
	b := createSessionBalancer(t)

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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
		},
	}

	// Pick for a specific session UUID
	picked1 := b.Pick(target, tablets, WithSessionUUID("a"))
	require.NotNil(t, picked1)

	// Pick again with same session hash, should return same tablet
	picked2 := b.Pick(target, tablets, WithSessionUUID("a"))
	require.Equal(t, picked1, picked2, fmt.Sprintf("expected %s, got %s", tabletAlias(picked1), tabletAlias(picked2)))

	// Pick with different session hash, empirically know that it should return tablet2
	picked3 := b.Pick(target, tablets, WithSessionUUID("b"))
	require.NotNil(t, picked3)
	require.NotEqual(t, picked2, picked3, fmt.Sprintf("expected different tablets, got %s for both", tabletAlias(picked3)))
}

func TestSessionPickPreferLocal(t *testing.T) {
	b := createSessionBalancer(t)

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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "external",
			},
			Serving: true,
		},
	}

	// Pick should prefer local cell
	picked1 := b.Pick(target, tablets, WithSessionUUID("a"))
	require.NotNil(t, picked1)
	require.Equal(t, "local", picked1.Target.Cell)

	// Pick should pick the same tablet consistently
	for range 20 {
		picked := b.Pick(target, tablets, WithSessionUUID("a"))
		require.Equal(t, picked1, picked, fmt.Sprintf("expected %s, got %s", tabletAlias(picked1), tabletAlias(picked)))
	}
}

func TestSessionPickNoLocal(t *testing.T) {
	b := createSessionBalancer(t)

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
		},
		{
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
		},
	}

	// Pick should return external cell since there are no local cells
	picked1 := b.Pick(target, tablets, WithSessionUUID("a"))
	require.NotNil(t, picked1)
	require.Equal(t, "external", picked1.Target.Cell)

	// Pick should pick the same tablet consistently
	for range 20 {
		picked := b.Pick(target, tablets, WithSessionUUID("a"))
		require.Equal(t, picked1, picked, fmt.Sprintf("expected %s, got %s", tabletAlias(picked1), tabletAlias(picked)))
	}
}

func TestSessionPickNoOpts(t *testing.T) {
	b := createSessionBalancer(t)

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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
		},
	}

	// Test with no opts (no session UUID)
	result := b.Pick(target, tablets)
	require.Nil(t, result)
}

func TestSessionPickInvalidTablets(t *testing.T) {
	b := createSessionBalancer(t)

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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
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
			Target: &querypb.Target{
				Keyspace:   "keyspace",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
				Cell:       "local",
			},
			Serving: true,
		},
	}

	// Get a tablet regularly
	tablet := b.Pick(target, tablets, WithSessionUUID("a"))
	require.NotNil(t, tablet)

	// Filter out the returned tablet as invalid
	tablets = slices.DeleteFunc(tablets, func(t *discovery.TabletHealth) bool {
		return topoproto.TabletAliasString(t.Tablet.Alias) == topoproto.TabletAliasString(tablet.Tablet.Alias)
	})

	// Pick should now return a different tablet
	tablet2 := b.Pick(target, tablets, WithSessionUUID("a"))
	require.NotNil(t, tablet2)
	require.NotEqual(t, tablet, tablet2)

	// Filter out the last tablet, Pick should return nothing
	tablet3 := b.Pick(target, []*discovery.TabletHealth{}, WithSessionUUID("a"))
	require.Nil(t, tablet3)
}
