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

package vtcombo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestInitRoutingRules(t *testing.T) {
	tests := []struct {
		name  string
		rules *vschemapb.RoutingRules
	}{
		{
			name:  "nil routing rules",
			rules: nil,
		},
		{
			name: "valid routing rules",
			rules: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "table1",
						ToTables:  []string{"table2"},
					},
				},
			},
		},
		{
			name: "empty routing rules",
			rules: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{},
			},
		},
		{
			name: "multiple routing rules",
			rules: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "table1",
						ToTables:  []string{"table2"},
					},
					{
						FromTable: "table3",
						ToTables:  []string{"table4", "table5"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ts := memorytopo.NewServer(ctx, "cell1")

			err := InitRoutingRules(ctx, ts, tt.rules)
			require.NoError(t, err)

			if tt.rules != nil && len(tt.rules.Rules) > 0 {
				savedRules, err := ts.GetRoutingRules(ctx)
				require.NoError(t, err)
				require.NotNil(t, savedRules)
				assert.Equal(t, tt.rules.Rules, savedRules.Rules)
			}
		})
	}
}

func TestInitMirrorRules(t *testing.T) {
	tests := []struct {
		name  string
		rules *vschemapb.MirrorRules
	}{
		{
			name:  "nil mirror rules",
			rules: nil,
		},
		{
			name: "valid mirror rules",
			rules: &vschemapb.MirrorRules{
				Rules: []*vschemapb.MirrorRule{
					{
						FromTable: "table1",
						ToTable:   "table2",
						Percent:   50.0,
					},
				},
			},
		},
		{
			name: "empty mirror rules",
			rules: &vschemapb.MirrorRules{
				Rules: []*vschemapb.MirrorRule{},
			},
		},
		{
			name: "multiple mirror rules with different percentages",
			rules: &vschemapb.MirrorRules{
				Rules: []*vschemapb.MirrorRule{
					{
						FromTable: "table1",
						ToTable:   "table2",
						Percent:   50.0,
					},
					{
						FromTable: "table3",
						ToTable:   "table4",
						Percent:   100.0,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ts := memorytopo.NewServer(ctx, "cell1")

			err := InitMirrorRules(ctx, ts, tt.rules)
			require.NoError(t, err)

			if tt.rules != nil && len(tt.rules.Rules) > 0 {
				savedRules, err := ts.GetMirrorRules(ctx)
				require.NoError(t, err)
				require.NotNil(t, savedRules)
				assert.Equal(t, tt.rules.Rules, savedRules.Rules)
			}
		})
	}
}

func TestDialer(t *testing.T) {
	originalTabletMap := tabletMap
	t.Cleanup(func() {
		tabletMap = originalTabletMap
	})

	tests := []struct {
		name          string
		setupTablet   func() *topodatapb.Tablet
		expectError   bool
		errorContains string
	}{
		{
			name: "tablet exists in map",
			setupTablet: func() *topodatapb.Tablet {
				tabletMap = make(map[uint32]*comboTablet)
				tabletMap[100] = &comboTablet{
					alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					keyspace: "test_keyspace",
					shard:    "0",
				}
				return &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				}
			},
			expectError: false,
		},
		{
			name: "tablet does not exist in map",
			setupTablet: func() *topodatapb.Tablet {
				tabletMap = make(map[uint32]*comboTablet)
				return &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  999,
					},
				}
			},
			expectError:   true,
			errorContains: "connection refused",
		},
		{
			name: "empty tablet map",
			setupTablet: func() *topodatapb.Tablet {
				tabletMap = make(map[uint32]*comboTablet)
				return &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  1,
					},
				}
			},
			expectError:   true,
			errorContains: "connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tablet := tt.setupTablet()

			qs, err := dialer(ctx, tablet, grpcclient.FailFast(false))

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, qs)
			} else {
				require.NoError(t, err)
				require.NotNil(t, qs)

				itc, ok := qs.(*internalTabletConn)
				require.True(t, ok, "dialer should return *internalTabletConn")
				assert.NotNil(t, itc.tablet)
				assert.Equal(t, tablet, itc.topoTablet)
			}
		})
	}
}

func TestInternalTabletConn_Tablet(t *testing.T) {
	expectedTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
	}

	itc := &internalTabletConn{
		topoTablet: expectedTablet,
	}

	result := itc.Tablet()
	assert.Equal(t, expectedTablet, result)
}

func TestInternalTabletConn_Close(t *testing.T) {
	ctx := context.Background()
	itc := &internalTabletConn{}

	err := itc.Close(ctx)
	assert.NoError(t, err)
}

func TestInternalTabletManagerClient_Ping(t *testing.T) {
	originalTabletMap := tabletMap
	t.Cleanup(func() {
		tabletMap = originalTabletMap
	})

	tests := []struct {
		name        string
		setupTablet func() *topodatapb.Tablet
		expectError bool
	}{
		{
			name: "tablet exists",
			setupTablet: func() *topodatapb.Tablet {
				tabletMap = make(map[uint32]*comboTablet)
				tabletMap[100] = &comboTablet{
					alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				}
				return &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				}
			},
			expectError: false,
		},
		{
			name: "tablet does not exist",
			setupTablet: func() *topodatapb.Tablet {
				tabletMap = make(map[uint32]*comboTablet)
				return &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  999,
					},
				}
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			itmc := &internalTabletManagerClient{}
			tablet := tt.setupTablet()

			err := itmc.Ping(ctx, tablet)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "cannot find tablet")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInternalTabletManagerClient_TabletNotFoundErrors(t *testing.T) {
	originalTabletMap := tabletMap
	t.Cleanup(func() {
		tabletMap = originalTabletMap
	})

	ctx := context.Background()
	itmc := &internalTabletManagerClient{}

	tabletMap = make(map[uint32]*comboTablet)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 999},
	}

	tests := []struct {
		name   string
		errGen func() error
	}{
		{"GetSchema", func() error {
			_, err := itmc.GetSchema(ctx, tablet, &tabletmanagerdatapb.GetSchemaRequest{})
			return err
		}},
		{"GetPermissions", func() error {
			_, err := itmc.GetPermissions(ctx, tablet)
			return err
		}},
		{"GetGlobalStatusVars", func() error {
			_, err := itmc.GetGlobalStatusVars(ctx, tablet, nil)
			return err
		}},
		{"ChangeTags", func() error {
			_, err := itmc.ChangeTags(ctx, tablet, nil, false)
			return err
		}},
		{"ChangeType", func() error {
			return itmc.ChangeType(ctx, tablet, topodatapb.TabletType_REPLICA, false)
		}},
		{"Sleep", func() error {
			return itmc.Sleep(ctx, tablet, time.Second)
		}},
		{"RefreshState", func() error {
			return itmc.RefreshState(ctx, tablet)
		}},
		{"RunHealthCheck", func() error {
			return itmc.RunHealthCheck(ctx, tablet)
		}},
		{"ReloadSchema", func() error {
			return itmc.ReloadSchema(ctx, tablet, "")
		}},
		{"PreflightSchema", func() error {
			_, err := itmc.PreflightSchema(ctx, tablet, nil)
			return err
		}},
		{"ApplySchema", func() error {
			_, err := itmc.ApplySchema(ctx, tablet, nil)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.errGen()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot find tablet")
		})
	}
}
