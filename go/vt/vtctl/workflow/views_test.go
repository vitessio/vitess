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

package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	sourceKs = "source_keyspace"
	targetKs = "target_keyspace"
)

func TestDeployViews(t *testing.T) {
	ctx := t.Context()

	topoServer, sourceShard, targetShard, tmc := setupKeyspaces(t)

	// Set up the source schema with a table and a view that references it.
	t1DDL := "CREATE TABLE `t1` (`id` int, `name` varchar(100))"
	view1DDL := "CREATE VIEW `view1` AS SELECT `id`, `name` FROM `t1`"
	tmc.schema[sourceKs+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "t1",
			Schema: t1DDL,
			Type:   "BASE TABLE",
		}},
	}
	tmc.schema[sourceKs+".view1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "view1",
			Schema: view1DDL,
			Type:   "VIEW",
		}},
	}

	// Expect the CREATE VIEW statement to be applied to the target.
	// schemadiff normalizes the DDL, so we expect the canonical form.
	tmc.expectVRQuery(200, view1DDL, &sqltypes.Result{})

	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Views:          []string{"view1"},
	}

	mz := &materializer{
		ctx:          ctx,
		ts:           topoServer,
		sourceTs:     topoServer,
		tmc:          tmc,
		ms:           ms,
		sourceShards: []*topo.ShardInfo{sourceShard},
		targetShards: []*topo.ShardInfo{targetShard},
		env:          vtenv.NewTestEnv(),
	}

	err := mz.deployViews(ctx, ms.Views)
	require.NoError(t, err)

	tmc.verifyQueries(t)
}

func TestDeployViewsAlreadyExists(t *testing.T) {
	ctx := t.Context()

	topoServer, sourceShard, targetShard, tmc := setupKeyspaces(t)

	// Set up the source schema with a table and a view.
	t1DDL := "CREATE TABLE `t1` (`id` int, `name` varchar(100))"
	view1DDL := "CREATE VIEW `view1` AS SELECT `id`, `name` FROM `t1`"
	tmc.schema[sourceKs+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "t1",
			Schema: t1DDL,
			Type:   "BASE TABLE",
		}},
	}
	tmc.schema[sourceKs+".view1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "view1",
			Schema: view1DDL,
			Type:   "VIEW",
		}},
	}

	// Set up the target schema with the view already existing.
	tmc.schema[targetKs+".view1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "view1",
			Schema: view1DDL,
			Type:   "VIEW",
		}},
	}

	// No queries should be expected since the view already exists.

	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Views:          []string{"view1"},
	}

	mz := &materializer{
		ctx:          ctx,
		ts:           topoServer,
		sourceTs:     topoServer,
		tmc:          tmc,
		ms:           ms,
		sourceShards: []*topo.ShardInfo{sourceShard},
		targetShards: []*topo.ShardInfo{targetShard},
		env:          vtenv.NewTestEnv(),
	}

	err := mz.deployViews(ctx, ms.Views)
	require.NoError(t, err)

	// Verify no unexpected queries were executed.
	tmc.verifyQueries(t)
}

// TestDeployViewsDependencyOrdering tests that views are created in dependency order.
// If view_b depends on view_a (i.e., view_b selects from view_a), then view_a must
// be created first. schemadiff handles this ordering for us.
func TestDeployViewsDependencyOrdering(t *testing.T) {
	ctx := t.Context()

	topoServer, sourceShard, targetShard, tmc := setupKeyspaces(t)

	// Set up views where view_b depends on view_a.
	t1DDL := "CREATE TABLE `t1` (`id` int, `name` varchar(100))"
	viewADDL := "CREATE VIEW `view_a` AS SELECT `id`, `name` FROM `t1`"
	viewBDDL := "CREATE VIEW `view_b` AS SELECT `id` FROM `view_a`"

	tmc.schema[sourceKs+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "t1",
			Schema: t1DDL,
			Type:   "BASE TABLE",
		}},
	}

	tmc.schema[sourceKs+".view_a"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "view_a",
			Schema: viewADDL,
			Type:   "VIEW",
		}},
	}
	tmc.schema[sourceKs+".view_b"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:   "view_b",
			Schema: viewBDDL,
			Type:   "VIEW",
		}},
	}

	// Expect view_a to be created first, then view_b.
	tmc.expectVRQuery(200, viewADDL, &sqltypes.Result{})
	tmc.expectVRQuery(200, "\n"+viewBDDL, &sqltypes.Result{})

	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Views:          []string{"view_b", "view_a"},
	}

	mz := &materializer{
		ctx:          ctx,
		ts:           topoServer,
		sourceTs:     topoServer,
		tmc:          tmc,
		ms:           ms,
		sourceShards: []*topo.ShardInfo{sourceShard},
		targetShards: []*topo.ShardInfo{targetShard},
		env:          vtenv.NewTestEnv(),
	}

	err := mz.deployViews(ctx, ms.Views)
	require.NoError(t, err)

	tmc.verifyQueries(t)
}

// setupKeyspaces sets up the inital state of keyspaces/tablets for the tests.
func setupKeyspaces(t *testing.T) (topoServer *topo.Server, sourceShard, targetShard *topo.ShardInfo, tmc *testMaterializerTMClient) {
	t.Helper()

	ctx := t.Context()

	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)

	shard := "0"

	err := ts.CreateKeyspace(ctx, sourceKs, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.CreateKeyspace(ctx, targetKs, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.CreateShard(ctx, sourceKs, shard)
	require.NoError(t, err)
	err = ts.CreateShard(ctx, targetKs, shard)
	require.NoError(t, err)

	sourceTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Keyspace: sourceKs,
		Shard:    shard,
	}
	targetTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  200,
		},
		Keyspace: targetKs,
		Shard:    shard,
	}

	err = ts.CreateTablet(ctx, sourceTablet)
	require.NoError(t, err)
	err = ts.CreateTablet(ctx, targetTablet)
	require.NoError(t, err)

	sourceShard, err = ts.UpdateShardFields(ctx, sourceKs, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = sourceTablet.Alias
		return nil
	})
	require.NoError(t, err)

	targetShard, err = ts.UpdateShardFields(ctx, targetKs, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = targetTablet.Alias
		return nil
	})
	require.NoError(t, err)

	tmc = newTestMaterializerTMClient(sourceKs, []string{shard}, nil)
	return ts, sourceShard, targetShard, tmc
}

func TestResolveViews(t *testing.T) {
	// Create a server for calling resolveViews.
	env := vtenv.NewTestEnv()
	s := &Server{env: env}

	// Helper to create a fresh sourceViews map for each test case.
	newSourceViews := func() map[string]*tabletmanagerdatapb.TableDefinition {
		return map[string]*tabletmanagerdatapb.TableDefinition{
			"view1": {
				Name:   "view1",
				Schema: "CREATE VIEW `view1` AS SELECT `id` FROM `t1`",
				Type:   "VIEW",
			},
			"view2": {
				Name:   "view2",
				Schema: "CREATE VIEW `view2` AS SELECT `id` FROM `t2`",
				Type:   "VIEW",
			},
			"view3": {
				Name:   "view3",
				Schema: "CREATE VIEW `view3` AS SELECT `id` FROM `t3`",
				Type:   "VIEW",
			},
		}
	}

	tests := []struct {
		name                string
		opts                func() resolveViewsOptions
		expected            []string
		expectedErr         error
		expectedErrContains []string
	}{
		{
			name: "no options passed returns no views",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
					},
					sourceViews: newSourceViews(),
				}
			},
			expected: nil,
		},
		{
			name: "AllViews returns all views",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						AllViews:       true,
					},
					sourceViews:  newSourceViews(),
					targetTables: []string{"t1", "t2", "t3"},
				}
			},
			expected: []string{"view1", "view2", "view3"},
		},
		{
			name: "IncludeViews returns subset of views",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view1", "view2"},
					},
					sourceViews:  newSourceViews(),
					targetTables: []string{"t1", "t2"},
				}
			},
			expected: []string{"view1", "view2"},
		},
		{
			name: "ExcludeViews excludes subset of views with AllViews",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						AllViews:       true,
						ExcludeViews:   []string{"view3"},
					},
					sourceViews:  newSourceViews(),
					targetTables: []string{"t1", "t2"},
				}
			},
			expected: []string{"view1", "view2"},
		},
		{
			name: "ExcludeViews excludes subset of views with IncludeViews",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view1", "view2"},
						ExcludeViews:   []string{"view2"},
					},
					sourceViews:  newSourceViews(),
					targetTables: []string{"t1"},
				}
			},
			expected: []string{"view1"},
		},
		{
			name: "IncludeViews includes views not in source keyspace returns an error",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view1", "nonexistent_view"},
					},
					sourceViews: newSourceViews(),
				}
			},
			expectedErr: errViewsNotFound,
		},
		{
			name: "ExcludeViews excludes views not in source keyspace returns an error",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						AllViews:       true,
						ExcludeViews:   []string{"nonexistent_view"},
					},
					sourceViews: newSourceViews(),
				}
			},
			expectedErr: errViewsNotFound,
		},
		{
			name: "ExcludeViews excludes all views returns an error",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						AllViews:       true,
						ExcludeViews:   []string{"view1", "view2", "view3"},
					},
					sourceViews: newSourceViews(),
				}
			},
			expectedErr: errNoViewsToMove,
		},
		{
			name: "skipViewValidation=true allows views with missing table references",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view1"},
					},
					sourceViews:        newSourceViews(),
					skipViewValidation: true,
					targetTables:       []string{"different_table"},
				}
			},
			expected: []string{"view1"},
		},
		{
			name: "skipViewValidation=false errors on views with missing table references",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view1"},
					},
					sourceViews:        newSourceViews(),
					skipViewValidation: false,
					targetTables:       []string{"different_table"},
				}
			},
			expectedErr: errViewMissingTable,
		},
		{
			name: "multiple views with missing table references",
			opts: func() resolveViewsOptions {
				return resolveViewsOptions{
					req: &vtctldatapb.MoveTablesCreateRequest{
						SourceKeyspace: sourceKs,
						IncludeViews:   []string{"view2", "view3"},
					},
					sourceViews:  newSourceViews(),
					targetTables: []string{},
				}
			},
			expectedErr: errViewMissingTable,
			expectedErrContains: []string{
				`view "view2" is missing table(s) "t2"`,
				`view "view3" is missing table(s) "t3"`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			views, err := s.resolveViews(tc.opts())

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				for _, msg := range tc.expectedErrContains {
					require.ErrorContains(t, err, msg)
				}
				return
			}

			require.NoError(t, err)
			require.ElementsMatch(t, tc.expected, views)
		})
	}
}
