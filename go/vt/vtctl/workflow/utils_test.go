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

package workflow

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/race"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/etcd2topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestResolveWorkflowKeepData(t *testing.T) {
	t.Run("non-reverse workflow keeps existing default", func(t *testing.T) {
		keepData, warnings := resolveWorkflowKeepData("wf1", nil)
		require.False(t, keepData)
		require.Empty(t, warnings)
	})

	t.Run("reverse workflow defaults to keeping data", func(t *testing.T) {
		keepData, warnings := resolveWorkflowKeepData("wf1_reverse", nil)
		require.True(t, keepData)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "wf1_reverse")
		assert.Contains(t, warnings[0], "keeping data by default")
		assert.Contains(t, warnings[0], "keep_data=false")
		assert.Contains(t, warnings[0], "--keep-data=false")
	})

	t.Run("explicit false on reverse workflow is honored", func(t *testing.T) {
		keepDataValue := false
		keepData, warnings := resolveWorkflowKeepData("wf1_reverse", &keepDataValue)
		require.False(t, keepData)
		require.Empty(t, warnings)
	})

	t.Run("explicit true on reverse workflow is honored", func(t *testing.T) {
		keepDataValue := true
		keepData, warnings := resolveWorkflowKeepData("wf1_reverse", &keepDataValue)
		require.True(t, keepData)
		require.Empty(t, warnings)
	})
}

// TestGetTenantClause confirms that the tenant column name and a VARCHAR tenant
// id are escaped before they are interpolated into the filter SQL, so that a
// value containing a quote (or a column name containing a backtick) cannot break
// out of the literal/identifier and inject additional predicate.
func TestGetTenantClause(t *testing.T) {
	parser := sqlparser.NewTestParser()

	newSchema := func(colName string, colType querypb.Type) *vindexes.KeyspaceSchema {
		return &vindexes.KeyspaceSchema{
			MultiTenantSpec: &vschema.MultiTenantSpec{
				TenantIdColumnName: colName,
				TenantIdColumnType: colType,
			},
		}
	}

	t.Run("int64 tenant id is used verbatim", func(t *testing.T) {
		expr, err := getTenantClause(&vtctldata.WorkflowOptions{TenantId: "123"}, newSchema("tenant_id", sqltypes.Int64), parser)
		require.NoError(t, err)
		require.NotNil(t, expr)
		require.Equal(t, "tenant_id = 123", sqlparser.String(*expr))
	})

	t.Run("varchar tenant id with a quote stays a single literal", func(t *testing.T) {
		malicious := "acme' or '1'='1"
		expr, err := getTenantClause(&vtctldata.WorkflowOptions{TenantId: malicious}, newSchema("tenant_id", sqltypes.VarChar), parser)
		require.NoError(t, err)
		require.NotNil(t, expr)
		cmp, ok := (*expr).(*sqlparser.ComparisonExpr)
		require.True(t, ok, "expected a single comparison, got %T: %s", *expr, sqlparser.String(*expr))
		lit, ok := cmp.Right.(*sqlparser.Literal)
		require.True(t, ok, "expected a string literal on the right, got %T", cmp.Right)
		require.Equal(t, malicious, lit.Val)
	})

	t.Run("column name with a backtick is escaped as an identifier", func(t *testing.T) {
		expr, err := getTenantClause(&vtctldata.WorkflowOptions{TenantId: "acme"}, newSchema("tenant`id", sqltypes.VarChar), parser)
		require.NoError(t, err)
		require.NotNil(t, expr)
		cmp, ok := (*expr).(*sqlparser.ComparisonExpr)
		require.True(t, ok, "expected a single comparison, got %T: %s", *expr, sqlparser.String(*expr))
		col, ok := cmp.Left.(*sqlparser.ColName)
		require.True(t, ok, "expected a column on the left, got %T", cmp.Left)
		require.Equal(t, "tenant`id", col.Name.String())
	})
}

// TestCreateDefaultShardRoutingRules confirms that the default shard routing rules are created correctly for sharded
// and unsharded keyspaces.
func TestCreateDefaultShardRoutingRules(t *testing.T) {
	ctx := t.Context()

	ks1 := &testKeyspace{
		KeyspaceName: "sourceks",
	}
	ks2 := &testKeyspace{
		KeyspaceName: "targetks",
	}

	type testCase struct {
		name           string
		sourceKeyspace *testKeyspace
		targetKeyspace *testKeyspace
		shards         []string
		want           map[string]string
	}
	getExpectedRules := func(sourceKeyspace, targetKeyspace *testKeyspace) map[string]string {
		rules := make(map[string]string)
		for _, targetShard := range targetKeyspace.ShardNames {
			rules[fmt.Sprintf("%s.%s", targetKeyspace.KeyspaceName, targetShard)] = sourceKeyspace.KeyspaceName
		}
		return rules
	}
	testCases := []testCase{
		{
			name:           "unsharded",
			sourceKeyspace: ks1,
			targetKeyspace: ks2,
			shards:         []string{"0"},
		},
		{
			name:           "sharded",
			sourceKeyspace: ks2,
			targetKeyspace: ks1,
			shards:         []string{"-80", "80-"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.sourceKeyspace.ShardNames = tc.shards
			tc.targetKeyspace.ShardNames = tc.shards
			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			ms := &vtctldata.MaterializeSettings{
				Workflow:       "wf1",
				SourceKeyspace: tc.sourceKeyspace.KeyspaceName,
				TargetKeyspace: tc.targetKeyspace.KeyspaceName,
				TableSettings: []*vtctldata.TableMaterializeSettings{
					{
						TargetTable:      "t1",
						SourceExpression: "select * from t1",
					},
				},
				Cell:         "zone1",
				SourceShards: tc.sourceKeyspace.ShardNames,
			}
			err := createDefaultShardRoutingRules(ctx, ms, env.ts)
			require.NoError(t, err)
			rules, err := topotools.GetShardRoutingRules(ctx, env.ts)
			require.NoError(t, err)
			require.Len(t, rules, len(tc.shards))
			want := getExpectedRules(tc.sourceKeyspace, tc.targetKeyspace)
			require.Equal(t, want, rules)
		})
	}
}

// TestUpdateKeyspaceRoutingRule confirms that the keyspace routing rules are updated correctly.
func TestUpdateKeyspaceRoutingRule(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	routes := make(map[string]string)
	for _, tabletType := range tabletTypeSuffixes {
		routes["from"+tabletType] = "to"
	}
	err := updateKeyspaceRoutingRules(ctx, ts, "test", routes)
	require.NoError(t, err)
	rules, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err)
	require.Equal(t, routes, rules)
}

// TestConcurrentKeyspaceRoutingRulesUpdates runs multiple keyspace routing rules updates concurrently to test
// the locking mechanism.
func TestConcurrentKeyspaceRoutingRulesUpdates(t *testing.T) {
	if os.Getenv("GOCOVERDIR") != "" || race.Enabled {
		// While running this test in CI along with all other tests in for code coverage or race enabled,
		// this test hangs very often. Possibly due to some resource constraints, since this test is one
		// of the last. However just running this package by itself with code coverage works fine in CI.
		t.Logf("Skipping TestConcurrentKeyspaceRoutingRulesUpdates test in code coverage mode")
		t.Skip()
	}

	ctx := t.Context()

	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	t.Run("memtopo", func(t *testing.T) {
		testConcurrentKeyspaceRoutingRulesUpdates(t, ctx, ts)
	})

	etcdServerAddress := startEtcd(t)
	log.Info("Successfully started etcd server at " + etcdServerAddress)
	topoName := "etcd2_test" // "etcd2" is already registered on init(), so using a different name
	topo.RegisterFactory(topoName, etcd2topo.Factory{})
	ts, err := topo.OpenServer(topoName, etcdServerAddress, "/vitess")
	require.NoError(t, err)
	t.Run("etcd", func(t *testing.T) {
		testConcurrentKeyspaceRoutingRulesUpdates(t, ctx, ts)
		ts.Close()
	})
}

func testConcurrentKeyspaceRoutingRulesUpdates(t *testing.T, ctx context.Context, ts *topo.Server) {
	concurrency := 100
	duration := 10 * time.Second

	var wg sync.WaitGroup
	wg.Add(concurrency)

	shortCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	log.Info(fmt.Sprintf("Starting %d concurrent updates", concurrency))
	var (
		updateMu  sync.Mutex
		updateErr error
	)
	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-shortCtx.Done():
					return
				default:
					if err := update(shortCtx, ts, id); err != nil {
						updateMu.Lock()
						if updateErr == nil {
							updateErr = err
						}
						updateMu.Unlock()
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	require.NoError(t, updateErr)
	log.Info("All updates completed")
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Second)
	defer verifyCancel()
	rules, err := ts.GetKeyspaceRoutingRules(verifyCtx)
	require.NoError(t, err)
	require.LessOrEqual(t, concurrency, len(rules.Rules))
}

func update(ctx context.Context, ts *topo.Server, id int) error {
	s := fmt.Sprintf("%d_%d", id, rand.IntN(math.MaxInt))
	routes := make(map[string]string)
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		routes[from] = s + tabletType
	}
	err := updateKeyspaceRoutingRules(ctx, ts, "test", routes)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	got, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		if got[from] != s+tabletType {
			return fmt.Errorf("routing rule %q = %q, want %q", from, got[from], s+tabletType)
		}
	}
	return nil
}

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T) string {
	// Create a temporary directory.
	dataDir := t.TempDir()

	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", testfiles.GoVtVtctlWorkflowPort)
	peerAddr := fmt.Sprintf("http://localhost:%v", testfiles.GoVtVtctlWorkflowPeerPort)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)
	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)
	err := cmd.Start()
	require.NoError(t, err, "failed to start etcd")

	// Create a client to connect to the created etcd.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "newCellClient(%v) failed", clientAddr)
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			require.FailNow(t, "Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		if cmd.Process.Kill() != nil {
			log.Info(fmt.Sprintf("cmd.Process.Kill() failed : %v", err))
		}
	})

	return clientAddr
}

func TestValidateSourceTablesExist(t *testing.T) {
	ks := "source_keyspace"
	ksTables := []string{"table1", "table2"}

	testCases := []struct {
		name        string
		tables      []string
		errContains string
	}{
		{
			name:   "no error",
			tables: []string{"table2"},
		},
		{
			name:   "ignore internal table",
			tables: []string{"_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_", "table1", "table2"},
		},
		{
			name:        "table not found error",
			tables:      []string{"table3", "table1", "table2"},
			errContains: "table3",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSourceTablesExist(ks, ksTables, tc.tables)
			if tc.errContains != "" {
				assert.ErrorContains(t, err, tc.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLegacyBuildTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"

	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}

	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName),
				},
			},
		},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	result1 := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64",
		),
		"1|keyspace:\"source\" shard:\"-80\" filter:{rules:{match:\"t1\"} rules:{match:\"t2\"}}||||0|0|0",
	)
	result2 := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64",
		),
		"1|keyspace:\"source\" shard:\"80-\" filter:{rules:{match:\"t1\"} rules:{match:\"t2\"}}||||0|0|0",
		"2|keyspace:\"source\" shard:\"80-\" filter:{rules:{match:\"t3\"} rules:{match:\"t4\"}}||||0|0|0",
	)
	env.tmc.expectVRQuery(200, "select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow='wf1' and db_name='vt_targetks'", result1)
	env.tmc.expectVRQuery(210, "select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow='wf1' and db_name='vt_targetks'", result2)

	ti, err := LegacyBuildTargets(ctx, env.ts, env.tmc, targetKeyspace.KeyspaceName, workflowName, targetKeyspace.ShardNames)
	require.NoError(t, err)
	// Expect 2 targets as there are 2 target shards.
	assert.Len(t, ti.Targets, 2)

	assert.NotNil(t, ti.Targets["-80"])
	assert.NotNil(t, ti.Targets["80-"])

	t1 := ti.Targets["-80"]
	t2 := ti.Targets["80-"]
	assert.Len(t, t1.Sources, 1)
	assert.Len(t, t2.Sources, 2)
	assert.Len(t, t1.Sources[1].Filter.Rules, 2)

	assert.Equal(t, "t1", t1.Sources[1].Filter.Rules[0].Match)
	assert.Equal(t, "t2", t1.Sources[1].Filter.Rules[1].Match)
	assert.Equal(t, "-80", t1.Sources[1].Shard)

	assert.Len(t, t2.Sources[1].Filter.Rules, 2)
	assert.Len(t, t2.Sources[2].Filter.Rules, 2)

	assert.Equal(t, "80-", t2.Sources[1].Shard)
	assert.Equal(t, "80-", t2.Sources[2].Shard)
	assert.Equal(t, "t1", t2.Sources[1].Filter.Rules[0].Match)
	assert.Equal(t, "t2", t2.Sources[1].Filter.Rules[1].Match)
	assert.Equal(t, "t3", t2.Sources[2].Filter.Rules[0].Match)
	assert.Equal(t, "t4", t2.Sources[2].Filter.Rules[1].Match)
}
