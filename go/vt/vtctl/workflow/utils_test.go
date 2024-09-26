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

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/etcd2topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
)

// TestCreateDefaultShardRoutingRules confirms that the default shard routing rules are created correctly for sharded
// and unsharded keyspaces.
func TestCreateDefaultShardRoutingRules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			require.EqualValues(t, want, rules)
		})
	}
}

// TestUpdateKeyspaceRoutingRule confirms that the keyspace routing rules are updated correctly.
func TestUpdateKeyspaceRoutingRule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	require.EqualValues(t, routes, rules)
}

// TestConcurrentKeyspaceRoutingRulesUpdates runs multiple keyspace routing rules updates concurrently to test
// the locking mechanism.
func TestConcurrentKeyspaceRoutingRulesUpdates(t *testing.T) {
	if os.Getenv("GOCOVERDIR") != "" {
		// While running this test in CI along with all other tests in for code coverage this test hangs very often.
		// Possibly due to some resource constraints, since this test is one of the last.
		// However just running this package by itself with code coverage works fine in CI.
		t.Logf("Skipping TestConcurrentKeyspaceRoutingRulesUpdates test in code coverage mode")
		t.Skip()
	}

	ctx := context.Background()

	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	t.Run("memtopo", func(t *testing.T) {
		testConcurrentKeyspaceRoutingRulesUpdates(t, ctx, ts)
	})

	etcdServerAddress := startEtcd(t)
	log.Infof("Successfully started etcd server at %s", etcdServerAddress)
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
	log.Infof("Starting %d concurrent updates", concurrency)
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-shortCtx.Done():
					return
				default:
					update(t, ts, id)
				}
			}
		}(i)
	}
	wg.Wait()
	log.Infof("All updates completed")
	rules, err := ts.GetKeyspaceRoutingRules(ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, concurrency, len(rules.Rules))
}

func update(t *testing.T, ts *topo.Server, id int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := fmt.Sprintf("%d_%d", id, rand.IntN(math.MaxInt))
	routes := make(map[string]string)
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		routes[from] = s + tabletType
	}
	err := updateKeyspaceRoutingRules(ctx, ts, "test", routes)
	require.NoError(t, err)
	got, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err)
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		require.Equal(t, s+tabletType, got[from])
	}
}

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T) string {
	// Create a temporary directory.
	dataDir := t.TempDir()

	// Get our two ports to listen to.
	port := testfiles.GoVtTopoEtcd2topoPort
	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", port)
	peerAddr := fmt.Sprintf("http://localhost:%v", port+1)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			log.Infof("cmd.Process.Kill() failed : %v", err)
		}
	})

	return clientAddr
}
