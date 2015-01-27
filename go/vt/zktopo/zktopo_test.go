package zktopo

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo/test"
)

func TestKeyspace(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShard(context.Background(), t, ts)
}

func TestTablet(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckTablet(context.Background(), t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckServingGraph(context.Background(), t, ts)
}

func TestWatchEndPoints(t *testing.T) {
	WatchSleepDuration = 2 * time.Millisecond
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckWatchEndPoints(context.Background(), t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardLock(t, ts)
}

func TestSrvShardLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wait-based test in short mode.")
	}

	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckSrvShardLock(t, ts)
}

func TestVSchema(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckVSchema(t, ts)
}
