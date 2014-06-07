package zktopo

import (
	"testing"

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
	test.CheckShard(t, ts)
}

func TestTablet(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckTablet(t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckServingGraph(t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckShardLock(t, ts)
}

func TestSrvShardLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckSrvShardLock(t, ts)
}

func TestPid(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckPid(t, ts)
}

func TestActions(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	defer ts.Close()
	test.CheckActions(t, ts)
}
