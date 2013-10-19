package zktopo

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo/test"
)

func TestKeyspace(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckKeyspace(t, ts)
}

func TestShard(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckShard(t, ts)
}

func TestTablet(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckTablet(t, ts)
}

func TestShardReplication(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckShardReplication(t, ts)
}

func TestServingGraph(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckServingGraph(t, ts)
}

func TestKeyspaceLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckKeyspaceLock(t, ts)
}

func TestShardLock(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckShardLock(t, ts)
}

func TestPid(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckPid(t, ts)
}

func TestActions(t *testing.T) {
	ts := NewTestServer(t, []string{"test"})
	test.CheckActions(t, ts)
}
