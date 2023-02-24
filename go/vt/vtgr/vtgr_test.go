package vtgr

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgr/config"
	"vitess.io/vitess/go/vt/vtgr/controller"
	"vitess.io/vitess/go/vt/vtgr/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSighupHandle(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	vtgr := newVTGR(
		ctx,
		ts,
		tmclient.NewTabletManagerClient(),
	)
	var shards []*controller.GRShard
	config := &config.VTGRConfig{
		DisableReadOnlyProtection:   false,
		BootstrapGroupSize:          5,
		MinNumReplica:               3,
		BackoffErrorWaitTimeSeconds: 10,
		BootstrapWaitTimeSeconds:    10 * 60,
	}
	shards = append(shards, controller.NewGRShard("ks", "0", nil, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, localDbPort, true))
	vtgr.Shards = shards
	shard := vtgr.Shards[0]
	shard.LockShard(ctx, "test")
	var res atomic.Bool
	vtgr.handleSignal(func(i int) {
		res.Store(true)
	})
	assert.NotNil(t, shard.GetUnlock())
	assert.False(t, vtgr.stopped.Load())
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, res.Load())
	assert.Nil(t, shard.GetUnlock())
	assert.True(t, vtgr.stopped.Load())
}
