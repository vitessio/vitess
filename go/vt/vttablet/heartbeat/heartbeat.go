package heartbeat

import (
	"context"
	"flag"
	"time"

	"github.com/youtube/vitess/go/stats"
)

var (
	enableHeartbeat = flag.Bool("enable_heartbeat", false, "Should vttablet record (if master) or check (if replica) replication heartbeat.")
	interval        = flag.Duration("heartbeat_interval", 1*time.Second, "How frequently to read and write replication heartbeat. Default 1s")

	counters = stats.NewCounters("HeartbeatCounters")
	_        = stats.NewRates("HeartbeatRates", counters, 15, 60*time.Second)
)

func waitOrExit(ctx context.Context, interval time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(interval):
		return false
	}
}
