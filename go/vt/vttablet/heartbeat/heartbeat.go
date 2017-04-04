// Package heartbeat contains a writer and reader of heartbeats for a master-slave cluster.
// This is similar to Percona's pt-heartbeat, and is meant to supplement the information
// returned from SHOW SLAVE STATUS. In some circumstances, lag returned from SHOW SLAVE STATUS
// is incorrect and is at best only at 1 second resolution. The heartbeat package directly
// tests replication by writing a record with a timestamp on the master, and comparing that
// timestamp after reading it on the slave. This happens at the interval defined by heartbeat_interval
//
// The data collected by the heartbeat package is made available in /debug/vars in HeartbeatCounters
// and HeartbeatRates. It's additionally used as a source for healthchecks and will impact the serving
// state of a tablet, if enabled. The heartbeat interval is purposefully kept distinct from the health check
// interval because lag measurement requires more frequent polling that the healthcheck typically is
// configured for.
package heartbeat

import (
	"context"
	"flag"
	"time"

	"github.com/youtube/vitess/go/stats"
)

var (
	enableHeartbeat = flag.Bool("enable_heartbeat", false, "If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the table _vt.heartbeat. The result is used to inform the serving state of the vttablet via healthchecks.")
	interval        = flag.Duration("heartbeat_interval", 1*time.Second, "How frequently to read and write replication heartbeat.")

	// HeartbeatCounters and HeartbeatRates provide information on the processing of heartbeats, when enabled.
	// For masters, we increment a Writes counter for every write.
	// For replicas, we increment a Reads counter for every read, and a LagNs counter. LagNs represents the
	//      time in nanoseconds that a replica is lagged at each report interval, by comparing the timestamp
	//      in the heartbeat table against the current time when reporting
	// Both masters and replicas will increment an Errors counter if any errors occur.
	counters = stats.NewCounters("HeartbeatCounters")
	// HeartbeatRates turns each counter in HeartbeatCounters into a rate. This allows
	// reporting on the moving average change in lag or validate that the reads/writes
	// are happening at the expected intervals over time.
	_ = stats.NewRates("HeartbeatRates", counters, 15, 60*time.Second)
)

// waitOrExit will wait until the interval is finished or the context is cancelled.
func waitOrExit(ctx context.Context, interval time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(interval):
		return false
	}
}
