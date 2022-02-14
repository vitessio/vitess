package throttler

import (
	"time"

	"vitess.io/vitess/go/vt/discovery"
)

// replicationLagRecord stores the tablet health data for a given point in time.
// This data is obtained via the LegacyHealthCheck module.
type replicationLagRecord struct {
	// time is the time at which "value" was observed.
	time time.Time

	// LegacyTabletStats holds a copy of the current health data of the tablet.
	discovery.LegacyTabletStats
}

func (r replicationLagRecord) isZero() bool {
	return r.Stats == nil && r.time.IsZero()
}

func (r replicationLagRecord) lag() int64 {
	return int64(r.Stats.ReplicationLagSeconds)
}
