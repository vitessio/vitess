// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"time"

	"github.com/youtube/vitess/go/vt/discovery"
)

// replicationLagRecord stores the tablet health data for a given point in time.
// This data is obtained via the HealthCheck module.
type replicationLagRecord struct {
	// time is the time at which "value" was observed.
	time time.Time

	// TabletStats holds a copy of the current health data of the tablet.
	discovery.TabletStats
}

func (r replicationLagRecord) isZero() bool {
	return r.Stats == nil && r.time.IsZero()
}

func (r replicationLagRecord) lag() int64 {
	return int64(r.Stats.SecondsBehindMaster)
}
