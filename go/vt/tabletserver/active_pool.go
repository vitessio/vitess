// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
)

type ActivePool struct {
	pool       *pools.Numbered
	timeout    sync2.AtomicDuration
	ticks      *timer.Timer
	connKiller *ConnectionKiller
}

func NewActivePool(name string, queryTimeout time.Duration, connKiller *ConnectionKiller) *ActivePool {
	ap := &ActivePool{
		pool:       pools.NewNumbered(),
		timeout:    sync2.AtomicDuration(queryTimeout),
		ticks:      timer.NewTimer(queryTimeout / 10),
		connKiller: connKiller,
	}
	stats.Publish(name+"Size", stats.IntFunc(ap.pool.Size))
	stats.Publish(
		name+"Timeout",
		stats.DurationFunc(func() time.Duration { return ap.timeout.Get() }),
	)
	return ap
}

func (ap *ActivePool) Open() {
	ap.ticks.Start(func() { ap.killOutdatedQueries() })
}

func (ap *ActivePool) Close() {
	ap.ticks.Stop()
	ap.pool = pools.NewNumbered()
}

func (ap *ActivePool) killOutdatedQueries() {
	defer logError()
	for _, v := range ap.pool.GetOutdated(time.Duration(ap.Timeout()), "for abort") {
		ap.connKiller.Kill(v.(int64))
	}
}

func (ap *ActivePool) Put(id int64) {
	ap.pool.Register(id, id)
}

func (ap *ActivePool) Remove(id int64) {
	ap.pool.Unregister(id)
}

func (ap *ActivePool) Timeout() time.Duration {
	return ap.timeout.Get()
}

func (ap *ActivePool) SetTimeout(timeout time.Duration) {
	ap.timeout.Set(timeout)
	ap.ticks.SetInterval(timeout / 10)
}

func (ap *ActivePool) StatsJSON() string {
	s, t := ap.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Timeout\": %v}", s, int64(t))
}

func (ap *ActivePool) Stats() (size int64, timeout time.Duration) {
	return ap.pool.Size(), ap.Timeout()
}
