/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gc

import (
	"context"
	"flag"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

const (
	leaderCheckInterval = 5 * time.Second
)

var checkInterval = flag.Duration("gc_check_interval", 1*time.Hour, "Interval between garbage collection checks")

// TODO(shlomi): implement shortly
// var (
// 	purgeLimit = 50
// 	purgeQuery = "delete from %a limit %d"
// )

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TableDropper is the main entity in the table garbage collection mechanism. This service checks for tables to
// be held, purged, and dropped.
type TableDropper struct {
	keyspace string
	shard    string

	lagThrottler *throttle.Throttler
	isPrimary    bool
	isOpen       int64

	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server

	//TODO(shlomi): implement
	//	lastCheckTimeNano int64

	initOnce  sync.Once
	initMutex sync.Mutex
}

// DropperStatus published some status valus from the dropper
type DropperStatus struct {
	Keyspace string
	Shard    string

	IsLeader bool
	IsOpen   bool
}

// NewTableDropper creates a table dropper
func NewTableDropper(env tabletenv.Env, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType, lagThrottler *throttle.Throttler) *TableDropper {
	dropper := &TableDropper{
		lagThrottler: lagThrottler,
		isPrimary:    false,
		isOpen:       0,

		env:            env,
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		pool: connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}

	return dropper
}

// InitDBConfig initializes keyspace and shard
func (dropper *TableDropper) InitDBConfig(keyspace, shard string) {
	dropper.keyspace = keyspace
	dropper.shard = shard
}

// Open opens database pool and initializes the schema
func (dropper *TableDropper) Open() error {
	dropper.initMutex.Lock()
	defer dropper.initMutex.Unlock()
	if atomic.LoadInt64(&dropper.isOpen) > 0 {
		// already open
		return nil
	}

	dropper.pool.Open(dropper.env.Config().DB.AppWithDB(), dropper.env.Config().DB.DbaWithDB(), dropper.env.Config().DB.AppDebugWithDB())
	dropper.initOnce.Do(func() {
		// Operate() will be mindful of Open/Close state changes, so we only need to start it once.
		go dropper.Operate(context.Background())
	})
	atomic.StoreInt64(&dropper.isOpen, 1)

	return nil
}

// Close frees resources
func (dropper *TableDropper) Close() {
	dropper.initMutex.Lock()
	defer dropper.initMutex.Unlock()
	if atomic.LoadInt64(&dropper.isOpen) == 0 {
		// not open
		return
	}

	dropper.pool.Close()
	atomic.StoreInt64(&dropper.isOpen, 0)
}

// Operate is the main entry point for the throttler operation and logic. It will
// run the probes, colelct metrics, refresh inventory, etc.
func (dropper *TableDropper) Operate(ctx context.Context) {

	tableCheckTicker := time.NewTicker(*checkInterval)
	leaderCheckTicker := time.NewTicker(leaderCheckInterval)

	for {
		select {
		case <-leaderCheckTicker.C:
			{
				// sparse
				wasPreviouslyPrimary := dropper.isPrimary
				shouldBePrimary := false
				if atomic.LoadInt64(&dropper.isOpen) > 0 {
					if dropper.tabletTypeFunc() == topodatapb.TabletType_MASTER {
						shouldBePrimary = true
					}
				}
				dropper.isPrimary = shouldBePrimary

				if dropper.isPrimary && !wasPreviouslyPrimary {
					log.Infof("TableDropper: transition into leadership")
				}
				if wasPreviouslyPrimary && !dropper.isPrimary {
					log.Infof("TableDropper: transition out of leadership")
				}
			}
		case <-tableCheckTicker.C:
			{
			}
		}
		if !dropper.isPrimary {
			log.Infof("TableDropper: not leader")
			time.Sleep(1 * time.Second)
		}
	}
}

// Status exports a status breakdown
func (dropper *TableDropper) Status() *DropperStatus {
	return &DropperStatus{
		Keyspace: dropper.keyspace,
		Shard:    dropper.shard,

		IsLeader: dropper.isPrimary,
		IsOpen:   (atomic.LoadInt64(&dropper.isOpen) > 0),
	}
}
