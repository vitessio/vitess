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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

const (
	leaderCheckInterval = 5 * time.Second
	evacHours           = 72
)

var checkInterval = flag.Duration("gc_check_interval", 1*time.Hour, "Interval between garbage collection checks")

// TODO(shlomi) uncomment
// var gcLifecycle = flag.String("table_gc_lifecycle", "hold,purge,evac,drop", "States for a DROP TABLE garbage collection cycle. Default is 'hold,purge,evac,drop', use any subset ('drop' implcitly always included)")

var (
	// TODO(shlomi): implement shortly
	// 	purgeLimit = 50
	// 	purgeQuery = "delete from %a limit %d"
	sqlShowVtTables = `show tables like '\_vt\_%'`
	sqlDropTable    = "drop table if exists `%a`"
)

type transitionRequest struct {
	fromTableName string
	toGCState     schema.TableGCState
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TableGC is the main entity in the table garbage collection mechanism. This service checks for tables to
// be held, purged, and dropped.
type TableGC struct {
	keyspace string
	shard    string
	dbName   string

	lagThrottler *throttle.Throttler
	isPrimary    int64
	isOpen       int64

	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server

	//TODO(shlomi): implement
	//	lastThrottleCheckTime time.Time

	initOnce   sync.Once
	initMutex  sync.Mutex
	purgeMutex sync.Mutex

	purgingTables          map[string]bool
	dropTablesChan         chan string
	transitionRequestsChan chan *transitionRequest
}

// DropperStatus published some status valus from the collector
type DropperStatus struct {
	Keyspace string
	Shard    string

	isPrimary bool
	IsOpen    bool

	purgingTables []string
}

// NewTableGC creates a table collector
func NewTableGC(env tabletenv.Env, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType, lagThrottler *throttle.Throttler) *TableGC {
	collector := &TableGC{
		lagThrottler: lagThrottler,
		isPrimary:    0,
		isOpen:       0,

		env:            env,
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		pool: connpool.NewPool(env, "TableGCPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),

		purgingTables:          map[string]bool{},
		dropTablesChan:         make(chan string),
		transitionRequestsChan: make(chan *transitionRequest),
	}

	return collector
}

// InitDBConfig initializes keyspace and shard
func (collector *TableGC) InitDBConfig(keyspace, shard, dbName string) {
	collector.keyspace = keyspace
	collector.shard = shard
	collector.dbName = dbName
}

// Open opens database pool and initializes the schema
func (collector *TableGC) Open() error {
	collector.initMutex.Lock()
	defer collector.initMutex.Unlock()
	if atomic.LoadInt64(&collector.isOpen) > 0 {
		// already open
		return nil
	}

	collector.pool.Open(collector.env.Config().DB.AppWithDB(), collector.env.Config().DB.DbaWithDB(), collector.env.Config().DB.AppDebugWithDB())
	collector.initOnce.Do(func() {
		// Operate() will be mindful of Open/Close state changes, so we only need to start it once.
		go collector.Operate(context.Background())
	})
	atomic.StoreInt64(&collector.isOpen, 1)

	return nil
}

// Close frees resources
func (collector *TableGC) Close() {
	collector.initMutex.Lock()
	defer collector.initMutex.Unlock()
	if atomic.LoadInt64(&collector.isOpen) == 0 {
		// not open
		return
	}

	collector.pool.Close()
	atomic.StoreInt64(&collector.isOpen, 0)
}

// Operate is the main entry point for the table garbage collector operation and logic.
func (collector *TableGC) Operate(ctx context.Context) {
	tableCheckTicker := time.NewTicker(*checkInterval)
	leaderCheckTicker := time.NewTicker(leaderCheckInterval)

	for {
		select {
		case <-leaderCheckTicker.C:
			{
				// sparse
				wasPreviouslyPrimary := (collector.isPrimary > 0)
				shouldBePrimary := false
				if atomic.LoadInt64(&collector.isOpen) > 0 {
					if collector.tabletTypeFunc() == topodatapb.TabletType_MASTER {
						shouldBePrimary = true
					}
				}
				if shouldBePrimary {
					atomic.StoreInt64(&collector.isPrimary, 1)
				} else {
					atomic.StoreInt64(&collector.isPrimary, 0)
				}

				if collector.isPrimary > 0 && wasPreviouslyPrimary {
					log.Infof("TableGC: transition into leadership")
				}
				if collector.isPrimary == 0 && wasPreviouslyPrimary {
					log.Infof("TableGC: transition out of leadership")
				}
			}
		case <-tableCheckTicker.C:
			{
				_ = collector.checkTables(ctx)
			}
		case dropTableName := <-collector.dropTablesChan:
			{
				_ = collector.dropTable(ctx, dropTableName)
			}
		case transition := <-collector.transitionRequestsChan:
			{
				_ = collector.transitionTable(ctx, transition)
			}
		}

		if collector.isPrimary == 0 {
			log.Infof("TableGC: not primary")
			time.Sleep(1 * time.Second)
		}
	}
}

func (collector *TableGC) checkTables(ctx context.Context) error {

	if atomic.LoadInt64(&collector.isPrimary) == 0 {
		return nil
	}

	conn, err := collector.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, sqlShowVtTables, 10000, true)
	if err != nil {
		return err
	}

	for _, row := range tm.Rows {
		tableName := row[0].ToString()

		isGCTable, state, t, err := schema.AnalyzeGCTableName(tableName)
		if err != nil {
			log.Errorf("TableGC: error while checking tables: %+v", err)
			continue
		}
		timeNow := time.Now().UTC()
		if !isGCTable {
			// irrelevant table
			continue
		}
		if timeNow.Before(t) {
			// net yet time to operate on this table
			continue
		}

		transition := func(fromTableName string, toGCState schema.TableGCState) {
			go func() {
				collector.transitionRequestsChan <- &transitionRequest{
					fromTableName: fromTableName,
					toGCState:     toGCState,
				}
			}()
		}

		if state == schema.HoldTableGCState {
			// Hold period expired. Moving to next state
			// TODO(shlomi): transition to next state
			transition(tableName, schema.PurgeTableGCState)
		}
		if state == schema.PurgeTableGCState {
			// This table needs to be purged. Make sure to enlist it (we may already have)
			collector.addPurgingTable(tableName)
			// TODO(shlomi): this should only happen after rows have been purged
			transition(tableName, schema.EvacTableGCState)
		}
		if state == schema.EvacTableGCState {
			// This table was in EVAC state for the required period. It will transition into DROP state
			transition(tableName, schema.DropTableGCState)
		}
		if state == schema.DropTableGCState {
			// This table needs to be dropped immediately.
			go func() { collector.dropTablesChan <- tableName }()
		}
	}

	return nil
}

func (collector *TableGC) dropTable(ctx context.Context, tableName string) error {

	if atomic.LoadInt64(&collector.isPrimary) == 0 {
		return nil
	}

	conn, err := collector.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	parsed := sqlparser.BuildParsedQuery(sqlDropTable, tableName)

	log.Infof("TableGC: dropping table: %s", tableName)
	_, err = conn.Exec(ctx, parsed.Query, 1, true)
	if err != nil {
		return err
	}
	log.Infof("TableGC: dropped table: %s", tableName)
	return nil
}

func (collector *TableGC) transitionTable(ctx context.Context, transition *transitionRequest) error {

	if atomic.LoadInt64(&collector.isPrimary) == 0 {
		return nil
	}

	conn, err := collector.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// when we transition into PURGE, that means we want to begin purging immediately
	// when we transition into DROP, that means we want to drop immediately
	t := time.Now().UTC()
	switch transition.toGCState {
	case schema.EvacTableGCState:
		// EVAC means we want the table pages to evacuate from the buffer pool. We self determine evac time
		t = t.Add(evacHours * time.Hour)
	}

	renameStatement, toTableName, err := schema.GenerateRenameStatement(transition.fromTableName, transition.toGCState, t)
	if err != nil {
		return err
	}

	log.Infof("TableGC: renaming table: %s to %s", transition.fromTableName, toTableName)
	_, err = conn.Exec(ctx, renameStatement, 1, true)
	if err != nil {
		return err
	}
	log.Infof("TableGC: renamed table: %s", transition.fromTableName)
	return nil
}

// addPurgingTable adds a table to the list of droppingpurging (or pending purging) tables
func (collector *TableGC) addPurgingTable(tableName string) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	collector.purgingTables[tableName] = true
}

// TODO(shlomi) uncomment
// // removePurgingTable removes a table from the purging list; likely this is called when
// // the table is fully purged and is renamed away to be dropped.
// func (collector *TableGC) removePurgingTable(tableName string) {
// 	collector.purgeMutex.Lock()
// 	defer collector.purgeMutex.Unlock()

// 	delete(collector.purgingTables, tableName)
// }

// nextTableToPurge returns the name of the next table we should start purging.
// We pick the table with the oldest timestamp.
func (collector *TableGC) nextTableToPurge() (tableName string, ok bool) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	if len(collector.purgingTables) == 0 {
		return "", false
	}
	tableNames := []string{}
	for tableName := range collector.purgingTables {
		tableNames = append(tableNames, tableName)
	}
	sort.SliceStable(tableNames, func(i, j int) bool {
		_, _, ti, _ := schema.AnalyzeGCTableName(tableNames[i])
		_, _, tj, _ := schema.AnalyzeGCTableName(tableNames[j])

		return ti.Before(tj)
	})
	return tableNames[0], true
}

// Status exports a status breakdown
func (collector *TableGC) Status() *DropperStatus {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	status := &DropperStatus{
		Keyspace: collector.keyspace,
		Shard:    collector.shard,

		isPrimary: (atomic.LoadInt64(&collector.isPrimary) > 0),
		IsOpen:    (atomic.LoadInt64(&collector.isOpen) > 0),
	}
	for tableName := range collector.purgingTables {
		status.purgingTables = append(status.purgingTables, tableName)
	}

	return status
}
