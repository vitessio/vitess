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
	"fmt"
	"math"
	"math/rand"
	"net/http"
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
	leaderCheckInterval     = 5 * time.Second
	purgeReentranceInterval = 1 * time.Minute
	// throttleCheckDuration controls both how frequently the throttler is checked. as well as
	// how long to sleep if throttler blocks us
	throttleCheckDuration = 250 * time.Millisecond
	evacHours             = 72
	throttlerAppName      = "tablegc"
)

var checkInterval = flag.Duration("gc_check_interval", 1*time.Hour, "Interval between garbage collection checks")
var gcLifecycle = flag.String("table_gc_lifecycle", "hold,purge,evac,drop", "States for a DROP TABLE garbage collection cycle. Default is 'hold,purge,evac,drop', use any subset ('drop' implcitly always included)")

var (
	sqlPurgeTable   = `delete from %a limit 50`
	sqlShowVtTables = `show tables like '\_vt\_%'`
	sqlDropTable    = "drop table if exists `%a`"
	throttleFlags   = &throttle.CheckFlags{
		LowPriority: true,
	}
	purgeReentranceFlag int64
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

	lastSuccessfulThrottleCheck time.Time

	initOnce   sync.Once
	initMutex  sync.Mutex
	purgeMutex sync.Mutex

	purgingTables          map[string]bool
	dropTablesChan         chan string
	transitionRequestsChan chan *transitionRequest
	purgeRequestsChan      chan bool
	// lifecycleStates indicates what states a GC table goes through. The user can set
	// this with -table_gc_lifecycle, such that some states can be skipped.
	lifecycleStates map[schema.TableGCState]bool
}

// GCStatus published some status valus from the collector
type GCStatus struct {
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
			Size:               3, // must be at least 2: purge cycle can use 1 busily.
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),

		purgingTables:          map[string]bool{},
		dropTablesChan:         make(chan string),
		transitionRequestsChan: make(chan *transitionRequest),
		purgeRequestsChan:      make(chan bool),
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
func (collector *TableGC) Open() (err error) {
	collector.initMutex.Lock()
	defer collector.initMutex.Unlock()
	if atomic.LoadInt64(&collector.isOpen) > 0 {
		// already open
		return nil
	}
	collector.lifecycleStates, err = schema.ParseGCLifecycle(*gcLifecycle)
	if err != nil {
		return fmt.Errorf("Error parsing -table_gc_lifecycle flag: %+v", err)
	}
	collector.lifecycleStates[schema.DropTableGCState] = true

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
	purgeReentranceTicker := time.NewTicker(purgeReentranceInterval)

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
		case <-purgeReentranceTicker.C:
			{
				fmt.Printf("========= purgeReentranceTicker\n")
				go collector.purge(ctx)
			}
		case <-collector.purgeRequestsChan:
			{
				fmt.Printf("========= purgeRequestsChan\n")
				go collector.purge(ctx)
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
		fmt.Printf("========= status=%+v\n", collector.Status())
	}
}

// nextState evaluates what the next state should be, given a state; this takes into account
// lifecycleStates (as generated by user supplied -table_gc_lifecycle flag)
func (collector *TableGC) nextState(fromState schema.TableGCState) *schema.TableGCState {
	var state schema.TableGCState
	switch fromState {
	case schema.HoldTableGCState:
		state = schema.PurgeTableGCState
	case schema.PurgeTableGCState:
		state = schema.EvacTableGCState
	case schema.EvacTableGCState:
		state = schema.DropTableGCState
	case schema.DropTableGCState:
		return nil
	default:
		return nil
	}
	if _, ok := collector.lifecycleStates[state]; !ok {
		return collector.nextState(state)
	}
	return &state
}

// generateTansition creates a transition request, based on current state and taking configured lifecycleStates
// into consideration (we may skip some states)
func (collector *TableGC) generateTansition(ctx context.Context, fromState schema.TableGCState, fromTableName string) *transitionRequest {
	nextState := collector.nextState(fromState)
	if nextState == nil {
		return nil
	}
	return &transitionRequest{
		fromTableName: fromTableName,
		toGCState:     *nextState,
	}
}

func (collector *TableGC) submitTransitionRequest(ctx context.Context, fromState schema.TableGCState, fromTableName string) {
	go func() {
		transition := collector.generateTansition(ctx, fromState, fromTableName)
		if transition != nil {
			collector.transitionRequestsChan <- transition
		}
	}()
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

	res, err := conn.Exec(ctx, sqlShowVtTables, math.MaxInt32, true)
	if err != nil {
		return err
	}

	for _, row := range res.Rows {
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

		if state == schema.HoldTableGCState {
			// Hold period expired. Moving to next state
			collector.submitTransitionRequest(ctx, state, tableName)
		}
		if state == schema.PurgeTableGCState {
			// This table needs to be purged. Make sure to enlist it (we may already have)
			collector.addPurgingTable(tableName)
		}
		if state == schema.EvacTableGCState {
			// This table was in EVAC state for the required period. It will transition into DROP state
			collector.submitTransitionRequest(ctx, state, tableName)
		}
		if state == schema.DropTableGCState {
			// This table needs to be dropped immediately.
			go func() { collector.dropTablesChan <- tableName }()
		}
	}

	return nil
}

// purge continuously purges rows from a table.
// This function is non-reentrant: there's only one instance of this function running at any given time.
// A timer keeps calling this function, so if it bails out (e.g. on error) it will later resume work
func (collector *TableGC) purge(ctx context.Context) error {
	fmt.Printf("========= purge\n")
	if atomic.CompareAndSwapInt64(&purgeReentranceFlag, 0, 1) {
		defer atomic.StoreInt64(&purgeReentranceFlag, 0)
	} else {
		// An instance of this function is already running
		return nil
	}
	fmt.Printf("========= purge ENTRY\n")

	tableName, found := collector.nextTableToPurge()
	if !found {
		// Nothing do do here...
		return nil
	}
	fmt.Printf("========= purge tableName=%+v\n", tableName)
	conn, err := collector.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// Disable binary logging, re-enable afterwards
	if _, err := conn.Exec(ctx, "SET sql_log_bin = OFF", 0, false); err != nil {
		fmt.Printf("========= 1 err=%+v\n", err)
		return err
	}
	fmt.Printf("======  SQL_LOG_BIN=OFF\n")
	defer func() {
		if !conn.IsClosed() {
			if _, err := conn.Exec(ctx, "SET sql_log_bin = ON", 0, false); err != nil {
				fmt.Printf("========= 2 err=%+v\n", err)
				// if we can't reset the sql_log_bin flag, let's just close the connection.
				conn.Close()
			}
			fmt.Printf("======  SQL_LOG_BIN=ON\n")
		}
	}()

	for {
		fmt.Printf("========= purge tableName=%+v LOOP\n", tableName)
		if time.Since(collector.lastSuccessfulThrottleCheck) > throttleCheckDuration {
			// It's time to run a throttler check
			checkResult := collector.lagThrottler.Check(ctx, throttlerAppName, "", throttleFlags)
			if checkResult.StatusCode != http.StatusOK {
				// whoops, we got throttled. Back off, sleep, try again
				time.Sleep(throttleCheckDuration)
				continue
			}
			collector.lastSuccessfulThrottleCheck = time.Now()
		}
		// OK, we're clear to go!

		// Issue a DELETE
		parsed := sqlparser.BuildParsedQuery(sqlPurgeTable, tableName)
		fmt.Printf("========= purge tableName=%+v parsed=%+v\n", tableName, parsed)
		res, err := conn.Exec(ctx, parsed.Query, 1, true)
		if err != nil {
			return err
		}
		if res.RowsAffected == 0 {
			fmt.Printf("========= purge tableName=%+v DONE\n", tableName)
			// The table is now empty!
			// we happen to know at this time that the table is in PURGE state,
			// I mean, that's why we're here. We can hard code that.
			collector.submitTransitionRequest(ctx, schema.PurgeTableGCState, tableName)
			collector.removePurgingTable(tableName)
			go func() { collector.purgeRequestsChan <- true }()
			return nil
		}
	}
}

// dropTable runs an actual DROP TABLE statement, and marks the end of the line for the
// tables' GC lifecycle.
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

// transitionTable is called upon a transition request. The actual implementation of a transition
// is a RENAME TABLE statement.
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
	// Thereforce the default timestamp is Now
	t := time.Now().UTC()
	switch transition.toGCState {
	case schema.EvacTableGCState:
		// in EVAC state  we want the table pages to evacuate from the buffer pool. We therefore
		// set the timestamp to some point the future, which we self determine
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

// removePurgingTable removes a table from the purging list; likely this is called when
// the table is fully purged and is renamed away to be dropped.
func (collector *TableGC) removePurgingTable(tableName string) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	delete(collector.purgingTables, tableName)
}

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
func (collector *TableGC) Status() *GCStatus {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	status := &GCStatus{
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
