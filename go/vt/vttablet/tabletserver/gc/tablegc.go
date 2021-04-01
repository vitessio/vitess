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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconnpool"
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
	// evacHours is a hard coded, reasonable time for a table to spend in EVAC state
	evacHours        = 72
	throttlerAppName = "tablegc"
)

// checkInterval marks the interval between looking for tables in mysql server/schema
var checkInterval = flag.Duration("gc_check_interval", 1*time.Hour, "Interval between garbage collection checks")

// gcLifecycle is the sequence of steps the table goes through in the process of getting dropped
var gcLifecycle = flag.String("table_gc_lifecycle", "hold,purge,evac,drop", "States for a DROP TABLE garbage collection cycle. Default is 'hold,purge,evac,drop', use any subset ('drop' implcitly always included)")

var (
	sqlPurgeTable       = `delete from %a limit 50`
	sqlShowVtTables     = `show tables like '\_vt\_%'`
	sqlDropTable        = "drop table if exists `%a`"
	purgeReentranceFlag int64
)

// transitionRequest encapsulates a request to transition a table to next state
type transitionRequest struct {
	fromTableName string
	toGCState     schema.TableGCState
	uuid          string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TableGC is the main entity in the table garbage collection mechanism.
// This service "garbage collects" tables:
// - it checks for magically-named tables (e.g. _vt_EVAC_f6338b2af8af11eaa210f875a4d24e90_20200920063522)
// - it analyzes a table's state from its name
// - it applies operations on the table (namely purge for PURGE tables)
// - when due time, it transitions a table (via RENAME TABLE) to the next state
// - finally, it issues a DROP TABLE
// The sequence of steps is controlled by the command line variable -table_gc_lifecycle
type TableGC struct {
	keyspace string
	shard    string
	dbName   string

	isPrimary int64
	isOpen    int64

	throttlerClient *throttle.Client

	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server

	initMutex  sync.Mutex
	purgeMutex sync.Mutex

	tickers [](*timer.SuspendableTicker)

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
		throttlerClient: throttle.NewBackgroundClient(lagThrottler, throttlerAppName, throttle.ThrottleCheckPrimaryWrite),
		isPrimary:       0,
		isOpen:          0,

		env:            env,
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		pool: connpool.NewPool(env, "TableGCPool", tabletenv.ConnPoolConfig{
			Size:               2,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),

		tickers: [](*timer.SuspendableTicker){},

		purgingTables:          map[string]bool{},
		dropTablesChan:         make(chan string),
		transitionRequestsChan: make(chan *transitionRequest),
		purgeRequestsChan:      make(chan bool),
	}

	return collector
}

// InitDBConfig initializes keyspace and shard
func (collector *TableGC) InitDBConfig(keyspace, shard, dbName string) {
	log.Info("TableGC: init")
	collector.keyspace = keyspace
	collector.shard = shard
	collector.dbName = dbName
	go collector.Operate(context.Background())
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

	log.Info("TableGC: opening")
	collector.pool.Open(collector.env.Config().DB.AppWithDB(), collector.env.Config().DB.DbaWithDB(), collector.env.Config().DB.AppDebugWithDB())
	atomic.StoreInt64(&collector.isOpen, 1)

	for _, t := range collector.tickers {
		t.Resume()
	}

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

	log.Info("TableGC: closing")
	for _, t := range collector.tickers {
		t.Suspend()
	}
	collector.pool.Close()
	atomic.StoreInt64(&collector.isOpen, 0)
}

// Operate is the main entry point for the table garbage collector operation and logic.
func (collector *TableGC) Operate(ctx context.Context) {

	addTicker := func(d time.Duration) *timer.SuspendableTicker {
		collector.initMutex.Lock()
		defer collector.initMutex.Unlock()

		t := timer.NewSuspendableTicker(d, true)
		collector.tickers = append(collector.tickers, t)
		return t
	}
	tableCheckTicker := addTicker(*checkInterval)
	leaderCheckTicker := addTicker(leaderCheckInterval)
	purgeReentranceTicker := addTicker(purgeReentranceInterval)

	log.Info("TableGC: operating")
	for {
		select {
		case <-leaderCheckTicker.C:
			{
				func() {
					collector.initMutex.Lock()
					defer collector.initMutex.Unlock()
					// sparse
					shouldBePrimary := false
					if atomic.LoadInt64(&collector.isOpen) > 0 {
						if collector.tabletTypeFunc() == topodatapb.TabletType_MASTER {
							shouldBePrimary = true
						}
					}

					if collector.isPrimary == 0 && shouldBePrimary {
						log.Infof("TableGC: transition into primary")
					}
					if collector.isPrimary > 0 && !shouldBePrimary {
						log.Infof("TableGC: transition out of primary")
					}

					if shouldBePrimary {
						atomic.StoreInt64(&collector.isPrimary, 1)
					} else {
						atomic.StoreInt64(&collector.isPrimary, 0)
					}
				}()
			}
		case <-tableCheckTicker.C:
			{
				_ = collector.checkTables(ctx)
			}
		case <-purgeReentranceTicker.C:
			{
				// relay the request
				go func() { collector.purgeRequestsChan <- true }()
			}
		case <-collector.purgeRequestsChan:
			{
				go func() {
					if tableName, err := collector.purge(ctx); err != nil {
						log.Errorf("TableGC: error purging table %s: %+v", tableName, err)
					}
				}()
			}
		case dropTableName := <-collector.dropTablesChan:
			{
				if err := collector.dropTable(ctx, dropTableName); err != nil {
					log.Errorf("TableGC: error dropping table %s: %+v", dropTableName, err)
				}
			}
		case transition := <-collector.transitionRequestsChan:
			{
				if err := collector.transitionTable(ctx, transition); err != nil {
					log.Errorf("TableGC: error transitioning table %s to %+v: %+v", transition.fromTableName, transition.toGCState, err)
				}
			}
		}
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
func (collector *TableGC) generateTansition(ctx context.Context, fromState schema.TableGCState, fromTableName, uuid string) *transitionRequest {
	nextState := collector.nextState(fromState)
	if nextState == nil {
		return nil
	}
	return &transitionRequest{
		fromTableName: fromTableName,
		toGCState:     *nextState,
		uuid:          uuid,
	}
}

// submitTransitionRequest generates and queues a transition request for a given table
func (collector *TableGC) submitTransitionRequest(ctx context.Context, fromState schema.TableGCState, fromTableName, uuid string) {
	log.Infof("TableGC: submitting transition request for %s", fromTableName)
	go func() {
		transition := collector.generateTansition(ctx, fromState, fromTableName, uuid)
		if transition != nil {
			collector.transitionRequestsChan <- transition
		}
	}()
}

// shouldTransitionTable checks if the given table is a GC table and if it's time to transition it to next state
func (collector *TableGC) shouldTransitionTable(tableName string) (shouldTransition bool, state schema.TableGCState, uuid string, err error) {
	isGCTable, state, uuid, t, err := schema.AnalyzeGCTableName(tableName)
	if err != nil {
		return false, state, uuid, err
	}
	if !isGCTable {
		// irrelevant table
		return false, state, uuid, nil
	}
	if _, ok := collector.lifecycleStates[state]; ok {
		// this state is in our expected lifecycle. Let's check table's time hint:
		timeNow := time.Now().UTC()
		if timeNow.Before(t) {
			// not yet time to operate on this table
			return false, state, uuid, nil
		}
		// If the state is not in our expected lifecycle, we ignore the time hint and just move it to the next phase
	}
	return true, state, uuid, nil
}

// checkTables looks for potential GC tables in the MySQL server+schema.
// It lists _vt_% tables, then filters through those which are due-date.
// It then applies the necessary operation per table.
func (collector *TableGC) checkTables(ctx context.Context) error {
	if atomic.LoadInt64(&collector.isPrimary) == 0 {
		return nil
	}

	conn, err := collector.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	log.Infof("TableGC: check tables")

	res, err := conn.Exec(ctx, sqlShowVtTables, math.MaxInt32, true)
	if err != nil {
		return err
	}

	for _, row := range res.Rows {
		tableName := row[0].ToString()

		shouldTransition, state, uuid, err := collector.shouldTransitionTable(tableName)

		if err != nil {
			log.Errorf("TableGC: error while checking tables: %+v", err)
			continue
		}
		if !shouldTransition {
			// irrelevant table
			continue
		}

		log.Infof("TableGC: will operate on table %s", tableName)

		if state == schema.HoldTableGCState {
			// Hold period expired. Moving to next state
			collector.submitTransitionRequest(ctx, state, tableName, uuid)
		}
		if state == schema.PurgeTableGCState {
			// This table needs to be purged. Make sure to enlist it (we may already have)
			collector.addPurgingTable(tableName)
		}
		if state == schema.EvacTableGCState {
			// This table was in EVAC state for the required period. It will transition into DROP state
			collector.submitTransitionRequest(ctx, state, tableName, uuid)
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
func (collector *TableGC) purge(ctx context.Context) (tableName string, err error) {
	if atomic.CompareAndSwapInt64(&purgeReentranceFlag, 0, 1) {
		defer atomic.StoreInt64(&purgeReentranceFlag, 0)
	} else {
		// An instance of this function is already running
		return "", nil
	}

	tableName, found := collector.nextTableToPurge()
	if !found {
		// Nothing do do here...
		return "", nil
	}

	conn, err := dbconnpool.NewDBConnection(ctx, collector.env.Config().DB.DbaWithDB())
	if err != nil {
		return tableName, err
	}
	defer conn.Close()

	// Disable binary logging, re-enable afterwards
	// The idea is that DROP TABLE can be expensive, on the primary, if the table is not empty.
	// However, on replica the price is not as high. Therefore, we only purge the rows on the primary.
	// This saves a lot of load from the replication stream, avoiding excessive lags. It also
	// avoids excessive IO on the replicas.
	// (note that the user may skip the PURGE step if they want, but the step is on by default)

	// However, disabling SQL_LOG_BIN requires SUPER privileges, and we don't know that we have that.
	// Any externally managed database might not give SUPER privileges to the vitess accounts, and this is known to be the case for Amazon Aurora.
	// We therefore disable log bin on best-effort basis. The logic is still fine and sound if binary logging
	// is left enabled. We just lose some optimization.
	disableLogBin := func() (bool, error) {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err == nil {
			return true, nil
		}
		if merr, ok := err.(*mysql.SQLError); ok {
			if merr.Num == mysql.ERSpecifiedAccessDenied {
				// We do not have privileges to disable binary logging. That's fine, we're on best effort,
				// so we're going to silently ignore this error.
				return false, nil
			}
		}
		// We do not tolerate other errors, though.
		return false, err
	}
	sqlLogBinDisabled, err := disableLogBin()
	if err != nil {
		return tableName, err
	}

	defer func() {
		if sqlLogBinDisabled && !conn.IsClosed() {
			if _, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false); err != nil {
				log.Errorf("TableGC: error setting sql_log_bin = ON: %+v", err)
				// a followup defer() will run conn.Close() at any case.
			}
		}
	}()

	log.Infof("TableGC: purge begin for %s", tableName)
	for {
		if !collector.throttlerClient.ThrottleCheckOKOrWait(ctx) {
			continue
		}
		// OK, we're clear to go!

		// Issue a DELETE
		parsed := sqlparser.BuildParsedQuery(sqlPurgeTable, tableName)
		res, err := conn.ExecuteFetch(parsed.Query, 1, true)
		if err != nil {
			return tableName, err
		}
		if res.RowsAffected == 0 {
			// The table is now empty!
			// we happen to know at this time that the table is in PURGE state,
			// I mean, that's why we're here. We can hard code that.
			_, _, uuid, _, _ := schema.AnalyzeGCTableName(tableName)
			collector.submitTransitionRequest(ctx, schema.PurgeTableGCState, tableName, uuid)
			collector.removePurgingTable(tableName)
			// finished with this table. Maybe more tables are looking to be purged.
			// Trigger another call to purge(), instead of waiting a full purgeReentranceInterval cycle
			log.Infof("TableGC: purge complete for %s", tableName)
			time.AfterFunc(time.Second, func() { collector.purgeRequestsChan <- true })
			return tableName, nil
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

	renameStatement, toTableName, err := schema.GenerateRenameStatementWithUUID(transition.fromTableName, transition.toGCState, transition.uuid, t)
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
		_, _, _, ti, _ := schema.AnalyzeGCTableName(tableNames[i])
		_, _, _, tj, _ := schema.AnalyzeGCTableName(tableNames[j])

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
