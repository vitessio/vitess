/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/withddl"
)

const (
	reshardingJournalTableName = "_vt.resharding_journal"
	vreplicationTableName      = "_vt.vreplication"
	copyStateTableName         = "_vt.copy_state"

	createReshardingJournalTable = `create table if not exists _vt.resharding_journal(
  id bigint,
  db_name varbinary(255),
  val blob,
  primary key (id)
)`

	createCopyState = `create table if not exists _vt.copy_state (
  vrepl_id int,
  table_name varbinary(128),
  lastpk varbinary(2000),
  primary key (vrepl_id, table_name))`
)

var withDDL *withddl.WithDDL

const (
	throttlerAppName = "vreplication"
)

func init() {
	allddls := append([]string{}, binlogplayer.CreateVReplicationTable()...)
	allddls = append(allddls, binlogplayer.AlterVReplicationTable...)
	allddls = append(allddls, createReshardingJournalTable, createCopyState)
	withDDL = withddl.New(allddls)
}

// this are the default tablet_types that will be used by the tablet picker to find sources for a vreplication stream
// it can be overridden by passing a different list to the MoveTables or Reshard commands
var tabletTypesStr = flag.String("vreplication_tablet_type", "MASTER,REPLICA", "comma separated list of tablet types used as a source")

// waitRetryTime can be changed to a smaller value for tests.
// A VReplication stream can be created by sending an insert statement
// to the Engine. Such a stream can also be updated or deleted. The fields
// of the table are described in binlogplayer.binlog_player.go. Changing
// values in a vreplication row will cause the Engine to accordingly react.
// For example, setting the state to 'Stopped' will cause that stream to
// stop replicating.
var waitRetryTime = 1 * time.Second

// Engine is the engine for handling vreplication.
type Engine struct {
	// mu synchronizes isOpen, cancelRetry, controllers and wg.
	mu     sync.Mutex
	isOpen bool
	// If cancelRetry is set, then a retry loop is running.
	// Invoking the function guarantees that there will be
	// no more retries.
	cancelRetry context.CancelFunc
	controllers map[int]*controller
	// wg is used by in-flight functions that can run for long periods.
	wg sync.WaitGroup

	// ctx is the root context for all controllers.
	ctx context.Context
	// cancel will cancel the root context, thereby all controllers.
	cancel context.CancelFunc

	ts              *topo.Server
	cell            string
	mysqld          mysqlctl.MysqlDaemon
	dbClientFactory func() binlogplayer.DBClient
	dbName          string

	journaler map[string]*journalEvent
	ec        *externalConnector

	throttlerClient *throttle.Client
}

type journalEvent struct {
	journal      *binlogdatapb.Journal
	participants map[string]int
	shardGTIDs   map[string]*binlogdatapb.ShardGtid
}

// NewEngine creates a new Engine.
// A nil ts means that the Engine is disabled.
func NewEngine(config *tabletenv.TabletConfig, ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, lagThrottler *throttle.Throttler) *Engine {
	vre := &Engine{
		controllers:     make(map[int]*controller),
		ts:              ts,
		cell:            cell,
		mysqld:          mysqld,
		journaler:       make(map[string]*journalEvent),
		ec:              newExternalConnector(config.ExternalConnections),
		throttlerClient: throttle.NewBackgroundClient(lagThrottler, throttlerAppName, throttle.ThrottleCheckPrimaryWrite),
	}
	return vre
}

// InitDBConfig should be invoked after the db name is computed.
func (vre *Engine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	// If we're already initilized, it's a test engine. Ignore the call.
	if vre.dbClientFactory != nil {
		return
	}
	vre.dbClientFactory = func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(dbcfgs.FilteredWithDB())
	}
	vre.dbName = dbcfgs.DBName
}

// NewTestEngine creates a new Engine for testing.
func NewTestEngine(ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, dbClientFactory func() binlogplayer.DBClient, dbname string, externalConfig map[string]*dbconfigs.DBConfigs) *Engine {
	vre := &Engine{
		controllers:     make(map[int]*controller),
		ts:              ts,
		cell:            cell,
		mysqld:          mysqld,
		dbClientFactory: dbClientFactory,
		dbName:          dbname,
		journaler:       make(map[string]*journalEvent),
		ec:              newExternalConnector(externalConfig),
	}
	return vre
}

// Open starts the Engine service.
func (vre *Engine) Open(ctx context.Context) {
	vre.mu.Lock()
	defer vre.mu.Unlock()

	if vre.ts == nil {
		return
	}
	if vre.isOpen {
		return
	}
	log.Infof("VReplication Engine: opening")

	// Cancel any existing retry loops.
	// This guarantees that there will be no more
	// retries unless we start a new loop.
	if vre.cancelRetry != nil {
		vre.cancelRetry()
		vre.cancelRetry = nil
	}

	if err := vre.openLocked(ctx); err != nil {
		ctx, cancel := context.WithCancel(ctx)
		vre.cancelRetry = cancel
		go vre.retry(ctx, err)
	}
}

func (vre *Engine) openLocked(ctx context.Context) error {
	rows, err := vre.readAllRows(ctx)
	if err != nil {
		return err
	}

	vre.ctx, vre.cancel = context.WithCancel(ctx)
	vre.isOpen = true
	vre.initControllers(rows)
	vre.updateStats()
	return nil
}

var openRetryInterval = sync2.NewAtomicDuration(1 * time.Second)

func (vre *Engine) retry(ctx context.Context, err error) {
	log.Errorf("Error starting vreplication engine: %v, will keep retrying.", err)
	for {
		timer := time.NewTimer(openRetryInterval.Get())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		vre.mu.Lock()
		// Recheck the context within the lock.
		// This guarantees that we will not retry
		// after the context was canceled. This
		// can almost never happen.
		select {
		case <-ctx.Done():
			vre.mu.Unlock()
			return
		default:
		}
		if err := vre.openLocked(ctx); err == nil {
			// Don't invoke cancelRetry because openLocked
			// will hold on to this context for later cancelation.
			vre.cancelRetry = nil
			vre.mu.Unlock()
			return
		}
		vre.mu.Unlock()
	}
}

func (vre *Engine) initControllers(rows []map[string]string) {
	for _, row := range rows {
		ct, err := newController(vre.ctx, row, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, nil, vre)
		if err != nil {
			log.Errorf("Controller could not be initialized for stream: %v", row)
			continue
		}
		vre.controllers[int(ct.id)] = ct
	}
}

// IsOpen returns true if Engine is open.
func (vre *Engine) IsOpen() bool {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	return vre.isOpen
}

// Close closes the Engine service.
func (vre *Engine) Close() {
	vre.mu.Lock()
	defer vre.mu.Unlock()

	// If we're retrying, we're not open.
	// Just cancel the retry loop.
	if vre.cancelRetry != nil {
		vre.cancelRetry()
		vre.cancelRetry = nil
		return
	}

	if !vre.isOpen {
		return
	}

	vre.ec.Close()
	vre.cancel()
	// We still have to wait for all controllers to stop.
	for _, ct := range vre.controllers {
		ct.Stop()
	}
	vre.controllers = make(map[int]*controller)

	// Wait for long-running functions to exit.
	vre.wg.Wait()

	vre.mysqld.DisableBinlogPlayback()
	vre.isOpen = false

	vre.updateStats()
	log.Infof("VReplication Engine: closed")
}

// Exec executes the query and the related actions.
// Example insert statement:
// insert into _vt.vreplication
//	(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state)
//	values ('Resharding', 'keyspace:"ks" shard:"0" tables:"a" tables:"b" ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`
// Example update statement:
// update _vt.vreplication set state='Stopped', message='testing stop' where id=1
// Example delete: delete from _vt.vreplication where id=1
// Example select: select * from _vt.vreplication
func (vre *Engine) Exec(query string) (*sqltypes.Result, error) {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vreplication engine is closed")
	}
	if vre.cancelRetry != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "engine is still trying to open")
	}
	defer vre.updateStats()

	plan, err := buildControllerPlan(query)
	if err != nil {
		return nil, err
	}
	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	// Change the database to ensure that these events don't get
	// replicated by another vreplication. This can happen when
	// we reverse replication.
	if _, err := withDDL.Exec(vre.ctx, "use _vt", dbClient.ExecuteFetch); err != nil {
		return nil, err
	}

	switch plan.opcode {
	case insertQuery:
		qr, err := withDDL.Exec(vre.ctx, plan.query, dbClient.ExecuteFetch)
		if err != nil {
			return nil, err
		}
		if qr.InsertID == 0 {
			return nil, fmt.Errorf("insert failed to generate an id")
		}
		for id := int(qr.InsertID); id < int(qr.InsertID)+plan.numInserts; id++ {
			if ct := vre.controllers[id]; ct != nil {
				// Unreachable. Just a failsafe.
				ct.Stop()
				delete(vre.controllers, id)
			}
			params, err := readRow(dbClient, id)
			if err != nil {
				return nil, err
			}
			ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, nil, vre)
			if err != nil {
				return nil, err
			}
			vre.controllers[id] = ct
		}
		return qr, nil
	case updateQuery:
		ids, bv, err := vre.fetchIDs(dbClient, plan.selector)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return &sqltypes.Result{}, nil
		}
		blpStats := make(map[int]*binlogplayer.Stats)
		for _, id := range ids {
			if ct := vre.controllers[id]; ct != nil {
				// Stop the current controller.
				ct.Stop()
				blpStats[id] = ct.blpStats
			}
		}
		query, err := plan.applier.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		qr, err := withDDL.Exec(vre.ctx, query, dbClient.ExecuteFetch)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			params, err := readRow(dbClient, id)
			if err != nil {
				return nil, err
			}
			// Create a new controller in place of the old one.
			// For continuity, the new controller inherits the previous stats.
			ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, blpStats[id], vre)
			if err != nil {
				return nil, err
			}
			vre.controllers[id] = ct
		}
		return qr, nil
	case deleteQuery:
		ids, bv, err := vre.fetchIDs(dbClient, plan.selector)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return &sqltypes.Result{}, nil
		}
		// Stop and delete the current controllers.
		for _, id := range ids {
			if ct := vre.controllers[id]; ct != nil {
				ct.Stop()
				delete(vre.controllers, id)
			}
		}
		if err := dbClient.Begin(); err != nil {
			return nil, err
		}
		query, err := plan.applier.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		qr, err := withDDL.Exec(vre.ctx, query, dbClient.ExecuteFetch)
		if err != nil {
			return nil, err
		}
		delQuery, err := plan.delCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		// Legacy vreplication won't create this table. So, ignore schema errors.
		if _, err := withDDL.ExecIgnore(vre.ctx, delQuery, dbClient.ExecuteFetch); err != nil {
			return nil, err
		}
		if err := dbClient.Commit(); err != nil {
			return nil, err
		}
		return qr, nil
	case selectQuery, reshardingJournalQuery:
		// select and resharding journal queries are passed through.
		return withDDL.Exec(vre.ctx, plan.query, dbClient.ExecuteFetch)
	}
	panic("unreachable")
}

func (vre *Engine) fetchIDs(dbClient binlogplayer.DBClient, selector string) (ids []int, bv map[string]*querypb.BindVariable, err error) {
	qr, err := dbClient.ExecuteFetch(selector, 10000)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range qr.Rows {
		id, err := evalengine.ToInt64(row[0])
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, int(id))
	}
	bvval, err := sqltypes.BuildBindVariable(ids)
	if err != nil {
		// Unreachable.
		return nil, nil, err
	}
	bv = map[string]*querypb.BindVariable{"ids": bvval}
	return ids, bv, nil
}

// registerJournal is invoked if any of the vreplication streams encounters a journal event.
// Multiple registerJournal functions collaborate to converge on the final action.
// The first invocation creates an entry in vre.journaler. The entry is initialized
// with the list of participants that also need to converge.
// The middle invocation happens on the first and subsequent calls: the current participant
// marks itself as having joined the wait.
// The final invocation happens for the last participant that joins. Having confirmed
// that all the participants have joined, transitionJournal is invoked, which deletes
// all current participant streams and creates new ones to replace them.
// A unified journal event is identified by the workflow name and journal id.
// Multiple independent journal events can go through this cycle concurrently.
func (vre *Engine) registerJournal(journal *binlogdatapb.Journal, id int) error {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		// Unreachable.
		return nil
	}

	workflow := vre.controllers[id].workflow
	key := fmt.Sprintf("%s:%d", workflow, journal.Id)
	ks := fmt.Sprintf("%s:%s", vre.controllers[id].source.Keyspace, vre.controllers[id].source.Shard)
	log.Infof("Journal encountered for (%s %s): %v", key, ks, journal)
	je, ok := vre.journaler[key]
	if !ok {
		log.Infof("First stream for workflow %s has joined, creating journaler entry", workflow)
		je = &journalEvent{
			journal:      journal,
			participants: make(map[string]int),
			shardGTIDs:   make(map[string]*binlogdatapb.ShardGtid),
		}
		vre.journaler[key] = je
	}
	// Middle invocation. Register yourself
	controllerSources := make(map[string]bool)
	for _, ct := range vre.controllers {
		if ct.workflow != workflow {
			// Only compare with streams that belong to the current workflow.
			continue
		}
		ks := fmt.Sprintf("%s:%s", ct.source.Keyspace, ct.source.Shard)
		controllerSources[ks] = true
	}
	for _, jks := range journal.Participants {
		ks := fmt.Sprintf("%s:%s", jks.Keyspace, jks.Shard)
		if _, ok := controllerSources[ks]; !ok {
			log.Errorf("cannot redirect on journal: not all sources are present in this workflow: missing %v", ks)
			return fmt.Errorf("cannot redirect on journal: not all sources are present in this workflow: missing %v", ks)
		}
		if _, ok := je.participants[ks]; !ok {
			log.Infof("New participant %s found for workflow %s", ks, workflow)
			je.participants[ks] = 0
		} else {
			log.Infof("Participant %s:%d already exists for workflow %s", ks, je.participants[ks], workflow)
		}
	}
	for _, gtid := range journal.ShardGtids {
		je.shardGTIDs[gtid.Shard] = gtid
	}

	je.participants[ks] = id
	// Check if all participants have joined.
	for ks, pid := range je.participants {
		if pid == 0 {
			// Still need to wait.
			log.Infof("Not all participants have joined, including %s", ks)
			return nil
		}
	}
	// Final invocation. Perform the transition.
	delete(vre.journaler, key)
	go vre.transitionJournal(je)
	return nil
}

// transitionJournal stops all existing participants, deletes their vreplication
// entries, and creates new ones as instructed by the journal metadata.
func (vre *Engine) transitionJournal(je *journalEvent) {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		return
	}

	log.Infof("Transitioning for journal:workload %v", je)

	//sort both participants and shardgtids
	participants := make([]string, 0)
	for ks := range je.participants {
		participants = append(participants, ks)
	}
	sort.Sort(ShardSorter(participants))
	log.Infof("Participants %+v, oldParticipants %+v", participants, je.participants)
	shardGTIDs := make([]string, 0)
	for shard := range je.shardGTIDs {
		shardGTIDs = append(shardGTIDs, shard)
	}
	sort.Strings(shardGTIDs)

	// Wait for participating controllers to stop.
	// Also collect one id reference.
	refid := 0
	for id := range participants {
		ks := participants[id]
		refid = je.participants[ks]
		vre.controllers[refid].Stop()
	}

	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		log.Errorf("transitionJournal: unable to connect to the database: %v", err)
		return
	}
	defer dbClient.Close()

	if err := dbClient.Begin(); err != nil {
		log.Errorf("transitionJournal: %v", err)
		return
	}

	// Use the reference row to copy other fields like cell, tablet_types, etc.
	params, err := readRow(dbClient, refid)
	if err != nil {
		log.Errorf("transitionJournal: %v", err)
		return
	}
	var newids []int
	for _, shard := range shardGTIDs {
		sgtid := je.shardGTIDs[shard]
		bls := vre.controllers[refid].source
		bls.Keyspace, bls.Shard = sgtid.Keyspace, sgtid.Shard
		ig := NewInsertGenerator(binlogplayer.BlpRunning, vre.dbName)
		ig.AddRow(params["workflow"], &bls, sgtid.Gtid, params["cell"], params["tablet_types"])
		qr, err := withDDL.Exec(vre.ctx, ig.String(), dbClient.ExecuteFetch)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		log.Infof("Created stream: %v for %v", qr.InsertID, sgtid)
		newids = append(newids, int(qr.InsertID))
	}
	for _, ks := range participants {
		id := je.participants[ks]
		_, err := withDDL.Exec(vre.ctx, binlogplayer.DeleteVReplication(uint32(id)), dbClient.ExecuteFetch)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		log.Infof("Deleted stream: %v", id)
	}
	if err := dbClient.Commit(); err != nil {
		log.Errorf("transitionJournal: %v", err)
		return
	}

	for id := range participants {
		ks := participants[id]
		id := je.participants[ks]
		delete(vre.controllers, id)
	}

	for _, id := range newids {
		params, err := readRow(dbClient, id)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, nil, vre)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		vre.controllers[id] = ct
	}
	log.Infof("Completed transition for journal:workload %v", je)
}

// WaitForPos waits for the replication to reach the specified position.
func (vre *Engine) WaitForPos(ctx context.Context, id int, pos string) error {
	start := time.Now()
	mPos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}

	if err := func() error {
		vre.mu.Lock()
		defer vre.mu.Unlock()
		if !vre.isOpen {
			return errors.New("vreplication engine is closed")
		}

		// Ensure that the engine won't be closed while this is running.
		vre.wg.Add(1)
		return nil
	}(); err != nil {
		return err
	}
	defer vre.wg.Done()

	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	for {
		qr, err := dbClient.ExecuteFetch(binlogplayer.ReadVReplicationStatus(uint32(id)), 10)
		switch {
		case err != nil:
			return err
		case len(qr.Rows) == 0:
			return fmt.Errorf("vreplication stream %d not found", id)
		case len(qr.Rows) > 1 || len(qr.Rows[0]) != 3:
			return fmt.Errorf("unexpected result: %v", qr)
		}
		current, err := mysql.DecodePosition(qr.Rows[0][0].ToString())
		if err != nil {
			return err
		}

		if current.AtLeast(mPos) {
			log.Infof("position: %s reached, wait time: %v", pos, time.Since(start))
			return nil
		}

		if qr.Rows[0][1].ToString() == binlogplayer.BlpStopped {
			return fmt.Errorf("replication has stopped at %v before reaching position %v, message: %s", current, mPos, qr.Rows[0][2].ToString())
		}

		select {
		case <-ctx.Done():
			log.Errorf("Error waiting for pos: %s, last pos: %s: %v, wait time: %v", pos, qr.Rows[0][0].ToString(), ctx.Err(), time.Since(start))
			return fmt.Errorf("error waiting for pos: %s, last pos: %s: %v, wait time: %v", pos, qr.Rows[0][0].ToString(), ctx.Err(), time.Since(start))
		case <-vre.ctx.Done():
			return fmt.Errorf("vreplication is closing: %v", vre.ctx.Err())
		case <-tkr.C:
		}
	}
}

// UpdateStats must be called with lock held.
func (vre *Engine) updateStats() {
	globalStats.mu.Lock()
	defer globalStats.mu.Unlock()

	globalStats.isOpen = vre.isOpen
	globalStats.controllers = make(map[int]*controller, len(vre.controllers))
	for id, ct := range vre.controllers {
		globalStats.controllers[id] = ct
	}
}

func (vre *Engine) readAllRows(ctx context.Context) ([]map[string]string, error) {
	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()
	qr, err := withDDL.ExecIgnore(ctx, fmt.Sprintf("select * from _vt.vreplication where db_name=%v", encodeString(vre.dbName)), dbClient.ExecuteFetch)
	if err != nil {
		return nil, err
	}
	maps := make([]map[string]string, len(qr.Rows))
	for i := range qr.Rows {
		mrow, err := rowToMap(qr, i)
		if err != nil {
			return nil, err
		}
		maps[i] = mrow
	}
	return maps, nil
}

func readRow(dbClient binlogplayer.DBClient, id int) (map[string]string, error) {
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf("select * from _vt.vreplication where id = %d", id), 10)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("unexpected number of rows: %v", qr)
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, fmt.Errorf("fields don't match rows: %v", qr)
	}
	return rowToMap(qr, 0)
}

// rowToMap converts a row into a map for easier processing.
func rowToMap(qr *sqltypes.Result, rownum int) (map[string]string, error) {
	row := qr.Rows[rownum]
	m := make(map[string]string, len(row))
	for i, fld := range qr.Fields {
		if row[i].IsNull() {
			continue
		}
		m[fld.Name] = row[i].ToString()
	}
	return m, nil
}
