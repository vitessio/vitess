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
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	reshardingJournalTableName = "resharding_journal"
	vreplicationTableName      = "vreplication"
	copyStateTableName         = "copy_state"
	postCopyActionTableName    = "post_copy_action"

	maxRows = 10000
)

const (
	PostCopyActionNone PostCopyActionType = iota
	PostCopyActionSQL
)

// waitRetryTime can be changed to a smaller value for tests.
// A VReplication stream can be created by sending an insert statement
// to the Engine. Such a stream can also be updated or deleted. The fields
// of the table are described in binlogplayer.binlog_player.go. Changing
// values in a vreplication row will cause the Engine to accordingly react.
// For example, setting the state to 'Stopped' will cause that stream to
// stop replicating.
var waitRetryTime = 1 * time.Second

// How frequently vcopier will update _vt.vreplication rows_copied
var rowsCopiedUpdateInterval = 30 * time.Second

// How frequently vcopier will garbage collect old copy_state rows.
// By default, do it in between every 2nd and 3rd rows copied update.
var copyStateGCInterval = (rowsCopiedUpdateInterval * 3) - (rowsCopiedUpdateInterval / 2)

// Engine is the engine for handling vreplication.
type Engine struct {
	// mu synchronizes isOpen, cancelRetry, controllers and wg.
	mu     sync.Mutex
	isOpen bool
	// If cancelRetry is set, then a retry loop is running.
	// Invoking the function guarantees that there will be
	// no more retries.
	cancelRetry context.CancelFunc
	controllers map[int32]*controller
	// wg is used by in-flight functions that can run for long periods.
	wg sync.WaitGroup

	// ctx is the root context for all controllers.
	ctx context.Context
	// cancel will cancel the root context, thereby all controllers.
	cancel context.CancelFunc

	ts                      *topo.Server
	cell                    string
	mysqld                  mysqlctl.MysqlDaemon
	dbClientFactoryFiltered func() binlogplayer.DBClient
	dbClientFactoryDba      func() binlogplayer.DBClient
	dbName                  string

	journaler map[string]*journalEvent
	ec        *externalConnector

	throttlerClient *throttle.Client

	// This should only be set in Test Engines in order to short
	// circuit functions as needed in unit tests. It's automatically
	// enabled in NewSimpleTestEngine. This should NOT be used in
	// production.
	shortcircuit bool

	env *vtenv.Environment
}

type journalEvent struct {
	journal      *binlogdatapb.Journal
	participants map[string]int32
	shardGTIDs   map[string]*binlogdatapb.ShardGtid
}

type PostCopyActionType int
type PostCopyAction struct {
	Type PostCopyActionType `json:"type"`
	Task string             `json:"task"`
}

// NewEngine creates a new Engine.
// A nil ts means that the Engine is disabled.
func NewEngine(env *vtenv.Environment, config *tabletenv.TabletConfig, ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, lagThrottler *throttle.Throttler) *Engine {
	vre := &Engine{
		env:             env,
		controllers:     make(map[int32]*controller),
		ts:              ts,
		cell:            cell,
		mysqld:          mysqld,
		journaler:       make(map[string]*journalEvent),
		ec:              newExternalConnector(env, config.ExternalConnections),
		throttlerClient: throttle.NewBackgroundClient(lagThrottler, throttlerapp.VReplicationName, throttle.ThrottleCheckPrimaryWrite),
	}

	return vre
}

// InitDBConfig should be invoked after the db name is computed.
func (vre *Engine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	// If we're already initialized, it's a test engine. Ignore the call.
	if vre.dbClientFactoryFiltered != nil && vre.dbClientFactoryDba != nil {
		return
	}
	vre.dbClientFactoryFiltered = func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(dbcfgs.FilteredWithDB(), vre.env.Parser())
	}
	vre.dbClientFactoryDba = func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(dbcfgs.DbaWithDB(), vre.env.Parser())
	}
	vre.dbName = dbcfgs.DBName
}

// NewTestEngine creates a new Engine for testing.
func NewTestEngine(ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, dbClientFactoryFiltered func() binlogplayer.DBClient, dbClientFactoryDba func() binlogplayer.DBClient, dbname string, externalConfig map[string]*dbconfigs.DBConfigs) *Engine {
	env := vtenv.NewTestEnv()
	vre := &Engine{
		env:                     env,
		controllers:             make(map[int32]*controller),
		ts:                      ts,
		cell:                    cell,
		mysqld:                  mysqld,
		dbClientFactoryFiltered: dbClientFactoryFiltered,
		dbClientFactoryDba:      dbClientFactoryDba,
		dbName:                  dbname,
		journaler:               make(map[string]*journalEvent),
		ec:                      newExternalConnector(env, externalConfig),
	}
	return vre
}

// NewSimpleTestEngine creates a new Engine for testing that can
// also short circuit functions as needed.
func NewSimpleTestEngine(ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, dbClientFactoryFiltered func() binlogplayer.DBClient, dbClientFactoryDba func() binlogplayer.DBClient, dbname string, externalConfig map[string]*dbconfigs.DBConfigs) *Engine {
	env := vtenv.NewTestEnv()
	vre := &Engine{
		env:                     env,
		controllers:             make(map[int32]*controller),
		ts:                      ts,
		cell:                    cell,
		mysqld:                  mysqld,
		dbClientFactoryFiltered: dbClientFactoryFiltered,
		dbClientFactoryDba:      dbClientFactoryDba,
		dbName:                  dbname,
		journaler:               make(map[string]*journalEvent),
		ec:                      newExternalConnector(env, externalConfig),
		shortcircuit:            true,
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
		log.Infof("openLocked error: %s", err)
		ctx, cancel := context.WithCancel(ctx)
		vre.cancelRetry = cancel
		go vre.retry(ctx, err)
	}
	log.Infof("VReplication engine opened successfully")
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

var openRetryInterval atomic.Int64

func init() {
	openRetryInterval.Store((1 * time.Second).Nanoseconds())
}

func (vre *Engine) retry(ctx context.Context, err error) {
	log.Errorf("Error starting vreplication engine: %v, will keep retrying.", err)
	for {
		timer := time.NewTimer(time.Duration(openRetryInterval.Load()))
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
			// will hold on to this context for later cancellation.
			vre.cancelRetry = nil
			vre.mu.Unlock()
			return
		}
		vre.mu.Unlock()
	}
}

func (vre *Engine) initControllers(rows []map[string]string) {
	for _, row := range rows {
		ct, err := newController(vre.ctx, row, vre.dbClientFactoryFiltered, vre.mysqld, vre.ts, vre.cell, tabletTypesStr, nil, vre)
		if err != nil {
			log.Errorf("Controller could not be initialized for stream: %v", row)
			continue
		}
		vre.controllers[ct.id] = ct
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
	vre.controllers = make(map[int32]*controller)

	// Wait for long-running functions to exit.
	vre.wg.Wait()

	vre.isOpen = false

	vre.updateStats()
	log.Infof("VReplication Engine: closed")
}

func (vre *Engine) getDBClient(isAdmin bool) binlogplayer.DBClient {
	if isAdmin {
		return vre.dbClientFactoryDba()
	}
	return vre.dbClientFactoryFiltered()
}

// ExecWithDBA runs the specified query as the DBA user
func (vre *Engine) ExecWithDBA(query string) (*sqltypes.Result, error) {
	return vre.exec(query, true /*runAsAdmin*/)
}

// Exec runs the specified query as the Filtered user
func (vre *Engine) Exec(query string) (*sqltypes.Result, error) {
	return vre.exec(query, false /*runAsAdmin*/)
}

// Exec executes the query and the related actions.
// Example insert statement:
// insert into _vt.vreplication
//
//	(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state)
//	values ('Resharding', 'keyspace:"ks" shard:"0" tables:"a" tables:"b" ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`
//
// Example update statement:
// update _vt.vreplication set state='Stopped', message='testing stop' where id=1
// Example delete: delete from _vt.vreplication where id=1
// Example select: select * from _vt.vreplication
func (vre *Engine) exec(query string, runAsAdmin bool) (*sqltypes.Result, error) {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vreplication engine is closed")
	}
	if vre.cancelRetry != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "engine is still trying to open")
	}
	defer vre.updateStats()

	plan, err := buildControllerPlan(query, vre.env.Parser())
	if err != nil {
		return nil, err
	}

	dbClient := vre.getDBClient(runAsAdmin)
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	// Change the database to ensure that these events don't get
	// replicated by another vreplication. This can happen when
	// we reverse replication.
	if _, err := dbClient.ExecuteFetch(fmt.Sprintf("use %s", sidecar.GetIdentifier()), 1); err != nil {
		return nil, err
	}

	stats := binlogplayer.NewStats()
	defer stats.Stop()
	switch plan.opcode {
	case insertQuery:
		qr, err := dbClient.ExecuteFetch(plan.query, 1)
		if err != nil {
			return nil, err
		}
		if qr.InsertID == 0 {
			return nil, fmt.Errorf("insert failed to generate an id")
		}
		maxInsert := qr.InsertID + uint64(plan.numInserts)
		if maxInsert > math.MaxInt32 {
			return nil, fmt.Errorf("insert id %v out of range", qr.InsertID)
		}

		vdbc := newVDBClient(dbClient, stats)

		// If we are creating multiple streams, for example in a
		// merge workflow going from 2 shards to 1 shard, we
		// will be inserting multiple rows. To get the ids of
		// subsequent streams we need to know what the
		// auto_increment_increment step is. In a multi-primary
		// environment, like a Galera cluster, for example,
		// we will often encounter auto_increment steps > 1.
		autoIncrementStep, err := vre.getAutoIncrementStep(dbClient)
		if err != nil {
			return nil, err
		}
		firstID := int32(qr.InsertID)
		lastID := firstID + int32(autoIncrementStep)*(int32(plan.numInserts)-1)
		for id := firstID; id <= lastID; id += int32(autoIncrementStep) {
			if ct := vre.controllers[id]; ct != nil {
				// Unreachable. Just a failsafe.
				ct.Stop()
				delete(vre.controllers, id)
			}
			params, err := readRow(dbClient, id)
			if err != nil {
				return nil, err
			}
			ct, err := newController(vre.ctx, params, vre.dbClientFactoryFiltered, vre.mysqld, vre.ts, vre.cell, tabletTypesStr, nil, vre)
			if err != nil {
				return nil, err
			}
			vre.controllers[id] = ct
			if err := insertLogWithParams(vdbc, LogStreamCreate, id, params); err != nil {
				return nil, err
			}
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
		blpStats := make(map[int32]*binlogplayer.Stats)
		for _, id := range ids {
			if ct := vre.controllers[id]; ct != nil {
				// Stop the current controller.
				ct.Stop()
				blpStats[id] = ct.blpStats
			}
		}
		query, err = plan.applier.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		qr, err := dbClient.ExecuteFetch(query, maxRows)
		if err != nil {
			return nil, err
		}
		vdbc := newVDBClient(dbClient, stats)
		for _, id := range ids {
			params, err := readRow(dbClient, id)
			if err != nil {
				return nil, err
			}
			// Create a new controller in place of the old one.
			// For continuity, the new controller inherits the previous stats.
			ct, err := newController(vre.ctx, params, vre.dbClientFactoryFiltered, vre.mysqld, vre.ts, vre.cell, tabletTypesStr, blpStats[id], vre)
			if err != nil {
				return nil, err
			}
			vre.controllers[id] = ct
			if err := insertLog(vdbc, LogStateChange, id, params["state"], ""); err != nil {
				return nil, err
			}
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
		vdbc := newVDBClient(dbClient, stats)
		for _, id := range ids {
			if ct := vre.controllers[id]; ct != nil {
				ct.Stop()
				delete(vre.controllers, id)
			}
			if err := insertLogWithParams(vdbc, LogStreamDelete, id, nil); err != nil {
				return nil, err
			}
		}
		if err := dbClient.Begin(); err != nil {
			return nil, err
		}
		query, err := plan.applier.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		qr, err := dbClient.ExecuteFetch(query, maxRows)
		if err != nil {
			return nil, err
		}
		delQuery, err := plan.delCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		_, err = dbClient.ExecuteFetch(delQuery, maxRows)
		if err != nil {
			return nil, err
		}
		delQuery, err = plan.delPostCopyAction.GenerateQuery(bv, nil)
		if err != nil {
			return nil, err
		}
		_, err = dbClient.ExecuteFetch(delQuery, maxRows)
		if err != nil {
			return nil, err
		}
		if err := dbClient.Commit(); err != nil {
			return nil, err
		}
		return qr, nil
	case selectQuery, reshardingJournalQuery:
		// Selects and resharding journal queries are passed through.
		return dbClient.ExecuteFetch(plan.query, maxRows)
	}
	panic("unreachable")
}

func (vre *Engine) fetchIDs(dbClient binlogplayer.DBClient, selector string) (ids []int32, bv map[string]*querypb.BindVariable, err error) {
	qr, err := dbClient.ExecuteFetch(selector, 10000)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range qr.Rows {
		id, err := row[0].ToInt32()
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
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
func (vre *Engine) registerJournal(journal *binlogdatapb.Journal, id int32) error {
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
			participants: make(map[string]int32),
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
	var refid int32
	for id := range participants {
		ks := participants[id]
		refid = je.participants[ks]
		vre.controllers[refid].Stop()
	}

	dbClient := vre.dbClientFactoryFiltered()
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
	var newids []int32
	for _, shard := range shardGTIDs {
		sgtid := je.shardGTIDs[shard]
		bls := vre.controllers[refid].source.CloneVT()
		bls.Keyspace, bls.Shard = sgtid.Keyspace, sgtid.Shard

		workflowType, _ := strconv.ParseInt(params["workflow_type"], 10, 32)
		workflowSubType, _ := strconv.ParseInt(params["workflow_sub_type"], 10, 32)
		deferSecondaryKeys, _ := strconv.ParseBool(params["defer_secondary_keys"])
		ig := NewInsertGenerator(binlogdatapb.VReplicationWorkflowState_Running, vre.dbName)
		ig.AddRow(params["workflow"], bls, sgtid.Gtid, params["cell"], params["tablet_types"],
			binlogdatapb.VReplicationWorkflowType(workflowType), binlogdatapb.VReplicationWorkflowSubType(workflowSubType), deferSecondaryKeys)
		qr, err := dbClient.ExecuteFetch(ig.String(), maxRows)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		log.Infof("Created stream: %v for %v", qr.InsertID, sgtid)
		if qr.InsertID > math.MaxInt32 {
			log.Errorf("transitionJournal: InsertID %v too large", qr.InsertID)
			return
		}
		newids = append(newids, int32(qr.InsertID))
	}
	for _, ks := range participants {
		id := je.participants[ks]
		_, err := dbClient.ExecuteFetch(binlogplayer.DeleteVReplication(id), maxRows)
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
		ct, err := newController(vre.ctx, params, vre.dbClientFactoryFiltered, vre.mysqld, vre.ts, vre.cell, tabletTypesStr, nil, vre)
		if err != nil {
			log.Errorf("transitionJournal: %v", err)
			return
		}
		vre.controllers[id] = ct
	}
	log.Infof("Completed transition for journal:workload %v", je)
}

// WaitForPos waits for the replication to reach the specified position.
func (vre *Engine) WaitForPos(ctx context.Context, id int32, pos string) error {
	start := time.Now()
	mPos, err := binlogplayer.DecodePosition(pos)
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

	if vre.shortcircuit {
		return nil
	}

	dbClient := vre.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	for {
		qr, err := dbClient.ExecuteFetch(binlogplayer.ReadVReplicationStatus(id), 10)
		switch {
		case err != nil:
			// We have high contention on the _vt.vreplication row, so retry if our read gets
			// killed off by the deadlock detector and should be re-tried.
			// The full error we get back from MySQL in that case is:
			// Deadlock found when trying to get lock; try restarting transaction (errno 1213) (sqlstate 40001)
			// Docs: https://dev.mysql.com/doc/mysql-errors/en/server-error-reference.html#error_er_lock_deadlock
			if sqlErr, ok := err.(*sqlerror.SQLError); ok && sqlErr.Number() == sqlerror.ERLockDeadlock {
				log.Infof("Deadlock detected waiting for pos %s: %v; will retry", pos, err)
			} else {
				return err
			}
		case len(qr.Rows) == 0:
			return fmt.Errorf("vreplication stream %d not found", id)
		case len(qr.Rows) > 1 || len(qr.Rows[0]) != 3:
			return fmt.Errorf("unexpected result: %v", qr)
		}

		// When err is not nil then we got a retryable error and will loop again.
		if err == nil {
			current, dcerr := binlogplayer.DecodePosition(qr.Rows[0][0].ToString())
			if dcerr != nil {
				return dcerr
			}

			if current.AtLeast(mPos) {
				log.Infof("position: %s reached, wait time: %v", pos, time.Since(start))
				return nil
			}

			if qr.Rows[0][1].ToString() == binlogdatapb.VReplicationWorkflowState_Stopped.String() {
				return fmt.Errorf("replication has stopped at %v before reaching position %v, message: %s", current, mPos, qr.Rows[0][2].ToString())
			}
		}

		select {
		case <-ctx.Done():
			var doneErr error
			if err != nil { // we had a retryable error and never got status info
				doneErr = fmt.Errorf("error waiting for pos: %s, unable to get vreplication status for id %d: %v, wait time: %v",
					pos, id, err, time.Since(start))
			} else {
				doneErr = fmt.Errorf("error waiting for pos: %s, last pos: %s: %v, wait time: %v: %s",
					pos, qr.Rows[0][0].ToString(), ctx.Err(), time.Since(start),
					"possibly no tablets are available to stream in the source keyspace for your cell and tablet_types setting")
			}
			log.Error(doneErr.Error())
			return doneErr
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
	globalStats.controllers = make(map[int32]*controller, len(vre.controllers))
	for id, ct := range vre.controllers {
		globalStats.controllers[id] = ct
	}
}

func (vre *Engine) readAllRows(ctx context.Context) ([]map[string]string, error) {
	dbClient := vre.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf("select * from _vt.vreplication where db_name=%s", encodeString(vre.dbName)), maxRows)
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

func (vre *Engine) getAutoIncrementStep(dbClient binlogplayer.DBClient) (uint16, error) {
	qr, err := dbClient.ExecuteFetch("select @@session.auto_increment_increment", 1)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		// Handles case where underlying mysql doesn't support auto_increment_increment for any reason.
		return 1, nil
	}
	autoIncrement, err := qr.Rows[0][0].ToUint16()
	if err != nil {
		return 0, err
	}
	return autoIncrement, nil
}

func readRow(dbClient binlogplayer.DBClient, id int32) (map[string]string, error) {
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
	row, err := rowToMap(qr, 0)
	if err != nil {
		return nil, err
	}
	gtid, ok := row["pos"]
	if ok {
		b := binlogplayer.MysqlUncompress(gtid)
		if b != nil {
			gtid = string(b)
			row["pos"] = gtid
		}
	}
	return row, nil
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
