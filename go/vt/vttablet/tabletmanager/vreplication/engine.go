/*
Copyright 2018 The Vitess Authors.

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
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
)

const (
	reshardingJournalTableName   = "_vt.resharding_journal"
	vreplicationTableName        = "_vt.vreplication"
	createReshardingJournalTable = `create table if not exists _vt.resharding_journal(
  id bigint,
  db_name varbinary(255),
  val blob,
  primary key (id)
) ENGINE=InnoDB`
)

var tabletTypesStr = flag.String("vreplication_tablet_type", "REPLICA", "comma separated list of tablet types used as a source")

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
	// mu synchronizes isOpen, controllers and wg.
	mu          sync.Mutex
	isOpen      bool
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
}

// NewEngine creates a new Engine.
// A nil ts means that the Engine is disabled.
func NewEngine(ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, dbClientFactory func() binlogplayer.DBClient, dbName string) *Engine {
	vre := &Engine{
		controllers:     make(map[int]*controller),
		ts:              ts,
		cell:            cell,
		mysqld:          mysqld,
		dbClientFactory: dbClientFactory,
		dbName:          dbName,
	}
	return vre
}

// Open starts the Engine service.
func (vre *Engine) Open(ctx context.Context) error {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if vre.ts == nil {
		log.Info("ts is nil: disabling vreplication engine")
		return nil
	}
	if vre.isOpen {
		return nil
	}

	vre.ctx, vre.cancel = context.WithCancel(ctx)
	vre.isOpen = true
	if err := vre.initAll(); err != nil {
		go vre.Close()
		return err
	}
	vre.updateStats()
	return nil
}

// executeFetchMaybeCreateTable calls DBClient.ExecuteFetch and does one retry if
// there's a failure due to mysql.ERNoSuchTable or mysql.ERBadDb which can be fixed
// by re-creating the vreplication tables.
func (vre *Engine) executeFetchMaybeCreateTable(dbClient binlogplayer.DBClient, query string, maxrows int) (qr *sqltypes.Result, err error) {
	qr, err = dbClient.ExecuteFetch(query, maxrows)

	if err == nil {
		return
	}

	// If it's a bad table or db, it could be because the vreplication tables weren't created.
	// In that case we can try creating them again.
	merr, isSQLErr := err.(*mysql.SQLError)
	if !isSQLErr || !(merr.Num == mysql.ERNoSuchTable || merr.Num == mysql.ERBadDb || merr.Num == mysql.ERBadFieldError) {
		return qr, err
	}

	log.Info("Looks like the vreplcation tables may not exist. Trying to recreate... ")
	if merr.Num == mysql.ERNoSuchTable || merr.Num == mysql.ERBadDb {
		for _, query := range binlogplayer.CreateVReplicationTable() {
			if _, merr := dbClient.ExecuteFetch(query, 0); merr != nil {
				log.Warningf("Failed to ensure %s exists: %v", vreplicationTableName, merr)
				return nil, err
			}
		}
		if _, merr := dbClient.ExecuteFetch(createReshardingJournalTable, 0); merr != nil {
			log.Warningf("Failed to ensure %s exists: %v", reshardingJournalTableName, merr)
			return nil, err
		}
	}
	if merr.Num == mysql.ERBadFieldError {
		log.Infof("Adding column to table %s", vreplicationTableName)
		for _, query := range binlogplayer.AlterVReplicationTable() {
			if _, merr := dbClient.ExecuteFetch(query, 0); merr != nil {
				merr, isSQLErr := err.(*mysql.SQLError)
				if !isSQLErr || !(merr.Num == mysql.ERDupFieldName) {
					log.Warningf("Failed to alter %s table: %v", vreplicationTableName, merr)
					return nil, err
				}
			}
		}
	}
	return dbClient.ExecuteFetch(query, maxrows)
}

func (vre *Engine) initAll() error {
	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	rows, err := readAllRows(dbClient, vre.dbName)
	if err != nil {
		// Handle Table not found.
		if merr, ok := err.(*mysql.SQLError); ok && merr.Num == mysql.ERNoSuchTable {
			log.Info("_vt.vreplication table not found. Will create it later if needed")
			return nil
		}
		// Handle missing field
		if merr, ok := err.(*mysql.SQLError); ok && merr.Num == mysql.ERBadFieldError {
			log.Info("_vt.vreplication table found but is missing field db_name. Will add it later if needed")
			return nil
		}
		return err
	}
	for _, row := range rows {
		ct, err := newController(vre.ctx, row, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, nil)
		if err != nil {
			return err
		}
		vre.controllers[int(ct.id)] = ct
	}
	return nil
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
	if !vre.isOpen {
		return
	}

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
		return nil, errors.New("vreplication engine is closed")
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
	if _, err := vre.executeFetchMaybeCreateTable(dbClient, "use _vt", 1); err != nil {
		return nil, err
	}

	switch plan.opcode {
	case insertQuery:
		qr, err := vre.executeFetchMaybeCreateTable(dbClient, plan.query, 1)
		if err != nil {
			return nil, err
		}
		if qr.InsertID == 0 {
			return nil, fmt.Errorf("insert failed to generate an id")
		}
		params, err := readRow(dbClient, int(qr.InsertID))
		if err != nil {
			return nil, err
		}
		// Create a controller for the newly created row.
		ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, nil)
		if err != nil {
			return nil, err
		}
		vre.controllers[int(qr.InsertID)] = ct
		return qr, nil
	case updateQuery:
		var blpStats *binlogplayer.Stats
		if ct := vre.controllers[plan.id]; ct != nil {
			// Stop the current controller.
			ct.Stop()
			blpStats = ct.blpStats
		}
		qr, err := vre.executeFetchMaybeCreateTable(dbClient, plan.query, 1)
		if err != nil {
			return nil, err
		}
		params, err := readRow(dbClient, plan.id)
		if err != nil {
			return nil, err
		}
		// Create a new controller in place of the old one.
		// For continuity, the new controller inherits the previous stats.
		ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr, blpStats)
		if err != nil {
			return nil, err
		}
		vre.controllers[plan.id] = ct
		return qr, nil
	case deleteQuery:
		// Stop and delete the current controller.
		if ct := vre.controllers[plan.id]; ct != nil {
			ct.Stop()
			delete(vre.controllers, plan.id)
		}
		return vre.executeFetchMaybeCreateTable(dbClient, plan.query, 1)
	case selectQuery, reshardingJournalQuery:
		// select and resharding journal queries are passed through.
		return vre.executeFetchMaybeCreateTable(dbClient, plan.query, 10000)
	}
	panic("unreachable")
}

// WaitForPos waits for the replication to reach the specified position.
func (vre *Engine) WaitForPos(ctx context.Context, id int, pos string) error {
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
			return nil
		}

		if qr.Rows[0][1].ToString() == binlogplayer.BlpStopped {
			return fmt.Errorf("replication has stopped at %v before reaching position %v, message: %s", current, mPos, qr.Rows[0][2].ToString())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
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

func readAllRows(dbClient binlogplayer.DBClient, dbName string) ([]map[string]string, error) {
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf("select * from _vt.vreplication where db_name=%v", encodeString(dbName)), 10000)
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
