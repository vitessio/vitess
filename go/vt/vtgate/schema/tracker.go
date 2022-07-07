/*
Copyright 2021 The Vitess Authors.

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

package schema

import (
	"context"
	"sync"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/callerid"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	keyspaceStr  = string
	tableNameStr = string

	// Tracker contains the required fields to perform schema tracking.
	Tracker struct {
		ch     chan *discovery.TabletHealth
		cancel context.CancelFunc

		mu     sync.Mutex
		tables *tableMap
		ctx    context.Context
		signal func() // a function that we'll call whenever we have new schema data

		// map of keyspace currently tracked
		tracked      map[keyspaceStr]*updateController
		consumeDelay time.Duration
	}
)

// defaultConsumeDelay is the default time, the updateController will wait before checking the schema fetch request queue.
const defaultConsumeDelay = 1 * time.Second

// aclErrorMessageLog is for logging a warning when an acl error message is received for querying schema tracking table.
const aclErrorMessageLog = "Table ACL might be enabled, --schema_change_signal_user needs to be passed to VTGate for schema tracking to work. Check 'schema tracking' docs on vitess.io"

// NewTracker creates the tracker object.
func NewTracker(ch chan *discovery.TabletHealth, user *string) *Tracker {
	ctx := context.Background()
	// Set the caller on the context if the user is provided.
	// This user that will be sent down to vttablet calls.
	if user != nil && *user != "" {
		ctx = callerid.NewContext(ctx, nil, callerid.NewImmediateCallerID(*user))
	}

	return &Tracker{
		ctx:          ctx,
		ch:           ch,
		tables:       &tableMap{m: map[keyspaceStr]map[tableNameStr][]vindexes.Column{}},
		tracked:      map[keyspaceStr]*updateController{},
		consumeDelay: defaultConsumeDelay,
	}
}

// LoadKeyspace loads the keyspace schema.
func (t *Tracker) LoadKeyspace(conn queryservice.QueryService, target *querypb.Target) error {
	res, err := conn.Execute(t.ctx, target, mysql.FetchTables, nil, 0, 0, nil)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// We must clear out any previous schema before loading it here as this is called
	// whenever a shard's primary tablet starts and sends the initial signal. Without
	// clearing out the previous schema we can end up with duplicate entries when the
	// tablet is simply restarted or potentially when we elect a new primary.
	t.clearKeyspaceTables(target.Keyspace)
	t.updateTables(target.Keyspace, res)
	t.tracked[target.Keyspace].setLoaded(true)
	log.Infof("finished loading schema for keyspace %s. Found %d columns in total across the tables", target.Keyspace, len(res.Rows))
	return nil
}

// Start starts the schema tracking.
func (t *Tracker) Start() {
	log.Info("Starting schema tracking")
	ctx, cancel := context.WithCancel(t.ctx)
	t.cancel = cancel
	go func(ctx context.Context, t *Tracker) {
		for {
			select {
			case th := <-t.ch:
				ksUpdater := t.getKeyspaceUpdateController(th)
				ksUpdater.add(th)
			case <-ctx.Done():
				// closing of the channel happens outside the scope of the tracker. It is the responsibility of the one who created this tracker.
				return
			}
		}
	}(ctx, t)
}

// getKeyspaceUpdateController returns the updateController for the given keyspace
// the updateController will be created if there was none.
func (t *Tracker) getKeyspaceUpdateController(th *discovery.TabletHealth) *updateController {
	t.mu.Lock()
	defer t.mu.Unlock()

	ksUpdater, exists := t.tracked[th.Target.Keyspace]
	if !exists {
		ksUpdater = t.newUpdateController()
		t.tracked[th.Target.Keyspace] = ksUpdater
	}
	return ksUpdater
}

func (t *Tracker) newUpdateController() *updateController {
	return &updateController{update: t.updateSchema, reloadKeyspace: t.initKeyspace, signal: t.signal, consumeDelay: t.consumeDelay}
}

func (t *Tracker) initKeyspace(th *discovery.TabletHealth) error {
	err := t.LoadKeyspace(th.Conn, th.Target)
	if err != nil {
		log.Warningf("Unable to add the %s keyspace to the schema tracker: %v", th.Target.Keyspace, err)
		code := vterrors.Code(err)
		if code == vtrpcpb.Code_UNAUTHENTICATED || code == vtrpcpb.Code_PERMISSION_DENIED {
			log.Warning(aclErrorMessageLog)
		}
		return err
	}
	return nil
}

// Stop stops the schema tracking
func (t *Tracker) Stop() {
	log.Info("Stopping schema tracking")
	t.cancel()
}

// GetColumns returns the column list for table in the given keyspace.
func (t *Tracker) GetColumns(ks string, tbl string) []vindexes.Column {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.tables.get(ks, tbl)
}

// Tables returns a map with the columns for all known tables in the keyspace
func (t *Tracker) Tables(ks string) map[string][]vindexes.Column {
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.tables.m[ks]
	if m == nil {
		return map[string][]vindexes.Column{} // we know nothing about this KS, so that is the info we can give out
	}

	return m
}

func (t *Tracker) updateSchema(th *discovery.TabletHealth) bool {
	tablesUpdated := th.Stats.TableSchemaChanged
	tables, err := sqltypes.BuildBindVariable(tablesUpdated)
	if err != nil {
		log.Errorf("failed to read updated tables from TabletHealth: %v", err)
		return false
	}
	bv := map[string]*querypb.BindVariable{"tableNames": tables}
	res, err := th.Conn.Execute(t.ctx, th.Target, mysql.FetchUpdatedTables, bv, 0, 0, nil)
	if err != nil {
		t.tracked[th.Target.Keyspace].setLoaded(false)
		// TODO: optimize for the tables that got errored out.
		log.Warningf("error fetching new schema for %v, making them non-authoritative: %v", tablesUpdated, err)
		code := vterrors.Code(err)
		if code == vtrpcpb.Code_UNAUTHENTICATED || code == vtrpcpb.Code_PERMISSION_DENIED {
			log.Warning(aclErrorMessageLog)
		}
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// first we empty all prior schema. deleted tables will not show up in the result,
	// so this is the only chance to delete
	for _, tbl := range tablesUpdated {
		t.tables.delete(th.Target.Keyspace, tbl)
	}
	t.updateTables(th.Target.Keyspace, res)
	return true
}

func (t *Tracker) updateTables(keyspace string, res *sqltypes.Result) {
	for _, row := range res.Rows {
		tbl := row[0].ToString()
		colName := row[1].ToString()
		colType := row[2].ToString()
		collation := row[3].ToString()

		cType := sqlparser.ColumnType{Type: colType}
		col := vindexes.Column{Name: sqlparser.NewIdentifierCI(colName), Type: cType.SQLType(), CollationName: collation}
		cols := t.tables.get(keyspace, tbl)

		t.tables.set(keyspace, tbl, append(cols, col))
	}
}

// RegisterSignalReceiver allows a function to register to be called when new schema is available
func (t *Tracker) RegisterSignalReceiver(f func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, controller := range t.tracked {
		controller.signal = f
	}
	t.signal = f
}

// AddNewKeyspace adds keyspace to the tracker.
func (t *Tracker) AddNewKeyspace(conn queryservice.QueryService, target *querypb.Target) error {
	updateController := t.newUpdateController()
	t.tracked[target.Keyspace] = updateController
	err := t.LoadKeyspace(conn, target)
	if err != nil {
		updateController.setIgnore(checkIfWeShouldIgnoreKeyspace(err))
	}
	return err
}

type tableMap struct {
	m map[keyspaceStr]map[tableNameStr][]vindexes.Column
}

func (tm *tableMap) set(ks, tbl string, cols []vindexes.Column) {
	m := tm.m[ks]
	if m == nil {
		m = make(map[tableNameStr][]vindexes.Column)
		tm.m[ks] = m
	}
	m[tbl] = cols
}

func (tm *tableMap) get(ks, tbl string) []vindexes.Column {
	m := tm.m[ks]
	if m == nil {
		return nil
	}
	return m[tbl]
}

func (tm *tableMap) delete(ks, tbl string) {
	m := tm.m[ks]
	if m == nil {
		return
	}
	delete(m, tbl)
}

// This empties out any previous schema for for all tables in a keyspace.
// You should call this before initializing/loading a keyspace of the same
// name in the cache.
func (t *Tracker) clearKeyspaceTables(ks string) {
	if t.tables != nil && t.tables.m != nil {
		delete(t.tables.m, ks)
	}
}
