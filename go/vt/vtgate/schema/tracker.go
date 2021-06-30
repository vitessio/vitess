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

// NewTracker creates the tracker object.
func NewTracker(ch chan *discovery.TabletHealth) *Tracker {
	return &Tracker{
		ctx:          context.Background(),
		ch:           ch,
		tables:       &tableMap{m: map[keyspaceStr]map[tableNameStr][]vindexes.Column{}},
		tracked:      map[keyspaceStr]*updateController{},
		consumeDelay: defaultConsumeDelay,
	}
}

// LoadKeyspace loads the keyspace schema.
func (t *Tracker) LoadKeyspace(conn queryservice.QueryService, target *querypb.Target) error {
	res, err := conn.Execute(context.Background(), target, mysql.FetchTables, nil, 0, 0, nil)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.updateTables(target.Keyspace, res)
	t.tracked[target.Keyspace].setLoaded(true)
	log.Infof("finished loading schema for keyspace %s. Found %d tables", target.Keyspace, len(res.Rows))
	return nil
}

// Start starts the schema tracking.
func (t *Tracker) Start() {
	log.Info("Starting schema tracking")
	ctx, cancel := context.WithCancel(context.Background())
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

func (t *Tracker) initKeyspace(th *discovery.TabletHealth) bool {
	err := t.LoadKeyspace(th.Conn, th.Target)
	if err != nil {
		log.Warningf("Unable to add keyspace to tracker: %v", err)
		return false
	}
	return true
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

		cType := sqlparser.ColumnType{Type: colType}
		col := vindexes.Column{Name: sqlparser.NewColIdent(colName), Type: cType.SQLType()}
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
	t.tracked[target.Keyspace] = t.newUpdateController()
	return t.LoadKeyspace(conn, target)
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
