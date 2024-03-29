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
	"maps"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/ptr"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

type (
	keyspaceStr  = string
	tableNameStr = string
	viewNameStr  = string

	// Tracker contains the required fields to perform schema tracking.
	Tracker struct {
		ch     chan *discovery.TabletHealth
		cancel context.CancelFunc

		mu     sync.Mutex
		tables *tableMap
		views  *viewMap
		ctx    context.Context
		signal func() // a function that we'll call whenever we have new schema data

		// map of keyspace currently tracked
		tracked      map[keyspaceStr]*updateController
		consumeDelay time.Duration

		parser *sqlparser.Parser
	}
)

// defaultConsumeDelay is the default time, the updateController will wait before checking the schema fetch request queue.
const defaultConsumeDelay = 1 * time.Second

// NewTracker creates the tracker object.
func NewTracker(ch chan *discovery.TabletHealth, enableViews bool, parser *sqlparser.Parser) *Tracker {
	t := &Tracker{
		ctx:          context.Background(),
		ch:           ch,
		tables:       &tableMap{m: make(map[keyspaceStr]map[tableNameStr]*vindexes.TableInfo)},
		tracked:      map[keyspaceStr]*updateController{},
		consumeDelay: defaultConsumeDelay,
		parser:       parser,
	}

	if enableViews {
		t.views = &viewMap{m: map[keyspaceStr]map[viewNameStr]sqlparser.SelectStatement{}, parser: parser}
	}
	return t
}

// LoadKeyspace loads the keyspace schema.
func (t *Tracker) LoadKeyspace(conn queryservice.QueryService, target *querypb.Target) error {
	err := t.loadTables(conn, target)
	if err != nil {
		return err
	}
	err = t.loadViews(conn, target)
	if err != nil {
		return err
	}

	t.tracked[target.Keyspace].setLoaded(true)
	return nil
}

func (t *Tracker) loadTables(conn queryservice.QueryService, target *querypb.Target) error {
	if t.tables == nil {
		// this can only happen in testing
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// We must clear out any previous schema before loading it here as this is called
	// whenever a shard's primary tablet starts and sends the initial signal. Without
	// clearing out the previous schema we can end up with duplicate entries when the
	// tablet is simply restarted or potentially when we elect a new primary.
	t.clearKeyspaceTables(target.Keyspace)

	var numTables int
	err := conn.GetSchema(t.ctx, target, querypb.SchemaTableType_TABLES, nil, func(schemaRes *querypb.GetSchemaResponse) error {
		t.updateTables(target.Keyspace, schemaRes.TableDefinition)
		numTables += len(schemaRes.TableDefinition)
		return nil
	})
	if err != nil {
		return err
	}
	log.Infof("finished loading tables for keyspace %s. Found %d tables", target.Keyspace, numTables)

	return nil
}

func (t *Tracker) loadViews(conn queryservice.QueryService, target *querypb.Target) error {
	if t.views == nil {
		// This happens only when views are not enabled.
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	// We must clear out any previous view definition before loading it here as this is called
	// whenever a shard's primary tablet starts and sends the initial signal.
	// This is needed clear out any stale view definitions.
	t.clearKeyspaceViews(target.Keyspace)

	var numViews int
	err := conn.GetSchema(t.ctx, target, querypb.SchemaTableType_VIEWS, nil, func(schemaRes *querypb.GetSchemaResponse) error {
		t.updateViews(target.Keyspace, schemaRes.TableDefinition)
		numViews += len(schemaRes.TableDefinition)
		return nil
	})
	if err != nil {
		return err
	}
	log.Infof("finished loading views for keyspace %s. Found %d views", target.Keyspace, numViews)
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
				if th == nil {
					// channel closed
					return
				}
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

	tblInfo := t.tables.get(ks, tbl)
	return tblInfo.Columns
}

// GetForeignKeys returns the foreign keys for table in the given keyspace.
func (t *Tracker) GetForeignKeys(ks string, tbl string) []*sqlparser.ForeignKeyDefinition {
	t.mu.Lock()
	defer t.mu.Unlock()

	tblInfo := t.tables.get(ks, tbl)
	return tblInfo.ForeignKeys
}

// GetIndexes returns the indexes for table in the given keyspace.
func (t *Tracker) GetIndexes(ks string, tbl string) []*sqlparser.IndexDefinition {
	t.mu.Lock()
	defer t.mu.Unlock()

	tblInfo := t.tables.get(ks, tbl)
	return tblInfo.Indexes
}

// Tables returns a map with the columns for all known tables in the keyspace
func (t *Tracker) Tables(ks string) map[string]*vindexes.TableInfo {
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.tables.m[ks]
	if m == nil {
		return map[string]*vindexes.TableInfo{} // we know nothing about this KS, so that is the info we can give out
	}

	return maps.Clone(m)
}

// Views returns all known views in the keyspace with their definition.
func (t *Tracker) Views(ks string) map[string]sqlparser.SelectStatement {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.views == nil {
		return nil
	}

	m := t.views.m[ks]
	return maps.Clone(m)
}

func (t *Tracker) updateSchema(th *discovery.TabletHealth) bool {
	success := true
	if th.Stats.TableSchemaChanged != nil {
		success = t.updatedTableSchema(th)
	}
	if !success || th.Stats.ViewSchemaChanged == nil {
		return success
	}
	// there is view definition change in the tablet
	return t.updatedViewSchema(th)
}

func (t *Tracker) updatedTableSchema(th *discovery.TabletHealth) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	tablesUpdated := th.Stats.TableSchemaChanged

	// first we empty all prior schema. deleted tables will not show up in the result,
	// so this is the only chance to delete
	for _, tbl := range tablesUpdated {
		t.tables.delete(th.Target.Keyspace, tbl)
	}
	err := th.Conn.GetSchema(t.ctx, th.Target, querypb.SchemaTableType_TABLES, tablesUpdated, func(schemaRes *querypb.GetSchemaResponse) error {
		t.updateTables(th.Target.Keyspace, schemaRes.TableDefinition)
		return nil
	})
	if err != nil {
		t.tracked[th.Target.Keyspace].setLoaded(false)
		// TODO: optimize for the tables that got errored out.
		log.Warningf("error fetching new schema for %v, making them non-authoritative: %v", tablesUpdated, err)
		return false
	}
	return true
}

func (t *Tracker) updateTables(keyspace string, res map[string]string) {
	for tableName, tableDef := range res {
		stmt, err := t.parser.Parse(tableDef)
		if err != nil {
			log.Warningf("error parsing table definition for %s: %v", tableName, err)
			continue
		}
		ddl, ok := stmt.(*sqlparser.CreateTable)
		if !ok {
			log.Warningf("parsed table definition for '%s' is not a create table definition", tableName)
			continue
		}

		cols := getColumns(ddl.TableSpec)
		fks := getForeignKeys(ddl.TableSpec)
		t.tables.set(keyspace, tableName, cols, fks, ddl.TableSpec.Indexes)
	}
}

func getColumns(tblSpec *sqlparser.TableSpec) []vindexes.Column {
	tblCollation := getTableCollation(tblSpec)
	cols := make([]vindexes.Column, 0, len(tblSpec.Columns))
	for _, column := range tblSpec.Columns {
		colCollation := getColumnCollation(tblCollation, column)
		size := ptr.Unwrap(column.Type.Length, 0)
		scale := ptr.Unwrap(column.Type.Scale, 0)
		nullable := ptr.Unwrap(column.Type.Options.Null, true)
		cols = append(cols,
			vindexes.Column{
				Name:          column.Name,
				Type:          column.Type.SQLType(),
				CollationName: colCollation,
				Default:       column.Type.Options.Default,
				Invisible:     column.Type.Invisible(),
				Size:          int32(size),
				Scale:         int32(scale),
				Nullable:      nullable,
				Values:        column.Type.EnumValues,
			})
	}
	return cols
}

func getForeignKeys(tblSpec *sqlparser.TableSpec) []*sqlparser.ForeignKeyDefinition {
	if tblSpec.Constraints == nil {
		return nil
	}
	var fks []*sqlparser.ForeignKeyDefinition
	for _, constraint := range tblSpec.Constraints {
		fkDef, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition)
		if !ok {
			continue
		}
		fks = append(fks, fkDef)
	}
	return fks
}

func getTableCollation(tblSpec *sqlparser.TableSpec) string {
	if tblSpec.Options == nil {
		return ""
	}
	collate := sqlparser.KeywordString(sqlparser.COLLATE)
	for _, option := range tblSpec.Options {
		if strings.EqualFold(option.Name, collate) {
			return option.String
		}
	}
	return ""
}

func getColumnCollation(defaultCollation string, column *sqlparser.ColumnDefinition) string {
	if column.Type.Options == nil || column.Type.Options.Collate == "" {
		switch strings.ToLower(column.Type.Type) {
		case "enum", "set", "text", "tinytext", "mediumtext", "longtext", "varchar", "char":
			return defaultCollation
		case "json":
			return "utf8mb4_bin"
		}
		return "binary"
	}
	return column.Type.Options.Collate
}

func (t *Tracker) updatedViewSchema(th *discovery.TabletHealth) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	viewsUpdated := th.Stats.ViewSchemaChanged

	// first we empty all prior schema. deleted tables will not show up in the result,
	// so this is the only chance to delete
	for _, view := range viewsUpdated {
		t.views.delete(th.Target.Keyspace, view)
	}
	err := th.Conn.GetSchema(t.ctx, th.Target, querypb.SchemaTableType_VIEWS, viewsUpdated, func(schemaRes *querypb.GetSchemaResponse) error {
		t.updateViews(th.Target.Keyspace, schemaRes.TableDefinition)
		return nil
	})
	if err != nil {
		t.tracked[th.Target.Keyspace].setLoaded(false)
		// TODO: optimize for the views that got errored out.
		log.Warningf("error fetching new views definition for %v", viewsUpdated, err)
		return false
	}
	return true
}

func (t *Tracker) updateViews(keyspace string, res map[string]string) {
	for viewName, viewDef := range res {
		t.views.set(keyspace, viewName, viewDef)
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
	m map[keyspaceStr]map[tableNameStr]*vindexes.TableInfo
}

func (tm *tableMap) set(ks, tbl string, cols []vindexes.Column, fks []*sqlparser.ForeignKeyDefinition, indexes []*sqlparser.IndexDefinition) {
	m := tm.m[ks]
	if m == nil {
		m = make(map[tableNameStr]*vindexes.TableInfo)
		tm.m[ks] = m
	}
	m[tbl] = &vindexes.TableInfo{Columns: cols, ForeignKeys: fks, Indexes: indexes}
}

func (tm *tableMap) get(ks, tbl string) *vindexes.TableInfo {
	m := tm.m[ks]
	if m == nil {
		return &vindexes.TableInfo{}
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

// This empties out any previous schema for all tables in a keyspace.
// You should call this before initializing/loading a keyspace of the same
// name in the cache.
func (t *Tracker) clearKeyspaceTables(ks string) {
	if t.tables != nil && t.tables.m != nil {
		delete(t.tables.m, ks)
	}
}

type viewMap struct {
	m      map[keyspaceStr]map[viewNameStr]sqlparser.SelectStatement
	parser *sqlparser.Parser
}

func (vm *viewMap) set(ks, tbl, sql string) {
	m := vm.m[ks]
	if m == nil {
		m = make(map[tableNameStr]sqlparser.SelectStatement)
		vm.m[ks] = m
	}
	stmt, err := vm.parser.Parse(sql)
	if err != nil {
		log.Warningf("ignoring view '%s', parsing error in view definition: '%s'", tbl, sql)
		return
	}
	cv, ok := stmt.(*sqlparser.CreateView)
	if !ok {
		log.Warningf("ignoring view '%s', view definition is not a create view query: %T", tbl, stmt)
		return
	}
	m[tbl] = cv.Select
}

func (vm *viewMap) get(ks, tbl string) sqlparser.SelectStatement {
	m := vm.m[ks]
	if m == nil {
		return nil
	}
	return m[tbl]
}

func (vm *viewMap) delete(ks, tbl string) {
	m := vm.m[ks]
	if m == nil {
		return
	}
	delete(m, tbl)
}

func (t *Tracker) clearKeyspaceViews(ks string) {
	if t.views != nil && t.views.m != nil {
		delete(t.views.m, ks)
	}
}

// GetViews returns the view statement for the given keyspace and view name.
func (t *Tracker) GetViews(ks string, tbl string) sqlparser.SelectStatement {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.views.get(ks, tbl)
}
