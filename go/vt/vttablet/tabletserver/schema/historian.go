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

package schema

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/vt/sqlparser"
)

const getSchemaVersions = "select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > %d order by id asc"

// vl defines the glog verbosity level for the package
const vl = 10

// Historian defines the interface to reload a db schema or get the schema of a table for a given position
type Historian interface {
	RegisterVersionEvent() error
	GetTableForPos(tableName sqlparser.TableIdent, pos string) *binlogdatapb.MinimalTable
	Open() error
	Close()
	SetTrackSchemaVersions(val bool)
}

// TrackedSchema has the snapshot of the table at a given pos (reached by ddl)
type TrackedSchema struct {
	schema map[string]*binlogdatapb.MinimalTable
	pos    mysql.Position
	ddl    string
}

var _ Historian = (*HistorianSvc)(nil)

// HistorianSvc implements the Historian interface by calling schema.Engine for the underlying schema
// and supplying a schema for a specific version by loading the cached values from the schema_version table
// The schema version table is populated by the Tracker
type HistorianSvc struct {
	se                  *Engine
	lastID              int64
	schemas             []*TrackedSchema
	mu                  sync.Mutex
	trackSchemaVersions bool
	latestSchema        map[string]*binlogdatapb.MinimalTable
	isOpen              bool
}

// NewHistorian creates a new historian. It expects a schema.Engine instance
func NewHistorian(se *Engine) *HistorianSvc {
	sh := HistorianSvc{se: se, lastID: 0, trackSchemaVersions: true}
	return &sh
}

// SetTrackSchemaVersions can be used to turn on/off the use of the schema_version history in the historian
// Only used for testing
func (h *HistorianSvc) SetTrackSchemaVersions(val bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.trackSchemaVersions = val
}

// readRow converts a row from the schema_version table to a TrackedSchema
func (h *HistorianSvc) readRow(row []sqltypes.Value) (*TrackedSchema, int64, error) {
	id, _ := evalengine.ToInt64(row[0])
	pos, err := mysql.DecodePosition(string(row[1].ToBytes()))
	if err != nil {
		return nil, 0, err
	}
	ddl := string(row[2].ToBytes())
	timeUpdated, err := evalengine.ToInt64(row[3])
	if err != nil {
		return nil, 0, err
	}
	sch := &binlogdatapb.MinimalSchema{}
	if err := proto.Unmarshal(row[4].ToBytes(), sch); err != nil {
		return nil, 0, err
	}
	log.V(vl).Infof("Read tracked schema from db: id %d, pos %v, ddl %s, schema len %d, time_updated %d \n",
		id, mysql.EncodePosition(pos), ddl, len(sch.Tables), timeUpdated)

	tables := map[string]*binlogdatapb.MinimalTable{}
	for _, t := range sch.Tables {
		tables[t.Name] = t
	}
	trackedSchema := &TrackedSchema{
		schema: tables,
		pos:    pos,
		ddl:    ddl,
	}
	return trackedSchema, id, nil
}

// loadFromDB loads all rows from the schema_version table that the historian does not have as yet
// caller should have locked h.mu
func (h *HistorianSvc) loadFromDB(ctx context.Context) error {
	conn, err := h.se.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	tableData, err := conn.Exec(ctx, fmt.Sprintf(getSchemaVersions, h.lastID), 10000, true)
	if err != nil {
		log.Infof("Error reading schema_tracking table %v, will operate with the latest available schema", err)
		return nil
	}
	for _, row := range tableData.Rows {
		trackedSchema, id, err := h.readRow(row)
		if err != nil {
			return err
		}
		h.schemas = append(h.schemas, trackedSchema)
		h.lastID = id
	}
	h.sortSchemas()
	return nil
}

// Open opens the underlying schema Engine. Called directly by a user purely interested in schema.Engine functionality
func (h *HistorianSvc) Open() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.isOpen {
		return nil
	}
	if err := h.se.Open(); err != nil {
		return err
	}
	ctx := tabletenv.LocalContext()
	h.reload()
	if err := h.loadFromDB(ctx); err != nil {
		return err
	}
	h.se.RegisterNotifier("historian", h.schemaChanged)

	h.isOpen = true
	return nil
}

// Close closes the underlying schema engine and empties the version cache
func (h *HistorianSvc) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.isOpen {
		return
	}
	h.schemas = nil
	h.se.UnregisterNotifier("historian")
	h.se.Close()
	h.isOpen = false
}

// convert from schema table representation to minimal tables and store as latestSchema
func (h *HistorianSvc) storeLatest(tables map[string]*Table) {
	minTables := make(map[string]*binlogdatapb.MinimalTable)
	for _, t := range tables {
		table := &binlogdatapb.MinimalTable{
			Name:   t.Name.String(),
			Fields: t.Fields,
		}
		var pkc []int64
		for _, pk := range t.PKColumns {
			pkc = append(pkc, int64(pk))
		}
		table.PKColumns = pkc
		minTables[table.Name] = table
	}
	h.latestSchema = minTables
}

// reload gets the latest schema and replaces the latest copy of the schema maintained by the historian
// caller should lock h.mu
func (h *HistorianSvc) reload() {
	h.storeLatest(h.se.tables)
}

// schema notifier callback
func (h *HistorianSvc) schemaChanged(tables map[string]*Table, _, _, _ []string) {
	if !h.isOpen {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.storeLatest(tables)
}

// GetTableForPos returns a best-effort schema for a specific gtid
func (h *HistorianSvc) GetTableForPos(tableName sqlparser.TableIdent, gtid string) *binlogdatapb.MinimalTable {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.V(2).Infof("GetTableForPos called for %s with pos %s", tableName, gtid)
	if gtid != "" {
		pos, err := mysql.DecodePosition(gtid)
		if err != nil {
			log.Errorf("Error decoding position for %s: %v", gtid, err)
			return nil
		}
		var t *binlogdatapb.MinimalTable
		if h.trackSchemaVersions && len(h.schemas) > 0 {
			t = h.getTableFromHistoryForPos(tableName, pos)
		}
		if t != nil {
			log.V(2).Infof("Returning table %s from history for pos %s, schema %s", tableName, gtid, t)
			return t
		}
	}
	if h.latestSchema == nil || h.latestSchema[tableName.String()] == nil {
		h.reload()
		if h.latestSchema == nil {
			return nil
		}
	}
	log.V(2).Infof("Returning table %s from latest schema for pos %s, schema %s", tableName, gtid, h.latestSchema[tableName.String()])
	return h.latestSchema[tableName.String()]
}

// sortSchemas sorts entries in ascending order of gtid, ex: 40,44,48
func (h *HistorianSvc) sortSchemas() {
	sort.Slice(h.schemas, func(i int, j int) bool {
		return h.schemas[j].pos.AtLeast(h.schemas[i].pos)
	})
}

// getTableFromHistoryForPos looks in the cache for a schema for a specific gtid
func (h *HistorianSvc) getTableFromHistoryForPos(tableName sqlparser.TableIdent, pos mysql.Position) *binlogdatapb.MinimalTable {
	idx := sort.Search(len(h.schemas), func(i int) bool {
		return pos.Equal(h.schemas[i].pos) || !pos.AtLeast(h.schemas[i].pos)
	})
	if idx >= len(h.schemas) || idx == 0 && !pos.Equal(h.schemas[idx].pos) { // beyond the range of the cache
		log.Infof("Schema not found in cache for %s with pos %s", tableName, pos)
		return nil
	}
	if pos.Equal(h.schemas[idx].pos) { //exact match to a cache entry
		return h.schemas[idx].schema[tableName.String()]
	}
	//not an exact match, so based on our sort algo idx is one less than found: from 40,44,48 : 43 < 44 but we want 40
	return h.schemas[idx-1].schema[tableName.String()]
}

// RegisterVersionEvent is called by the vstream when it encounters a version event (an insert into _vt.schema_tracking)
// It triggers the historian to load the newer rows from the database to update its cache
func (h *HistorianSvc) RegisterVersionEvent() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.trackSchemaVersions {
		return nil
	}
	ctx := tabletenv.LocalContext()
	if err := h.loadFromDB(ctx); err != nil {
		return err
	}
	return nil
}
