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
	"vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/vt/sqlparser"
)

const getSchemaVersions = "select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > %d order by id asc"

// Historian defines the interface to reload a db schema or get the schema of a table for a given position
type Historian interface {
	RegisterVersionEvent() error
	Reload(ctx context.Context) error
	GetTableForPos(tableName sqlparser.TableIdent, pos string) *binlogdata.MinimalTable
	Open() error
	Init(tabletType topodatapb.TabletType) error
	SetTrackSchemaVersions(val bool)
}

// HistoryEngine exposes only those schema.Engine APIs that are needed by the HistorianSvc
// We define an interface to facilitate a mock for testing
var _ HistoryEngine = (*Engine)(nil)

// HistoryEngine defines the interface which is implemented by schema.Engine for the historian to get current schema
// information from the database
type HistoryEngine interface {
	Open() error
	Reload(ctx context.Context) error
	GetConnection(ctx context.Context) (*connpool.DBConn, error)
	GetSchema() map[string]*Table
}

// TrackedSchema has the snapshot of the table at a given pos (reached by ddl)
type TrackedSchema struct {
	schema map[string]*binlogdata.MinimalTable
	pos    mysql.Position
	ddl    string
}

var _ Historian = (*HistorianSvc)(nil)

// HistorianSvc implements the Historian interface by calling schema.Engine for the underlying schema
// and supplying a schema for a specific version by loading the cached values from the schema_version table
// The schema version table is populated by the Tracker
type HistorianSvc struct {
	he                  HistoryEngine
	lastID              int64
	schemas             []*TrackedSchema
	isMaster            bool
	mu                  sync.Mutex
	trackSchemaVersions bool
	lastSchema          map[string]*binlogdata.MinimalTable
}

// Reload gets the latest schema from the database
func (h *HistorianSvc) Reload(ctx context.Context) error {
	return h.reload(ctx)
}

// NewHistorian creates a new historian. It expects a schema.Engine instance
func NewHistorian(he HistoryEngine) *HistorianSvc {
	sh := HistorianSvc{he: he, lastID: 0}
	return &sh
}

// SetTrackSchemaVersions can be used to turn on/off the use of the schema_version history in the historian
func (h *HistorianSvc) SetTrackSchemaVersions(val bool) {
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
	timeUpdated, _ := evalengine.ToInt64(row[3])
	sch := &binlogdata.MinimalSchema{}
	if err := proto.Unmarshal(row[4].ToBytes(), sch); err != nil {
		return nil, 0, err
	}
	log.Infof("Read tracked schema from db: id %d, pos %v, ddl %s, schema len %d, time_updated %d \n", id, mysql.EncodePosition(pos), ddl, len(sch.Tables), timeUpdated)

	tables := map[string]*binlogdata.MinimalTable{}
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
func (h *HistorianSvc) loadFromDB(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	conn, err := h.he.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	log.Infof("In loadFromDb for lastID %d", h.lastID)
	tableData, err := conn.Exec(ctx, fmt.Sprintf(getSchemaVersions, h.lastID), 10000, true)
	if err != nil {
		log.Errorf("Error reading schema_tracking table %v", err)
		return err
	}
	log.Infof("Received rows from schema_version: %v", len(tableData.Rows))
	if len(tableData.Rows) > 0 {
		for _, row := range tableData.Rows {
			trackedSchema, id, err := h.readRow(row)
			if err != nil {
				return err
			}
			h.schemas = append(h.schemas, trackedSchema)
			h.lastID = id
		}
		h.sortSchemas()
	}
	return nil
}

// Init needs to be called once the tablet's type is known. It also warms the cache from the db
func (h *HistorianSvc) Init(tabletType topodatapb.TabletType) error {
	h.Open()
	ctx := tabletenv.LocalContext()

	if !h.trackSchemaVersions {
		return nil
	}

	h.isMaster = tabletType == topodatapb.TabletType_MASTER
	if h.isMaster {
		h.Reload(ctx)
		if err := h.loadFromDB(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Open opens the underlying schema Engine. Called directly by a user purely interested in schema.Engine functionality
// Other users like tabletserver and tests call Init
func (h *HistorianSvc) Open() error {
	return h.he.Open()
}

// reload gets the latest schema and replaces the latest copy of the schema maintained by the historian
func (h *HistorianSvc) reload(ctx context.Context) error {
	log.Info("In historian reload")
	if err := h.he.Reload(ctx); err != nil {
		return err
	}
	tables := map[string]*binlogdata.MinimalTable{}
	log.Infof("Schema returned by engine has %d tables", len(h.he.GetSchema()))
	for _, t := range h.he.GetSchema() {
		table := &binlogdata.MinimalTable{
			Name:   t.Name.String(),
			Fields: t.Fields,
		}
		var pkc []int64
		for _, pk := range t.PKColumns {
			pkc = append(pkc, int64(pk))
		}
		table.PKColumns = pkc
		tables[table.Name] = table
		//log.Infof("Historian, found table %s", table.Name)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSchema = tables
	return nil
}

// GetTableForPos returns a best-effort schema for a specific gtid
func (h *HistorianSvc) GetTableForPos(tableName sqlparser.TableIdent, gtid string) *binlogdata.MinimalTable {
	log.Infof("GetTableForPos called for %s with pos %s", tableName, gtid)
	if gtid != "" {
		pos, err := mysql.DecodePosition(gtid)
		if err != nil {
			log.Errorf("Error decoding position for %s: %v", gtid, err)
			return nil
		}
		var t *binlogdata.MinimalTable
		if h.trackSchemaVersions && len(h.schemas) > 0 {
			t = h.getTableFromHistoryForPos(tableName, pos)
		}
		if t != nil {
			log.Infof("Returning table %s from history for pos %s, schema %s", tableName, gtid, t)
			return t
		}
	}
	h.reload(context.Background())
	if h.lastSchema == nil {
		return nil
	}
	log.Infof("Returning table %s from latest schema for pos %s, schema %s", tableName, gtid, h.lastSchema[tableName.String()])
	return h.lastSchema[tableName.String()]
}

// sortSchemas sorts entries in ascending order of gtid, ex: 40,44,48
func (h *HistorianSvc) sortSchemas() {
	log.Infof("In sortSchemas with len %d", len(h.schemas))
	sort.Slice(h.schemas, func(i int, j int) bool {
		return h.schemas[j].pos.AtLeast(h.schemas[i].pos)
	})
	log.Infof("Ending sortSchemas with len %d", len(h.schemas))
}

// getTableFromHistoryForPos looks in the cache for a schema for a specific gtid
func (h *HistorianSvc) getTableFromHistoryForPos(tableName sqlparser.TableIdent, pos mysql.Position) *binlogdata.MinimalTable {
	idx := sort.Search(len(h.schemas), func(i int) bool {
		log.Infof("idx %d pos %s checking pos %s equality %t less %t", i, mysql.EncodePosition(pos), mysql.EncodePosition(h.schemas[i].pos), pos.Equal(h.schemas[i].pos), !pos.AtLeast(h.schemas[i].pos))
		return pos.Equal(h.schemas[i].pos) || !pos.AtLeast(h.schemas[i].pos)
	})
	log.Infof("Index %d: len schemas %d", idx, len(h.schemas))
	if idx >= len(h.schemas) || idx == 0 && !pos.Equal(h.schemas[idx].pos) { // beyond the range of the cache
		log.Infof("Not found schema in cache for %s with pos %s", tableName, pos)
		return nil
	}
	if pos.Equal(h.schemas[idx].pos) { //exact match to a cache entry
		log.Infof("Found tracked schema for pos %s, ddl %s", pos, h.schemas[idx].ddl)
		return h.schemas[idx].schema[tableName.String()]
	}
	//not an exact match, so based on our sort algo idx is one less than found: from 40,44,48 : 43 < 44 but we want 40
	log.Infof("Found tracked schema for pos %s, ddl %s", pos, h.schemas[idx-1].ddl)
	return h.schemas[idx-1].schema[tableName.String()]
}

// RegisterVersionEvent is called by the vstream when it encounters a version event (an insert into _vt.schema_tracking)
// It triggers the historian to load the newer rows from the database to update its cache
func (h *HistorianSvc) RegisterVersionEvent() error {
	if !h.trackSchemaVersions {
		return nil
	}
	ctx := tabletenv.LocalContext()
	if err := h.loadFromDB(ctx); err != nil {
		return err
	}
	return nil
}
