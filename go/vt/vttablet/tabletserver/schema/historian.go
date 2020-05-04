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

const getSchemaVersions = "select id, pos, ddl, time_updated, schemax from _vt.schema_version where id >= %d order by id desc"

// Historian defines the interface to reload a db schema or get the schema of a table for a given position
type Historian interface {
	RegisterVersionEvent()
	Reload(ctx context.Context) error
	GetTableForPos(tableName sqlparser.TableIdent, pos string) *binlogdata.TableMetaData
	Open() error
	Init(tabletType topodatapb.TabletType) error
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

// TODO: rename TableMetaData and TableMetaDataCollection: to MinimalSchema and MinimalTable?

// TrackedSchema has the snapshot of the table at a given pos (reached by ddl)
type TrackedSchema struct {
	schema map[string]*binlogdata.TableMetaData
	pos    mysql.Position
	ddl    string
}

var _ Historian = (*HistorianSvc)(nil)

// HistorianSvc implements the Historian interface by calling schema.Engine for the underlying schema
// and supplying a schema for a specific version by loading the cached values from the schema_version table
// The schema version table is populated by the Tracker
type HistorianSvc struct {
	he                  HistoryEngine
	lastID              int
	schemas             []*TrackedSchema
	isMaster            bool
	mu                  sync.Mutex
	trackSchemaVersions bool
	lastSchema          map[string]*binlogdata.TableMetaData
}

// Reload gets the latest schema from the database
func (h *HistorianSvc) Reload(ctx context.Context) error {
	return h.reload(ctx)
}

// NewHistorian creates a new historian. It expects a schema.Engine instance
func NewHistorian(he HistoryEngine) *HistorianSvc {
	sh := HistorianSvc{he: he}
	sh.trackSchemaVersions = true //TODO use flag

	return &sh
}

// readRow converts a row from the schema_version table to a TrackedSchema
func (h *HistorianSvc) readRow(row []sqltypes.Value) (*TrackedSchema, error) {
	id, _ := evalengine.ToInt64(row[0])
	pos, err := mysql.DecodePosition(string(row[1].ToBytes()))
	if err != nil {
		return nil, err
	}
	ddl := string(row[2].ToBytes())
	timeUpdated, _ := evalengine.ToInt64(row[3])
	sch := &binlogdata.TableMetaDataCollection{}
	if err := proto.Unmarshal(row[4].ToBytes(), sch); err != nil {
		return nil, err
	}
	log.Infof("Read tracked schema from db: id %d, pos %v, ddl %s, schema len %d, time_updated %d \n", id, pos, ddl, len(sch.Tables), timeUpdated)

	tables := map[string]*binlogdata.TableMetaData{}
	for _, t := range sch.Tables {
		tables[t.Name] = t
	}
	trackedSchema := &TrackedSchema{
		schema: tables,
		pos:    pos,
		ddl:    ddl,
	}
	return trackedSchema, nil
}

// loadFromDB loads all rows from the schema_version table that the historian does not have as yet
func (h *HistorianSvc) loadFromDB(ctx context.Context, lastID int) error {
	conn, err := h.he.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	log.Infof("In loadFromDb for lastID %d", lastID)
	tableData, err := conn.Exec(ctx, fmt.Sprintf(getSchemaVersions, lastID), 10000, true)
	if err != nil {
		log.Errorf("Error reading schema_tracking table %v", err)
		return err
	}
	log.Infof("Received rows from schema_version: %v", len(tableData.Rows))
	if len(tableData.Rows) > 0 {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, row := range tableData.Rows {
			trackedSchema, err := h.readRow(row)
			if err != nil {
				return err
			}
			h.schemas = append(h.schemas, trackedSchema)
		}
		h.sortSchemas()
	}
	return nil
}

func (h *HistorianSvc) sortSchemas() {
	sort.Slice(h.schemas, func(i int, j int) bool {
		return h.schemas[j].pos.AtLeast(h.schemas[i].pos)
	})
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
		if err := h.loadFromDB(ctx, h.lastID); err != nil {
			return err
		}
	}
	return nil
}

// Open opens the underlying schema Engine. Called directly by any user of the historian other than tabletserver which calls Init
func (h *HistorianSvc) Open() error {
	return h.he.Open()
}

// reload gets the latest schema and replaces the latest copy of the schema maintained by the historian
func (h *HistorianSvc) reload(ctx context.Context) error {
	log.Info("In historian reload")
	if err := h.he.Reload(ctx); err != nil {
		return err
	}
	tables := map[string]*binlogdata.TableMetaData{}
	log.Infof("Schema returned by engine is %d", len(h.he.GetSchema()))
	for _, t := range h.he.GetSchema() {
		table := &binlogdata.TableMetaData{
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
	h.lastSchema = tables
	return nil
}

// GetTableForPos returns a best-effort schema for a specific gtid
func (h *HistorianSvc) GetTableForPos(tableName sqlparser.TableIdent, gtid string) *binlogdata.TableMetaData {
	log.Infof("GetTableForPos called for %s with pos %s", tableName, gtid)
	if gtid != "" {
		pos, err := mysql.DecodePosition(gtid)
		if err != nil {
			log.Errorf("Error decoding position for %s: %v", gtid, err)
			return nil
		}
		var t *binlogdata.TableMetaData
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

// getTableFromHistoryForPos looks in the cache for a schema for a specific gtid
func (h *HistorianSvc) getTableFromHistoryForPos(tableName sqlparser.TableIdent, pos mysql.Position) *binlogdata.TableMetaData {
	idx := sort.Search(len(h.schemas), func(i int) bool {
		return !pos.AtLeast(h.schemas[i].pos)
	})
	log.Infof("Found index of schema to be %d", idx)
	if idx >= len(h.schemas) || idx == 0 && !h.schemas[idx].pos.Equal(pos) {
		log.Infof("Not found schema in cache for %s with pos %s", tableName, pos)
		return nil
	}
	log.Infof("Found tracked schema. Looking for %s, found %s", pos, h.schemas[idx])
	return h.schemas[idx].schema[tableName.String()]
}

// RegisterVersionEvent is called by the vstream when it encounters a version event (an insert into _vt.schema_tracking)
// It triggers the historian to load the newer rows from the database to update its cache
func (h *HistorianSvc) RegisterVersionEvent() {
	if !h.trackSchemaVersions {
		return
	}
	ctx := tabletenv.LocalContext()
	if err := h.loadFromDB(ctx, h.lastID); err != nil {
		log.Errorf("Error loading schema version information from database in RegisterVersionEvent(): %v", err)
	}
}
