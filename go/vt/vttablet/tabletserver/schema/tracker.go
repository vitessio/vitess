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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sidecardb"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// VStreamer defines  the functions of VStreamer
// that the replicationWatcher needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
}

// Tracker watches the replication and saves the latest schema into the schema_version table when a DDL is encountered.
type Tracker struct {
	enabled bool

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup

	env    tabletenv.Env
	vs     VStreamer
	engine *Engine
}

// NewTracker creates a Tracker, needs an Open SchemaEngine (which implements the trackerEngine interface)
func NewTracker(env tabletenv.Env, vs VStreamer, engine *Engine) *Tracker {
	return &Tracker{
		enabled: env.Config().TrackSchemaVersions,
		env:     env,
		vs:      vs,
		engine:  engine,
	}
}

// Open enables the tracker functionality
func (tr *Tracker) Open() {
	if !tr.enabled {
		return
	}
	log.Info("Schema Tracker: opening")

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	tr.cancel = cancel
	tr.wg.Add(1)

	go tr.process(ctx)
}

// Close disables the tracker functionality
func (tr *Tracker) Close() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel == nil {
		return
	}

	tr.cancel()
	tr.cancel = nil
	tr.wg.Wait()
	log.Info("Schema Tracker: closed")
}

// Enable forces tracking to be on or off.
// Only used for testing.
func (tr *Tracker) Enable(enabled bool) {
	tr.mu.Lock()
	tr.enabled = enabled
	tr.mu.Unlock()
	if enabled {
		tr.Open()
	} else {
		tr.Close()
	}
}

func (tr *Tracker) process(ctx context.Context) {
	defer tr.env.LogError()
	defer tr.wg.Done()
	if err := tr.possiblyInsertInitialSchema(ctx); err != nil {
		log.Errorf("error inserting initial schema: %v", err)
		return
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	var gtid string
	for {
		err := tr.vs.Stream(ctx, "current", nil, filter, func(events []*binlogdatapb.VEvent) error {
			for _, event := range events {
				if event.Type == binlogdatapb.VEventType_GTID {
					gtid = event.Gtid
				}
				if event.Type == binlogdatapb.VEventType_DDL &&
					MustReloadSchemaOnDDL(event.Statement, tr.engine.cp.DBName()) {

					if err := tr.schemaUpdated(gtid, event.Statement, event.Timestamp); err != nil {
						tr.env.Stats().ErrorCounters.Add(vtrpcpb.Code_INTERNAL.String(), 1)
						log.Errorf("Error updating schema: %s for ddl %s, gtid %s",
							sqlparser.TruncateForLog(err.Error()), event.Statement, gtid)
					}
				}
			}
			return nil
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		log.Infof("Tracker's vStream ended: %v, retrying in 5 seconds", err)
		time.Sleep(5 * time.Second)
	}
}

func (tr *Tracker) currentPosition(ctx context.Context) (mysql.Position, error) {
	conn, err := tr.engine.cp.Connect(ctx)
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Close()
	return conn.PrimaryPosition()
}

func (tr *Tracker) isSchemaVersionTableEmpty(ctx context.Context) (bool, error) {
	conn, err := tr.engine.GetConnection(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()
	result, err := conn.Exec(ctx, sqlparser.BuildParsedQuery("select id from %s.schema_version limit 1",
		sidecardb.GetIdentifier()).Query, 1, false)
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return true, nil
	}
	return false, nil
}

// possiblyInsertInitialSchema stores the latest schema when a tracker starts and the schema_version table is empty
// this enables the right schema to be available between the time the tracker starts first and the first DDL is applied
func (tr *Tracker) possiblyInsertInitialSchema(ctx context.Context) error {
	var err error
	needsWarming, err := tr.isSchemaVersionTableEmpty(ctx)
	if err != nil {
		return err
	}
	if !needsWarming { // the schema_version table is not empty, nothing to do here
		return nil
	}
	if err = tr.engine.Reload(ctx); err != nil {
		return err
	}

	timestamp := time.Now().UnixNano() / 1e9
	ddl := ""
	pos, err := tr.currentPosition(ctx)
	if err != nil {
		return err
	}
	gtid := mysql.EncodePosition(pos)
	log.Infof("Saving initial schema for gtid %s", gtid)

	return tr.saveCurrentSchemaToDb(ctx, gtid, ddl, timestamp)
}

func (tr *Tracker) schemaUpdated(gtid string, ddl string, timestamp int64) error {
	log.Infof("Processing schemaUpdated event for gtid %s, ddl %s", gtid, ddl)
	if gtid == "" || ddl == "" {
		return fmt.Errorf("got invalid gtid or ddl in schemaUpdated")
	}
	ctx := context.Background()
	// Engine will have reloaded the schema because vstream will reload it on a DDL
	return tr.saveCurrentSchemaToDb(ctx, gtid, ddl, timestamp)
}

func (tr *Tracker) saveCurrentSchemaToDb(ctx context.Context, gtid, ddl string, timestamp int64) error {
	tables := tr.engine.GetSchema()
	dbSchema := &binlogdatapb.MinimalSchema{
		Tables: []*binlogdatapb.MinimalTable{},
	}
	for _, table := range tables {
		dbSchema.Tables = append(dbSchema.Tables, newMinimalTable(table))
	}
	blob, _ := proto.Marshal(dbSchema)

	conn, err := tr.engine.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	query := sqlparser.BuildParsedQuery("insert into %s.schema_version "+
		"(pos, ddl, schemax, time_updated) "+
		"values (%s, %s, %s, %d)", sidecardb.GetIdentifier(), encodeString(gtid),
		encodeString(ddl), encodeString(string(blob)), timestamp).Query
	_, err = conn.Exec(ctx, query, 1, false)
	if err != nil {
		return err
	}
	return nil
}

func newMinimalTable(st *Table) *binlogdatapb.MinimalTable {
	table := &binlogdatapb.MinimalTable{
		Name:   st.Name.String(),
		Fields: st.Fields,
	}
	var pkc []int64
	for _, pk := range st.PKColumns {
		pkc = append(pkc, int64(pk))
	}
	table.PKColumns = pkc
	return table
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

// MustReloadSchemaOnDDL returns true if the ddl is for the db which is part of the workflow and is not an online ddl artifact
func MustReloadSchemaOnDDL(sql string, dbname string) bool {
	ast, err := sqlparser.Parse(sql)
	if err != nil {
		return false
	}
	switch stmt := ast.(type) {
	case sqlparser.DBDDLStatement:
		return false
	case sqlparser.DDLStatement:
		tables := []sqlparser.TableName{stmt.GetTable()}
		tables = append(tables, stmt.GetToTables()...)
		for _, table := range tables {
			if table.IsEmpty() {
				continue
			}
			if !table.Qualifier.IsEmpty() && table.Qualifier.String() != dbname {
				continue
			}
			tableName := table.Name.String()
			if schema.IsOnlineDDLTableName(tableName) {
				continue
			}
			return true
		}
	}
	return false
}
