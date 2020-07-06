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

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/withddl"
)

const createSchemaTrackingTable = `CREATE TABLE IF NOT EXISTS _vt.schema_version (
		 id INT AUTO_INCREMENT,
		  pos VARBINARY(10000) NOT NULL,
		  time_updated BIGINT(20) NOT NULL,
		  ddl VARBINARY(1000) DEFAULT NULL,
		  schemax BLOB NOT NULL,
		  PRIMARY KEY (id)
		) ENGINE=InnoDB`
const alterSchemaTrackingTable = "alter table _vt.schema_version modify column ddl BLOB NOT NULL"

var withDDL = withddl.New([]string{createSchemaTrackingTable, alterSchemaTrackingTable})

// VStreamer defines  the functions of VStreamer
// that the replicationWatcher needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
}

// Tracker watches the replication and saves the latest schema into _vt.schema_version when a DDL is encountered.
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
		log.Info("Schema tracker is not enabled.")
		return
	}

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	tr.cancel = cancel
	tr.wg.Add(1)
	log.Info("Schema tracker enabled.")
	go tr.process(ctx)
}

// Close disables the tracker functionality
func (tr *Tracker) Close() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel == nil {
		return
	}

	log.Info("Schema tracker stopped.")
	tr.cancel()
	tr.cancel = nil
	tr.wg.Wait()
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
				if event.Type == binlogdatapb.VEventType_DDL {
					if err := tr.schemaUpdated(gtid, event.Ddl, event.Timestamp); err != nil {
						tr.env.Stats().ErrorCounters.Add(vtrpcpb.Code_INTERNAL.String(), 1)
						log.Errorf("Error updating schema: %v", err)
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

func (tr *Tracker) schemaUpdated(gtid string, ddl string, timestamp int64) error {
	log.Infof("Processing schemaUpdated event for gtid %s, ddl %s", gtid, ddl)
	if gtid == "" || ddl == "" {
		return fmt.Errorf("got invalid gtid or ddl in schemaUpdated")
	}
	ctx := context.Background()

	// Engine will have reloaded the schema because vstream will reload it on a DDL
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

	query := fmt.Sprintf("insert into _vt.schema_version "+
		"(pos, ddl, schemax, time_updated) "+
		"values (%v, %v, %v, %d)", encodeString(gtid), encodeString(ddl), encodeString(string(blob)), timestamp)
	_, err = withDDL.Exec(ctx, query, conn.Exec)
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
