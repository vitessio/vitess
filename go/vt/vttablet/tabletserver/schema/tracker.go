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
)

const createSchemaTrackingTable = `CREATE TABLE IF NOT EXISTS _vt.schema_version (
		 id INT AUTO_INCREMENT,
		  pos VARBINARY(10000) NOT NULL,
		  time_updated BIGINT(20) NOT NULL,
		  ddl VARBINARY(1000) DEFAULT NULL,
		  schemax BLOB NOT NULL,
		  PRIMARY KEY (id)
		) ENGINE=InnoDB`

// VStreamer defines  the functions of VStreamer
// that the replicationWatcher needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
}

// Tracker watches the replication and saves the latest schema into _vt.schema_version when a DDL is encountered.
type Tracker struct {
	env    tabletenv.Env
	vs     VStreamer
	engine *Engine
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTracker creates a Tracker, needs an Open SchemaEngine (which implements the trackerEngine interface)
func NewTracker(env tabletenv.Env, vs VStreamer, engine *Engine) *Tracker {
	return &Tracker{
		env:    env,
		vs:     vs,
		engine: engine,
	}
}

// Open enables the tracker functionality
func (tr *Tracker) Open() {
	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	tr.cancel = cancel
	tr.wg.Add(1)
	go tr.process(ctx)
}

// Close disables the tracker functionality
func (tr *Tracker) Close() {
	if tr.cancel == nil {
		return
	}
	tr.cancel()
	tr.cancel = nil
	tr.wg.Wait()
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
	for name, table := range tables {
		tr := &binlogdatapb.MinimalTable{
			Name:   name,
			Fields: table.Fields,
		}
		pks := make([]int64, 0)
		for _, pk := range table.PKColumns {
			pks = append(pks, int64(pk))
		}
		tr.PKColumns = pks
		dbSchema.Tables = append(dbSchema.Tables, tr)
	}
	blob, _ := proto.Marshal(dbSchema)

	conn, err := tr.engine.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	_, err = conn.Exec(ctx, createSchemaTrackingTable, 1, false)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("insert into _vt.schema_version "+
		"(pos, ddl, schemax, time_updated) "+
		"values (%v, %v, %v, %d)", encodeString(gtid), encodeString(ddl), encodeString(string(blob)), timestamp)

	_, err = conn.Exec(ctx, query, 1, false)
	if err != nil {
		return err
	}
	return nil
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}
