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

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

const createSchemaTrackingTable = `CREATE TABLE IF NOT EXISTS _vt.schema_tracking (
		 id INT AUTO_INCREMENT,
		  pos VARBINARY(10000) NOT NULL,
		  time_updated BIGINT(20) NOT NULL,
		  ddl VARBINARY(1000) DEFAULT NULL,
		  schema longblob NOT NULL,
		  PRIMARY KEY (id)
		) ENGINE=InnoDB`

type trackerEngine interface {
	GetConnection(ctx context.Context) (*connpool.DBConn, error)
	Reload(ctx context.Context) error
	GetSchema() map[string]*Table
}

//SchemaSubscriber will get notified when the schema has been updated
type SchemaSubscriber interface {
	SchemaUpdated(gtid string, ddl string, timestamp int64)
}

var _ SchemaSubscriber = (*Tracker)(nil)

type Tracker struct {
	engine trackerEngine
}

type TableMetaData struct {
	Name      string
	Fields    []*querypb.Field
	PKColumns []int
}

func NewTracker(engine trackerEngine) *Tracker {
	return &Tracker{engine: engine}
}

func (st *Tracker) SchemaUpdated(gtid string, ddl string, timestamp int64) {
	ctx := context.Background()

	st.engine.Reload(ctx)
	newSchema := st.engine.GetSchema()
	dbSchema := &binlogdatapb.TableMetaDataCollection{
		Tables: []*binlogdatapb.TableMetaData{},
	}
	blob, err := proto.Marshal(dbSchema)
	for name, table := range newSchema {
		t := &binlogdatapb.TableMetaData{
			Name:   name,
			Fields: table.Fields,
			//Pkcolumns: table.PKColumns,
		}
		dbSchema.Tables = append(dbSchema.Tables, t)
	}
	query := fmt.Sprintf("insert into _vt.schema_version "+
		"(pos, ddl, schema, time_updated) "+
		"values (%v, %v, %v, %d)", gtid, ddl, encodeString(string(blob)), timestamp)

	conn, err := st.engine.GetConnection(ctx)
	if err != nil {
		return
	}
	defer conn.Recycle()
	log.Infof("Creating schema tracking table")
	_, err = conn.Exec(ctx, createSchemaTrackingTable, 1, false)
	if err != nil {
		log.Errorf("Error creating schema_tracking table %v", err)
		return
	}
	log.Infof("Inserting version for position %s", gtid)
	_, err = conn.Exec(ctx, query, 1, false)
	if err != nil {
		log.Errorf("Error inserting version for position %s %v", gtid, err)
		return
	}
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}
