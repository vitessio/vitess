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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

const createSchemaTrackingTable = `CREATE TABLE IF NOT EXISTS _vt.schema_version (
		 id INT AUTO_INCREMENT,
		  pos VARBINARY(10000) NOT NULL,
		  time_updated BIGINT(20) NOT NULL,
		  ddl VARBINARY(1000) DEFAULT NULL,
		  schemax BLOB NOT NULL,
		  PRIMARY KEY (id)
		) ENGINE=InnoDB`

type trackerEngine interface {
	GetConnection(ctx context.Context) (*connpool.DBConn, error)
	Reload(ctx context.Context) error
	GetSchema() map[string]*Table
}

//Subscriber will get notified when the schema has been updated
type Subscriber interface {
	SchemaUpdated(gtid string, ddl string, timestamp int64) error
}

var _ Subscriber = (*Tracker)(nil)

// Tracker implements Subscriber and persists versions into the ddb
type Tracker struct {
	engine trackerEngine
}

// NewTracker creates a Tracker, needs an Open SchemaEngine (which implements the trackerEngine interface)
func NewTracker(engine trackerEngine) *Tracker {
	return &Tracker{engine: engine}
}

// SchemaUpdated is called by a vstream when it encounters a DDL
func (t *Tracker) SchemaUpdated(gtid string, ddl string, timestamp int64) error {

	if gtid == "" || ddl == "" {
		return fmt.Errorf("got invalid gtid or ddl in SchemaUpdated")
	}
	ctx := context.Background()
	t.engine.Reload(ctx)
	tables := t.engine.GetSchema()
	dbSchema := &binlogdatapb.MinimalSchema{
		Tables: []*binlogdatapb.MinimalTable{},
	}
	for name, table := range tables {
		t := &binlogdatapb.MinimalTable{
			Name:   name,
			Fields: table.Fields,
		}
		pks := make([]int64, 0)
		for _, pk := range table.PKColumns {
			pks = append(pks, int64(pk))
		}
		t.PKColumns = pks
		dbSchema.Tables = append(dbSchema.Tables, t)
	}
	blob, _ := proto.Marshal(dbSchema)

	conn, err := t.engine.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	_, err = conn.Exec(ctx, createSchemaTrackingTable, 1, false)
	if err != nil {
		return err
	}
	log.Infof("Inserting version for position %s: %s : %+v", gtid, ddl, len(dbSchema.Tables))
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
