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

package tabletserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

type schemaEngine interface {
	Reload(ctx context.Context) error
	GetSchema() map[string]*schema.Table
}

type schemaTracker struct {
	engine schemaEngine
	conn   *connpool.Pool
}

func (s *schemaTracker) SchemaUpdated(gtid string, ddl string, timestamp int64) {
	ctx := context.Background()

	s.engine.Reload(ctx)
	//newSchema := s.engine.GetSchema()
	//conn, err := s.conn.Get(ctx)
	//if err != nil {
	//	panic(err)
	//}

	//for name, table := range newSchema {
	//	
	//	//td := tabletmanagerdatapb.TableDefinition{
	//	//	Name:                 name,
	//	//	Fields:               table.Fields,
	//	//}
	//	
	//}
}

var _ SchemaSubscriber = (*schemaTracker)(nil)

// go binary encoder
func ToGOB64(m map[string]*schema.Table) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return b.Bytes()
}

// go binary decoder
func FromGOB64(bites []byte) map[string]*schema.Table {
	m := map[string]*schema.Table{}
	b := bytes.Buffer{}
	b.Write(bites)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return m
}
