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

package schemamanager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/topo"
)

// OnlineSchemaChange encapsulates the relevant information in an online schema change request
type OnlineSchemaChange struct {
	Keyspace string `json:"keyspace,omitempty"`
	Table    string `json:"table,omitempty"`
	SQL      string `json:"sql,omitempty"`
	UUID     string `json:"uuid,omitempty"`
	Online   bool   `json:"online,omitempty"`
}

// WriteTopoOnlineSchemaChange writes an online schema change request in global topo
func WriteTopoOnlineSchemaChange(ctx context.Context, ts *topo.Server, keyspace string, table string, sql string, uuid string) error {
	if uuid == "" {
		return fmt.Errorf("online-schema-change UUID not found; keyspace=%s, sql=%s", keyspace, sql)
	}
	onlineSchemaChange := &OnlineSchemaChange{
		Keyspace: keyspace,
		Table:    "",
		SQL:      sql,
		UUID:     uuid,
		Online:   true,
	}
	bytes, err := json.Marshal(onlineSchemaChange)
	if err != nil {
		return fmt.Errorf("online-schema-change marshall error:%s, keyspace=%s, sql=%s", err.Error(), keyspace, sql)
	}
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return fmt.Errorf("online-schema-change ConnForCell error:%s, keyspace=%s, sql=%s", err.Error(), keyspace, sql)
	}
	topoMigrationPath := fmt.Sprintf("schema-migration/%s", onlineSchemaChange.UUID)
	_, err = conn.Create(ctx, topoMigrationPath, bytes)
	if err != nil {
		return fmt.Errorf("online-schema-change topo create error:%s, keyspace=%s, sql=%s", err.Error(), keyspace, sql)
	}
	return nil
}

// CreateUUID creates a globally unique ID, returned as string
func CreateUUID() (string, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
