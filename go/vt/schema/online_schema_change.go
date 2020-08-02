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
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/topo"
)

// OnlineSchemaChange encapsulates the relevant information in an online schema change request
type OnlineSchemaChange struct {
	Keyspace    string `json:"keyspace,omitempty"`
	Table       string `json:"table,omitempty"`
	SQL         string `json:"sql,omitempty"`
	UUID        string `json:"uuid,omitempty"`
	Online      bool   `json:"online,omitempty"`
	RequestTime int64  `json:"time_created,omitempty"`
}

func NewOnlineSchemaChange(keyspace string, table string, sql string) (*OnlineSchemaChange, error) {
	uuid, err := CreateUUID()
	if err != nil {
		return nil, err
	}
	return &OnlineSchemaChange{
		Keyspace:    keyspace,
		Table:       table,
		SQL:         sql,
		UUID:        uuid,
		Online:      true,
		RequestTime: time.Now().UnixNano(),
	}, nil
}

// WriteTopoOnlineSchemaChange writes an online schema change request in global topo
func WriteTopoOnlineSchemaChange(ctx context.Context, ts *topo.Server, change *OnlineSchemaChange) error {
	if change == nil {
		return fmt.Errorf("online-schema-change nil change received")
	}
	if change.UUID == "" {
		return fmt.Errorf("online-schema-change UUID not found; keyspace=%s, sql=%s", change.Keyspace, change.SQL)
	}
	bytes, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("online-schema-change marshall error:%s, keyspace=%s, sql=%s", err.Error(), change.Keyspace, change.SQL)
	}
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return fmt.Errorf("online-schema-change ConnForCell error:%s, keyspace=%s, sql=%s", err.Error(), change.Keyspace, change.SQL)
	}
	topoMigrationPath := fmt.Sprintf("schema-migration/%s", change.UUID)
	_, err = conn.Create(ctx, topoMigrationPath, bytes)
	if err != nil {
		return fmt.Errorf("online-schema-change topo create error:%s, keyspace=%s, sql=%s", err.Error(), change.Keyspace, change.SQL)
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
