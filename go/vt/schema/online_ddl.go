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

var (
	MigrationBasePath = "schema-migration"
)

func MigrationKeyspacePath(keyspace string) string {
	return fmt.Sprintf("%s/%s", MigrationBasePath, keyspace)
}

// OnlineDDL encapsulates the relevant information in an online schema change request
type OnlineDDL struct {
	Keyspace    string `json:"keyspace,omitempty"`
	Table       string `json:"table,omitempty"`
	SQL         string `json:"sql,omitempty"`
	UUID        string `json:"uuid,omitempty"`
	Online      bool   `json:"online,omitempty"`
	RequestTime int64  `json:"time_created,omitempty"`
}

// NewOnlineDDL creates a schema change request with self generated UUID and RequestTime
func NewOnlineDDL(keyspace string, table string, sql string) (*OnlineDDL, error) {
	uuid, err := CreateUUID()
	if err != nil {
		return nil, err
	}
	return &OnlineDDL{
		Keyspace:    keyspace,
		Table:       table,
		SQL:         sql,
		UUID:        uuid,
		Online:      true,
		RequestTime: time.Now().UnixNano(),
	}, nil
}

// MigrationRequestPath returns the relative path in topo where this schema migration request is stored
func (change *OnlineDDL) MigrationRequestPath() string {
	return fmt.Sprintf("%s/%s/request", MigrationKeyspacePath(change.Keyspace), change.UUID)
}

// WriteTopoOnlineDDL writes an online schema change request in global topo
func WriteTopoOnlineDDL(ctx context.Context, ts *topo.Server, onlineDDL *OnlineDDL) error {
	if onlineDDL == nil {
		return fmt.Errorf("online-schema-change nil change received")
	}
	if onlineDDL.UUID == "" {
		return fmt.Errorf("online-schema-change UUID not found; keyspace=%s, sql=%s", onlineDDL.Keyspace, onlineDDL.SQL)
	}
	bytes, err := json.Marshal(onlineDDL)
	if err != nil {
		return fmt.Errorf("online-schema-change marshall error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return fmt.Errorf("online-schema-change ConnForCell error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	_, err = conn.Create(ctx, onlineDDL.MigrationRequestPath(), bytes)
	if err != nil {
		return fmt.Errorf("online-schema-change topo create error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
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
