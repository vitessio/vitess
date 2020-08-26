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
	"strings"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
)

var (
	migrationBasePath = "schema-migration"
)

// MigrationBasePath is the root for all schema migration entries
func MigrationBasePath() string {
	return migrationBasePath
}

// MigrationRequestsPath is the base path for all newly received schema migration requests.
// such requests need to be investigates/reviewed, and to be assigned to all shards
func MigrationRequestsPath() string {
	return fmt.Sprintf("%s/requests", MigrationBasePath())
}

// MigrationQueuedPath is the base path for schema migrations that have been reviewed and
// queued for execution. Kept for historical reference
func MigrationQueuedPath() string {
	return fmt.Sprintf("%s/queued", MigrationBasePath())
}

// MigrationJobsKeyspacePath is the base path for a tablet job, by keyspace
func MigrationJobsKeyspacePath(keyspace string) string {
	return fmt.Sprintf("%s/jobs/%s", MigrationBasePath(), keyspace)
}

// MigrationJobsKeyspaceShardPath is the base path for a tablet job, by keyspace and shard
func MigrationJobsKeyspaceShardPath(keyspace, shard string) string {
	return fmt.Sprintf("%s/%s", MigrationJobsKeyspacePath(keyspace), shard)
}

// OnlineDDLStatus is an indicator to a online DDL status
type OnlineDDLStatus string

const (
	OnlineDDLStatusRequested OnlineDDLStatus = "requested"
	OnlineDDLStatusCancelled OnlineDDLStatus = "cancelled"
	OnlineDDLStatusQueued    OnlineDDLStatus = "queued"
	OnlineDDLStatusReady     OnlineDDLStatus = "ready"
	OnlineDDLStatusRunning   OnlineDDLStatus = "running"
	OnlineDDLStatusComplete  OnlineDDLStatus = "complete"
	OnlineDDLStatusFailed    OnlineDDLStatus = "failed"
)

const (
	DDLStrategyNormal sqlparser.DDLStrategy = ""
	DDLStrategyGhost  sqlparser.DDLStrategy = "gh-ost"
	DDLStrategyPTOSC  sqlparser.DDLStrategy = "pt-osc"
)

// OnlineDDL encapsulates the relevant information in an online schema change request
type OnlineDDL struct {
	Keyspace    string                `json:"keyspace,omitempty"`
	Table       string                `json:"table,omitempty"`
	Schema      string                `json:"schema,omitempty"`
	SQL         string                `json:"sql,omitempty"`
	UUID        string                `json:"uuid,omitempty"`
	Strategy    sqlparser.DDLStrategy `json:"strategy,omitempty"`
	Options     string                `json:"options,omitempty"`
	RequestTime int64                 `json:"time_created,omitempty"`
	Status      OnlineDDLStatus       `json:"status,omitempty"`
}

// FromJSON creates an OnlineDDL from json
func FromJSON(bytes []byte) (*OnlineDDL, error) {
	onlineDDL := &OnlineDDL{}
	err := json.Unmarshal(bytes, onlineDDL)
	return onlineDDL, err
}

// ReadTopo reads a OnlineDDL object from given topo connection
func ReadTopo(ctx context.Context, conn topo.Conn, entryPath string) (*OnlineDDL, error) {
	bytes, _, err := conn.Get(ctx, entryPath)
	if err != nil {
		return nil, fmt.Errorf("ReadTopo Get %s error: %s", entryPath, err.Error())
	}
	onlineDDL, err := FromJSON(bytes)
	if err != nil {
		return nil, fmt.Errorf("ReadTopo unmarshal %s error: %s", entryPath, err.Error())
	}
	return onlineDDL, nil
}

// NewOnlineDDL creates a schema change request with self generated UUID and RequestTime
func NewOnlineDDL(keyspace string, table string, sql string, strategy sqlparser.DDLStrategy, options string) (*OnlineDDL, error) {
	uuid, err := CreateUUID()
	if err != nil {
		return nil, err
	}
	return &OnlineDDL{
		Keyspace:    keyspace,
		Table:       table,
		SQL:         sql,
		UUID:        uuid,
		Strategy:    strategy,
		Options:     options,
		RequestTime: time.Now().UnixNano(),
		Status:      OnlineDDLStatusRequested,
	}, nil
}

// RequestTimeSeconds converts request time to seconds (losing nano precision)
func (onlineDDL *OnlineDDL) RequestTimeSeconds() int64 {
	return onlineDDL.RequestTime / int64(time.Second)
}

// JobsKeyspaceShardPath returns job/<keyspace>/<shard>/<uuid>
func (onlineDDL *OnlineDDL) JobsKeyspaceShardPath(shard string) string {
	return MigrationJobsKeyspaceShardPath(onlineDDL.Keyspace, shard)
}

// ToJSON exports this onlineDDL to JSON
func (onlineDDL *OnlineDDL) ToJSON() ([]byte, error) {
	return json.Marshal(onlineDDL)
}

// WriteTopo writes this online DDL to given topo connection, based on basePath and and this DDL's UUID
func (onlineDDL *OnlineDDL) WriteTopo(ctx context.Context, conn topo.Conn, basePath string) error {
	if onlineDDL.UUID == "" {
		return fmt.Errorf("onlineDDL UUID not found; keyspace=%s, sql=%s", onlineDDL.Keyspace, onlineDDL.SQL)
	}
	bytes, err := onlineDDL.ToJSON()
	if err != nil {
		return fmt.Errorf("onlineDDL marshall error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	_, err = conn.Create(ctx, fmt.Sprintf("%s/%s", basePath, onlineDDL.UUID), bytes)
	if err != nil {
		return fmt.Errorf("onlineDDL topo create error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	return nil
}

// CreateUUID creates a globally unique ID, returned as string
// example result: 55d00cdc_e6ab_11ea_bfe6_0242ac1c000d
func CreateUUID() (string, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	uuid := u.String()
	uuid = strings.Replace(uuid, "-", "_", -1)
	return uuid, nil
}
