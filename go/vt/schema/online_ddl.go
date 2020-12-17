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
	"regexp"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
)

var (
	migrationBasePath                 = "schema-migration"
	onlineDdlUUIDRegexp               = regexp.MustCompile(`^[0-f]{8}_[0-f]{4}_[0-f]{4}_[0-f]{4}_[0-f]{12}$`)
	strategyParserRegexp              = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
	onlineDDLGeneratedTableNameRegexp = regexp.MustCompile(`^_[0-f]{8}_[0-f]{4}_[0-f]{4}_[0-f]{4}_[0-f]{12}_([0-9]{14})_(gho|ghc|del|new)$`)
	ptOSCGeneratedTableNameRegexp     = regexp.MustCompile(`^_.*_old$`)
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

// DDLStrategy suggests how an ALTER TABLE should run (e.g. "" for normal, "gh-ost" or "pt-osc")
type DDLStrategy string

const (
	// DDLStrategyNormal means not an online-ddl migration. Just a normal MySQL ALTER TABLE
	DDLStrategyNormal DDLStrategy = ""
	// DDLStrategyGhost requests gh-ost to run the migration
	DDLStrategyGhost DDLStrategy = "gh-ost"
	// DDLStrategyPTOSC requests pt-online-schema-change to run the migration
	DDLStrategyPTOSC DDLStrategy = "pt-osc"
)

// OnlineDDL encapsulates the relevant information in an online schema change request
type OnlineDDL struct {
	Keyspace       string          `json:"keyspace,omitempty"`
	Table          string          `json:"table,omitempty"`
	Schema         string          `json:"schema,omitempty"`
	SQL            string          `json:"sql,omitempty"`
	UUID           string          `json:"uuid,omitempty"`
	Strategy       DDLStrategy     `json:"strategy,omitempty"`
	Options        string          `json:"options,omitempty"`
	RequestTime    int64           `json:"time_created,omitempty"`
	RequestContext string          `json:"context,omitempty"`
	Status         OnlineDDLStatus `json:"status,omitempty"`
	TabletAlias    string          `json:"tablet,omitempty"`
	Retries        int64           `json:"retries,omitempty"`
}

// ParseDDLStrategy validates the given ddl_strategy variable value , and parses the strategy and options parts.
func ParseDDLStrategy(strategyVariable string) (strategy DDLStrategy, options string, err error) {
	strategyName := strategyVariable
	if submatch := strategyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		options = submatch[2]
	}

	switch strategy = DDLStrategy(strategyName); strategy {
	case DDLStrategyGhost, DDLStrategyPTOSC, DDLStrategyNormal:
		return strategy, options, nil
	default:
		return strategy, options, fmt.Errorf("Unknown online DDL strategy: '%v'", strategy)
	}
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

// getOnlineDDLAction parses the given SQL into a statement and returns the action type of the DDL statement, or error
// if the statement is not a DDL
func getOnlineDDLAction(sql string) (action sqlparser.DDLAction, ddlStmt sqlparser.DDLStatement, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return action, ddlStmt, fmt.Errorf("Error parsing statement: SQL=%s, error=%+v", sql, err)
	}
	switch ddlStmt := stmt.(type) {
	case sqlparser.DDLStatement:
		return ddlStmt.GetAction(), ddlStmt, nil
	}
	return action, ddlStmt, fmt.Errorf("Unsupported query type: %s", sql)
}

// NewOnlineDDL creates a schema change request with self generated UUID and RequestTime
func NewOnlineDDL(keyspace string, table string, sql string, strategy DDLStrategy, options string, requestContext string) (*OnlineDDL, error) {
	u, err := createUUID("_")
	if err != nil {
		return nil, err
	}
	return &OnlineDDL{
		Keyspace:       keyspace,
		Table:          table,
		SQL:            sql,
		UUID:           u,
		Strategy:       strategy,
		Options:        options,
		RequestTime:    time.Now().UnixNano(),
		RequestContext: requestContext,
		Status:         OnlineDDLStatusRequested,
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

// GetAction extracts the DDL action type from the online DDL statement
func (onlineDDL *OnlineDDL) GetAction() (action sqlparser.DDLAction, err error) {
	action, _, err = getOnlineDDLAction(onlineDDL.SQL)
	return action, err
}

// GetActionStr returns a string representation of the DDL action
func (onlineDDL *OnlineDDL) GetActionStr() (actionStr string, err error) {
	action, err := onlineDDL.GetAction()
	if err != nil {
		return actionStr, err
	}
	switch action {
	case sqlparser.CreateDDLAction:
		return sqlparser.CreateStr, nil
	case sqlparser.AlterDDLAction:
		return sqlparser.AlterStr, nil
	case sqlparser.DropDDLAction:
		return sqlparser.DropStr, nil
	}
	return "", fmt.Errorf("Unsupported online DDL action. SQL=%s", onlineDDL.SQL)
}

// ToString returns a simple string representation of this instance
func (onlineDDL *OnlineDDL) ToString() string {
	return fmt.Sprintf("OnlineDDL: keyspace=%s, table=%s, sql=%s", onlineDDL.Keyspace, onlineDDL.Table, onlineDDL.SQL)
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

// IsOnlineDDLUUID answers 'true' when the given string is an online-ddl UUID, e.g.:
// a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a
func IsOnlineDDLUUID(uuid string) bool {
	return onlineDdlUUIDRegexp.MatchString(uuid)
}

// IsOnlineDDLTableName answers 'true' when the given table name _appears to be_ a name
// generated by an online DDL operation; either the name determined by the online DDL Executor, or
// by pt-online-schema-change.
// There is no guarantee that the tables _was indeed_ generated by an online DDL flow.
func IsOnlineDDLTableName(tableName string) bool {
	if onlineDDLGeneratedTableNameRegexp.MatchString(tableName) {
		return true
	}
	if ptOSCGeneratedTableNameRegexp.MatchString(tableName) {
		return true
	}
	return false
}
