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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	migrationBasePath                 = "schema-migration"
	onlineDdlUUIDRegexp               = regexp.MustCompile(`^[0-f]{8}_[0-f]{4}_[0-f]{4}_[0-f]{4}_[0-f]{12}$`)
	onlineDDLGeneratedTableNameRegexp = regexp.MustCompile(`^_[0-f]{8}_[0-f]{4}_[0-f]{4}_[0-f]{4}_[0-f]{12}_([0-9]{14})_(gho|ghc|del|new|vrepl)$`)
	ptOSCGeneratedTableNameRegexp     = regexp.MustCompile(`^_.*_old$`)
)

var (
	// ErrDirectDDLDisabled is returned when direct DDL is disabled, and a user attempts to run a DDL statement
	ErrDirectDDLDisabled = errors.New("direct DDL is disabled")
	// ErrOnlineDDLDisabled is returned when online DDL is disabled, and a user attempts to run an online DDL operation (submit, review, control)
	ErrOnlineDDLDisabled = errors.New("online DDL is disabled")
	// ErrForeignKeyFound indicates any finding of FOREIGN KEY clause in a DDL statement
	ErrForeignKeyFound = errors.New("Foreign key found")
	// ErrRenameTableFound indicates finding of ALTER TABLE...RENAME in ddl statement
	ErrRenameTableFound = errors.New("RENAME clause found")
)

const (
	SchemaMigrationsTableName = "schema_migrations"
	RevertActionStr           = "revert"
)

func validateWalk(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable,
		*sqlparser.TableSpec, *sqlparser.AddConstraintDefinition, *sqlparser.ConstraintDefinition:
		return true, nil
	case *sqlparser.ForeignKeyDefinition:
		return false, ErrForeignKeyFound
	case *sqlparser.RenameTableName:
		return false, ErrRenameTableFound
	}
	return false, nil
}

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
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "ReadTopo Get %s error: %s", entryPath, err.Error())
	}
	onlineDDL, err := FromJSON(bytes)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "ReadTopo unmarshal %s error: %s", entryPath, err.Error())
	}
	return onlineDDL, nil
}

// ParseOnlineDDLStatement parses the given SQL into a statement and returns the action type of the DDL statement, or error
// if the statement is not a DDL
func ParseOnlineDDLStatement(sql string) (ddlStmt sqlparser.DDLStatement, action sqlparser.DDLAction, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error parsing statement: SQL=%s, error=%+v", sql, err)
	}
	switch ddlStmt := stmt.(type) {
	case sqlparser.DDLStatement:
		return ddlStmt, ddlStmt.GetAction(), nil
	}
	return ddlStmt, action, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported query type: %s", sql)
}

func onlineDDLStatementSanity(sql string, ddlStmt sqlparser.DDLStatement) error {
	// SQL statement sanity checks:
	if !ddlStmt.IsFullyParsed() {
		if _, err := sqlparser.ParseStrictDDL(sql); err != nil {
			// More information about the reason why the statement is not fully parsed:
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "%v", err)
		}
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "cannot parse statement: %v", sql)
	}

	if err := sqlparser.Walk(validateWalk, ddlStmt); err != nil {
		switch err {
		case ErrForeignKeyFound:
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "foreign key constraints are not supported in online DDL, see https://code.openark.org/blog/mysql/the-problem-with-mysql-foreign-key-constraints-in-online-schema-changes")
		case ErrRenameTableFound:
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "ALTER TABLE ... RENAME is not supported in online DDL")
		}
	}
	return nil
}

// NewOnlineDDLs takes a single DDL statement, normalizes it (potentially break down into multiple statements), and generates one or more OnlineDDL instances, one for each normalized statement
func NewOnlineDDLs(keyspace string, sql string, ddlStmt sqlparser.DDLStatement, ddlStrategySetting *DDLStrategySetting, requestContext string) (onlineDDLs [](*OnlineDDL), err error) {
	appendOnlineDDL := func(tableName string, ddlStmt sqlparser.DDLStatement) error {
		if err := onlineDDLStatementSanity(sql, ddlStmt); err != nil {
			return err
		}
		onlineDDL, err := NewOnlineDDL(keyspace, tableName, sqlparser.String(ddlStmt), ddlStrategySetting, requestContext)
		if err != nil {
			return err
		}
		onlineDDLs = append(onlineDDLs, onlineDDL)
		return nil
	}
	switch ddlStmt := ddlStmt.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable:
		if err := appendOnlineDDL(ddlStmt.GetTable().Name.String(), ddlStmt); err != nil {
			return nil, err
		}
	case *sqlparser.DropTable:
		tables := ddlStmt.GetFromTables()
		for _, table := range tables {
			ddlStmt.SetFromTables([]sqlparser.TableName{table})
			if err := appendOnlineDDL(table.Name.String(), ddlStmt); err != nil {
				return nil, err
			}
		}
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported statement for Online DDL: %v", sqlparser.String(ddlStmt))
	}

	return onlineDDLs, nil
}

// NewOnlineDDL creates a schema change request with self generated UUID and RequestTime
func NewOnlineDDL(keyspace string, table string, sql string, ddlStrategySetting *DDLStrategySetting, requestContext string) (*OnlineDDL, error) {
	if ddlStrategySetting == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "NewOnlineDDL: found nil DDLStrategySetting")
	}
	u, err := CreateOnlineDDLUUID()
	if err != nil {
		return nil, err
	}

	{
		encodeDirective := func(directive string) string {
			return strconv.Quote(hex.EncodeToString([]byte(directive)))
		}
		var comments sqlparser.Comments
		if ddlStrategySetting.IsSkipTopo() {
			comments = sqlparser.Comments{
				fmt.Sprintf(`/*vt+ uuid=%s context=%s table=%s strategy=%s options=%s */`,
					encodeDirective(u),
					encodeDirective(requestContext),
					encodeDirective(table),
					encodeDirective(string(ddlStrategySetting.Strategy)),
					encodeDirective(ddlStrategySetting.Options),
				)}
			if uuid, err := legacyParseRevertUUID(sql); err == nil {
				sql = fmt.Sprintf("revert vitess_migration '%s'", uuid)
			}
		}

		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			isLegacyRevertStatement := false
			// query validation and rebuilding
			if _, err := legacyParseRevertUUID(sql); err == nil {
				// This is a revert statement of the form "revert <uuid>". We allow this for now. Future work will
				// make sure the statement is a valid, parseable "revert vitess_migration '<uuid>'", but we must
				// be backwards compatible for now.
				isLegacyRevertStatement = true
			}
			if !isLegacyRevertStatement {
				// otherwise the statement should have been parseable!
				return nil, err
			}
		} else {
			switch stmt := stmt.(type) {
			case sqlparser.DDLStatement:
				stmt.SetComments(comments)
			case *sqlparser.RevertMigration:
				stmt.SetComments(comments)
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unsupported statement for Online DDL: %v", sqlparser.String(stmt))
			}
			sql = sqlparser.String(stmt)
		}
	}

	return &OnlineDDL{
		Keyspace:       keyspace,
		Table:          table,
		SQL:            sql,
		UUID:           u,
		Strategy:       ddlStrategySetting.Strategy,
		Options:        ddlStrategySetting.Options,
		RequestTime:    time.Now().UnixNano(),
		RequestContext: requestContext,
		Status:         OnlineDDLStatusRequested,
	}, nil
}

// OnlineDDLFromCommentedStatement creates a schema  instance based on a commented query. The query is expected
// to be commented as e.g. `CREATE /*vt+ uuid=... context=... table=... strategy=... options=... */ TABLE ...`
func OnlineDDLFromCommentedStatement(stmt sqlparser.Statement) (onlineDDL *OnlineDDL, err error) {
	var sql string
	var comments sqlparser.Comments
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		comments = stmt.GetComments()
		// We want sql to be clean of comments, so we temporarily remove, then restore the comments
		stmt.SetComments(sqlparser.Comments{})
		sql = sqlparser.String(stmt)
		stmt.SetComments(comments)
	case *sqlparser.RevertMigration:
		comments = stmt.Comments[:]
		// We want sql to be clean of comments, so we temporarily remove, then restore the comments
		stmt.SetComments(sqlparser.Comments{})
		sql = sqlparser.String(stmt)
		stmt.SetComments(comments)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported statement for Online DDL: %v", sqlparser.String(stmt))
	}
	if len(comments) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no comments found in statement: %v", sqlparser.String(stmt))
	}
	directives := sqlparser.ExtractCommentDirectives(comments)

	decodeDirective := func(name string) (string, error) {
		value := fmt.Sprintf("%s", directives[name])
		if value == "" {
			return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no value found for comment directive %s", name)
		}
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return "", err
		}
		b, err := hex.DecodeString(unquoted)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	onlineDDL = &OnlineDDL{
		SQL: sql,
	}
	if onlineDDL.UUID, err = decodeDirective("uuid"); err != nil {
		return nil, err
	}
	if !IsOnlineDDLUUID(onlineDDL.UUID) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid UUID read from statement %s", sqlparser.String(stmt))
	}
	if onlineDDL.Table, err = decodeDirective("table"); err != nil {
		return nil, err
	}
	if strategy, err := decodeDirective("strategy"); err == nil {
		onlineDDL.Strategy = DDLStrategy(strategy)
	} else {
		return nil, err
	}
	if options, err := decodeDirective("options"); err == nil {
		onlineDDL.Options = options
	} else {
		return nil, err
	}
	if onlineDDL.RequestContext, err = decodeDirective("context"); err != nil {
		return nil, err
	}
	return onlineDDL, nil
}

// StrategySetting returns the ddl strategy setting associated with this online DDL
func (onlineDDL *OnlineDDL) StrategySetting() *DDLStrategySetting {
	return NewDDLStrategySetting(onlineDDL.Strategy, onlineDDL.Options)
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
	if _, err := onlineDDL.GetRevertUUID(); err == nil {
		return sqlparser.RevertDDLAction, nil
	}

	_, action, err = ParseOnlineDDLStatement(onlineDDL.SQL)
	return action, err
}

// GetActionStr returns a string representation of the DDL action
func (onlineDDL *OnlineDDL) GetActionStr() (action sqlparser.DDLAction, actionStr string, err error) {
	action, err = onlineDDL.GetAction()
	if err != nil {
		return action, actionStr, err
	}
	switch action {
	case sqlparser.RevertDDLAction:
		return action, RevertActionStr, nil
	case sqlparser.CreateDDLAction:
		return action, sqlparser.CreateStr, nil
	case sqlparser.AlterDDLAction:
		return action, sqlparser.AlterStr, nil
	case sqlparser.DropDDLAction:
		return action, sqlparser.DropStr, nil
	}
	return action, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported online DDL action. SQL=%s", onlineDDL.SQL)
}

// GetRevertUUID works when this migration is a revert for another migration. It returns the UUID
// fo the reverted migration.
// The function returns error when this is not a revert migration.
func (onlineDDL *OnlineDDL) GetRevertUUID() (uuid string, err error) {
	if uuid, err := legacyParseRevertUUID(onlineDDL.SQL); err == nil {
		return uuid, nil
	}
	if stmt, err := sqlparser.Parse(onlineDDL.SQL); err == nil {
		if revert, ok := stmt.(*sqlparser.RevertMigration); ok {
			return revert.UUID, nil
		}
	}
	return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "not a Revert DDL: '%s'", onlineDDL.SQL)
}

// ToString returns a simple string representation of this instance
func (onlineDDL *OnlineDDL) ToString() string {
	return fmt.Sprintf("OnlineDDL: keyspace=%s, table=%s, sql=%s", onlineDDL.Keyspace, onlineDDL.Table, onlineDDL.SQL)
}

// WriteTopo writes this online DDL to given topo connection, based on basePath and and this DDL's UUID
func (onlineDDL *OnlineDDL) WriteTopo(ctx context.Context, conn topo.Conn, basePath string) error {
	if onlineDDL.UUID == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "onlineDDL UUID not found; keyspace=%s, sql=%s", onlineDDL.Keyspace, onlineDDL.SQL)
	}
	bytes, err := onlineDDL.ToJSON()
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "onlineDDL marshall error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	_, err = conn.Create(ctx, fmt.Sprintf("%s/%s", basePath, onlineDDL.UUID), bytes)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "onlineDDL topo create error:%s, keyspace=%s, sql=%s", err.Error(), onlineDDL.Keyspace, onlineDDL.SQL)
	}
	return nil
}

// GetGCUUID gets this OnlineDDL UUID in GC UUID format
func (onlineDDL *OnlineDDL) GetGCUUID() string {
	return OnlineDDLToGCUUID(onlineDDL.UUID)
}

// CreateOnlineDDLUUID creates a UUID in OnlineDDL format, e.g.:
// a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a
func CreateOnlineDDLUUID() (string, error) {
	return createUUID("_")
}

// IsOnlineDDLUUID answers 'true' when the given string is an online-ddl UUID, e.g.:
// a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a
func IsOnlineDDLUUID(uuid string) bool {
	return onlineDdlUUIDRegexp.MatchString(uuid)
}

// OnlineDDLToGCUUID converts a UUID in online-ddl format to GC-table format
func OnlineDDLToGCUUID(uuid string) string {
	return strings.Replace(uuid, "_", "", -1)
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
