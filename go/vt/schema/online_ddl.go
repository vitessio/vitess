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
	"vitess.io/vitess/go/vt/vterrors"
)

var (
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

// when validateWalk returns true, then the child nodes are also visited
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
	Keyspace         string          `json:"keyspace,omitempty"`
	Table            string          `json:"table,omitempty"`
	Schema           string          `json:"schema,omitempty"`
	SQL              string          `json:"sql,omitempty"`
	UUID             string          `json:"uuid,omitempty"`
	Strategy         DDLStrategy     `json:"strategy,omitempty"`
	Options          string          `json:"options,omitempty"`
	RequestTime      int64           `json:"time_created,omitempty"`
	MigrationContext string          `json:"context,omitempty"`
	Status           OnlineDDLStatus `json:"status,omitempty"`
	TabletAlias      string          `json:"tablet,omitempty"`
	Retries          int64           `json:"retries,omitempty"`
	ReadyToComplete  int64           `json:"ready_to_complete,omitempty"`
}

// FromJSON creates an OnlineDDL from json
func FromJSON(bytes []byte) (*OnlineDDL, error) {
	onlineDDL := &OnlineDDL{}
	err := json.Unmarshal(bytes, onlineDDL)
	return onlineDDL, err
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
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "foreign key constraints are not supported in online DDL, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/")
		case ErrRenameTableFound:
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "ALTER TABLE ... RENAME is not supported in online DDL")
		}
	}
	return nil
}

// NewOnlineDDLs takes a single DDL statement, normalizes it (potentially break down into multiple statements), and generates one or more OnlineDDL instances, one for each normalized statement
func NewOnlineDDLs(keyspace string, sql string, ddlStmt sqlparser.DDLStatement, ddlStrategySetting *DDLStrategySetting, migrationContext string, providedUUID string) (onlineDDLs [](*OnlineDDL), err error) {
	appendOnlineDDL := func(tableName string, ddlStmt sqlparser.DDLStatement) error {
		if err := onlineDDLStatementSanity(sql, ddlStmt); err != nil {
			return err
		}
		onlineDDL, err := NewOnlineDDL(keyspace, tableName, sqlparser.String(ddlStmt), ddlStrategySetting, migrationContext, providedUUID)
		if err != nil {
			return err
		}
		if len(onlineDDLs) > 0 && providedUUID != "" {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "UUID %s provided but multiple DDLs generated", providedUUID)
		}
		onlineDDLs = append(onlineDDLs, onlineDDL)
		return nil
	}
	switch ddlStmt := ddlStmt.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable, *sqlparser.CreateView, *sqlparser.AlterView:
		if err := appendOnlineDDL(ddlStmt.GetTable().Name.String(), ddlStmt); err != nil {
			return nil, err
		}
	case *sqlparser.DropTable, *sqlparser.DropView:
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
func NewOnlineDDL(keyspace string, table string, sql string, ddlStrategySetting *DDLStrategySetting, migrationContext string, providedUUID string) (onlineDDL *OnlineDDL, err error) {
	if ddlStrategySetting == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "NewOnlineDDL: found nil DDLStrategySetting")
	}
	var onlineDDLUUID string
	if providedUUID != "" {
		if !IsOnlineDDLUUID(providedUUID) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "NewOnlineDDL: not a valid UUID: %s", providedUUID)
		}
		onlineDDLUUID = providedUUID
	} else {
		// No explicit UUID provided. We generate our own
		onlineDDLUUID, err = CreateOnlineDDLUUID()
		if err != nil {
			return nil, err
		}
	}

	{
		encodeDirective := func(directive string) string {
			return strconv.Quote(hex.EncodeToString([]byte(directive)))
		}
		comments := sqlparser.Comments{
			fmt.Sprintf(`/*vt+ uuid=%s context=%s table=%s strategy=%s options=%s */`,
				encodeDirective(onlineDDLUUID),
				encodeDirective(migrationContext),
				encodeDirective(table),
				encodeDirective(string(ddlStrategySetting.Strategy)),
				encodeDirective(ddlStrategySetting.Options),
			)}
		if uuid, err := legacyParseRevertUUID(sql); err == nil {
			sql = fmt.Sprintf("revert vitess_migration '%s'", uuid)
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
		Keyspace:         keyspace,
		Table:            table,
		SQL:              sql,
		UUID:             onlineDDLUUID,
		Strategy:         ddlStrategySetting.Strategy,
		Options:          ddlStrategySetting.Options,
		RequestTime:      time.Now().UnixNano(),
		MigrationContext: migrationContext,
		Status:           OnlineDDLStatusRequested,
	}, nil
}

func formatWithoutComments(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if _, ok := node.(*sqlparser.ParsedComments); ok {
		return
	}
	node.Format(buf)
}

// OnlineDDLFromCommentedStatement creates a schema  instance based on a commented query. The query is expected
// to be commented as e.g. `CREATE /*vt+ uuid=... context=... table=... strategy=... options=... */ TABLE ...`
func OnlineDDLFromCommentedStatement(stmt sqlparser.Statement) (onlineDDL *OnlineDDL, err error) {
	var comments *sqlparser.ParsedComments
	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		comments = stmt.GetParsedComments()
	case *sqlparser.RevertMigration:
		comments = stmt.Comments
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported statement for Online DDL: %v", sqlparser.String(stmt))
	}

	if comments.Length() == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no comments found in statement: %v", sqlparser.String(stmt))
	}

	directives := comments.Directives()
	decodeDirective := func(name string) (string, error) {
		value, ok := directives.GetString(name, "")
		if !ok {
			return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no value found for comment directive %s", name)
		}
		b, err := hex.DecodeString(value)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	buf := sqlparser.NewTrackedBuffer(formatWithoutComments)
	stmt.Format(buf)

	onlineDDL = &OnlineDDL{
		SQL: buf.String(),
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
	if onlineDDL.MigrationContext, err = decodeDirective("context"); err != nil {
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

// ToJSON exports this onlineDDL to JSON
func (onlineDDL *OnlineDDL) ToJSON() ([]byte, error) {
	return json.Marshal(onlineDDL)
}

// sqlWithoutComments returns the SQL statement without comment directives. Useful for tests
func (onlineDDL *OnlineDDL) sqlWithoutComments() (sql string, err error) {
	sql = onlineDDL.SQL
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// query validation and rebuilding
		if _, err := legacyParseRevertUUID(sql); err == nil {
			// This is a revert statement of the form "revert <uuid>". We allow this for now. Future work will
			// make sure the statement is a valid, parseable "revert vitess_migration '<uuid>'", but we must
			// be backwards compatible for now.
			return sql, nil
		}
		// otherwise the statement should have been parseable!
		return "", err
	}

	switch stmt := stmt.(type) {
	case sqlparser.DDLStatement:
		stmt.SetComments(nil)
	case *sqlparser.RevertMigration:
		stmt.SetComments(nil)
	}
	sql = sqlparser.String(stmt)
	return sql, nil
}

// GetAction extracts the DDL action type from the online DDL statement
func (onlineDDL *OnlineDDL) GetAction() (action sqlparser.DDLAction, err error) {
	if _, err := onlineDDL.GetRevertUUID(); err == nil {
		return sqlparser.RevertDDLAction, nil
	}

	_, action, err = ParseOnlineDDLStatement(onlineDDL.SQL)
	return action, err
}

// IsView returns 'true' when the statement affects a VIEW
func (onlineDDL *OnlineDDL) IsView() bool {
	stmt, _, err := ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return false
	}
	switch stmt.(type) {
	case *sqlparser.CreateView, *sqlparser.DropView, *sqlparser.AlterView:
		return true
	}
	return false
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

// GetGCUUID gets this OnlineDDL UUID in GC UUID format
func (onlineDDL *OnlineDDL) GetGCUUID() string {
	return OnlineDDLToGCUUID(onlineDDL.UUID)
}

// CreateOnlineDDLUUID creates a UUID in OnlineDDL format, e.g.:
// a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a
func CreateOnlineDDLUUID() (string, error) {
	return CreateUUIDWithDelimiter("_")
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
