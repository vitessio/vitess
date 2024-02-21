/*
Copyright 2022 The Vitess Authors.

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

package vterrors

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// Errors added to the list of variables below must be added to the Errors slice a little below in this same file.
// This will enable the auto-documentation of error code in the website repository.

var (
	VT03001 = errorWithState("VT03001", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "aggregate functions take a single argument '%s'", "This aggregation function only takes a single argument.")
	VT03002 = errorWithState("VT03002", vtrpcpb.Code_INVALID_ARGUMENT, ForbidSchemaChange, "changing schema from '%s' to '%s' is not allowed", "This schema change is not allowed. You cannot change the keyspace of a table.")
	VT03003 = errorWithState("VT03003", vtrpcpb.Code_INVALID_ARGUMENT, UnknownTable, "unknown table '%s' in MULTI DELETE", "The specified table in this DELETE statement is unknown.")
	VT03004 = errorWithState("VT03004", vtrpcpb.Code_INVALID_ARGUMENT, NonUpdateableTable, "the target table %s of the DELETE is not updatable", "You cannot delete something that is not a real MySQL table.")
	VT03005 = errorWithState("VT03005", vtrpcpb.Code_INVALID_ARGUMENT, WrongGroupField, "cannot group on '%s'", "The planner does not allow grouping on certain field. For instance, aggregation function.")
	VT03006 = errorWithState("VT03006", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueCountOnRow, "column count does not match value count with the row", "The number of columns you want to insert do not match the number of columns of your SELECT query.")
	VT03007 = errorWithoutState("VT03007", vtrpcpb.Code_INVALID_ARGUMENT, "keyspace not specified", "You need to add a keyspace qualifier.")
	VT03008 = errorWithState("VT03008", vtrpcpb.Code_INVALID_ARGUMENT, CantUseOptionHere, "incorrect usage/placement of '%s'", "The given token is not usable in this situation. Please refer to the MySQL documentation to learn more about your token's syntax.")
	VT03009 = errorWithState("VT03009", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueForVar, "unexpected value type for '%s': %v", "You cannot assign this type to the given variable.")
	VT03010 = errorWithState("VT03010", vtrpcpb.Code_INVALID_ARGUMENT, IncorrectGlobalLocalVar, "variable '%s' is a read only variable", "You cannot set the given variable as it is a read-only variable.")
	VT03011 = errorWithoutState("VT03011", vtrpcpb.Code_INVALID_ARGUMENT, "invalid value type: %v", "The given value type is not accepted.")
	VT03012 = errorWithoutState("VT03012", vtrpcpb.Code_INVALID_ARGUMENT, "invalid syntax: %s", "The syntax is invalid. Please refer to the MySQL documentation for the proper syntax.")
	VT03013 = errorWithState("VT03013", vtrpcpb.Code_INVALID_ARGUMENT, NonUniqTable, "not unique table/alias: '%s'", "This table or alias name is already use. Please use another one that is unique.")
	VT03014 = errorWithState("VT03014", vtrpcpb.Code_INVALID_ARGUMENT, BadFieldError, "unknown column '%s' in '%s'", "The given column is unknown.")
	VT03015 = errorWithoutState("VT03015", vtrpcpb.Code_INVALID_ARGUMENT, "column has duplicate set values: '%v'", "Cannot assign multiple values to a column in an update statement.")
	VT03016 = errorWithoutState("VT03016", vtrpcpb.Code_INVALID_ARGUMENT, "unknown vindex column: '%s'", "The given column is unknown in the vindex table.")
	VT03017 = errorWithState("VT03017", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "where clause can only be of the type 'pos > <value>'", "This vstream where clause can only be a greater than filter.")
	VT03018 = errorWithoutState("VT03018", vtrpcpb.Code_INVALID_ARGUMENT, "NEXT used on a non-sequence table", "You cannot use the NEXT syntax on a table that is not a sequence table.")
	VT03019 = errorWithoutState("VT03019", vtrpcpb.Code_INVALID_ARGUMENT, "column %s not found", "The given column was not found or is not available.")
	VT03020 = errorWithoutState("VT03020", vtrpcpb.Code_INVALID_ARGUMENT, "column %s not found in subquery", "The given column was not found in the subquery.")
	VT03021 = errorWithoutState("VT03021", vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous column reference: %v", "The given column is ambiguous. You can use a table qualifier to make it unambiguous.")
	VT03022 = errorWithoutState("VT03022", vtrpcpb.Code_INVALID_ARGUMENT, "column %v not found in %v", "The given column cannot be found.")
	VT03023 = errorWithoutState("VT03023", vtrpcpb.Code_INVALID_ARGUMENT, "INSERT not supported when targeting a key range: %s", "When targeting a range of shards, Vitess does not know which shard to send the INSERT to.")
	VT03024 = errorWithoutState("VT03024", vtrpcpb.Code_INVALID_ARGUMENT, "'%s' user defined variable does not exists", "The query cannot be prepared using the user defined variable as it does not exists for this session.")
	VT03025 = errorWithState("VT03025", vtrpcpb.Code_INVALID_ARGUMENT, WrongArguments, "Incorrect arguments to %s", "The execute statement have wrong number of arguments")
	VT03026 = errorWithoutState("VT03024", vtrpcpb.Code_INVALID_ARGUMENT, "'%s' bind variable does not exists", "The query cannot be executed as missing the bind variable.")
	VT03027 = errorWithState("VT03027", vtrpcpb.Code_INVALID_ARGUMENT, BadNullError, "Column '%s' cannot be null", "The column cannot have null value.")
	VT03028 = errorWithState("VT03028", vtrpcpb.Code_INVALID_ARGUMENT, BadNullError, "Column '%s' cannot be null on row %d, col %d", "The column cannot have null value.")
	VT03029 = errorWithState("VT03029", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueCountOnRow, "column count does not match value count with the row for vindex '%s'", "The number of columns you want to insert do not match the number of columns of your SELECT query.")
	VT03030 = errorWithState("VT03030", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueCountOnRow, "lookup column count does not match value count with the row (columns, count): (%v, %d)", "The number of columns you want to insert do not match the number of columns of your SELECT query.")
	VT03031 = errorWithoutState("VT03031", vtrpcpb.Code_INVALID_ARGUMENT, "EXPLAIN is only supported for single keyspace", "EXPLAIN has to be sent down as a single query to the underlying MySQL, and this is not possible if it uses tables from multiple keyspaces")
	VT03032 = errorWithState("VT03031", vtrpcpb.Code_INVALID_ARGUMENT, NonUpdateableTable, "the target table %s of the UPDATE is not updatable", "You cannot update a table that is not a real MySQL table.")

	VT05001 = errorWithState("VT05001", vtrpcpb.Code_NOT_FOUND, DbDropExists, "cannot drop database '%s'; database does not exists", "The given database does not exist; Vitess cannot drop it.")
	VT05002 = errorWithState("VT05002", vtrpcpb.Code_NOT_FOUND, BadDb, "cannot alter database '%s'; unknown database", "The given database does not exist; Vitess cannot alter it.")
	VT05003 = errorWithState("VT05003", vtrpcpb.Code_NOT_FOUND, BadDb, "unknown database '%s' in vschema", "The given database does not exist in the VSchema.")
	VT05004 = errorWithState("VT05004", vtrpcpb.Code_NOT_FOUND, UnknownTable, "table '%s' does not exist", "The given table is unknown.")
	VT05005 = errorWithState("VT05005", vtrpcpb.Code_NOT_FOUND, NoSuchTable, "table '%s' does not exist in keyspace '%s'", "The given table does not exist in this keyspace.")
	VT05006 = errorWithState("VT05006", vtrpcpb.Code_NOT_FOUND, UnknownSystemVariable, "unknown system variable '%s'", "The given system variable is unknown.")
	VT05007 = errorWithoutState("VT05007", vtrpcpb.Code_NOT_FOUND, "no table info", "Table information is not available.")

	VT06001 = errorWithState("VT06001", vtrpcpb.Code_ALREADY_EXISTS, DbCreateExists, "cannot create database '%s'; database exists", "The given database name already exists.")

	VT07001 = errorWithState("VT07001", vtrpcpb.Code_PERMISSION_DENIED, KillDeniedError, "%s", "Kill statement is not allowed. More in docs about how to enable it and its limitations.")

	VT09001 = errorWithState("VT09001", vtrpcpb.Code_FAILED_PRECONDITION, RequiresPrimaryKey, PrimaryVindexNotSet, "the table does not have a primary vindex, the operation is impossible.")
	VT09002 = errorWithState("VT09002", vtrpcpb.Code_FAILED_PRECONDITION, InnodbReadOnly, "%s statement with a replica target", "This type of DML statement is not allowed on a replica target.")
	VT09003 = errorWithoutState("VT09003", vtrpcpb.Code_FAILED_PRECONDITION, "INSERT query does not have primary vindex column '%v' in the column list", "A vindex column is mandatory for the insert, please provide one.")
	VT09004 = errorWithoutState("VT09004", vtrpcpb.Code_FAILED_PRECONDITION, "INSERT should contain column list or the table should have authoritative columns in vschema", "You need to provide the list of columns you want to insert, or provide a VSchema with authoritative columns. If schema tracking is disabled you can enable it to automatically have authoritative columns.")
	VT09005 = errorWithState("VT09005", vtrpcpb.Code_FAILED_PRECONDITION, NoDB, "no database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)", "A database must be selected.")
	VT09006 = errorWithoutState("VT09006", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_MIGRATION works only on primary tablet", "VITESS_MIGRATION commands work only on primary tablets, you must send such commands to a primary tablet.")
	VT09007 = errorWithoutState("VT09007", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_THROTTLED_APPS works only on primary tablet", "VITESS_THROTTLED_APPS commands work only on primary tablet, you must send such commands to a primary tablet.")
	VT09008 = errorWithoutState("VT09008", vtrpcpb.Code_FAILED_PRECONDITION, "vexplain queries/all will actually run queries", "vexplain queries/all will actually run queries. `/*vt+ EXECUTE_DML_QUERIES */` must be set to run DML queries in vtexplain. Example: `vexplain /*vt+ EXECUTE_DML_QUERIES */ queries delete from t1`")
	VT09009 = errorWithoutState("VT09009", vtrpcpb.Code_FAILED_PRECONDITION, "stream is supported only for primary tablet type, current type: %v", "Stream is only supported for primary tablets, please use a stream on those tablets.")
	VT09010 = errorWithoutState("VT09010", vtrpcpb.Code_FAILED_PRECONDITION, "SHOW VITESS_THROTTLER STATUS works only on primary tablet", "SHOW VITESS_THROTTLER STATUS works only on primary tablet.")
	VT09011 = errorWithState("VT09011", vtrpcpb.Code_FAILED_PRECONDITION, UnknownStmtHandler, "Unknown prepared statement handler (%s) given to %s", "The prepared statement is not available")
	VT09012 = errorWithoutState("VT09012", vtrpcpb.Code_FAILED_PRECONDITION, "%s statement with %s tablet not allowed", "This type of statement is not allowed on the given tablet.")
	VT09013 = errorWithoutState("VT09013", vtrpcpb.Code_FAILED_PRECONDITION, "semi-sync plugins are not loaded", "Durability policy wants Vitess to use semi-sync, but the MySQL instances don't have the semi-sync plugin loaded.")
	VT09014 = errorWithoutState("VT09014", vtrpcpb.Code_FAILED_PRECONDITION, "vindex cannot be modified", "The vindex cannot be used as table in DML statement")
	VT09015 = errorWithoutState("VT09015", vtrpcpb.Code_FAILED_PRECONDITION, "schema tracking required", "This query cannot be planned without more information on the SQL schema. Please turn on schema tracking or add authoritative columns information to your VSchema.")
	VT09016 = errorWithState("VT09016", vtrpcpb.Code_FAILED_PRECONDITION, RowIsReferenced2, "Cannot delete or update a parent row: a foreign key constraint fails", "SET DEFAULT is not supported by InnoDB")
	VT09017 = errorWithoutState("VT09017", vtrpcpb.Code_FAILED_PRECONDITION, "%s", "Invalid syntax for the statement type.")
	VT09018 = errorWithoutState("VT09018", vtrpcpb.Code_FAILED_PRECONDITION, "%s", "Invalid syntax for the vindex function statement.")
	VT09019 = errorWithoutState("VT09019", vtrpcpb.Code_FAILED_PRECONDITION, "keyspace '%s' has cyclic foreign keys", "Vitess doesn't support cyclic foreign keys.")
	VT09020 = errorWithoutState("VT09020", vtrpcpb.Code_FAILED_PRECONDITION, "can not use multiple vindex hints for table %s", "Vitess does not allow using multiple vindex hints on the same table.")
	VT09021 = errorWithState("VT09021", vtrpcpb.Code_FAILED_PRECONDITION, KeyDoesNotExist, "Vindex '%s' does not exist in table '%s'", "Vindex hints have to reference an existing vindex, and no such vindex could be found for the given table.")

	VT10001 = errorWithoutState("VT10001", vtrpcpb.Code_ABORTED, "foreign key constraints are not allowed", "Foreign key constraints are not allowed, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/.")

	VT12001 = errorWithoutState("VT12001", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", "This statement is unsupported by Vitess. Please rewrite your query to use supported syntax.")
	VT12002 = errorWithoutState("VT12002", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard foreign keys", "Vitess does not support cross shard foreign keys.")

	// VT13001 General Error
	VT13001 = errorWithoutState("VT13001", vtrpcpb.Code_INTERNAL, "[BUG] %s", "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/vitessio/vitess/issues/new/choose.")
	VT13002 = errorWithoutState("VT13002", vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/vitessio/vitess/issues/new/choose.")

	VT14001 = errorWithoutState("VT14001", vtrpcpb.Code_UNAVAILABLE, "connection error", "The connection failed.")
	VT14002 = errorWithoutState("VT14002", vtrpcpb.Code_UNAVAILABLE, "no available connection", "No available connection.")
	VT14003 = errorWithoutState("VT14003", vtrpcpb.Code_UNAVAILABLE, "no connection for tablet %v", "No connection for the given tablet.")
	VT14004 = errorWithoutState("VT14004", vtrpcpb.Code_UNAVAILABLE, "cannot find keyspace for: %s", "The specified keyspace could not be found.")
	VT14005 = errorWithoutState("VT14005", vtrpcpb.Code_UNAVAILABLE, "cannot lookup sidecar database for keyspace: %s", "Failed to read sidecar database identifier.")

	// Errors is a list of errors that must match all the variables
	// defined above to enable auto-documentation of error codes.
	Errors = []func(args ...any) *VitessError{
		VT03001,
		VT03002,
		VT03003,
		VT03004,
		VT03005,
		VT03006,
		VT03007,
		VT03008,
		VT03009,
		VT03010,
		VT03011,
		VT03012,
		VT03013,
		VT03014,
		VT03015,
		VT03016,
		VT03017,
		VT03018,
		VT03019,
		VT03020,
		VT03021,
		VT03022,
		VT03023,
		VT03024,
		VT03025,
		VT03026,
		VT03027,
		VT03028,
		VT03029,
		VT03030,
		VT03031,
		VT03032,
		VT05001,
		VT05002,
		VT05003,
		VT05004,
		VT05005,
		VT05006,
		VT05007,
		VT06001,
		VT07001,
		VT09001,
		VT09002,
		VT09003,
		VT09004,
		VT09005,
		VT09006,
		VT09007,
		VT09008,
		VT09009,
		VT09010,
		VT09011,
		VT09012,
		VT09013,
		VT09014,
		VT09015,
		VT09016,
		VT09017,
		VT09018,
		VT09019,
		VT10001,
		VT12001,
		VT12002,
		VT13001,
		VT13002,
		VT14001,
		VT14002,
		VT14003,
		VT14004,
		VT14005,
	}
)

type VitessError struct {
	Err         error
	Description string
	ID          string
	State       State
}

func (o *VitessError) Error() string {
	return o.Err.Error()
}

func (o *VitessError) Cause() error {
	return o.Err
}

var _ error = (*VitessError)(nil)

func errorWithoutState(id string, code vtrpcpb.Code, short, long string) func(args ...any) *VitessError {
	return func(args ...any) *VitessError {
		s := short
		if len(args) != 0 {
			s = fmt.Sprintf(s, args...)
		}

		return &VitessError{
			Err:         New(code, id+": "+s),
			Description: long,
			ID:          id,
		}
	}
}

func errorWithState(id string, code vtrpcpb.Code, state State, short, long string) func(args ...any) *VitessError {
	return func(args ...any) *VitessError {
		return &VitessError{
			Err:         NewErrorf(code, state, id+": "+short, args...),
			Description: long,
			ID:          id,
			State:       state,
		}
	}
}
