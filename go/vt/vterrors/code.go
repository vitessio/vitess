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

var (
	VT03001 = errorWithState("VT03001", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "aggregate functions take a single argument '%s'", "The planner accepts aggregate functions that take a single argument only.")
	VT03002 = errorWithState("VT03002", vtrpcpb.Code_INVALID_ARGUMENT, ForbidSchemaChange, "Changing schema from '%s' to '%s' is not allowed", "aa")
	VT03003 = errorWithState("VT03003", vtrpcpb.Code_INVALID_ARGUMENT, UnknownTable, "Unknown table '%s' in MULTI DELETE", "aa")
	VT03004 = errorWithState("VT03004", vtrpcpb.Code_INVALID_ARGUMENT, NonUpdateableTable, "The target table %s of the DELETE is not updatable", "aa")
	VT03005 = errorWithState("VT03005", vtrpcpb.Code_INVALID_ARGUMENT, WrongGroupField, "Can't group on '%s'", "aa")
	VT03006 = errorWithState("VT03006", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueCountOnRow, "Column count doesn't match value count at row 1", "aa")
	VT03007 = errorWithoutState("VT03007", vtrpcpb.Code_INVALID_ARGUMENT, "keyspace not specified", "aa")
	VT03008 = errorWithState("VT03008", vtrpcpb.Code_INVALID_ARGUMENT, CantUseOptionHere, "Incorrect usage/placement of '%s'", "aa")
	VT03009 = errorWithState("VT03009", vtrpcpb.Code_INVALID_ARGUMENT, WrongValueForVar, "unexpected value type for '%s': %v", "aa")
	VT03010 = errorWithState("VT03010", vtrpcpb.Code_INVALID_ARGUMENT, IncorrectGlobalLocalVar, "variable '%s' is a read only variable", "aa")
	VT03011 = errorWithoutState("VT03011", vtrpcpb.Code_INVALID_ARGUMENT, "invalid value type: %v", "aa")
	VT03012 = errorWithoutState("VT03012", vtrpcpb.Code_INVALID_ARGUMENT, "invalid syntax: %s", "aa")
	VT03013 = errorWithState("VT03013", vtrpcpb.Code_INVALID_ARGUMENT, NonUniqTable, "Not unique table/alias: '%s'", "aa")
	VT03014 = errorWithState("VT03014", vtrpcpb.Code_INVALID_ARGUMENT, BadFieldError, "Unknown column '%d' in '%s'", "aa")
	VT03015 = errorWithoutState("VT03015", vtrpcpb.Code_INVALID_ARGUMENT, "column has duplicate set values: '%v'", "aa")
	VT03016 = errorWithoutState("VT03016", vtrpcpb.Code_INVALID_ARGUMENT, "unknown vindex column: '%s'", "aa")
	VT03017 = errorWithState("VT03017", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "where clause can only be of the type 'pos > <value>'", "aa")
	VT03018 = errorWithoutState("VT03018", vtrpcpb.Code_INVALID_ARGUMENT, "NEXT used on a non-sequence table", "aa")
	VT03019 = errorWithoutState("VT03019", vtrpcpb.Code_INVALID_ARGUMENT, "symbol %s not found", "aa")
	VT03020 = errorWithoutState("VT03020", vtrpcpb.Code_INVALID_ARGUMENT, "symbol %s not found in subquery", "aa")
	VT03021 = errorWithoutState("VT03021", vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous symbol reference: %v", "aa")
	VT03022 = errorWithoutState("VT03022", vtrpcpb.Code_INVALID_ARGUMENT, "column %v not found in %v", "aa")

	VT05001 = errorWithState("VT05001", vtrpcpb.Code_NOT_FOUND, DbDropExists, "Can't drop database '%s'; database doesn't exists", "aa")
	VT05002 = errorWithState("VT05002", vtrpcpb.Code_NOT_FOUND, BadDb, "Can't alter database '%s'; unknown database", "aa")
	VT05003 = errorWithState("VT05003", vtrpcpb.Code_NOT_FOUND, BadDb, "Unknown database '%s' in vschema", "aa")
	VT05004 = errorWithState("VT05004", vtrpcpb.Code_NOT_FOUND, UnknownTable, "Table '%s' does not exist", "aa")
	VT05005 = errorWithState("VT05005", vtrpcpb.Code_NOT_FOUND, NoSuchTable, "Table '%s' does not exist in keyspace '%s'", "aa")
	VT05006 = errorWithState("VT05006", vtrpcpb.Code_NOT_FOUND, UnknownSystemVariable, "Unknown system variable '%s'", "aa")
	VT05007 = errorWithoutState("VT05007", vtrpcpb.Code_NOT_FOUND, "no table info", "aa")

	VT06001 = errorWithState("VT06001", vtrpcpb.Code_ALREADY_EXISTS, DbCreateExists, "Can't create database '%s'; database exists", "aa")

	VT09001 = errorWithState("VT09001", vtrpcpb.Code_FAILED_PRECONDITION, RequiresPrimaryKey, PrimaryVindexNotSet, "aa")
	VT09002 = errorWithState("VT09002", vtrpcpb.Code_FAILED_PRECONDITION, InnodbReadOnly, "%s statement with a replica target", "aa")
	VT09003 = errorWithoutState("VT09003", vtrpcpb.Code_FAILED_PRECONDITION, "insert query does not have sharding column '%v' in the column list", "aa")
	VT09004 = errorWithoutState("VT09004", vtrpcpb.Code_FAILED_PRECONDITION, "insert should contain column list or the table should have authoritative columns in vschema", "aa")
	VT09005 = errorWithState("VT09005", vtrpcpb.Code_FAILED_PRECONDITION, NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)", "aa")
	VT09006 = errorWithoutState("VT09006", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_MIGRATION works only on primary tablet", "aa")
	VT09007 = errorWithoutState("VT09007", vtrpcpb.Code_FAILED_PRECONDITION, "%s VITESS_THROTTLED_APPS works only on primary tablet", "aa")

	VT12001 = errorWithoutState("VT12001", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", "aa")

	// VT13001 General Error
	VT13001 = errorWithoutState("VT13001", vtrpcpb.Code_INTERNAL, "[BUG] %s", "aa")
	VT13002 = errorWithoutState("VT13002", vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", "aa")

	Errors = []func(args ...any) *OurError{
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
		VT05001,
		VT05002,
		VT05003,
		VT05004,
		VT05005,
		VT05006,
		VT05007,
		VT06001,
		VT09001,
		VT09002,
		VT09003,
		VT09004,
		VT09005,
		VT09006,
		VT09007,
		VT12001,
		VT13001,
		VT13002,
	}
)

type OurError struct {
	Err         error
	Description string
	ID          string
	State       State
}

func (o *OurError) Error() string {
	return o.Err.Error()
}

var _ error = (*OurError)(nil)

func errorWithoutState(id string, code vtrpcpb.Code, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		if len(args) != 0 {
			short = fmt.Sprintf(short, args...)
		}

		return &OurError{
			Err:         New(code, id+": "+short),
			Description: long,
			ID:          id,
		}
	}
}

func errorWithState(id string, code vtrpcpb.Code, state State, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		return &OurError{
			Err:         NewErrorf(code, state, id+": "+short, args...),
			Description: long,
			ID:          id,
			State:       state,
		}
	}
}
