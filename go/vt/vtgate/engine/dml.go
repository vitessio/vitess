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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// DML contains the common elements between Update and Delete plans
type DML struct {
	// Opcode is the execution opcode.
	Opcode DMLOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies the destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	// Vindex specifies the vindex to be used.
	Vindex vindexes.SingleColumn

	// Values specifies the vindex values to use for routing.
	// For now, only one value is specified.
	Values []sqltypes.PlanValue

	// Keyspace Id Vindex
	KsidVindex vindexes.SingleColumn

	// Table specifies the table for the update.
	Table *vindexes.Table

	// OwnedVindexQuery is used for updating changes in lookup vindexes.
	OwnedVindexQuery string

	// Option to override the standard behavior and allow a multi-shard update
	// to use single round trip autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	txNeeded
}

// DMLOpcode is a number representing the opcode
// for the Update or Delete primitve.
type DMLOpcode int

// This is the list of UpdateOpcode values.
const (
	// UpdateUnsharded is for routing an update statement
	// to an unsharded keyspace.
	Unsharded = DMLOpcode(iota)
	// UpdateEqual is for routing an update statement
	// to a single shard: Requires: A Vindex, and
	// a single Value.
	Equal
	// UpdateScatter is for routing a scattered
	// update statement.
	Scatter
	// UpdateByDestination is to route explicitly to a given
	// target destination. Is used when the query explicitly sets a target destination:
	// in the clause:
	// e.g: UPDATE `keyspace[-]`.x1 SET foo=1
	ByDestination
)
