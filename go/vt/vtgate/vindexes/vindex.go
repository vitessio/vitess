/*
Copyright 2017 Google Inc.

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

package vindexes

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file defines interfaces and registration for vindexes.

// A VCursor is an interface that allows you to execute queries
// in the current context and session of a VTGate request. Vindexes
// can use this interface to execute lookup queries.
type VCursor interface {
	Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error)
	ExecutePre(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error)
	ExecutePost(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error)
	ExecuteAutocommit(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error)

	ExecuteKeyspaceID(keyspace string, ksid []byte, query *querypb.BoundQuery, isDML, autocommit bool) (*sqltypes.Result, error)
}

// Vindex defines the interface required to register a vindex.
type Vindex interface {
	// String returns the name of the Vindex instance.
	// It's used for testing and diagnostics. Use pointer
	// comparison to see if two objects refer to the same
	// Vindex.
	String() string

	// Cost is used by planbuilder to prioritize vindexes.
	// The cost can be 0 if the id is basically a keyspace id.
	// The cost can be 1 if the id can be hashed to a keyspace id.
	// The cost can be 2 or above if the id needs to be looked up
	// from an external data source. These guidelines are subject
	// to change in the future.
	Cost() int

	// IsUnique returns true if the Vindex is unique.
	// Which means Map() maps to either a KeyRange or a single KeyspaceID.
	IsUnique() bool

	// IsFunctional returns true if the Vindex can compute
	// the keyspace id from the id without a lookup.
	// A Functional vindex is also required to be Unique.
	// Which means Map() maps to either a KeyRange or a single KeyspaceID.
	IsFunctional() bool

	// Verify must be implented by all vindexes. It should return
	// true if the ids can be mapped to the keyspace ids.
	Verify(cursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error)

	// Map can map ids to key.Destination objects.
	// If the Vindex is unique, each id would map to either
	// a KeyRange, or a single KeyspaceID.
	// If the Vindex is non-unique, each id would map to either
	// a KeyRange, or a list of KeyspaceID.
	// If the error returned if nil, then the array len of the
	// key.Destination array must match len(ids).
	Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error)
}

// A Reversible vindex is one that can perform a
// reverse lookup from a keyspace id to an id. This
// is optional. If present, VTGate can use it to
// fill column values based on the target keyspace id.
type Reversible interface {
	ReverseMap(cursor VCursor, ks [][]byte) ([]sqltypes.Value, error)
}

// A Lookup vindex is one that needs to lookup
// a previously stored map to compute the keyspace
// id from an id. This means that the creation of
// a lookup vindex entry requires a keyspace id as
// input.
// A Lookup vindex need not be unique because the
// keyspace_id, which must be supplied, can be used
// to determine the target shard for an insert operation.
type Lookup interface {
	// Create creates an association between ids and ksids. If ignoreMode
	// is true, then the Create should ignore dup key errors.
	Create(vc VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error

	Delete(vc VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error

	// Update replaces the mapping of old values with new values for a keyspace id.
	Update(vc VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error
}

// WantOwnerInfo defines the interface that a vindex must
// satisfy to info about the owner table. This information can
// be used to query the owner's table for the owning row's presence.
type WantOwnerInfo interface {
	SetOwnerInfo(keyspace, table string, cols []sqlparser.ColIdent) error
}

// A NewVindexFunc is a function that creates a Vindex based on the
// properties specified in the input map. Every vindex must
// register a NewVindexFunc under a unique vindexType.
type NewVindexFunc func(string, map[string]string) (Vindex, error)

var registry = make(map[string]NewVindexFunc)

// Register registers a vindex under the specified vindexType.
// A duplicate vindexType will generate a panic.
// New vindexes will be created using these functions at the
// time of vschema loading.
func Register(vindexType string, newVindexFunc NewVindexFunc) {
	if _, ok := registry[vindexType]; ok {
		panic(fmt.Sprintf("%s is already registered", vindexType))
	}
	registry[vindexType] = newVindexFunc
}

// CreateVindex creates a vindex of the specified type using the
// supplied params. The type must have been previously registered.
func CreateVindex(vindexType, name string, params map[string]string) (Vindex, error) {
	f, ok := registry[vindexType]
	if !ok {
		return nil, fmt.Errorf("vindexType %q not found", vindexType)
	}
	return f(name, params)
}
