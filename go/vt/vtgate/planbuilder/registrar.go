// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

// This file defines interfaces and registration for vindexes.

// A VCursor is an interface that allows you to execute queries
// in the current context and session of a VTGate request. Vindexes
// can use this interface to execute lookup queries.
type VCursor interface {
	Execute(query *querytypes.BoundQuery) (*sqltypes.Result, error)
}

// Vindex defines the interface required to register a vindex.
// Additional to these functions, a vindex also needs
// to satisfy the Unique or NonUnique interface.
type Vindex interface {
	// Cost is used by planbuilder to prioritize vindexes.
	// The cost can be 0 if the id is basically a keyspace id.
	// The cost can be 1 if the id can be hashed to a keyspace id.
	// The cost can be 2 or above if the id needs to be looked up
	// from an external data source. These guidelines are subject
	// to change in the future.
	Cost() int

	// Verify must be implented by all vindexes. It should return
	// true if the id can be mapped to the keyspace id.
	Verify(cursor VCursor, id interface{}, ks []byte) (bool, error)
}

// Unique defines the interface for a unique vindex.
// For a vindex to be unique, an id has to map to at most
// one keyspace id.
type Unique interface {
	Map(cursor VCursor, ids []interface{}) ([][]byte, error)
}

// NonUnique defines the interface for a non-unique vindex.
// This means that an id can map to multiple keyspace ids.
type NonUnique interface {
	Map(cursor VCursor, ids []interface{}) ([][][]byte, error)
}

// IsUnique returns true if the Vindex is Unique.
func IsUnique(v Vindex) bool {
	_, ok := v.(Unique)
	return ok
}

// A Reversible vindex is one that can perform a
// reverse lookup from a keyspace id to an id. This
// is optional. If present, VTGate can use it to
// fill column values based on the target keyspace id.
type Reversible interface {
	ReverseMap(cursor VCursor, ks []byte) (interface{}, error)
}

// A Functional vindex is an index that can compute
// the keyspace id from the id without a lookup. This
// means that the creation of a functional vindex entry
// does not take the keyspace id as input. In general,
// the main reason to support creation functions for
// functional indexes is for auto-generating ids.
// A Functional vindex is also required to be Unique.
// If it's not unique, we cannot determine the target shard
// for an insert operation.
type Functional interface {
	Create(VCursor, interface{}) error
	Delete(VCursor, []interface{}, []byte) error
	Unique
}

// A FunctionalGenerator vindex is a Functional vindex
// that can generate new ids.
type FunctionalGenerator interface {
	Functional
	Generate(cursor VCursor) (id int64, err error)
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
	Create(VCursor, interface{}, []byte) error
	Delete(VCursor, []interface{}, []byte) error
}

// A LookupGenerator vindex is a Lookup that can
// generate new ids.
type LookupGenerator interface {
	Lookup
	Generate(VCursor, []byte) (id int64, err error)
}

// A NewVindexFunc is a function that creates a Vindex based on the
// properties specified in the input map. Every vindex must
// register a NewVindexFunc under a unique vindexType.
type NewVindexFunc func(map[string]interface{}) (Vindex, error)

var registry = make(map[string]NewVindexFunc)

// Register registers a vindex under the specified vindexType.
// A duplicate vindexType will generate a panic.
// New vindexes will be created using these functions at the
// time of schema loading.
func Register(vindexType string, newVindexFunc NewVindexFunc) {
	if _, ok := registry[vindexType]; ok {
		panic(fmt.Sprintf("%s is already registered", vindexType))
	}
	registry[vindexType] = newVindexFunc
}

// CreateVindex creates a vindex of the specified type using the
// supplied params. The type must have been previously registered.
func CreateVindex(vindexType string, params map[string]interface{}) (Vindex, error) {
	f, ok := registry[vindexType]
	if !ok {
		return nil, fmt.Errorf("vindexType %s not found", vindexType)
	}
	return f(params)
}
