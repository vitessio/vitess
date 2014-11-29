// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
)

// stFU satisfies Functional, Unique.
type stFU struct {
	Params map[string]interface{}
}

func (_ *stFU) Cost() int { return 0 }

func (_ *stFU) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }

func (_ *stFU) Map(_ []interface{}) ([]key.KeyspaceId, error) { return nil, nil }

func (_ *stFU) Create(_ interface{}) error { return nil }

func (_ *stFU) Delete(_ interface{}, _ key.KeyspaceId) error { return nil }

func NewSTFU(params map[string]interface{}) (Vindex, error) {
	return &stFU{Params: params}, nil
}

// stF satisfies Functional, but no Map. Invalid vindex.
type stF struct {
	Params map[string]interface{}
}

func (_ *stF) Cost() int { return 0 }

func (_ *stF) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }

func (_ *stF) Create(_ interface{}) error { return nil }

func (_ *stF) Delete(_ interface{}, _ key.KeyspaceId) error { return nil }

func NewSTF(params map[string]interface{}) (Vindex, error) {
	return &stF{Params: params}, nil
}

// stLN satisfies Lookup, NonUnique.
type stLN struct {
	Params map[string]interface{}
}

func (_ *stLN) Cost() int { return 0 }

func (_ *stLN) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }

func (_ *stLN) Map(_ []interface{}) ([][]key.KeyspaceId, error) { return nil, nil }

func (_ *stLN) Create(_ interface{}, _ key.KeyspaceId) error { return nil }

func (_ *stLN) Delete(_ interface{}, _ key.KeyspaceId) error { return nil }

func NewSTLN(params map[string]interface{}) (Vindex, error) {
	return &stLN{Params: params}, nil
}

// stLU satisfies Lookup, Unique.
type stLU struct {
	Params map[string]interface{}
}

func (_ *stLU) Cost() int { return 0 }

func (_ *stLU) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }

func (_ *stLU) Map(_ []interface{}) ([]key.KeyspaceId, error) { return nil, nil }

func (_ *stLU) Create(_ interface{}, _ key.KeyspaceId) error { return nil }

func (_ *stLU) Delete(_ interface{}, _ key.KeyspaceId) error { return nil }

func NewSTLU(params map[string]interface{}) (Vindex, error) {
	return &stLU{Params: params}, nil
}

func init() {
	Register("stfu", NewSTFU)
	Register("stf", NewSTF)
	Register("stln", NewSTLN)
	Register("stlu", NewSTLU)
}

func TestUnshardedSchema(t *testing.T) {
	good := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"unsharded": {
				Tables: map[string]TableFormal{
					"t1": {},
				},
			},
		},
	}
	got, err := BuildSchema(&good)
	if err != nil {
		t.Error(err)
	}
	want := &Schema{
		Tables: map[string]*Table{
			"t1": &Table{
				Name: "t1",
				Keyspace: &Keyspace{
					Name: "unsharded",
				},
				Vindexes: nil,
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildSchema:s\n%v, want\n%v", got, want)
	}
}

func TestShardedSchemaOwned(t *testing.T) {
	good := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu1": {
						Type: "stfu",
						Params: map[string]interface{}{
							"stfu1": 1,
						},
						Owner: "t1",
					},
					"stln1": {
						Type:  "stln",
						Owner: "t1",
					},
				},
				Tables: map[string]TableFormal{
					"t1": {
						ColumnVindexes: []ColumnVindexFormal{
							{
								Column: "c1",
								Name:   "stfu1",
							}, {
								Column: "c2",
								Name:   "stln1",
							},
						},
					},
				},
			},
		},
	}
	got, err := BuildSchema(&good)
	if err != nil {
		t.Error(err)
	}
	want := &Schema{
		Tables: map[string]*Table{
			"t1": &Table{
				Name: "t1",
				Keyspace: &Keyspace{
					Name:    "sharded",
					Sharded: true,
				},
				Vindexes: []*ColumnVindex{
					&ColumnVindex{
						Column: "c1",
						Type:   "stfu",
						Name:   "stfu1",
						Owned:  true,
						Vindex: &stFU{
							Params: map[string]interface{}{
								"stfu1": 1,
							},
						},
					},
					&ColumnVindex{
						Column: "c2",
						Type:   "stln",
						Name:   "stln1",
						Owned:  true,
						Vindex: &stLN{},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildSchema:s\n%v, want\n%v", got, want)
	}
}

func TestShardedSchemaNotOwned(t *testing.T) {
	good := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stlu1": {
						Type:  "stlu",
						Owner: "",
					},
					"stfu1": {
						Type:  "stfu",
						Owner: "",
					},
				},
				Tables: map[string]TableFormal{
					"t1": {
						ColumnVindexes: []ColumnVindexFormal{
							{
								Column: "c1",
								Name:   "stlu1",
							}, {
								Column: "c2",
								Name:   "stfu1",
							},
						},
					},
				},
			},
		},
	}
	got, err := BuildSchema(&good)
	if err != nil {
		t.Error(err)
	}
	want := &Schema{
		Tables: map[string]*Table{
			"t1": &Table{
				Name: "t1",
				Keyspace: &Keyspace{
					Name:    "sharded",
					Sharded: true,
				},
				Vindexes: []*ColumnVindex{
					&ColumnVindex{
						Column: "c1",
						Type:   "stlu",
						Name:   "stlu1",
						Owned:  false,
						Vindex: &stLU{},
					},
					&ColumnVindex{
						Column: "c2",
						Type:   "stfu",
						Name:   "stfu1",
						Owned:  false,
						Vindex: &stFU{},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildSchema:s\n%v, want\n%v", got, want)
	}
}
