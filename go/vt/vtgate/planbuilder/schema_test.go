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

func (_ *stFU) Cost() int                                                       { return 1 }
func (_ *stFU) Verify(_ VCursor, _ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *stFU) Map(_ VCursor, _ []interface{}) ([]key.KeyspaceId, error)        { return nil, nil }
func (_ *stFU) Create(_ VCursor, _ interface{}) error                           { return nil }
func (_ *stFU) Delete(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }

func NewSTFU(params map[string]interface{}) (Vindex, error) {
	return &stFU{Params: params}, nil
}

// stF satisfies Functional, but no Map. Invalid vindex.
type stF struct {
	Params map[string]interface{}
}

func (_ *stF) Cost() int                                                       { return 0 }
func (_ *stF) Verify(_ VCursor, _ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *stF) Create(_ VCursor, _ interface{}) error                           { return nil }
func (_ *stF) Delete(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }

func NewSTF(params map[string]interface{}) (Vindex, error) {
	return &stF{Params: params}, nil
}

// stLN satisfies Lookup, NonUnique.
type stLN struct {
	Params map[string]interface{}
}

func (_ *stLN) Cost() int                                                       { return 0 }
func (_ *stLN) Verify(_ VCursor, _ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *stLN) Map(_ VCursor, _ []interface{}) ([][]key.KeyspaceId, error)      { return nil, nil }
func (_ *stLN) Create(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }
func (_ *stLN) Delete(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }

func NewSTLN(params map[string]interface{}) (Vindex, error) {
	return &stLN{Params: params}, nil
}

// stLU satisfies Lookup, Unique.
type stLU struct {
	Params map[string]interface{}
}

func (_ *stLU) Cost() int                                                       { return 2 }
func (_ *stLU) Verify(_ VCursor, _ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *stLU) Map(_ VCursor, _ []interface{}) ([]key.KeyspaceId, error)        { return nil, nil }
func (_ *stLU) Create(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }
func (_ *stLU) Delete(_ VCursor, _ interface{}, _ key.KeyspaceId) error         { return nil }

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
				ColVindexes: nil,
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
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stfu1",
							}, {
								Col:  "c2",
								Name: "stln1",
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
				ColVindexes: []*ColVindex{
					&ColVindex{
						Col:   "c1",
						Type:  "stfu",
						Name:  "stfu1",
						Owned: true,
						Vindex: &stFU{
							Params: map[string]interface{}{
								"stfu1": 1,
							},
						},
					},
					&ColVindex{
						Col:    "c2",
						Type:   "stln",
						Name:   "stln1",
						Owned:  true,
						Vindex: &stLN{},
					},
				},
			},
		},
	}
	want.Tables["t1"].Ordered = []*ColVindex{
		want.Tables["t1"].ColVindexes[1],
		want.Tables["t1"].ColVindexes[0],
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
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stlu1",
							}, {
								Col:  "c2",
								Name: "stfu1",
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
				ColVindexes: []*ColVindex{
					&ColVindex{
						Col:    "c1",
						Type:   "stlu",
						Name:   "stlu1",
						Owned:  false,
						Vindex: &stLU{},
					},
					&ColVindex{
						Col:    "c2",
						Type:   "stfu",
						Name:   "stfu1",
						Owned:  false,
						Vindex: &stFU{},
					},
				},
			},
		},
	}
	want.Tables["t1"].Ordered = []*ColVindex{
		want.Tables["t1"].ColVindexes[1],
		want.Tables["t1"].ColVindexes[0],
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildSchema:s\n%v, want\n%v", got, want)
	}
}
