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

func (*stFU) Cost() int                                                 { return 1 }
func (*stFU) Verify(VCursor, interface{}, key.KeyspaceId) (bool, error) { return false, nil }
func (*stFU) Map(VCursor, []interface{}) ([]key.KeyspaceId, error)      { return nil, nil }
func (*stFU) Create(VCursor, interface{}) error                         { return nil }
func (*stFU) Delete(VCursor, []interface{}, key.KeyspaceId) error       { return nil }

func NewSTFU(params map[string]interface{}) (Vindex, error) {
	return &stFU{Params: params}, nil
}

// stF satisfies Functional, but no Map. Invalid vindex.
type stF struct {
	Params map[string]interface{}
}

func (*stF) Cost() int                                                 { return 0 }
func (*stF) Verify(VCursor, interface{}, key.KeyspaceId) (bool, error) { return false, nil }
func (*stF) Create(VCursor, interface{}) error                         { return nil }
func (*stF) Delete(VCursor, []interface{}, key.KeyspaceId) error       { return nil }

func NewSTF(params map[string]interface{}) (Vindex, error) {
	return &stF{Params: params}, nil
}

// stLN satisfies Lookup, NonUnique.
type stLN struct {
	Params map[string]interface{}
}

func (*stLN) Cost() int                                                 { return 0 }
func (*stLN) Verify(VCursor, interface{}, key.KeyspaceId) (bool, error) { return false, nil }
func (*stLN) Map(VCursor, []interface{}) ([][]key.KeyspaceId, error)    { return nil, nil }
func (*stLN) Create(VCursor, interface{}, key.KeyspaceId) error         { return nil }
func (*stLN) Delete(VCursor, []interface{}, key.KeyspaceId) error       { return nil }

func NewSTLN(params map[string]interface{}) (Vindex, error) {
	return &stLN{Params: params}, nil
}

// stLU satisfies Lookup, Unique.
type stLU struct {
	Params map[string]interface{}
}

func (*stLU) Cost() int                                                 { return 2 }
func (*stLU) Verify(VCursor, interface{}, key.KeyspaceId) (bool, error) { return false, nil }
func (*stLU) Map(VCursor, []interface{}) ([]key.KeyspaceId, error)      { return nil, nil }
func (*stLU) Create(VCursor, interface{}, key.KeyspaceId) error         { return nil }
func (*stLU) Delete(VCursor, []interface{}, key.KeyspaceId) error       { return nil }

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
	want.Tables["t1"].Owned = want.Tables["t1"].ColVindexes
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
