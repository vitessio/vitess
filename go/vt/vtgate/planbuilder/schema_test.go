// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"reflect"
	"strings"
	"testing"
)

// stFU satisfies Functional, Unique.
type stFU struct {
	Params map[string]interface{}
}

func (*stFU) Cost() int                                         { return 1 }
func (*stFU) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stFU) Map(VCursor, []interface{}) ([][]byte, error)      { return nil, nil }
func (*stFU) Create(VCursor, interface{}) error                 { return nil }
func (*stFU) Delete(VCursor, []interface{}, []byte) error       { return nil }

func NewSTFU(params map[string]interface{}) (Vindex, error) {
	return &stFU{Params: params}, nil
}

// stF satisfies Functional, but no Map. Invalid vindex.
type stF struct {
	Params map[string]interface{}
}

func (*stF) Cost() int                                         { return 0 }
func (*stF) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }

func NewSTF(params map[string]interface{}) (Vindex, error) {
	return &stF{Params: params}, nil
}

// stLN satisfies Lookup, NonUnique.
type stLN struct {
	Params map[string]interface{}
}

func (*stLN) Cost() int                                         { return 0 }
func (*stLN) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stLN) Map(VCursor, []interface{}) ([][][]byte, error)    { return nil, nil }
func (*stLN) Create(VCursor, interface{}, []byte) error         { return nil }
func (*stLN) Delete(VCursor, []interface{}, []byte) error       { return nil }

func NewSTLN(params map[string]interface{}) (Vindex, error) {
	return &stLN{Params: params}, nil
}

// stLU satisfies Lookup, Unique.
type stLU struct {
	Params map[string]interface{}
}

func (*stLU) Cost() int                                         { return 2 }
func (*stLU) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stLU) Map(VCursor, []interface{}) ([][]byte, error)      { return nil, nil }
func (*stLU) Create(VCursor, interface{}, []byte) error         { return nil }
func (*stLU) Delete(VCursor, []interface{}, []byte) error       { return nil }

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
				Tables: map[string]string{
					"t1": "",
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
			"t1": {
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
				Classes: map[string]ClassFormal{
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
				Tables: map[string]string{
					"t1": "t1",
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
			"t1": {
				Name: "t1",
				Keyspace: &Keyspace{
					Name:    "sharded",
					Sharded: true,
				},
				ColVindexes: []*ColVindex{
					{
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
					{
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
				Classes: map[string]ClassFormal{
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
				Tables: map[string]string{
					"t1": "t1",
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
			"t1": {
				Name: "t1",
				Keyspace: &Keyspace{
					Name:    "sharded",
					Sharded: true,
				},
				ColVindexes: []*ColVindex{
					{
						Col:    "c1",
						Type:   "stlu",
						Name:   "stlu1",
						Owned:  false,
						Vindex: &stLU{},
					},
					{
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

func TestLoadSchemaFail(t *testing.T) {
	_, err := LoadFile("bogus file name")
	want := "ReadFile failed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("LoadFile: \n%q, should start with \n%q", err, want)
	}

	_, err = NewSchema([]byte("{,}"))
	want = "Unmarshal failed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("LoadFile: \n%q, should start with \n%q", err, want)
	}
}

func TestBuildSchemaClassNotFoundFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu": {
						Type: "stfu",
					},
				},
				Classes: map[string]ClassFormal{
					"notexist": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "noexist",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "class t1 not found for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaVindexNotFoundFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"noexist": {
						Type: "noexist",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "noexist",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "vindexType noexist not found"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaInvalidVindexFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stf": {
						Type: "stf",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stf",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "vindex stf needs to be Unique or NonUnique"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaDupTableFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu": {
						Type: "stfu",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stfu",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
			"sharded1": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu": {
						Type: "stfu",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stfu",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "table t1 has multiple definitions"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaNoindexFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu": {
						Type: "stfu",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "notexist",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "vindex notexist not found for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaNotUniqueFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stln": {
						Type: "stln",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stln",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "primary index stln is not Unique for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaPrimaryNonFunctionalFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stlu": {
						Type:  "stlu",
						Owner: "t1",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stlu",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "primary owned index stlu is not Functional for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}

func TestBuildSchemaNonPrimaryLookupFail(t *testing.T) {
	bad := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stlu": {
						Type: "stlu",
					},
					"stfu": {
						Type:  "stfu",
						Owner: "t1",
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stlu",
							}, {
								Col:  "c2",
								Name: "stfu",
							},
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildSchema(&bad)
	want := "non-primary owned index stfu is not Lookup for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildSchema: %v, want %v", err, want)
	}
}
