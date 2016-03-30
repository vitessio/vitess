// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// stFU satisfies Functional, Unique.
type stFU struct {
	name   string
	Params map[string]interface{}
}

func (v *stFU) String() string                                  { return v.name }
func (*stFU) Cost() int                                         { return 1 }
func (*stFU) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stFU) Map(VCursor, []interface{}) ([][]byte, error)      { return nil, nil }

func NewSTFU(name string, params map[string]interface{}) (Vindex, error) {
	return &stFU{name: name, Params: params}, nil
}

// stF satisfies Functional, but no Map. Invalid vindex.
type stF struct {
	name   string
	Params map[string]interface{}
}

func (v *stF) String() string                                  { return v.name }
func (*stF) Cost() int                                         { return 0 }
func (*stF) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }

func NewSTF(name string, params map[string]interface{}) (Vindex, error) {
	return &stF{name: name, Params: params}, nil
}

// stLN satisfies Lookup, NonUnique.
type stLN struct {
	name   string
	Params map[string]interface{}
}

func (v *stLN) String() string                                  { return v.name }
func (*stLN) Cost() int                                         { return 0 }
func (*stLN) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stLN) Map(VCursor, []interface{}) ([][][]byte, error)    { return nil, nil }
func (*stLN) Create(VCursor, interface{}, []byte) error         { return nil }
func (*stLN) Delete(VCursor, []interface{}, []byte) error       { return nil }

func NewSTLN(name string, params map[string]interface{}) (Vindex, error) {
	return &stLN{name: name, Params: params}, nil
}

// stLU satisfies Lookup, Unique.
type stLU struct {
	name   string
	Params map[string]interface{}
}

func (v *stLU) String() string                                  { return v.name }
func (*stLU) Cost() int                                         { return 2 }
func (*stLU) Verify(VCursor, interface{}, []byte) (bool, error) { return false, nil }
func (*stLU) Map(VCursor, []interface{}) ([][]byte, error)      { return nil, nil }
func (*stLU) Create(VCursor, interface{}, []byte) error         { return nil }
func (*stLU) Delete(VCursor, []interface{}, []byte) error       { return nil }

func NewSTLU(name string, params map[string]interface{}) (Vindex, error) {
	return &stLU{name: name, Params: params}, nil
}

func init() {
	Register("stfu", NewSTFU)
	Register("stf", NewSTF)
	Register("stln", NewSTLN)
	Register("stlu", NewSTLU)
}

func TestUnshardedVSchema(t *testing.T) {
	good := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"unsharded": {
				Tables: map[string]string{
					"t1": "",
				},
			},
		},
	}
	got, err := BuildVSchema(&good)
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name: "unsharded",
	}
	t1 := &Table{
		Name:     "t1",
		Keyspace: ks,
	}
	want := &VSchema{
		tables: map[string]*Table{
			"t1": t1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1": t1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildVSchema:s\n%v, want\n%v", got, want)
	}
}

func TestShardedVSchemaOwned(t *testing.T) {
	good := VSchemaFormal{
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
	got, err := BuildVSchema(&good)
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	t1 := &Table{
		Name:     "t1",
		Keyspace: ks,
		ColVindexes: []*ColVindex{
			{
				Col:  "c1",
				Type: "stfu",
				Name: "stfu1",
				Vindex: &stFU{
					name: "stfu1",
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
				Vindex: &stLN{name: "stln1"},
			},
		},
	}
	t1.Ordered = []*ColVindex{
		t1.ColVindexes[1],
		t1.ColVindexes[0],
	}
	t1.Owned = t1.ColVindexes[1:]
	want := &VSchema{
		tables: map[string]*Table{
			"t1": t1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1": t1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:s\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestShardedVSchemaNotOwned(t *testing.T) {
	good := VSchemaFormal{
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
	got, err := BuildVSchema(&good)
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	t1 := &Table{
		Name:     "t1",
		Keyspace: ks,
		ColVindexes: []*ColVindex{
			{
				Col:    "c1",
				Type:   "stlu",
				Name:   "stlu1",
				Owned:  false,
				Vindex: &stLU{name: "stlu1"},
			},
			{
				Col:    "c2",
				Type:   "stfu",
				Name:   "stfu1",
				Owned:  false,
				Vindex: &stFU{name: "stfu1"},
			},
		},
	}
	t1.Ordered = []*ColVindex{
		t1.ColVindexes[1],
		t1.ColVindexes[0],
	}
	want := &VSchema{
		tables: map[string]*Table{
			"t1": t1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1": t1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildVSchema:s\n%v, want\n%v", got, want)
	}
}

func TestLoadVSchemaFail(t *testing.T) {
	_, err := LoadFile("bogus file name")
	want := "ReadFile failed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("LoadFile: \n%q, should start with \n%q", err, want)
	}

	_, err = NewVSchema([]byte("{,}"))
	want = "Unmarshal failed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("LoadFile: \n%q, should start with \n%q", err, want)
	}
}

func TestBuildVSchemaClassNotFoundFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "class t1 not found for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaVindexNotFoundFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "vindexType noexist not found"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaInvalidVindexFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "vindex stf needs to be Unique or NonUnique"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaDupSeq(t *testing.T) {
	good := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ksa": {
				Classes: map[string]ClassFormal{
					"seq": {
						Type: "Sequence",
					},
				},
				Tables: map[string]string{
					"t1": "seq",
				},
			},
			"ksb": {
				Classes: map[string]ClassFormal{
					"seq": {
						Type: "Sequence",
					},
				},
				Tables: map[string]string{
					"t1": "seq",
				},
			},
		},
	}
	ksa := &Keyspace{
		Name: "ksa",
	}
	ksb := &Keyspace{
		Name: "ksb",
	}
	got, _ := BuildVSchema(&good)
	t1a := &Table{
		Name:       "t1",
		Keyspace:   ksa,
		IsSequence: true,
	}
	t1b := &Table{
		Name:       "t1",
		Keyspace:   ksb,
		IsSequence: true,
	}
	want := &VSchema{
		tables: map[string]*Table{
			"t1": nil,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"ksa": {
				Keyspace: ksa,
				Tables: map[string]*Table{
					"t1": t1a,
				},
			},
			"ksb": {
				Keyspace: ksb,
				Tables: map[string]*Table{
					"t1": t1b,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:s\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaDupTable(t *testing.T) {
	good := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ksa": {
				Tables: map[string]string{
					"t1": "",
				},
			},
			"ksb": {
				Tables: map[string]string{
					"t1": "",
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	ksa := &Keyspace{
		Name: "ksa",
	}
	t1a := &Table{
		Name:     "t1",
		Keyspace: ksa,
	}
	ksb := &Keyspace{
		Name: "ksb",
	}
	t1b := &Table{
		Name:     "t1",
		Keyspace: ksb,
	}
	want := &VSchema{
		tables: map[string]*Table{
			"t1": nil,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"ksa": {
				Keyspace: ksa,
				Tables: map[string]*Table{
					"t1": t1a,
				},
			},
			"ksb": {
				Keyspace: ksb,
				Tables: map[string]*Table{
					"t1": t1b,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:s\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaNoindexFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "vindex notexist not found for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaNotUniqueFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "primary vindex stln is not Unique for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaPrimaryNonFunctionalFail(t *testing.T) {
	bad := VSchemaFormal{
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
	_, err := BuildVSchema(&bad)
	want := "primary vindex stlu cannot be owned for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestSequence(t *testing.T) {
	good := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"unsharded": {
				Classes: map[string]ClassFormal{
					"seq": {
						Type: "Sequence",
					},
				},
				Tables: map[string]string{
					"seq": "seq",
				},
			},
			"sharded": {
				Sharded: true,
				Vindexes: map[string]VindexFormal{
					"stfu1": {
						Type: "stfu",
						Params: map[string]interface{}{
							"stfu1": 1,
						},
					},
				},
				Classes: map[string]ClassFormal{
					"t1": {
						ColVindexes: []ColVindexFormal{
							{
								Col:  "c1",
								Name: "stfu1",
							},
						},
						Autoinc: &AutoincFormal{
							Col:      "c1",
							Sequence: "seq",
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	got, err := BuildVSchema(&good)
	if err != nil {
		t.Error(err)
	}
	ksu := &Keyspace{
		Name: "unsharded",
	}
	kss := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	seq := &Table{
		Name:       "seq",
		Keyspace:   ksu,
		IsSequence: true,
	}
	t1 := &Table{
		Name:     "t1",
		Keyspace: kss,
		ColVindexes: []*ColVindex{
			{
				Col:  "c1",
				Type: "stfu",
				Name: "stfu1",
				Vindex: &stFU{
					name: "stfu1",
					Params: map[string]interface{}{
						"stfu1": 1,
					},
				},
			},
		},
		Autoinc: &Autoinc{
			Col:      "c1",
			Sequence: seq,
		},
	}
	t1.Ordered = []*ColVindex{
		t1.ColVindexes[0],
	}
	want := &VSchema{
		tables: map[string]*Table{
			"seq": seq,
			"t1":  t1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ksu,
				Tables: map[string]*Table{
					"seq": seq,
				},
			},
			"sharded": {
				Keyspace: kss,
				Tables: map[string]*Table{
					"t1": t1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:s\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBadSequence(t *testing.T) {
	bad := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"sharded": {
				Sharded: true,
				Classes: map[string]ClassFormal{
					"t1": {
						Autoinc: &AutoincFormal{
							Col:      "c1",
							Sequence: "seq",
						},
					},
				},
				Tables: map[string]string{
					"t1": "t1",
				},
			},
		},
	}
	_, err := BuildVSchema(&bad)
	want := "sequence seq not found for class t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestFind(t *testing.T) {
	input := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ksa": {
				Tables: map[string]string{
					"ta": "",
					"t1": "",
				},
			},
			"ksb": {
				Sharded: true,
				Classes: map[string]ClassFormal{
					"t": {},
				},
				Tables: map[string]string{
					"tb": "t",
					"t1": "t",
				},
			},
		},
	}
	vschema, _ := BuildVSchema(&input)
	_, err := vschema.Find("", "t1")
	wantErr := "ambiguous table reference: t1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Find(\"\"): %v, want %s", err, wantErr)
	}
	_, err = vschema.Find("", "none")
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Find(\"\"): %v, want %s", err, wantErr)
	}
	got, err := vschema.Find("", "ta")
	if err != nil {
		t.Error(err)
		return
	}
	ta := &Table{
		Name: "ta",
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	if !reflect.DeepEqual(got, ta) {
		t.Errorf("Find(\"t1a\"): %+v, want %+v", got, ta)
	}
	got, err = vschema.Find("ksa", "ta")
	if !reflect.DeepEqual(got, ta) {
		t.Errorf("Find(\"t1a\"): %+v, want %+v", got, ta)
	}
	none := &Table{
		Name: "none",
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, err = vschema.Find("ksa", "none")
	if !reflect.DeepEqual(got, none) {
		t.Errorf("Find(\"t1a\"): %+v, want %+v", got, none)
	}
	_, err = vschema.Find("ksb", "none")
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Find(\"\"): %v, want %s", err, wantErr)
	}
	_, err = vschema.Find("none", "aa")
	wantErr = "keyspace none not found in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Find(\"\"): %v, want %s", err, wantErr)
	}
}

func TestFindSingleKeyspace(t *testing.T) {
	input := VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ksa": {
				Tables: map[string]string{
					"ta": "",
					"t1": "",
				},
			},
		},
	}
	vschema, _ := BuildVSchema(&input)
	none := &Table{
		Name: "none",
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, _ := vschema.Find("", "none")
	if !reflect.DeepEqual(got, none) {
		t.Errorf("Find(\"t1a\"): %+v, want %+v", got, none)
	}
	input = VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ksb": {
				Sharded: true,
				Classes: map[string]ClassFormal{
					"t": {},
				},
				Tables: map[string]string{
					"tb": "t",
					"t1": "t",
				},
			},
		},
	}
	vschema, _ = BuildVSchema(&input)
	_, err := vschema.Find("", "none")
	wantErr := "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Find(\"\"): %v, want %s", err, wantErr)
	}
}
