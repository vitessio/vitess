// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)

// VSchema represents the denormalized version of VSchemaFormal,
// used for building routing plans.
type VSchema struct {
	Tables map[string]*Table
}

// Table represnts a table in VSchema.
type Table struct {
	Name        string
	Keyspace    *Keyspace
	ColVindexes []*ColVindex
	Ordered     []*ColVindex
	Owned       []*ColVindex
}

// Keyspace contains the keyspcae info for each Table.
type Keyspace struct {
	Name    string
	Sharded bool
}

// ColVindex contains the index info for each index of a table.
type ColVindex struct {
	Col    string
	Type   string
	Name   string
	Owned  bool
	Vindex Vindex
}

// BuildVSchema builds a VSchema from a VSchemaFormal.
func BuildVSchema(source *VSchemaFormal) (vschema *VSchema, err error) {
	vschema = &VSchema{Tables: make(map[string]*Table)}
	for ksname, ks := range source.Keyspaces {
		keyspace := &Keyspace{
			Name:    ksname,
			Sharded: ks.Sharded,
		}
		vindexes := make(map[string]Vindex)
		for vname, vindexInfo := range ks.Vindexes {
			vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
			if err != nil {
				return nil, err
			}
			switch vindex.(type) {
			case Unique:
			case NonUnique:
			default:
				return nil, fmt.Errorf("vindex %s needs to be Unique or NonUnique", vname)
			}
			vindexes[vname] = vindex
		}
		for tname, cname := range ks.Tables {
			if _, ok := vschema.Tables[tname]; ok {
				return nil, fmt.Errorf("table %s has multiple definitions", tname)
			}
			t := &Table{
				Name:     tname,
				Keyspace: keyspace,
			}
			if !keyspace.Sharded {
				vschema.Tables[tname] = t
				continue
			}
			class, ok := ks.Classes[cname]
			if !ok {
				return nil, fmt.Errorf("class %s not found for table %s", cname, tname)
			}
			for i, ind := range class.ColVindexes {
				vindexInfo, ok := ks.Vindexes[ind.Name]
				if !ok {
					return nil, fmt.Errorf("vindex %s not found for class %s", ind.Name, cname)
				}
				columnVindex := &ColVindex{
					Col:    strings.ToLower(ind.Col),
					Type:   vindexInfo.Type,
					Name:   ind.Name,
					Owned:  vindexInfo.Owner == tname,
					Vindex: vindexes[ind.Name],
				}
				if i == 0 {
					// Perform Primary vindex check.
					if _, ok := columnVindex.Vindex.(Unique); !ok {
						return nil, fmt.Errorf("primary index %s is not Unique for class %s", ind.Name, cname)
					}
					if columnVindex.Owned {
						if _, ok := columnVindex.Vindex.(Functional); !ok {
							return nil, fmt.Errorf("primary owned index %s is not Functional for class %s", ind.Name, cname)
						}
					}
				} else {
					// Perform non-primary vindex check.
					if columnVindex.Owned {
						if _, ok := columnVindex.Vindex.(Lookup); !ok {
							return nil, fmt.Errorf("non-primary owned index %s is not Lookup for class %s", ind.Name, cname)
						}
					}
				}
				t.ColVindexes = append(t.ColVindexes, columnVindex)
				if columnVindex.Owned {
					t.Owned = append(t.Owned, columnVindex)
				}
			}
			t.Ordered = colVindexSorted(t.ColVindexes)
			vschema.Tables[tname] = t
		}
	}
	return vschema, nil
}

// FindTable returns a pointer to the Table if found.
// Otherwise, it returns a reason, which is equivalent to an error.
func (vschema *VSchema) FindTable(tablename string) (table *Table, err error) {
	if tablename == "" {
		return nil, errors.New("unsupported: compex table expression in DML")
	}
	table = vschema.Tables[tablename]
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tablename)
	}
	return table, nil
}

// ByCost provides the interface needed for ColVindexes to
// be sorted by cost order.
type ByCost []*ColVindex

func (bc ByCost) Len() int           { return len(bc) }
func (bc ByCost) Swap(i, j int)      { bc[i], bc[j] = bc[j], bc[i] }
func (bc ByCost) Less(i, j int) bool { return bc[i].Vindex.Cost() < bc[j].Vindex.Cost() }

func colVindexSorted(cvs []*ColVindex) (sorted []*ColVindex) {
	for _, cv := range cvs {
		sorted = append(sorted, cv)
	}
	sort.Sort(ByCost(sorted))
	return sorted
}

// VSchemaFormal is the formal representation of the vschema
// as loaded from the source.
type VSchemaFormal struct {
	Keyspaces map[string]KeyspaceFormal
}

// KeyspaceFormal is the keyspace info for each keyspace
// as loaded from the source.
type KeyspaceFormal struct {
	Sharded  bool
	Vindexes map[string]VindexFormal
	Classes  map[string]ClassFormal
	Tables   map[string]string
}

// VindexFormal is the info for each index as loaded from
// the source.
type VindexFormal struct {
	Type   string
	Params map[string]interface{}
	Owner  string
}

// ClassFormal is the info for each table class as loaded from
// the source.
type ClassFormal struct {
	ColVindexes []ColVindexFormal
}

// ColVindexFormal is the info for each indexed column
// of a table as loaded from the source.
type ColVindexFormal struct {
	Col  string
	Name string
}

// LoadFile creates a new VSchema from a JSON file.
func LoadFile(filename string) (vschema *VSchema, err error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("ReadFile failed: %v %v", filename, err)
	}
	return NewVSchema(data)
}

// NewVSchema creates a new VSchema from a JSON byte array.
func NewVSchema(data []byte) (vschema *VSchema, err error) {
	var source VSchemaFormal
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, fmt.Errorf("Unmarshal failed: %v %s %v", source, data, err)
	}
	return BuildVSchema(&source)
}
