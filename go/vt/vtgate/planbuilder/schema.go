// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/youtube/vitess/go/jscfg"
)

// Schema represents the denormalized version of SchemaFormal,
// used for building routing plans.
type Schema struct {
	Tables map[string]*Table
}

func (s *Schema) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// Table represnts a table in Schema.
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

// Index contains the index info for each index of a table.
type ColVindex struct {
	Col    string
	Type   string
	Name   string
	Owned  bool
	Vindex Vindex
}

// BuildSchema builds a Schema from a SchemaFormal.
func BuildSchema(source *SchemaFormal) (schema *Schema, err error) {
	schema = &Schema{Tables: make(map[string]*Table)}
	for ksname, ks := range source.Keyspaces {
		keyspace := &Keyspace{
			Name:    ksname,
			Sharded: ks.Sharded,
		}
		vindexes := make(map[string]Vindex)
		for vname, vindexInfo := range ks.Vindexes {
			vindex, err := createVindex(vindexInfo.Type, vindexInfo.Params)
			if err != nil {
				return nil, err
			}
			switch vindex.(type) {
			case Unique:
			case NonUnique:
			default:
				return nil, fmt.Errorf("index %s is needs to be Unique or NonUnique", vname)
			}
			vindexes[vname] = vindex
		}
		for tname, table := range ks.Tables {
			if _, ok := schema.Tables[tname]; ok {
				return nil, fmt.Errorf("table %s has multiple definitions", tname)
			}
			t := &Table{
				Name:     tname,
				Keyspace: keyspace,
			}
			for i, ind := range table.ColVindexes {
				vindexInfo, ok := ks.Vindexes[ind.Name]
				if !ok {
					return nil, fmt.Errorf("index %s not found for table %s", ind.Name, tname)
				}
				columnVindex := &ColVindex{
					Col:    ind.Col,
					Type:   vindexInfo.Type,
					Name:   ind.Name,
					Owned:  vindexInfo.Owner == tname,
					Vindex: vindexes[ind.Name],
				}
				if i == 0 {
					// Perform Primary vindex check.
					if _, ok := columnVindex.Vindex.(Unique); !ok {
						return nil, fmt.Errorf("primary index %s is not Unique for table %s", ind.Name, tname)
					}
					if columnVindex.Owned {
						if _, ok := columnVindex.Vindex.(Functional); !ok {
							return nil, fmt.Errorf("primary owned index %s is not Functional for table %s", ind.Name, tname)
						}
					}
				} else {
					// Perform non-primary vindex check.
					if columnVindex.Owned {
						if _, ok := columnVindex.Vindex.(Lookup); !ok {
							return nil, fmt.Errorf("non-primary owned index %s is not Lookup for table %s", ind.Name, tname)
						}
					}
				}
				t.ColVindexes = append(t.ColVindexes, columnVindex)
				if columnVindex.Owned {
					t.Owned = append(t.Owned, columnVindex)
				}
			}
			t.Ordered = colVindexSorted(t.ColVindexes)
			schema.Tables[tname] = t
		}
	}
	return schema, nil
}

// FindTable returns a pointer to the Table if found.
// Otherwise, it returns a reason, which is equivalent to an error.
func (schema *Schema) FindTable(tablename string) (table *Table, reason string) {
	if tablename == "" {
		return nil, "complex table expression"
	}
	table = schema.Tables[tablename]
	if table == nil {
		return nil, fmt.Sprintf("table %s not found", tablename)
	}
	return table, ""
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

// SchemaFormal is the formal representation of the schema
// as loaded from the source.
type SchemaFormal struct {
	Keyspaces map[string]KeyspaceFormal
}

// KeyspaceFormal is the keyspace info for each keyspace
// as loaded from the source.
type KeyspaceFormal struct {
	Sharded  bool
	Vindexes map[string]VindexFormal
	Tables   map[string]TableFormal
}

// VindexFormal is the info for each index as loaded from
// the source.
type VindexFormal struct {
	Type   string
	Params map[string]interface{}
	Owner  string
}

// TableFormal is the info for each table as loaded from
// the source.
type TableFormal struct {
	ColVindexes []ColVindexFormal
}

// ColVindexFormal is the info for each indexed column
// of a table as loaded from the source.
type ColVindexFormal struct {
	Col  string
	Name string
}

// LoadSchemaJSON loads the formal representation of a schema
// from a JSON file and returns the more usable denormalized
// representaion (Schema) for it.
func LoadSchemaJSON(filename string) (schema *Schema, err error) {
	var source SchemaFormal
	if err := jscfg.ReadJson(filename, &source); err != nil {
		return nil, err
	}
	return BuildSchema(&source)
}
