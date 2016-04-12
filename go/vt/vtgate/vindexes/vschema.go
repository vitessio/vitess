// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)

// VSchema represents the denormalized version of VSchemaFormal,
// used for building routing plans.
type VSchema struct {
	tables    map[string]*Table
	Keyspaces map[string]*KeyspaceSchema
}

// Table represents a table in VSchema.
type Table struct {
	IsSequence  bool
	Name        string
	Keyspace    *Keyspace
	ColVindexes []*ColVindex
	Ordered     []*ColVindex
	Owned       []*ColVindex
	Autoinc     *Autoinc
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

// KeyspaceSchema contains the schema(table) for a keyspace.
type KeyspaceSchema struct {
	Keyspace *Keyspace
	Tables   map[string]*Table
}

// Autoinc contains the auto-inc information for a table.
type Autoinc struct {
	Col      string
	Sequence *Table
	// ColVindexNum is the index of the ColVindex
	// if the column is also a ColVindex. Otherwise, it's -1.
	ColVindexNum int
}

// BuildVSchema builds a VSchema from a VSchemaFormal.
func BuildVSchema(source *VSchemaFormal) (vschema *VSchema, err error) {
	vschema = &VSchema{
		tables:    make(map[string]*Table),
		Keyspaces: make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(source, vschema)
	err = buildTables(source, vschema)
	if err != nil {
		return nil, err
	}
	err = resolveAutoinc(source, vschema)
	if err != nil {
		return nil, err
	}
	return vschema, nil
}

// VSchemaFormalForKeyspace returns a VSchemaFormal for the single keyspace
// based on the JSON input.
func VSchemaFormalForKeyspace(input []byte, name string) (*VSchemaFormal, error) {
	var ks KeyspaceFormal
	if err := json.Unmarshal(input, &ks); err != nil {
		return nil, fmt.Errorf("Unmarshal failed: %v %s %v", ks, input, err)
	}

	return &VSchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			name: ks,
		},
	}, nil
}

// ValidateVSchema ensures that the the JSON representation
// of the keyspace vschema are valid.
// External references (like sequence) are not validated.
func ValidateVSchema(input []byte) error {
	formal, err := VSchemaFormalForKeyspace(input, "ks")
	if err != nil {
		return err
	}
	// We go through the motion of building the vschema,
	// but just for this keyspace
	vschema := &VSchema{
		tables:    make(map[string]*Table),
		Keyspaces: make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(formal, vschema)
	return buildTables(formal, vschema)
}

func buildKeyspaces(source *VSchemaFormal, vschema *VSchema) {
	for ksname, ks := range source.Keyspaces {
		vschema.Keyspaces[ksname] = &KeyspaceSchema{
			Keyspace: &Keyspace{
				Name:    ksname,
				Sharded: ks.Sharded,
			},
			Tables: make(map[string]*Table),
		}
	}
}

func buildTables(source *VSchemaFormal, vschema *VSchema) error {
	for ksname, ks := range source.Keyspaces {
		keyspace := vschema.Keyspaces[ksname].Keyspace
		vindexes := make(map[string]Vindex)
		for vname, vindexInfo := range ks.Vindexes {
			vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
			if err != nil {
				return err
			}
			switch vindex.(type) {
			case Unique:
			case NonUnique:
			default:
				return fmt.Errorf("vindex %s needs to be Unique or NonUnique", vname)
			}
			vindexes[vname] = vindex
		}
		for tname, table := range ks.Tables {
			t := &Table{
				Name:     tname,
				Keyspace: keyspace,
			}
			if _, ok := vschema.tables[tname]; ok {
				vschema.tables[tname] = nil
			} else {
				vschema.tables[tname] = t
			}
			vschema.Keyspaces[ksname].Tables[tname] = t
			if table.Type == "Sequence" {
				t.IsSequence = true
			}
			for i, ind := range table.ColVindexes {
				vindexInfo, ok := ks.Vindexes[ind.Name]
				if !ok {
					return fmt.Errorf("vindex %s not found for table %s", ind.Name, tname)
				}
				vindex := vindexes[ind.Name]
				owned := false
				if _, ok := vindex.(Lookup); ok && vindexInfo.Owner == tname {
					owned = true
				}
				columnVindex := &ColVindex{
					Col:    strings.ToLower(ind.Col),
					Type:   vindexInfo.Type,
					Name:   ind.Name,
					Owned:  owned,
					Vindex: vindex,
				}
				if i == 0 {
					// Perform Primary vindex check.
					if _, ok := columnVindex.Vindex.(Unique); !ok {
						return fmt.Errorf("primary vindex %s is not Unique for table %s", ind.Name, tname)
					}
					if owned {
						return fmt.Errorf("primary vindex %s cannot be owned for table %s", ind.Name, tname)
					}
				}
				t.ColVindexes = append(t.ColVindexes, columnVindex)
				if owned {
					t.Owned = append(t.Owned, columnVindex)
				}
			}
			t.Ordered = colVindexSorted(t.ColVindexes)
		}
	}
	return nil
}

func resolveAutoinc(source *VSchemaFormal, vschema *VSchema) error {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		for tname, table := range ks.Tables {
			t := ksvschema.Tables[tname]
			if table.Autoinc == nil {
				continue
			}
			t.Autoinc = &Autoinc{Col: table.Autoinc.Col, ColVindexNum: -1}
			seq := vschema.tables[table.Autoinc.Sequence]
			// TODO(sougou): improve this search.
			if seq == nil {
				return fmt.Errorf("sequence %s not found for table %s", table.Autoinc.Sequence, tname)
			}
			t.Autoinc.Sequence = seq
			for i, cv := range t.ColVindexes {
				if t.Autoinc.Col == cv.Col {
					t.Autoinc.ColVindexNum = i
					break
				}
			}
		}
	}
	return nil
}

// Find returns a pointer to the Table. If a keyspace is specified, only tables
// from that keyspace are searched. If the specified keyspace is unsharded
// and no tables matched, it's considered valid: Find will construct a table
// of that name and return it. If no kesypace is specified, then a table is returned
// only if its name is unique across all keyspaces. If there is only one
// keyspace in the vschema, and it's unsharded, then all table requests are considered
// valid and belonging to that keyspace.
func (vschema *VSchema) Find(keyspace, tablename string) (table *Table, err error) {
	if keyspace == "" {
		table, ok := vschema.tables[tablename]
		if table == nil {
			if ok {
				return nil, fmt.Errorf("ambiguous table reference: %s", tablename)
			}
			if len(vschema.Keyspaces) != 1 {
				return nil, fmt.Errorf("table %s not found", tablename)
			}
			// Loop happens only once.
			for _, ks := range vschema.Keyspaces {
				if ks.Keyspace.Sharded {
					return nil, fmt.Errorf("table %s not found", tablename)
				}
				return &Table{Name: tablename, Keyspace: ks.Keyspace}, nil
			}
		}
		return table, nil
	}
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace %s not found in vschema", keyspace)
	}
	table = ks.Tables[tablename]
	if table == nil {
		if ks.Keyspace.Sharded {
			return nil, fmt.Errorf("table %s not found", tablename)
		}
		return &Table{Name: tablename, Keyspace: ks.Keyspace}, nil
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
	Type        string
	ColVindexes []ColVindexFormal
	Autoinc     *AutoincFormal
}

// ColVindexFormal is the info for each indexed column
// of a table as loaded from the source.
type ColVindexFormal struct {
	Col  string
	Name string
}

// AutoincFormal represents the JSON format for auto-inc.
type AutoincFormal struct {
	Col      string
	Sequence string
}

// LoadFormal loads the JSON representation of VSchema from a file.
func LoadFormal(filename string) (*VSchemaFormal, error) {
	formal := &VSchemaFormal{}
	if filename == "" {
		return formal, nil
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, formal)
	if err != nil {
		return nil, err
	}
	return formal, nil
}
