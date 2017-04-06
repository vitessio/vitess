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

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// VSchema represents the denormalized version of SrvVSchema,
// used for building routing plans.
type VSchema struct {
	tables    map[string]*Table
	Keyspaces map[string]*KeyspaceSchema `json:"keyspaces"`
}

// Table represents a table in VSchema.
type Table struct {
	IsSequence     bool                 `json:"is_sequence,omitempty"`
	Name           sqlparser.TableIdent `json:"name"`
	Keyspace       *Keyspace            `json:"-"`
	ColumnVindexes []*ColumnVindex      `json:"column_vindexes,omitempty"`
	Ordered        []*ColumnVindex      `json:"ordered,omitempty"`
	Owned          []*ColumnVindex      `json:"owned,omitempty"`
	AutoIncrement  *AutoIncrement       `json:"auto_increment,omitempty"`
}

// Keyspace contains the keyspcae info for each Table.
type Keyspace struct {
	Name    string
	Sharded bool
}

// ColumnVindex contains the index info for each index of a table.
type ColumnVindex struct {
	Column sqlparser.ColIdent `json:"column"`
	Type   string             `json:"type"`
	Name   string             `json:"name"`
	Owned  bool               `json:"owned,omitempty"`
	Vindex Vindex             `json:"vindex"`
}

// KeyspaceSchema contains the schema(table) for a keyspace.
type KeyspaceSchema struct {
	Keyspace *Keyspace
	Tables   map[string]*Table
}

// MarshalJSON returns a JSON representation of KeyspaceSchema.
func (ks *KeyspaceSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Sharded bool              `json:"sharded,omitempty"`
		Tables  map[string]*Table `json:"tables,omitempty"`
	}{
		Sharded: ks.Keyspace.Sharded,
		Tables:  ks.Tables,
	})
}

// AutoIncrement contains the auto-inc information for a table.
type AutoIncrement struct {
	Column   sqlparser.ColIdent `json:"column"`
	Sequence *Table             `json:"sequence"`
	// ColumnVindexNum is the index of the ColumnVindex
	// if the column is also a ColumnVindex. Otherwise, it's -1.
	ColumnVindexNum int `json:"column_vindex_num"`
}

// BuildVSchema builds a VSchema from a SrvVSchema.
func BuildVSchema(source *vschemapb.SrvVSchema) (vschema *VSchema, err error) {
	vschema = &VSchema{
		tables:    make(map[string]*Table),
		Keyspaces: make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(source, vschema)
	err = buildTables(source, vschema)
	if err != nil {
		return nil, err
	}
	err = resolveAutoIncrement(source, vschema)
	if err != nil {
		return nil, err
	}
	return vschema, nil
}

// BuildKeyspaceSchema builds the vschema portion for one keyspace.
// The build ignores sequence references because those dependencies can
// go cross-keyspace.
func BuildKeyspaceSchema(input *vschemapb.Keyspace, keyspace string) (*KeyspaceSchema, error) {
	if input == nil {
		input = &vschemapb.Keyspace{}
	}
	formal := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			keyspace: input,
		},
	}
	vschema := &VSchema{
		tables:    make(map[string]*Table),
		Keyspaces: make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(formal, vschema)
	err := buildTables(formal, vschema)
	if err != nil {
		return nil, err
	}
	return vschema.Keyspaces[keyspace], nil
}

// ValidateKeyspace ensures that the keyspace vschema is valid.
// External references (like sequence) are not validated.
func ValidateKeyspace(input *vschemapb.Keyspace) error {
	_, err := BuildKeyspaceSchema(input, "")
	return err
}

func buildKeyspaces(source *vschemapb.SrvVSchema, vschema *VSchema) {
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

func buildTables(source *vschemapb.SrvVSchema, vschema *VSchema) error {
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
				Name:     sqlparser.NewTableIdent(tname),
				Keyspace: keyspace,
			}
			if _, ok := vschema.tables[tname]; ok {
				vschema.tables[tname] = nil
			} else {
				vschema.tables[tname] = t
			}
			vschema.Keyspaces[ksname].Tables[tname] = t
			if table.Type == "sequence" {
				t.IsSequence = true
			}
			if keyspace.Sharded && len(table.ColumnVindexes) == 0 {
				return fmt.Errorf("missing primary col vindex for table: %s", tname)
			}
			for i, ind := range table.ColumnVindexes {
				vindexInfo, ok := ks.Vindexes[ind.Name]
				if !ok {
					return fmt.Errorf("vindex %s not found for table %s", ind.Name, tname)
				}
				vindex := vindexes[ind.Name]
				owned := false
				if _, ok := vindex.(Lookup); ok && vindexInfo.Owner == tname {
					owned = true
				}
				columnVindex := &ColumnVindex{
					Column: sqlparser.NewColIdent(ind.Column),
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
				t.ColumnVindexes = append(t.ColumnVindexes, columnVindex)
				if owned {
					t.Owned = append(t.Owned, columnVindex)
				}
			}
			t.Ordered = colVindexSorted(t.ColumnVindexes)
		}
	}
	return nil
}

func resolveAutoIncrement(source *vschemapb.SrvVSchema, vschema *VSchema) error {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		for tname, table := range ks.Tables {
			t := ksvschema.Tables[tname]
			if table.AutoIncrement == nil {
				continue
			}
			t.AutoIncrement = &AutoIncrement{Column: sqlparser.NewColIdent(table.AutoIncrement.Column), ColumnVindexNum: -1}
			seq, err := vschema.findQualified(table.AutoIncrement.Sequence)
			if err != nil {
				return fmt.Errorf("cannot resolve sequence %s: %v", table.AutoIncrement.Sequence, err)
			}
			t.AutoIncrement.Sequence = seq
			for i, cv := range t.ColumnVindexes {
				if t.AutoIncrement.Column.Equal(cv.Column) {
					t.AutoIncrement.ColumnVindexNum = i
					break
				}
			}
		}
	}
	return nil
}

// findQualified finds a table t or k.t.
func (vschema *VSchema) findQualified(name string) (*Table, error) {
	splits := strings.Split(name, ".")
	switch len(splits) {
	case 1:
		return vschema.Find("", splits[0])
	case 2:
		return vschema.Find(splits[0], splits[1])
	}
	return nil, fmt.Errorf("table %s not found", name)
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
				return &Table{Name: sqlparser.NewTableIdent(tablename), Keyspace: ks.Keyspace}, nil
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
		return &Table{Name: sqlparser.NewTableIdent(tablename), Keyspace: ks.Keyspace}, nil
	}
	return table, nil
}

// ByCost provides the interface needed for ColumnVindexes to
// be sorted by cost order.
type ByCost []*ColumnVindex

func (bc ByCost) Len() int           { return len(bc) }
func (bc ByCost) Swap(i, j int)      { bc[i], bc[j] = bc[j], bc[i] }
func (bc ByCost) Less(i, j int) bool { return bc[i].Vindex.Cost() < bc[j].Vindex.Cost() }

func colVindexSorted(cvs []*ColumnVindex) (sorted []*ColumnVindex) {
	for _, cv := range cvs {
		sorted = append(sorted, cv)
	}
	sort.Sort(ByCost(sorted))
	return sorted
}

// LoadFormal loads the JSON representation of VSchema from a file.
func LoadFormal(filename string) (*vschemapb.SrvVSchema, error) {
	formal := &vschemapb.SrvVSchema{}
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

// LoadFormalKeyspace loads the JSON representation of VSchema from a file,
// for a single keyspace.
func LoadFormalKeyspace(filename string) (*vschemapb.Keyspace, error) {
	formal := &vschemapb.Keyspace{}
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
