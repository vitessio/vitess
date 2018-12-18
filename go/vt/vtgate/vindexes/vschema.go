/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// VSchema represents the denormalized version of SrvVSchema,
// used for building routing plans.
type VSchema struct {
	uniqueTables   map[string]*Table
	uniqueVindexes map[string]Vindex
	Keyspaces      map[string]*KeyspaceSchema `json:"keyspaces"`
}

// Table represents a table in VSchema.
type Table struct {
	IsSequence              bool                 `json:"is_sequence,omitempty"`
	Name                    sqlparser.TableIdent `json:"name"`
	Keyspace                *Keyspace            `json:"-"`
	ColumnVindexes          []*ColumnVindex      `json:"column_vindexes,omitempty"`
	Ordered                 []*ColumnVindex      `json:"ordered,omitempty"`
	Owned                   []*ColumnVindex      `json:"owned,omitempty"`
	AutoIncrement           *AutoIncrement       `json:"auto_increment,omitempty"`
	Columns                 []Column             `json:"columns,omitempty"`
	Pinned                  []byte               `json:"pinned,omitempty"`
	ColumnListAuthoritative bool                 `json:"column_list_authoritative,omitempty"`
}

// Keyspace contains the keyspcae info for each Table.
type Keyspace struct {
	Name    string
	Sharded bool
}

// ColumnVindex contains the index info for each index of a table.
type ColumnVindex struct {
	Columns []sqlparser.ColIdent `json:"columns"`
	Type    string               `json:"type"`
	Name    string               `json:"name"`
	Owned   bool                 `json:"owned,omitempty"`
	Vindex  Vindex               `json:"vindex"`
}

// Column describes a column.
type Column struct {
	Name sqlparser.ColIdent `json:"name"`
	Type querypb.Type       `json:"type"`
}

// MarshalJSON returns a JSON representation of Column.
func (col *Column) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name string `json:"name"`
		Type string `json:"type,omitempty"`
	}{
		Name: col.Name.String(),
		Type: querypb.Type_name[int32(col.Type)],
	})
}

// KeyspaceSchema contains the schema(table) for a keyspace.
type KeyspaceSchema struct {
	Keyspace *Keyspace
	Tables   map[string]*Table
	Vindexes map[string]Vindex
	Error    error
}

// MarshalJSON returns a JSON representation of KeyspaceSchema.
func (ks *KeyspaceSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Sharded  bool              `json:"sharded,omitempty"`
		Tables   map[string]*Table `json:"tables,omitempty"`
		Vindexes map[string]Vindex `json:"vindexes,omitempty"`
		Error    string            `json:"error,omitempty"`
	}{
		Sharded:  ks.Keyspace.Sharded,
		Tables:   ks.Tables,
		Vindexes: ks.Vindexes,
		Error: func(ks *KeyspaceSchema) string {
			if ks.Error == nil {
				return ""
			}
			return ks.Error.Error()
		}(ks),
	})
}

// AutoIncrement contains the auto-inc information for a table.
type AutoIncrement struct {
	Column   sqlparser.ColIdent `json:"column"`
	Sequence *Table             `json:"sequence"`
}

// BuildVSchema builds a VSchema from a SrvVSchema.
func BuildVSchema(source *vschemapb.SrvVSchema) (vschema *VSchema, err error) {
	vschema = &VSchema{
		uniqueTables:   make(map[string]*Table),
		uniqueVindexes: make(map[string]Vindex),
		Keyspaces:      make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(source, vschema)
	buildTables(source, vschema)
	resolveAutoIncrement(source, vschema)
	addDual(vschema)
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
		uniqueTables:   make(map[string]*Table),
		uniqueVindexes: make(map[string]Vindex),
		Keyspaces:      make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(formal, vschema)
	buildTables(formal, vschema)
	err := vschema.Keyspaces[keyspace].Error
	return vschema.Keyspaces[keyspace], err
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
			Tables:   make(map[string]*Table),
			Vindexes: make(map[string]Vindex),
		}
	}
}

func buildTables(source *vschemapb.SrvVSchema, vschema *VSchema) {
outer:
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		keyspace := ksvschema.Keyspace
		for vname, vindexInfo := range ks.Vindexes {
			vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
			if err != nil {
				ksvschema.Error = err
				continue outer
			}
			if _, ok := vschema.uniqueVindexes[vname]; ok {
				vschema.uniqueVindexes[vname] = nil
			} else {
				vschema.uniqueVindexes[vname] = vindex
			}
			vschema.Keyspaces[ksname].Vindexes[vname] = vindex
		}
		for tname, table := range ks.Tables {
			t := &Table{
				Name:                    sqlparser.NewTableIdent(tname),
				Keyspace:                keyspace,
				ColumnListAuthoritative: table.ColumnListAuthoritative,
			}
			if _, ok := vschema.uniqueTables[tname]; ok {
				vschema.uniqueTables[tname] = nil
			} else {
				vschema.uniqueTables[tname] = t
			}
			vschema.Keyspaces[ksname].Tables[tname] = t
			if table.Type == "sequence" {
				t.IsSequence = true
			}
			if table.Pinned != "" {
				decoded, err := hex.DecodeString(table.Pinned)
				if err != nil {
					ksvschema.Error = fmt.Errorf("could not decode the keyspace id for pin: %v", err)
					continue outer
				}
				t.Pinned = decoded
			} else if keyspace.Sharded && len(table.ColumnVindexes) == 0 {
				ksvschema.Error = fmt.Errorf("missing primary col vindex for table: %s", tname)
				continue outer
			}

			// Initialize Columns.
			colNames := make(map[string]bool)
			for _, col := range table.Columns {
				name := sqlparser.NewColIdent(col.Name)
				if colNames[name.Lowered()] {
					ksvschema.Error = fmt.Errorf("duplicate column name '%v' for table: %s", name, tname)
					continue outer
				}
				colNames[name.Lowered()] = true
				t.Columns = append(t.Columns, Column{Name: name, Type: col.Type})
			}

			// Initialize ColumnVindexes.
			for i, ind := range table.ColumnVindexes {
				vindexInfo, ok := ks.Vindexes[ind.Name]
				if !ok {
					ksvschema.Error = fmt.Errorf("vindex %s not found for table %s", ind.Name, tname)
					continue outer
				}
				vindex := vschema.Keyspaces[ksname].Vindexes[ind.Name]
				owned := false
				if _, ok := vindex.(Lookup); ok && vindexInfo.Owner == tname {
					owned = true
				}
				var columns []sqlparser.ColIdent
				if ind.Column != "" {
					if len(ind.Columns) > 0 {
						ksvschema.Error = fmt.Errorf("can't use column and columns at the same time in vindex (%s) and table (%s)", ind.Name, tname)
						continue outer
					}
					columns = []sqlparser.ColIdent{sqlparser.NewColIdent(ind.Column)}
				} else {
					if len(ind.Columns) == 0 {
						ksvschema.Error = fmt.Errorf("must specify at least one column for vindex (%s) and table (%s)", ind.Name, tname)
						continue outer
					}
					for _, indCol := range ind.Columns {
						columns = append(columns, sqlparser.NewColIdent(indCol))
					}
				}
				columnVindex := &ColumnVindex{
					Columns: columns,
					Type:    vindexInfo.Type,
					Name:    ind.Name,
					Owned:   owned,
					Vindex:  vindex,
				}
				if i == 0 {
					// Perform Primary vindex check.
					if !columnVindex.Vindex.IsUnique() {
						ksvschema.Error = fmt.Errorf("primary vindex %s is not Unique for table %s", ind.Name, tname)
						continue outer
					}
					if owned {
						ksvschema.Error = fmt.Errorf("primary vindex %s cannot be owned for table %s", ind.Name, tname)
						continue outer
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
}

func resolveAutoIncrement(source *vschemapb.SrvVSchema, vschema *VSchema) {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		for tname, table := range ks.Tables {
			t := ksvschema.Tables[tname]
			if table.AutoIncrement == nil {
				continue
			}
			t.AutoIncrement = &AutoIncrement{Column: sqlparser.NewColIdent(table.AutoIncrement.Column)}
			seq, err := vschema.findQualified(table.AutoIncrement.Sequence)
			if err != nil {
				ksvschema.Error = fmt.Errorf("cannot resolve sequence %s: %v", table.AutoIncrement.Sequence, err)
				break
			}
			t.AutoIncrement.Sequence = seq
		}
	}
}

// addDual adds dual as a valid table to all keyspaces.
// For sharded keyspaces, it gets pinned against keyspace id '0x00'.
func addDual(vschema *VSchema) {
	first := ""
	for ksname, ks := range vschema.Keyspaces {
		t := &Table{
			Name:     sqlparser.NewTableIdent("dual"),
			Keyspace: ks.Keyspace,
		}
		if ks.Keyspace.Sharded {
			t.Pinned = []byte{0}
		}
		ks.Tables["dual"] = t
		if first == "" || first > ksname {
			// In case of a reference to dual that's not qualified
			// by keyspace, we still want to resolve it to one of
			// the keyspaces. For consistency, we'll always use the
			// first keyspace by lexical ordering.
			first = ksname
			vschema.uniqueTables["dual"] = t
		}
	}
}

// findQualified finds a table t or k.t.
func (vschema *VSchema) findQualified(name string) (*Table, error) {
	splits := strings.Split(name, ".")
	switch len(splits) {
	case 1:
		return vschema.FindTable("", splits[0])
	case 2:
		return vschema.FindTable(splits[0], splits[1])
	}
	return nil, fmt.Errorf("table %s not found", name)
}

// FindTable returns a pointer to the Table. If a keyspace is specified, only tables
// from that keyspace are searched. If the specified keyspace is unsharded
// and no tables matched, it's considered valid: FindTable will construct a table
// of that name and return it. If no kesypace is specified, then a table is returned
// only if its name is unique across all keyspaces. If there is only one
// keyspace in the vschema, and it's unsharded, then all table requests are considered
// valid and belonging to that keyspace.
func (vschema *VSchema) FindTable(keyspace, tablename string) (*Table, error) {
	t, err := vschema.findTable(keyspace, tablename)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, fmt.Errorf("table %s not found", tablename)
	}
	return t, nil
}

// findTable is like FindTable, but does not return an error if a table is not found.
func (vschema *VSchema) findTable(keyspace, tablename string) (*Table, error) {
	if keyspace == "" {
		table, ok := vschema.uniqueTables[tablename]
		if table == nil {
			if ok {
				return nil, fmt.Errorf("ambiguous table reference: %s", tablename)
			}
			if len(vschema.Keyspaces) != 1 {
				return nil, nil
			}
			// Loop happens only once.
			for _, ks := range vschema.Keyspaces {
				if ks.Keyspace.Sharded {
					return nil, nil
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
	table := ks.Tables[tablename]
	if table == nil {
		if ks.Keyspace.Sharded {
			return nil, nil
		}
		return &Table{Name: sqlparser.NewTableIdent(tablename), Keyspace: ks.Keyspace}, nil
	}
	return table, nil
}

// FindTableOrVindex finds a table or a Vindex by name using Find and FindVindex.
func (vschema *VSchema) FindTableOrVindex(keyspace, name string) (*Table, Vindex, error) {
	t, err := vschema.findTable(keyspace, name)
	if err != nil {
		return nil, nil, err
	}
	if t != nil {
		return t, nil, nil
	}
	v, err := vschema.FindVindex(keyspace, name)
	if err != nil {
		return nil, nil, err
	}
	if v != nil {
		return nil, v, nil
	}
	return nil, nil, fmt.Errorf("table %s not found", name)
}

// FindVindex finds a vindex by name. If a keyspace is specified, only vindexes
// from that keyspace are searched. If no kesypace is specified, then a vindex
// is returned only if its name is unique across all keyspaces. The function
// returns an error only if the vindex name is ambiguous.
func (vschema *VSchema) FindVindex(keyspace, name string) (Vindex, error) {
	if keyspace == "" {
		vindex, ok := vschema.uniqueVindexes[name]
		if vindex == nil && ok {
			return nil, fmt.Errorf("ambiguous vindex reference: %s", name)
		}
		return vindex, nil
	}
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace %s not found in vschema", keyspace)
	}
	return ks.Vindexes[name], nil
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
	err = json2.Unmarshal(data, formal)
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
	err = json2.Unmarshal(data, formal)
	if err != nil {
		return nil, err
	}
	return formal, nil
}

// FindVindexForSharding searches through the given slice
// to find the lowest cost unique vindex
// primary vindex is always unique
// if two have the same cost, use the one that occurs earlier in the definition
// if the final result is too expensive, return nil
func FindVindexForSharding(tableName string, colVindexes []*ColumnVindex) (*ColumnVindex, error) {
	if len(colVindexes) == 0 {
		return nil, fmt.Errorf("no vindex definition for table %v", tableName)
	}
	result := colVindexes[0]
	for _, colVindex := range colVindexes {
		if colVindex.Vindex.Cost() < result.Vindex.Cost() && colVindex.Vindex.IsUnique() {
			result = colVindex
		}
	}
	if result.Vindex.Cost() > 1 || !result.Vindex.IsUnique() {
		return nil, fmt.Errorf("could not find a vindex to use for sharding table %v", tableName)
	}
	return result, nil
}
