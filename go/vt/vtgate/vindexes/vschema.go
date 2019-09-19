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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// TabletTypeSuffix maps the tablet type to its suffix string.
var TabletTypeSuffix = map[topodatapb.TabletType]string{
	0: "@unknown",
	1: "@master",
	2: "@replica",
	3: "@rdonly",
	4: "@spare",
	5: "@experimental",
	6: "@backup",
	7: "@restore",
	8: "@drained",
}

// The following constants represent table types.
const (
	TypeSequence  = "sequence"
	TypeReference = "reference"
)

// VSchema represents the denormalized version of SrvVSchema,
// used for building routing plans.
type VSchema struct {
	RoutingRules   map[string]*RoutingRule `json:"routing_rules"`
	uniqueTables   map[string]*Table
	uniqueVindexes map[string]Vindex
	Keyspaces      map[string]*KeyspaceSchema `json:"keyspaces"`
}

// RoutingRule represents one routing rule.
type RoutingRule struct {
	Tables []*Table
	Error  error
}

// MarshalJSON returns a JSON representation of Column.
func (rr *RoutingRule) MarshalJSON() ([]byte, error) {
	if rr.Error != nil {
		return json.Marshal(rr.Error.Error())
	}
	tables := make([]string, 0, len(rr.Tables))
	for _, t := range rr.Tables {
		tables = append(tables, t.Keyspace.Name+"."+t.Name.String())
	}
	return json.Marshal(tables)
}

// Table represents a table in VSchema.
type Table struct {
	Type                    string               `json:"type,omitempty"`
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
		RoutingRules:   make(map[string]*RoutingRule),
		uniqueTables:   make(map[string]*Table),
		uniqueVindexes: make(map[string]Vindex),
		Keyspaces:      make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(source, vschema)
	resolveAutoIncrement(source, vschema)
	addDual(vschema)
	buildRoutingRule(source, vschema)
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
		ksvschema := &KeyspaceSchema{
			Keyspace: &Keyspace{
				Name:    ksname,
				Sharded: ks.Sharded,
			},
			Tables:   make(map[string]*Table),
			Vindexes: make(map[string]Vindex),
		}
		vschema.Keyspaces[ksname] = ksvschema
		ksvschema.Error = buildTables(ks, vschema, ksvschema)
	}
}

func buildTables(ks *vschemapb.Keyspace, vschema *VSchema, ksvschema *KeyspaceSchema) error {
	keyspace := ksvschema.Keyspace
	for vname, vindexInfo := range ks.Vindexes {
		vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
		if err != nil {
			return err
		}
		// SNAPSHOT keyspaces are excluded from global routing.
		// We only have 2 keyspace types as of now, NORMAL and SNAPSHOT.
		// We check for NORMAL here so that if a new keyspace type is
		// introduced we will need to explicitly decide to include it
		// in the global routing rather than have it happen by default.
		if ks.KeyspaceType == topodatapb.KeyspaceType_NORMAL {
			if _, ok := vschema.uniqueVindexes[vname]; ok {
				vschema.uniqueVindexes[vname] = nil
			} else {
				vschema.uniqueVindexes[vname] = vindex
			}
		}
		ksvschema.Vindexes[vname] = vindex
	}
	for tname, table := range ks.Tables {
		t := &Table{
			Name:                    sqlparser.NewTableIdent(tname),
			Keyspace:                keyspace,
			ColumnListAuthoritative: table.ColumnListAuthoritative,
		}
		switch table.Type {
		case "", TypeReference:
			t.Type = table.Type
		case TypeSequence:
			if keyspace.Sharded && table.Pinned == "" {
				return fmt.Errorf("sequence table has to be in an unsharded keyspace or must be pinned: %s", tname)
			}
			t.Type = table.Type
		default:
			return fmt.Errorf("unidentified table type %s", table.Type)
		}
		if table.Pinned != "" {
			decoded, err := hex.DecodeString(table.Pinned)
			if err != nil {
				return fmt.Errorf("could not decode the keyspace id for pin: %v", err)
			}
			t.Pinned = decoded
		}

		// If keyspace is sharded, then any table that's not a reference or pinned must have vindexes.
		if keyspace.Sharded && t.Type != TypeReference && table.Pinned == "" && len(table.ColumnVindexes) == 0 {
			return fmt.Errorf("missing primary col vindex for table: %s", tname)
		}

		// Initialize Columns.
		colNames := make(map[string]bool)
		for _, col := range table.Columns {
			name := sqlparser.NewColIdent(col.Name)
			if colNames[name.Lowered()] {
				return fmt.Errorf("duplicate column name '%v' for table: %s", name, tname)
			}
			colNames[name.Lowered()] = true
			t.Columns = append(t.Columns, Column{Name: name, Type: col.Type})
		}

		// Initialize ColumnVindexes.
		for i, ind := range table.ColumnVindexes {
			vindexInfo, ok := ks.Vindexes[ind.Name]
			if !ok {
				return fmt.Errorf("vindex %s not found for table %s", ind.Name, tname)
			}
			vindex := ksvschema.Vindexes[ind.Name]
			owned := false
			if _, ok := vindex.(Lookup); ok && vindexInfo.Owner == tname {
				owned = true
			}
			var columns []sqlparser.ColIdent
			if ind.Column != "" {
				if len(ind.Columns) > 0 {
					return fmt.Errorf("can't use column and columns at the same time in vindex (%s) and table (%s)", ind.Name, tname)
				}
				columns = []sqlparser.ColIdent{sqlparser.NewColIdent(ind.Column)}
			} else {
				if len(ind.Columns) == 0 {
					return fmt.Errorf("must specify at least one column for vindex (%s) and table (%s)", ind.Name, tname)
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
					return fmt.Errorf("primary vindex %s is not Unique for table %s", ind.Name, tname)
				}
				if owned {
					return fmt.Errorf("primary vindex %s cannot be owned for table %s", ind.Name, tname)
				}
			}
			t.ColumnVindexes = append(t.ColumnVindexes, columnVindex)
			if owned {
				if setter, ok := vindex.(WantOwnerInfo); ok {
					if err := setter.SetOwnerInfo(keyspace.Name, tname, columns); err != nil {
						return err
					}
				}
				t.Owned = append(t.Owned, columnVindex)
			}
		}
		t.Ordered = colVindexSorted(t.ColumnVindexes)

		// Add the table to the map entries.
		// SNAPSHOT keyspaces are excluded from global routing.
		if ks.KeyspaceType == topodatapb.KeyspaceType_NORMAL {
			if _, ok := vschema.uniqueTables[tname]; ok {
				vschema.uniqueTables[tname] = nil
			} else {
				vschema.uniqueTables[tname] = t
			}
		}
		ksvschema.Tables[tname] = t
	}
	return nil
}

func resolveAutoIncrement(source *vschemapb.SrvVSchema, vschema *VSchema) {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		for tname, table := range ks.Tables {
			t := ksvschema.Tables[tname]
			if t == nil || table.AutoIncrement == nil {
				continue
			}
			t.AutoIncrement = &AutoIncrement{Column: sqlparser.NewColIdent(table.AutoIncrement.Column)}
			seq, err := vschema.findQualified(table.AutoIncrement.Sequence)
			if err != nil {
				ksvschema.Error = fmt.Errorf("cannot resolve sequence %s: %v", table.AutoIncrement.Sequence, err)
				continue
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

func buildRoutingRule(source *vschemapb.SrvVSchema, vschema *VSchema) {
	if source.RoutingRules == nil {
		return
	}
outer:
	for _, rule := range source.RoutingRules.Rules {
		rr := &RoutingRule{}
		for _, toTable := range rule.ToTables {
			if _, ok := vschema.RoutingRules[rule.FromTable]; ok {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: fmt.Errorf("duplicate rule for entry %s", rule.FromTable),
				}
				continue outer
			}
			parts := strings.Split(toTable, ".")
			if len(parts) != 2 {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: fmt.Errorf("table %s must be qualified", toTable),
				}
				continue outer
			}
			t, err := vschema.FindTable(parts[0], parts[1])
			if err != nil {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: err,
				}
				continue outer
			}
			for _, existing := range rr.Tables {
				if existing == t {
					vschema.RoutingRules[rule.FromTable] = &RoutingRule{
						Error: fmt.Errorf("table %s specified more than once", toTable),
					}
					continue outer
				}
			}
			rr.Tables = append(rr.Tables, t)
		}
		vschema.RoutingRules[rule.FromTable] = rr
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
// FindTable bypasses routing rules and returns at most one table.
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

func (vschema *VSchema) findTables(keyspace, tablename string, tabletType topodatapb.TabletType) ([]*Table, error) {
	qualified := tablename
	if keyspace != "" {
		qualified = keyspace + "." + tablename
	}
	fqtn := qualified + TabletTypeSuffix[tabletType]
	// First look for a fully qualified table name: ks.t@master.
	// Then look for one without tablet type: ks.t.
	for _, name := range []string{fqtn, qualified} {
		rr, ok := vschema.RoutingRules[name]
		if ok {
			if rr.Error != nil {
				return nil, rr.Error
			}
			if len(rr.Tables) == 0 {
				return nil, fmt.Errorf("table %s has been disabled", tablename)
			}
			return rr.Tables, nil
		}
	}
	t, err := vschema.findTable(keyspace, tablename)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	return []*Table{t}, nil
}

// FindTablesOrVindex finds a table or a Vindex by name using Find and FindVindex.
func (vschema *VSchema) FindTablesOrVindex(keyspace, name string, tabletType topodatapb.TabletType) ([]*Table, Vindex, error) {
	tables, err := vschema.findTables(keyspace, name, tabletType)
	if err != nil {
		return nil, nil, err
	}
	if tables != nil {
		return tables, nil, nil
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
	sorted = append(sorted, cvs...)
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
