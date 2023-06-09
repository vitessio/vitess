/*
Copyright 2019 The Vitess Authors.

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
	"os"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqlescape"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// TabletTypeSuffix maps the tablet type to its suffix string.
var TabletTypeSuffix = map[topodatapb.TabletType]string{
	0: "@unknown",
	1: "@primary",
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
	RoutingRules map[string]*RoutingRule `json:"routing_rules"`

	// globalTables contains the name of all tables in all keyspaces. If the
	// table is uniquely named, the value will be the qualified Table object
	// with the keyspace where this table exists. If multiple keyspaces have a
	// table with the same name, the value will be a `nil`.
	globalTables      map[string]*Table
	uniqueVindexes    map[string]Vindex
	Keyspaces         map[string]*KeyspaceSchema `json:"keyspaces"`
	ShardRoutingRules map[string]string          `json:"shard_routing_rules"`
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
		tables = append(tables, t.String())
	}

	return json.Marshal(tables)
}

// Table represents a table in VSchema.
type Table struct {
	Type                    string                 `json:"type,omitempty"`
	Name                    sqlparser.IdentifierCS `json:"name"`
	Keyspace                *Keyspace              `json:"-"`
	ColumnVindexes          []*ColumnVindex        `json:"column_vindexes,omitempty"`
	Ordered                 []*ColumnVindex        `json:"ordered,omitempty"`
	Owned                   []*ColumnVindex        `json:"owned,omitempty"`
	AutoIncrement           *AutoIncrement         `json:"auto_increment,omitempty"`
	Columns                 []Column               `json:"columns,omitempty"`
	Pinned                  []byte                 `json:"pinned,omitempty"`
	ColumnListAuthoritative bool                   `json:"column_list_authoritative,omitempty"`
	// ReferencedBy is an inverse mapping of tables in other keyspaces that
	// reference this table via Source.
	//
	// This is useful in route-planning for quickly selecting the optimal route
	// when JOIN-ing a reference table to a sharded table.
	ReferencedBy map[string]*Table `json:"-"`
	// Source is a keyspace-qualified table name that points to the source of a
	// reference table. Only applicable for tables with Type set to "reference".
	Source *Source `json:"source,omitempty"`
}

// Keyspace contains the keyspcae info for each Table.
type Keyspace struct {
	Name    string
	Sharded bool
}

// ColumnVindex contains the index info for each index of a table.
type ColumnVindex struct {
	Columns  []sqlparser.IdentifierCI `json:"columns"`
	Type     string                   `json:"type"`
	Name     string                   `json:"name"`
	Owned    bool                     `json:"owned,omitempty"`
	Vindex   Vindex                   `json:"vindex"`
	isUnique bool
	cost     int
	partial  bool
}

// IsUnique is used to tell whether the ColumnVindex
// will return a unique shard value or not when queried with
// the given column list
func (c *ColumnVindex) IsUnique() bool {
	return c.isUnique
}

// Cost represents the cost associated with using the
// ColumnVindex
func (c *ColumnVindex) Cost() int {
	return c.cost
}

// IsPartialVindex is used to let planner and engine know that this is a composite vindex missing one or more columns
func (c *ColumnVindex) IsPartialVindex() bool {
	return c.partial
}

// Column describes a column.
type Column struct {
	Name          sqlparser.IdentifierCI `json:"name"`
	Type          querypb.Type           `json:"type"`
	CollationName string                 `json:"collation_name"`
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
	Views    map[string]sqlparser.SelectStatement
	Error    error
}

type ksJSON struct {
	Sharded  bool              `json:"sharded,omitempty"`
	Tables   map[string]*Table `json:"tables,omitempty"`
	Vindexes map[string]Vindex `json:"vindexes,omitempty"`
	Views    map[string]string `json:"views,omitempty"`
	Error    string            `json:"error,omitempty"`
}

// findTable looks for the table with the requested tablename in the keyspace.
//
// If no table matching the requested tablename is found, and:
//
//   - constructUnshardedIfNotFound is not requested, than no table is returned.
//   - constructUnshardedIfNotFound is requested, and the keyspace is
//     unsharded, then a *Table representing that table is returned.
func (ks *KeyspaceSchema) findTable(
	tablename string,
	constructUnshardedIfNotFound bool,
) *Table {
	table := ks.Tables[tablename]
	if table != nil {
		return table
	}

	if constructUnshardedIfNotFound && !ks.Keyspace.Sharded {
		return &Table{Name: sqlparser.NewIdentifierCS(tablename), Keyspace: ks.Keyspace}
	}

	return nil
}

// MarshalJSON returns a JSON representation of KeyspaceSchema.
func (ks *KeyspaceSchema) MarshalJSON() ([]byte, error) {
	ksJ := ksJSON{
		Sharded:  ks.Keyspace.Sharded,
		Tables:   ks.Tables,
		Vindexes: ks.Vindexes,
	}
	if ks.Error != nil {
		ksJ.Error = ks.Error.Error()
	}
	if len(ks.Views) > 0 {
		ksJ.Views = make(map[string]string, len(ks.Views))
	}
	for view, def := range ks.Views {
		ksJ.Views[view] = sqlparser.String(def)
	}

	return json.Marshal(ksJ)
}

// AutoIncrement contains the auto-inc information for a table.
type AutoIncrement struct {
	Column   sqlparser.IdentifierCI `json:"column"`
	Sequence *Table                 `json:"sequence"`
}

type Source struct {
	sqlparser.TableName
}

func (source *Source) String() string {
	buf := sqlparser.NewTrackedBuffer(nil)
	source.Format(buf)
	return buf.String()
}

// BuildVSchema builds a VSchema from a SrvVSchema.
func BuildVSchema(source *vschemapb.SrvVSchema) (vschema *VSchema) {
	vschema = &VSchema{
		RoutingRules:   make(map[string]*RoutingRule),
		globalTables:   make(map[string]*Table),
		uniqueVindexes: make(map[string]Vindex),
		Keyspaces:      make(map[string]*KeyspaceSchema),
	}
	buildKeyspaces(source, vschema)
	// buildGlobalTables before buildReferences so that buildReferences can
	// resolve sources which reference global tables.
	buildGlobalTables(source, vschema)
	buildReferences(source, vschema)
	resolveAutoIncrement(source, vschema)
	buildRoutingRule(source, vschema)
	buildShardRoutingRule(source, vschema)
	return vschema
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
		globalTables:   make(map[string]*Table),
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

func (vschema *VSchema) AddView(ksname string, viewName, query string) error {
	ks, ok := vschema.Keyspaces[ksname]
	if !ok {
		return fmt.Errorf("keyspace %s not found in vschema", ksname)
	}
	ast, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}
	selectStmt, ok := ast.(sqlparser.SelectStatement)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected SELECT or UNION query, got %T", ast)
	}
	if ks.Views == nil {
		ks.Views = make(map[string]sqlparser.SelectStatement)
	}
	ks.Views[viewName] = selectStmt
	t := &Table{
		Type:                    "View",
		Name:                    sqlparser.NewIdentifierCS(viewName),
		Keyspace:                ks.Keyspace,
		ColumnListAuthoritative: true,
	}
	vschema.addTableName(t)
	return nil
}

func buildGlobalTables(source *vschemapb.SrvVSchema, vschema *VSchema) {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		// If the keyspace requires explicit routing, don't include any of
		// its tables in global tables.
		if ks.RequireExplicitRouting {
			continue
		}
		buildKeyspaceGlobalTables(vschema, ksvschema)
	}
}

func buildKeyspaceGlobalTables(vschema *VSchema, ksvschema *KeyspaceSchema) {
	for tname, t := range ksvschema.Tables {
		if gt, ok := vschema.globalTables[tname]; ok {
			// There is already an entry table stored in global tables
			// with this name.
			if gt == nil {
				// Table name is already marked ambiguous, nothing to do.
				continue
			} else {
				// Special handling for reference tables which specify their source.
				if t.Type == TypeReference && t.Source != nil {
					// If the reference table points to the already stored
					// global table, there is no ambiguity.
					if t.Source.Qualifier.IsEmpty() || t.Source.Qualifier.String() == gt.Keyspace.Name {
						continue
					}
				}
				// Otherwise, mark this table name ambiguous.
				vschema.globalTables[tname] = nil
			}
		} else {
			// Reference tables which define a source with the same name may be
			// globally routable through their source, as long as the source
			// doesn't require explicit routing.
			if t.Type == TypeReference && t.Source != nil && t.Source.Name.String() == t.Name.String() {
				continue
			}

			vschema.globalTables[tname] = t
		}
	}
}

func buildReferences(source *vschemapb.SrvVSchema, vschema *VSchema) {
	for ksname := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		if err := buildKeyspaceReferences(vschema, ksvschema); err != nil && ksvschema.Error == nil {
			ksvschema.Error = err
		}
	}
}

func buildKeyspaceReferences(vschema *VSchema, ksvschema *KeyspaceSchema) error {
	keyspace := ksvschema.Keyspace
	for tname, t := range ksvschema.Tables {
		source := t.Source

		if t.Type != TypeReference || source == nil {
			continue
		}

		sourceKsname := source.Qualifier.String()
		sourceTname := source.Name.String()

		// Prohibit self-references.
		if sourceKsname == keyspace.Name {
			return vterrors.Errorf(
				vtrpcpb.Code_UNIMPLEMENTED,
				"source %q may not reference a table in the same keyspace as table: %s",
				source,
				tname,
			)
		}

		// Verify the reference source can be resolved.
		sourceT, err := vschema.findTable(
			sourceKsname,
			sourceTname,
			false, /* constructTableIfNotFound */
		)
		// Rephrase errors to be more helpful in the context of VSchema linting.
		if err != nil {
			if vterrors.Code(err) != vtrpcpb.Code_NOT_FOUND || vterrors.ErrState(err) != vterrors.BadDb {
				return err
			}
			return vterrors.Errorf(
				vtrpcpb.Code_NOT_FOUND,
				"source %q references a non-existent keyspace %q",
				source,
				sourceKsname,
			)
		}
		if sourceT == nil {
			return vterrors.Errorf(
				vtrpcpb.Code_NOT_FOUND,
				"source %q references a table %q that is not present in the VSchema of keyspace %q",
				source,
				sourceTname,
				sourceKsname,
			)
		}

		// Validate source table types.
		if !(sourceT.Type == "" || sourceT.Type == TypeReference) {
			return vterrors.Errorf(
				vtrpcpb.Code_UNIMPLEMENTED,
				"source %q may not reference a table of type %q: %s",
				source,
				sourceT.Type,
				tname,
			)
		}

		// Update inverse reference table map.
		if ot := sourceT.getReferenceInKeyspace(keyspace.Name); ot != nil {
			names := []string{ot.Name.String(), tname}
			sort.Strings(names)
			return vterrors.Errorf(
				vtrpcpb.Code_UNIMPLEMENTED,
				"source %q may not be referenced more than once per keyspace: %s, %s",
				source,
				names[0],
				names[1],
			)
		}
		sourceT.addReferenceInKeyspace(keyspace.Name, t)

		// Forbid reference chains. This is not necessarily a technical
		// limitation. If people want this, in theory it should be possible as
		// long as reference chains are not circular.
		if sourceT.Source != nil {
			chain := fmt.Sprintf("%s => %s => %s", tname, sourceT, sourceT.Source)

			return vterrors.Errorf(
				vtrpcpb.Code_UNIMPLEMENTED,
				"reference chaining is not allowed %s: %s",
				chain,
				tname,
			)
		}
	}

	return nil
}

func buildTables(ks *vschemapb.Keyspace, vschema *VSchema, ksvschema *KeyspaceSchema) error {
	keyspace := ksvschema.Keyspace
	for vname, vindexInfo := range ks.Vindexes {
		vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
		if err != nil {
			return err
		}

		// If the keyspace requires explicit routing, don't include its indexes
		// in global routing.
		if !ks.RequireExplicitRouting {
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
			Name:                    sqlparser.NewIdentifierCS(tname),
			Keyspace:                keyspace,
			ColumnListAuthoritative: table.ColumnListAuthoritative,
		}
		switch table.Type {
		case "":
			t.Type = table.Type
		case TypeReference:
			if table.Source != "" {
				tableName, err := parseTable(table.Source)
				if err != nil {
					return vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"invalid source %q for reference table: %s; %v",
						table.Source,
						tname,
						err,
					)
				}
				t.Source = &Source{TableName: tableName}
			}
			t.Type = table.Type
		case TypeSequence:
			if keyspace.Sharded && table.Pinned == "" {
				return vterrors.Errorf(
					vtrpcpb.Code_FAILED_PRECONDITION,
					"sequence table has to be in an unsharded keyspace or must be pinned: %s",
					tname,
				)
			}
			t.Type = table.Type
		default:
			return vterrors.Errorf(
				vtrpcpb.Code_NOT_FOUND,
				"unidentified table type %s",
				table.Type,
			)
		}
		if table.Pinned != "" {
			decoded, err := hex.DecodeString(table.Pinned)
			if err != nil {
				return vterrors.Errorf(
					vtrpcpb.Code_INVALID_ARGUMENT,
					"could not decode the keyspace id for pin: %v",
					err,
				)
			}
			t.Pinned = decoded
		}

		// If keyspace is sharded, then any table that's not a reference or pinned must have vindexes.
		if keyspace.Sharded && t.Type != TypeReference && table.Pinned == "" && len(table.ColumnVindexes) == 0 {
			return vterrors.Errorf(
				vtrpcpb.Code_NOT_FOUND,
				"missing primary col vindex for table: %s",
				tname,
			)
		}

		// Initialize Columns.
		colNames := make(map[string]bool)
		for _, col := range table.Columns {
			name := sqlparser.NewIdentifierCI(col.Name)
			if colNames[name.Lowered()] {
				return vterrors.Errorf(
					vtrpcpb.Code_INVALID_ARGUMENT,
					"duplicate column name '%v' for table: %s",
					name,
					tname,
				)
			}
			colNames[name.Lowered()] = true
			t.Columns = append(t.Columns, Column{Name: name, Type: col.Type})
		}

		// Initialize ColumnVindexes.
		for i, ind := range table.ColumnVindexes {
			vindexInfo, ok := ks.Vindexes[ind.Name]
			if !ok {
				return vterrors.Errorf(
					vtrpcpb.Code_NOT_FOUND,
					"vindex %s not found for table %s",
					ind.Name,
					tname,
				)
			}
			vindex := ksvschema.Vindexes[ind.Name]
			owned := false
			if _, ok := vindex.(Lookup); ok && vindexInfo.Owner == tname {
				owned = true
			}
			var columns []sqlparser.IdentifierCI
			if ind.Column != "" {
				if len(ind.Columns) > 0 {
					return vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"can't use column and columns at the same time in vindex (%s) and table (%s)",
						ind.Name,
						tname,
					)
				}
				columns = []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI(ind.Column)}
			} else {
				if len(ind.Columns) == 0 {
					return vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"must specify at least one column for vindex (%s) and table (%s)",
						ind.Name,
						tname,
					)
				}
				for _, indCol := range ind.Columns {
					columns = append(columns, sqlparser.NewIdentifierCI(indCol))
				}
			}
			columnVindex := &ColumnVindex{
				Columns:  columns,
				Type:     vindexInfo.Type,
				Name:     ind.Name,
				Owned:    owned,
				Vindex:   vindex,
				isUnique: vindex.IsUnique(),
				cost:     vindex.Cost(),
			}
			if i == 0 {
				// Perform Primary vindex check.
				if !columnVindex.Vindex.IsUnique() {
					return vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"primary vindex %s is not Unique for table %s",
						ind.Name,
						tname,
					)
				}
				if owned {
					return vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"primary vindex %s cannot be owned for table %s",
						ind.Name,
						tname,
					)
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

			mcv, isMultiColumn := vindex.(MultiColumn)
			if !isMultiColumn {
				continue
			}
			if i != 0 {
				return vterrors.Errorf(
					vtrpcpb.Code_UNIMPLEMENTED,
					"multi-column vindex %s should be a primary vindex for table %s",
					ind.Name,
					tname,
				)
			}
			if !mcv.PartialVindex() {
				// Partial column selection not allowed.
				// Do not create subset column vindex.
				continue
			}
			cost := vindex.Cost()
			for i := len(columns) - 1; i > 0; i-- {
				columnSubset := columns[:i]
				cost++
				columnVindex = &ColumnVindex{
					Columns: columnSubset,
					Type:    vindexInfo.Type,
					Name:    ind.Name,
					Owned:   owned,
					Vindex:  vindex,
					cost:    cost,
					partial: true,
				}
				t.ColumnVindexes = append(t.ColumnVindexes, columnVindex)
			}
		}
		t.Ordered = colVindexSorted(t.ColumnVindexes)

		// Add the table to the map entries.
		ksvschema.Tables[tname] = t
	}

	return nil
}

func (vschema *VSchema) addTableName(t *Table) {
	tname := t.Name.String()
	if _, ok := vschema.globalTables[tname]; ok {
		vschema.globalTables[tname] = nil
	} else {
		vschema.globalTables[tname] = t
	}
}

func resolveAutoIncrement(source *vschemapb.SrvVSchema, vschema *VSchema) {
	for ksname, ks := range source.Keyspaces {
		ksvschema := vschema.Keyspaces[ksname]
		for tname, table := range ks.Tables {
			t := ksvschema.Tables[tname]
			if t == nil || table.AutoIncrement == nil {
				continue
			}
			seqks, seqtab, err := sqlparser.ParseTable(table.AutoIncrement.Sequence)
			var seq *Table
			if err == nil {
				seq, err = vschema.FindTable(seqks, seqtab)
			}
			if err != nil {
				// Better to remove the table than to leave it partially initialized.
				delete(ksvschema.Tables, tname)
				delete(vschema.globalTables, tname)
				ksvschema.Error = vterrors.Errorf(
					vtrpcpb.Code_NOT_FOUND,
					"cannot resolve sequence %s: %s",
					table.AutoIncrement.Sequence,
					err.Error(),
				)

				continue
			}
			t.AutoIncrement = &AutoIncrement{
				Column:   sqlparser.NewIdentifierCI(table.AutoIncrement.Column),
				Sequence: seq,
			}
		}
	}
}

// expects table name of the form <keyspace>.<tablename>
func escapeQualifiedTable(qualifiedTableName string) (string, error) {
	keyspace, tableName, err := extractTableParts(qualifiedTableName, false /* allowUnqualified */)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s",
		// unescape() first in case an already escaped string was passed
		sqlescape.EscapeID(sqlescape.UnescapeID(keyspace)),
		sqlescape.EscapeID(sqlescape.UnescapeID(tableName))), nil
}

func extractTableParts(tableName string, allowUnqualified bool) (string, string, error) {
	errMsgFormat := "invalid table name: %s, it must be of the "
	if allowUnqualified {
		errMsgFormat = errMsgFormat + "unqualified form <table_name> or the "
	}
	errMsgFormat = errMsgFormat + "qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)"

	// It's possible to have a database or table name with a dot in it, but
	// that's not otherwise supported within vitess today
	arr := strings.Split(tableName, ".")
	switch len(arr) {
	case 1:
		if allowUnqualified {
			return "", arr[0], nil
		}
	case 2:
		return arr[0], arr[1], nil
	}
	// Using fmt.Errorf instead of vterrors here because this error is always wrapped in vterrors.
	return "", "", fmt.Errorf(errMsgFormat, tableName)

}

func parseTable(tableName string) (sqlparser.TableName, error) {
	keyspace, tableName, err := extractTableParts(tableName, true /* allowUnqualified */)
	if err != nil {
		return sqlparser.TableName{}, err
	}
	return sqlparser.TableName{
		Qualifier: sqlparser.NewIdentifierCS(keyspace),
		Name:      sqlparser.NewIdentifierCS(tableName),
	}, nil
}

func buildRoutingRule(source *vschemapb.SrvVSchema, vschema *VSchema) {
	var err error
	if source.RoutingRules == nil {
		return
	}
outer:
	for _, rule := range source.RoutingRules.Rules {
		rr := &RoutingRule{}
		if len(rule.ToTables) > 1 {
			vschema.RoutingRules[rule.FromTable] = &RoutingRule{
				Error: vterrors.Errorf(
					vtrpcpb.Code_INVALID_ARGUMENT,
					"table %v has more than one target: %v",
					rule.FromTable,
					rule.ToTables,
				),
			}
			continue
		}
		for _, toTable := range rule.ToTables {
			if _, ok := vschema.RoutingRules[rule.FromTable]; ok {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: vterrors.Errorf(
						vtrpcpb.Code_ALREADY_EXISTS,
						"duplicate rule for entry %s",
						rule.FromTable,
					),
				}
				continue outer
			}

			// we need to backtick the keyspace and table name before calling ParseTable
			toTable, err = escapeQualifiedTable(toTable)
			if err != nil {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						err.Error(),
					),
				}
				continue outer
			}

			toKeyspace, toTableName, err := sqlparser.ParseTable(toTable)

			if err != nil {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: err,
				}
				continue outer
			}
			if toKeyspace == "" {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: vterrors.Errorf(
						vtrpcpb.Code_INVALID_ARGUMENT,
						"table %s must be qualified",
						toTable,
					),
				}
				continue outer
			}
			t, err := vschema.FindTable(toKeyspace, toTableName)
			if err != nil {
				vschema.RoutingRules[rule.FromTable] = &RoutingRule{
					Error: err,
				}
				continue outer
			}
			rr.Tables = append(rr.Tables, t)
		}
		vschema.RoutingRules[rule.FromTable] = rr
	}
}

func buildShardRoutingRule(source *vschemapb.SrvVSchema, vschema *VSchema) {
	if source.ShardRoutingRules == nil || len(source.ShardRoutingRules.Rules) == 0 {
		return
	}
	vschema.ShardRoutingRules = make(map[string]string)
	for _, rule := range source.ShardRoutingRules.Rules {
		vschema.ShardRoutingRules[getShardRoutingRulesKey(rule.FromKeyspace, rule.Shard)] = rule.ToKeyspace
	}
}

// FindTable returns a pointer to the Table. If a keyspace is specified, only tables
// from that keyspace are searched. If the specified keyspace is unsharded
// and no tables matched, it's considered valid: FindTable will construct a table
// of that name and return it. If no keyspace is specified, then a table is returned
// only if its name is unique across all keyspaces. If there is only one
// keyspace in the vschema, and it's unsharded, then all table requests are considered
// valid and belonging to that keyspace.
// FindTable bypasses routing rules and returns at most one table.
func (vschema *VSchema) FindTable(keyspace, tablename string) (*Table, error) {
	table, err := vschema.findTable(
		keyspace,
		tablename,
		true, /* constructUnshardedIfNotFound */
	)

	if table != nil || err != nil {
		return table, err
	}

	return nil, vterrors.NewErrorf(
		vtrpcpb.Code_NOT_FOUND,
		vterrors.UnknownTable,
		"table %s not found",
		tablename,
	)
}

// findGlobalTable looks for a table that is uniquely named across all
// keyspaces.
//
// If multiple tables with the requested tablename are found, an error
// indicating this ambiguity is returned.
//
// If no table matching requested table name is found, and:
//
//   - constructUnshardedIfNotFound is not requested, than no table is returned.
//   - constructUnshardedIfNotFound is requested, and there is only one keyspace,
//     and that keyspace is unsharded, then a *Table representing that table is
//     returned.
func (vschema *VSchema) findGlobalTable(
	tablename string,
	constructUnshardedIfNotFound bool,
) (*Table, error) {
	if len(vschema.Keyspaces) == 1 {
		for _, ks := range vschema.Keyspaces {
			table := ks.findTable(tablename, constructUnshardedIfNotFound)
			return table, nil
		}
	}

	table, ok := vschema.globalTables[tablename]

	if table != nil {
		return table, nil
	}

	if ok {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_FAILED_PRECONDITION,
			"ambiguous table reference: %s",
			tablename,
		)
	}

	return nil, nil
}

// findTable looks for a keyspace matching the provided keyspace, and then
// looks for a table in that keyspace using the provided tablename.
//
// If the requested keyspace is "", then (*VSchema).findGlobalTable is used to
// find a global table.
//
// Otherwise:
//
//   - If no matching keyspace is found, an error is returned.
//   - If a matching keyspace is found, (*Keyspace).findTable is used to find a
//     matching table.
//
// constructUnshardedIfNotFound is passed to (*VSchema).findGlobalTable or
// (*Keyspace).findTable, and is used to construct a *Table representing a
// table present in an unsharded keyspace but not defined in the vschema.
func (vschema *VSchema) findTable(
	keyspace,
	tablename string,
	constructUnshardedIfNotFound bool,
) (*Table, error) {
	if keyspace == "" {
		return vschema.findGlobalTable(tablename, constructUnshardedIfNotFound)
	}
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, vterrors.VT05003(keyspace)
	}
	table := ks.findTable(tablename, constructUnshardedIfNotFound)
	return table, nil
}

func (vschema *VSchema) FirstKeyspace() *Keyspace {
	var first string
	for ksname := range vschema.Keyspaces {
		if first == "" || first > ksname {
			first = ksname
		}
	}
	ks := vschema.Keyspaces[first]

	return ks.Keyspace
}

// FindRoutedTable finds a table checking the routing rules.
func (vschema *VSchema) FindRoutedTable(keyspace, tablename string, tabletType topodatapb.TabletType) (*Table, error) {
	qualified := tablename
	if keyspace != "" {
		qualified = keyspace + "." + tablename
	}
	fqtn := qualified + TabletTypeSuffix[tabletType]
	// First look for a fully qualified table name: keyspace.table@tablet_type.
	// Then look for one without tablet type: keyspace.table.
	for _, name := range []string{fqtn, qualified} {
		rr, ok := vschema.RoutingRules[name]
		if ok {
			if rr.Error != nil {
				return nil, rr.Error
			}
			if len(rr.Tables) == 0 {
				return nil, vterrors.Errorf(
					vtrpcpb.Code_FAILED_PRECONDITION,
					"table %s has been disabled",
					tablename,
				)
			}
			return rr.Tables[0], nil
		}
	}
	return vschema.findTable(
		keyspace,
		tablename,
		true, /* constructUnshardedTableIfNotFound */
	)
}

// FindTableOrVindex finds a table or a Vindex by name using Find and FindVindex.
func (vschema *VSchema) FindTableOrVindex(keyspace, name string, tabletType topodatapb.TabletType) (*Table, Vindex, error) {
	tables, err := vschema.FindRoutedTable(keyspace, name, tabletType)
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
	return nil, nil, NotFoundError{TableName: name}
}

func (vschema *VSchema) FindView(keyspace, name string) sqlparser.SelectStatement {
	if keyspace == "" {
		switch {
		case len(vschema.Keyspaces) == 1:
			for _, schema := range vschema.Keyspaces {
				keyspace = schema.Keyspace.Name
				break
			}
		default:
			t, ok := vschema.globalTables[name]
			if !ok {
				return nil
			}
			if ok && t == nil {
				return nil
			}
			keyspace = t.Keyspace.Name
		}
	}

	ks := vschema.Keyspaces[keyspace]
	if ks == nil {
		return nil
	}

	statement, ok := ks.Views[name]
	if !ok {
		return nil
	}

	// We do this to make sure there is no shared state between uses of this AST
	statement = sqlparser.CloneSelectStatement(statement)
	sqlparser.SafeRewrite(statement, nil, func(cursor *sqlparser.Cursor) bool {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if ok {
			cursor.Replace(sqlparser.NewColNameWithQualifier(col.Name.String(), col.Qualifier))
		}
		return true
	})
	return statement
}

// NotFoundError represents the error where the table name was not found
type NotFoundError struct {
	TableName string
}

func (n NotFoundError) Error() string {
	return fmt.Sprintf("table %s not found", n.TableName)
}

// FindVindex finds a vindex by name. If a keyspace is specified, only vindexes
// from that keyspace are searched. If no kesypace is specified, then a vindex
// is returned only if its name is unique across all keyspaces. The function
// returns an error only if the vindex name is ambiguous.
func (vschema *VSchema) FindVindex(keyspace, name string) (Vindex, error) {
	if keyspace == "" {
		vindex, ok := vschema.uniqueVindexes[name]
		if vindex == nil && ok {
			return nil, vterrors.Errorf(
				vtrpcpb.Code_FAILED_PRECONDITION,
				"ambiguous vindex reference: %s",
				name,
			)
		}
		return vindex, nil
	}
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, vterrors.VT05003(keyspace)
	}
	return ks.Vindexes[name], nil
}

func getShardRoutingRulesKey(keyspace, shard string) string {
	return fmt.Sprintf("%s.%s", keyspace, shard)
}

// FindRoutedShard looks up shard routing rules and returns the target keyspace if applicable
func (vschema *VSchema) FindRoutedShard(keyspace, shard string) (string, error) {
	if len(vschema.ShardRoutingRules) == 0 {
		return keyspace, nil
	}
	if ks, ok := vschema.ShardRoutingRules[getShardRoutingRulesKey(keyspace, shard)]; ok {
		return ks, nil
	}
	return keyspace, nil
}

// ByCost provides the interface needed for ColumnVindexes to
// be sorted by cost order.
type ByCost []*ColumnVindex

func (bc ByCost) Len() int           { return len(bc) }
func (bc ByCost) Swap(i, j int)      { bc[i], bc[j] = bc[j], bc[i] }
func (bc ByCost) Less(i, j int) bool { return bc[i].Cost() < bc[j].Cost() }

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
	data, err := os.ReadFile(filename)
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
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = json2.Unmarshal(data, formal)
	if err != nil {
		return nil, err
	}
	return formal, nil
}

// ChooseVindexForType chooses the most appropriate vindex for the give type.
func ChooseVindexForType(typ querypb.Type) (string, error) {
	switch {
	case sqltypes.IsIntegral(typ):
		return "hash", nil
	case sqltypes.IsText(typ):
		return "unicode_loose_md5", nil
	case sqltypes.IsBinary(typ):
		return "binary_md5", nil
	}
	return "", vterrors.Errorf(
		vtrpcpb.Code_INVALID_ARGUMENT,
		"type %v is not recommended for a vindex",
		typ,
	)
}

// FindBestColVindex finds the best ColumnVindex for VReplication.
func FindBestColVindex(table *Table) (*ColumnVindex, error) {
	if table.ColumnVindexes == nil || len(table.ColumnVindexes) == 0 {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"table %s has no vindex",
			table.Name.String(),
		)
	}
	var result *ColumnVindex
	for _, cv := range table.ColumnVindexes {
		if cv.Vindex.NeedsVCursor() {
			continue
		}
		if !cv.IsUnique() {
			continue
		}
		if result == nil || result.Cost() > cv.Cost() {
			result = cv
		}
	}
	if result == nil {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_NOT_FOUND,
			"could not find a vindex to compute keyspace id for table %v",
			table.Name.String(),
		)
	}
	return result, nil
}

// FindVindexForSharding searches through the given slice
// to find the lowest cost unique vindex
// primary vindex is always unique
// if two have the same cost, use the one that occurs earlier in the definition
// if the final result is too expensive, return nil
func FindVindexForSharding(tableName string, colVindexes []*ColumnVindex) (*ColumnVindex, error) {
	if len(colVindexes) == 0 {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_NOT_FOUND,
			"no vindex definition for table %v",
			tableName,
		)
	}
	result := colVindexes[0]
	for _, colVindex := range colVindexes {
		// Only allow SingleColumn for legacy resharding.
		if _, ok := colVindex.Vindex.(SingleColumn); !ok {
			continue
		}
		if colVindex.Cost() < result.Cost() && colVindex.IsUnique() {
			result = colVindex
		}
	}
	if result.Cost() > 1 || !result.IsUnique() {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_NOT_FOUND,
			"could not find a vindex to use for sharding table %v",
			tableName,
		)
	}
	return result, nil
}

// String prints the (possibly qualified) table name
func (t *Table) String() string {
	res := ""
	if t == nil {
		return res
	}
	if t.Keyspace != nil {
		res = t.Keyspace.Name + "."
	}
	return res + t.Name.String()
}

func (t *Table) addReferenceInKeyspace(keyspace string, table *Table) {
	if t.ReferencedBy == nil {
		t.ReferencedBy = make(map[string]*Table)
	}
	t.ReferencedBy[keyspace] = table
}

func (t *Table) getReferenceInKeyspace(keyspace string) *Table {
	if t.ReferencedBy == nil {
		return nil
	}
	t, ok := t.ReferencedBy[keyspace]
	if !ok {
		return nil
	}
	return t
}
