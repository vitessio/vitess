/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	_ Vindex        = (*ConsistentLookupUnique)(nil)
	_ Lookup        = (*ConsistentLookupUnique)(nil)
	_ WantOwnerInfo = (*ConsistentLookupUnique)(nil)
	_ Vindex        = (*ConsistentLookup)(nil)
	_ Lookup        = (*ConsistentLookup)(nil)
	_ WantOwnerInfo = (*ConsistentLookup)(nil)
)

func init() {
	Register("consistent_lookup", NewConsistentLookup)
	Register("consistent_lookup_unique", NewConsistentLookupUnique)
}

// ConsistentLookup is a non-unique lookup vindex that can stay
// consistent with respect to its owner table.
type ConsistentLookup struct {
	*clCommon
}

// NewConsistentLookup creates a ConsistentLookup vindex.
// The supplied map has the following required fields:
//   table: name of the backing table. It can be qualified by the keyspace.
//   from: list of columns in the table that have the 'from' values of the lookup vindex.
//   to: The 'to' column name of the table.
//
// The following fields are optional:
//   write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func NewConsistentLookup(name string, m map[string]string) (Vindex, error) {
	clc, err := newCLCommon(name, m)
	if err != nil {
		return nil, err
	}
	return &ConsistentLookup{clCommon: clc}, nil
}

// String returns the name of the vindex.
func (lu *ConsistentLookup) String() string {
	return lu.name
}

// Cost returns the cost of this vindex as 20.
func (lu *ConsistentLookup) Cost() int {
	return 20
}

// IsUnique returns false since the Vindex is non unique.
func (lu *ConsistentLookup) IsUnique() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (lu *ConsistentLookup) Map(vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	results, err := lu.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if len(result.Rows) == 0 {
			out = append(out, key.DestinationNone{})
			continue
		}
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			ksids = append(ksids, row[0].ToBytes())
		}
		out = append(out, key.DestinationKeyspaceIDs(ksids))
	}
	return out, nil
}

//====================================================================

// ConsistentLookupUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type ConsistentLookupUnique struct {
	*clCommon
}

// NewConsistentLookupUnique creates a ConsistentLookupUnique vindex.
// The supplied map has the following required fields:
//   table: name of the backing table. It can be qualified by the keyspace.
//   from: list of columns in the table that have the 'from' values of the lookup vindex.
//   to: The 'to' column name of the table.
//
// The following fields are optional:
//   write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func NewConsistentLookupUnique(name string, m map[string]string) (Vindex, error) {
	clc, err := newCLCommon(name, m)
	if err != nil {
		return nil, err
	}
	return &ConsistentLookupUnique{clCommon: clc}, nil
}

// Cost returns the cost of this vindex as 10.
func (lu *ConsistentLookupUnique) Cost() int {
	return 10
}

// IsUnique returns true since the Vindex is unique.
func (lu *ConsistentLookupUnique) IsUnique() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (lu *ConsistentLookupUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}
	results, err := lu.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, key.DestinationNone{})
		case 1:
			out = append(out, key.DestinationKeyspaceID(result.Rows[0][0].ToBytes()))
		default:
			return nil, fmt.Errorf("Lookup.Map: unexpected multiple results from vindex %s: %v", lu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

//====================================================================

// clCommon defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type clCommon struct {
	name         string
	writeOnly    bool
	lkp          lookupInternal
	keyspace     string
	ownerTable   string
	ownerColumns []string

	lockLookupQuery   string
	lockOwnerQuery    string
	insertLookupQuery string
	updateLookupQuery string
}

// newCLCommon is commone code for the consistent lookup vindexes.
func newCLCommon(name string, m map[string]string) (*clCommon, error) {
	lu := &clCommon{name: name}

	wo, err := boolFromMap(m, "write_only")
	if err != nil {
		return nil, err
	}
	lu.writeOnly = wo

	if err := lu.lkp.Init(m, false /* autocommit */, false /* upsert */); err != nil {
		return nil, err
	}

	lu.lockLookupQuery = lu.generateLockLookup()
	lu.lockOwnerQuery = lu.generateLockOwner()
	lu.insertLookupQuery = lu.generateInsertLookup()
	lu.updateLookupQuery = lu.generateUpdateLookup()
	return lu, nil
}

func (lu *clCommon) SetOwnerInfo(keyspace, table string, cols []sqlparser.ColIdent) error {
	lu.keyspace = keyspace
	lu.ownerTable = table
	if len(cols) != len(lu.lkp.FromColumns) {
		return fmt.Errorf("owner table column count does not match vindex %s", lu.name)
	}
	lu.ownerColumns = make([]string, len(cols))
	for i, col := range cols {
		lu.ownerColumns[i] = col.String()
	}
	return nil
}

// String returns the name of the vindex.
func (lu *clCommon) String() string {
	return lu.name
}

// IsFunctional returns false since the Vindex is not functional.
func (lu *clCommon) IsFunctional() bool {
	return false
}

// Verify returns true if ids maps to ksids.
func (lu *clCommon) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lu.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}
	return lu.lkp.Verify(vcursor, ids, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (lu *clCommon) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	err := lu.lkp.createCustom(vcursor.ExecutePre, rowsColValues, ksidsToValues(ksids), ignoreMode)
	if err == nil {
		return nil
	}
	return err
}

// Delete deletes the entry from the vindex table.
func (lu *clCommon) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	return lu.lkp.Delete(vcursor, rowsColValues, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// Update updates the entry in the vindex table.
func (lu *clCommon) Update(vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	if err := lu.Delete(vcursor, [][]sqltypes.Value{oldValues}, ksid); err != nil {
		return err
	}
	return lu.Create(vcursor, [][]sqltypes.Value{newValues}, [][]byte{ksid}, false /* ignoreMode */)
}

// MarshalJSON returns a JSON representation of clCommon.
func (lu *clCommon) MarshalJSON() ([]byte, error) {
	return json.Marshal(lu.lkp)
}

func (lu *clCommon) generateLockLookup() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "select %s from %s", lu.lkp.To, lu.lkp.Table)
	lu.addWhere(&buf, lu.lkp.FromColumns)
	fmt.Fprintf(&buf, " for update")
	return buf.String()
}

func (lu *clCommon) generateLockOwner() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "select %s from %s", lu.ownerColumns[0], lu.ownerTable)
	lu.addWhere(&buf, lu.ownerColumns)
	// We can lock in share mode because we only want to check
	// if the row exists. We still need to lock to make us wait
	// in case a previous transaction is creating it.
	fmt.Fprintf(&buf, " lock in share mode")
	return buf.String()
}

func (lu *clCommon) generateInsertLookup() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "insert into %s(", lu.lkp.Table)
	for _, col := range lu.lkp.FromColumns {
		fmt.Fprintf(&buf, "%s, ", col)
	}
	fmt.Fprintf(&buf, "%s) values(", lu.lkp.To)
	for _, col := range lu.lkp.FromColumns {
		fmt.Fprintf(&buf, ":%s, ", col)
	}
	fmt.Fprintf(&buf, ":%s)", lu.lkp.To)
	return buf.String()
}

func (lu *clCommon) generateUpdateLookup() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "update %s set %s=:%s", lu.lkp.Table, lu.lkp.To, lu.lkp.To)
	lu.addWhere(&buf, lu.lkp.FromColumns)
	return buf.String()
}

func (lu *clCommon) addWhere(buf *bytes.Buffer, cols []string) {
	for colIdx, column := range cols {
		if colIdx != 0 {
			buf.WriteString(" and ")
		}
		buf.WriteString(column + " = :" + column)
	}
}
