/*
Copyright 2019 Google Inc.

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

package engine

import (
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// BatchNestedHashJoin specifies the parameters for a join primitive. It does its work by building a hash map
// (a.k.a probe table) for the lhs input, and then queries the right hand side for all rows matching the ids coming from
// the left, by changing the ON predicate into a IN comparison and run this on the RHS.
// So for the query:
// SELECT * FROM A JOIN B ON A.id = B.id
// , the RHS query would look something like:
// SELECT * FROM B WHERE B.id IN %1
// , where %1 will be coming from the values on the LHS
// If the probe table reaches a maximum size, that chunk is probed for on the rhs, and then the next chunk is taken on,
// until the LHS has been fully consumed
type BatchNestedHashJoin struct {
	Left, Right Primitive
	Cols        []int

	// LeftJoinCols and RightJoinCols defines which columns from the lhs are part of the ON comparison
	// this is encoded using the normal offsets into the rows
	LeftJoinCol, RightJoinCol int
}

var _ Primitive = (*BatchNestedHashJoin)(nil)

// MarshalJSON allows us to add the opcode in, so we can see the join type used
func (jn *BatchNestedHashJoin) MarshalJSON() ([]byte, error) {
	type Alias BatchNestedHashJoin
	return json.Marshal(&struct {
		OpCode string `json:"Opcode"`
		*Alias
	}{
		OpCode: "BatchNestedHashJoin",
		Alias:  (*Alias)(jn),
	})
}

// Execute performs a non-streaming exec.
func (jn *BatchNestedHashJoin) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// First get results from the LHS
	lresult, err := jn.Left.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// Next we build a probe table from the LHS results
	table := newSingleIntProbeTable(jn.LeftJoinCol)
	for _, row := range lresult.Rows {
		err := table.Add(row)
		if err != nil {
			return nil, vterrors.Wrap(err, "failed to add data to probe table")
		}
	}

	// Now we iterate over the keys and create a call to the RHS with the LHS key values

	return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "todo")
}

// StreamExecute performs a streaming exec.
func (jn *BatchNestedHashJoin) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// RouteType returns a description of the query routing type used by the primitive
func (jn *BatchNestedHashJoin) RouteType() string {
	return "BatchNestedLoopJoin"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jn *BatchNestedHashJoin) GetKeyspaceName() string {
	return joinGetKeyspaceName(jn.Left, jn.Right)
}

// GetTableName specifies the table that this primitive routes to.
func (jn *BatchNestedHashJoin) GetTableName() string {
	return joinGetTableName(jn.Left, jn.Right)
}

// GetFields fetches the field info.
func (jn *BatchNestedHashJoin) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return joinGetFields(jn.Left, jn.Right, jn.Cols, vcursor, bindVars)
}

type singleIntProbeTableEntry struct {
	values [][]sqltypes.Value
}

type singleIntProbeTable struct {
	offset int
	table  map[int][]*singleIntProbeTableEntry
}

func newSingleIntProbeTable(offset int) singleIntProbeTable {
	return singleIntProbeTable{offset: offset, table: make(map[int][]*singleIntProbeTableEntry)}
}

// Add adds a new value to the probetable
func (p *singleIntProbeTable) Add(value []sqltypes.Value) error {
	key := value[p.offset]
	hash := p.calculateHashFor(key)
	entries, ok := p.table[hash]
	if !ok {
		// first hit for this hash
		p.table[hash] = []*singleIntProbeTableEntry{{values: [][]sqltypes.Value{value}}}
		return nil
	}

	// we already have entries for this hash code - find one with the exact value we are looking for
	entry, err := p.findEntry(entries, key)
	if err != nil {
		return err
	}

	if entry == nil {
		// no entry found with this keyvalue. we have a false map collision
		p.table[hash] = append(p.table[hash], &singleIntProbeTableEntry{values: [][]sqltypes.Value{value}})
	} else {
		// found other matches with the same key - let's add this one as well
		entry.values = append(entry.values, value)
	}

	return nil
}

// Get fetches all rows with matching key column values
func (p *singleIntProbeTable) Get(key sqltypes.Value) ([][]sqltypes.Value, error) {
	hash := p.calculateHashFor(key)
	entry, err := p.findEntry(p.table[hash], key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return [][]sqltypes.Value{}, nil
	}
	return entry.values, nil
}

// Returns an array of all the key column values found in the probe table
func (p *singleIntProbeTable) Keys() []sqltypes.Value {
	var keys []sqltypes.Value
	for _, tableEntries := range p.table {
		for _, v2 := range tableEntries {
			keys = append(keys, v2.values[0][p.offset])
		}
	}
	return keys
}

func (singleIntProbeTable) calculateHashFor(key sqltypes.Value) int {
	hash := 0
	for _, b := range key.Raw() {
		hash = 31*hash + int(b)
	}
	return hash
}

func (p *singleIntProbeTable) findEntry(entries []*singleIntProbeTableEntry, key sqltypes.Value) (*singleIntProbeTableEntry, error) {
	for _, entry := range entries {
		cmp, err := sqltypes.AreEqualNullable(key, entry.values[0][p.offset])
		if err != nil {
			return nil, err
		}
		if cmp {
			return entry, nil
		}
	}
	return nil, nil
}
