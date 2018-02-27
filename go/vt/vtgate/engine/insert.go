/*
Copyright 2018 Google Inc.

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
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlannotation"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Insert)(nil)

// Insert represents the instructions to perform an insert operation.
type Insert struct {
	// Opcode is the execution opcode.
	Opcode InsertOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Query specifies the query to be executed.
	// For InsertSharded plans, this value is unused,
	// and Prefix, Mid and Suffix are used instead.
	Query string

	// VindexValues specifies values for all the vindex columns.
	// This is a three-dimensonal data structure:
	// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
	// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
	// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
	VindexValues []sqltypes.PlanValue

	// Table sepcifies the table for the insert.
	Table *vindexes.Table

	// Generate is only set for inserts where a sequence must be generated.
	Generate *Generate

	// Prefix, Mid and Suffix are for sharded insert plans.
	Prefix string
	Mid    []string
	Suffix string
}

// MarshalJSON serializes the Insert into a JSON representation.
// It's used for testing and diagnostics.
func (ins *Insert) MarshalJSON() ([]byte, error) {
	var tname string
	if ins.Table != nil {
		tname = ins.Table.Name.String()
	}
	marshalInsert := struct {
		Opcode   InsertOpcode
		Keyspace *vindexes.Keyspace   `json:",omitempty"`
		Query    string               `json:",omitempty"`
		Values   []sqltypes.PlanValue `json:",omitempty"`
		Table    string               `json:",omitempty"`
		Generate *Generate            `json:",omitempty"`
		Prefix   string               `json:",omitempty"`
		Mid      []string             `json:",omitempty"`
		Suffix   string               `json:",omitempty"`
	}{
		Opcode:   ins.Opcode,
		Keyspace: ins.Keyspace,
		Query:    ins.Query,
		Values:   ins.VindexValues,
		Table:    tname,
		Generate: ins.Generate,
		Prefix:   ins.Prefix,
		Mid:      ins.Mid,
		Suffix:   ins.Suffix,
	}
	return jsonutil.MarshalNoEscape(marshalInsert)
}

// Generate represents the instruction to generate
// a value from a sequence.
type Generate struct {
	Keyspace *vindexes.Keyspace
	Query    string
	// Values are the supplied values for the column, which
	// will be stored as a list within the PlanValue. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values sqltypes.PlanValue
}

// InsertOpcode is a number representing the opcode
// for the Insert primitve.
type InsertOpcode int

const (
	// InsertUnsharded is for routing an insert statement
	// to an unsharded keyspace.
	InsertUnsharded = InsertOpcode(iota)
	// InsertSharded is for routing an insert statement
	// to individual shards. Requires: A list of Values, one
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	InsertSharded
	// InsertShardedIgnore is for INSERT IGNORE and
	// INSERT...ON DUPLICATE KEY constructs.
	InsertShardedIgnore
)

var insName = map[InsertOpcode]string{
	InsertUnsharded:     "InsertUnsharded",
	InsertSharded:       "InsertSharded",
	InsertShardedIgnore: "InsertShardedIgnore",
}

// MarshalJSON serializes the InsertOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code InsertOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(insName[code])
}

// Execute performs a non-streaming exec.
func (ins *Insert) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch ins.Opcode {
	case InsertUnsharded:
		return ins.execInsertUnsharded(vcursor, bindVars)
	case InsertSharded, InsertShardedIgnore:
		return ins.execInsertSharded(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported query route: %v", ins)
	}
}

// StreamExecute performs a streaming exec.
func (ins *Insert) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", ins.Query)
}

// GetFields fetches the field info.
func (ins *Insert) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unreachable code for %q", ins.Query)
}

func (ins *Insert) execInsertUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.processGenerate(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}

	ks, allShards, err := vcursor.GetKeyspaceShards(ins.Keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}
	if len(allShards) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", allShards)
	}
	result, err := execShard(vcursor, ins.Query, bindVars, ks, allShards[0].Name, true, true /* canAutocommit */)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}

	// If processGenerate generated new values, it supercedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

func (ins *Insert) execInsertSharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.processGenerate(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}
	keyspace, shardQueries, err := ins.getInsertShardedRoute(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}

	result, err := vcursor.ExecuteMultiShard(keyspace, shardQueries, true /* isDML */, true /* canAutocommit */)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}

	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

// processGenerate generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ins *Insert) processGenerate(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (insertID int64, err error) {
	if ins.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	resolved, err := ins.Generate.Values.ResolveList(bindVars)
	if err != nil {
		return 0, vterrors.Wrap(err, "processGenerate")
	}
	count := int64(0)
	for _, val := range resolved {
		if val.IsNull() {
			count++
		}
	}

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		ks, shard, err := anyShard(vcursor, ins.Generate.Keyspace)
		if err != nil {
			return 0, vterrors.Wrap(err, "processGenerate")
		}
		bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
		qr, err := vcursor.ExecuteStandalone(ins.Generate.Query, bindVars, ks, shard)
		if err != nil {
			return 0, err
		}
		// If no rows are returned, it's an internal error, and the code
		// must panic, which will be caught and reported.
		insertID, err = sqltypes.ToInt64(qr.Rows[0][0])
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range resolved {
		if v.IsNull() {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.Int64BindVariable(cur)
			cur++
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.ValueBindVariable(v)
		}
	}
	return insertID, nil
}

// getInsertShardedRoute performs all the vindex related work
// and returns a map of shard to queries.
// Using the primary vindex, it computes the target keyspace ids.
// For owned vindexes, it creates entries.
// For unowned vindexes with no input values, it reverse maps.
// For unowned vindexes with values, it validates.
// If it's an IGNORE or ON DUPLICATE key insert, it drops unroutable rows.
func (ins *Insert) getInsertShardedRoute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (keyspace string, shardQueries map[string]*querypb.BoundQuery, err error) {
	keyspace, allShards, err := vcursor.GetKeyspaceShards(ins.Keyspace)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}

	// vindexRowsValues builds the values of all vindex columns.
	// the 3-d structure indexes are colVindex, row, col. Note that
	// ins.Values indexes are colVindex, col, row. So, the conversion
	// involves a transpose.
	// The reason we need to transpose is because all the Vindex APIs
	// require inputs in that format.
	vindexRowsValues := make([][][]sqltypes.Value, len(ins.VindexValues))
	rowCount := 0
	for vIdx, vColValues := range ins.VindexValues {
		if len(vColValues.Values) != len(ins.Table.ColumnVindexes[vIdx].Columns) {
			return "", nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: supplied vindex column values don't match vschema: %v %v", vColValues, ins.Table.ColumnVindexes[vIdx].Columns)
		}
		for colIdx, colValues := range vColValues.Values {
			rowsResolvedValues, err := colValues.ResolveList(bindVars)
			if err != nil {
				return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
			}
			// This is the first iteration: allocate for transpose.
			if colIdx == 0 {
				if len(rowsResolvedValues) == 0 {
					return "", nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: rowcount is zero for inserts: %v", rowsResolvedValues)
				}
				if rowCount == 0 {
					rowCount = len(rowsResolvedValues)
				}
				if rowCount != len(rowsResolvedValues) {
					return "", nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: uneven row values for inserts: %d %d", rowCount, len(rowsResolvedValues))
				}
				vindexRowsValues[vIdx] = make([][]sqltypes.Value, rowCount)
			}
			// Perform the transpose.
			for rowNum, colVal := range rowsResolvedValues {
				vindexRowsValues[vIdx][rowNum] = append(vindexRowsValues[vIdx][rowNum], colVal)
			}
		}
	}

	// The output from the following 'process' functions is a list of
	// keyspace ids. For regular inserts, a failure to find a route
	// results in an error. For 'ignore' type inserts, the keyspace
	// id is returned as nil, which is used later to drop such rows.
	keyspaceIDs, err := ins.processPrimary(vcursor, vindexRowsValues[0], ins.Table.ColumnVindexes[0], bindVars)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}

	for vIdx := 1; vIdx < len(vindexRowsValues); vIdx++ {
		colVindex := ins.Table.ColumnVindexes[vIdx]
		var err error
		if colVindex.Owned {
			switch ins.Opcode {
			case InsertSharded:
				err = ins.processOwned(vcursor, vindexRowsValues[vIdx], colVindex, bindVars, keyspaceIDs)
			case InsertShardedIgnore:
				// For InsertShardedIgnore, the work is substantially different.
				// So, we use a separate function.
				err = ins.processOwnedIgnore(vcursor, vindexRowsValues[vIdx], colVindex, bindVars, keyspaceIDs)
			default:
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected opcode: %v", ins.Opcode)
			}
		} else {
			err = ins.processUnowned(vcursor, vindexRowsValues[vIdx], colVindex, bindVars, keyspaceIDs)
		}
		if err != nil {
			return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
	}

	shardKeyspaceIDMap := make(map[string][][]byte)
	routing := make(map[string][]string)
	for rowNum, ksid := range keyspaceIDs {
		if ksid == nil {
			continue
		}
		shard, err := vcursor.GetShardForKeyspaceID(allShards, ksid)
		if err != nil {
			return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
		shardKeyspaceIDMap[shard] = append(shardKeyspaceIDMap[shard], ksid)
		routing[shard] = append(routing[shard], ins.Mid[rowNum])
	}

	shardQueries = make(map[string]*querypb.BoundQuery, len(routing))
	for shard := range routing {
		rewritten := ins.Prefix + strings.Join(routing[shard], ",") + ins.Suffix
		rewritten = sqlannotation.AddKeyspaceIDs(rewritten, shardKeyspaceIDMap[shard], "")
		shardQueries[shard] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bindVars,
		}
	}

	return keyspace, shardQueries, nil
}

// processPrimary maps the primary vindex values to the kesypace ids.
func (ins *Insert) processPrimary(vcursor VCursor, vindexKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable) (keyspaceIDs [][]byte, err error) {
	mapper := colVindex.Vindex.(vindexes.Unique)
	var flattenedVidexKeys []sqltypes.Value
	// TODO: @rafael - this will change once vindex Primary keys also support multicolumns
	for _, val := range vindexKeys {
		for _, internalVal := range val {
			flattenedVidexKeys = append(flattenedVidexKeys, internalVal)
		}
	}
	ksids, err := mapper.Map(vcursor, flattenedVidexKeys)
	if err != nil {
		return nil, err
	}
	for _, ksid := range ksids {
		if err := ksid.ValidateUnique(); err != nil {
			return nil, err
		}
		keyspaceIDs = append(keyspaceIDs, ksid.ID)
	}

	for rowNum, vindexKey := range flattenedVidexKeys {
		if keyspaceIDs[rowNum] == nil {
			if ins.Opcode != InsertShardedIgnore {
				return nil, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
			}
			// InsertShardedIgnore: skip the row.
			continue
		}
		for _, col := range colVindex.Columns {
			bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
		}
	}
	return keyspaceIDs, nil
}

// processOwned creates vindex entries for the values of an owned column for InsertSharded.
func (ins *Insert) processOwned(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		for colIdx, vindexKey := range rowColumnKeys {
			if vindexKey.IsNull() {
				return fmt.Errorf("value must be supplied for column %v", colVindex.Columns[colIdx])
			}
			col := colVindex.Columns[colIdx]
			bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
		}
	}
	return colVindex.Vindex.(vindexes.Lookup).Create(vcursor, vindexColumnsKeys, ksids, false /* ignoreMode */)
}

// processOwnedIgnore creates vindex entries for the values of an owned column for InsertShardedIgnore.
func (ins *Insert) processOwnedIgnore(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	var createIndexes []int
	var createKeys [][]sqltypes.Value
	var createKsids [][]byte

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		var rowKeys []sqltypes.Value
		if ksids[rowNum] == nil {
			continue
		}
		createIndexes = append(createIndexes, rowNum)
		createKsids = append(createKsids, ksids[rowNum])

		for colIdx, vindexKey := range rowColumnKeys {
			if vindexKey.IsNull() {
				return fmt.Errorf("value must be supplied for column %v", colVindex.Columns)
			}
			rowKeys = append(rowKeys, vindexKey)
			col := colVindex.Columns[colIdx]
			bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
		}
		createKeys = append(createKeys, rowKeys)
	}
	if createKeys == nil {
		return nil
	}

	err := colVindex.Vindex.(vindexes.Lookup).Create(vcursor, createKeys, createKsids, true /* ignoreMode */)
	if err != nil {
		return err
	}
	// After creation, verify that the keys map to the keyspace ids. If not, remove
	// those that don't map.
	// If values were supplied, we validate against keyspace id.
	var ids []sqltypes.Value
	for _, vindexValues := range createKeys {
		ids = append(ids, vindexValues[0])
	}
	verified, err := colVindex.Vindex.Verify(vcursor, ids, createKsids)
	if err != nil {
		return err
	}
	for i, v := range verified {
		if !v {
			ksids[createIndexes[i]] = nil
		}
	}
	return nil
}

// processUnowned either reverse maps or validates the values for an unowned column.
func (ins *Insert) processUnowned(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	var reverseIndexes []int
	var reverseKsids [][]byte
	var verifyIndexes []int
	var verifyKeys []sqltypes.Value
	var verifyKsids [][]byte

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		// Right now, we only validate against the first column of a colvindex.
		// TODO(sougou): address this when we add multicolumn Map support.
		vindexKey := rowColumnKeys[0]
		if ksids[rowNum] == nil {
			continue
		}
		if vindexKey.IsNull() {
			reverseIndexes = append(reverseIndexes, rowNum)
			reverseKsids = append(reverseKsids, ksids[rowNum])
		} else {
			verifyIndexes = append(verifyIndexes, rowNum)
			verifyKeys = append(verifyKeys, vindexKey)
			verifyKsids = append(verifyKsids, ksids[rowNum])
		}
	}

	// For cases where a value was not supplied, we reverse map it
	// from the keyspace id, if possible.
	if reverseKsids != nil {
		reversible, ok := colVindex.Vindex.(vindexes.Reversible)
		if !ok {
			return fmt.Errorf("value must be supplied for column %v", colVindex.Columns)
		}
		reverseKeys, err := reversible.ReverseMap(vcursor, reverseKsids)
		if err != nil {
			return err
		}
		for i, reverseKey := range reverseKeys {
			rowNum := reverseIndexes[i]
			for colIdx, col := range colVindex.Columns {
				if colIdx == 0 {
					// Fill the first column with the reverse-mapped value.
					bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(reverseKey)
				} else {
					// Fill other columns with supplied values.
					bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexColumnsKeys[rowNum][colIdx])
				}
			}
		}
	}

	if verifyKsids != nil {
		// If values were supplied, we validate against keyspace id.
		verified, err := colVindex.Vindex.Verify(vcursor, verifyKeys, verifyKsids)
		if err != nil {
			return err
		}
		for i, v := range verified {
			rowNum := verifyIndexes[i]
			if !v {
				if ins.Opcode != InsertShardedIgnore {
					return fmt.Errorf("values %v for column %v does not map to keyspace ids", vindexColumnsKeys, colVindex.Columns)
				}
				// InsertShardedIgnore: skip the row.
				ksids[rowNum] = nil
				continue
			}
			for colIdx, col := range colVindex.Columns {
				bv[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexColumnsKeys[rowNum][colIdx])
			}
		}
	}
	return nil
}

func insertVarName(col sqlparser.ColIdent, rowNum int) string {
	return "_" + col.CompliantName() + strconv.Itoa(rowNum)
}
