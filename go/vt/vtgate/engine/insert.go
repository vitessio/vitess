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

package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
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
	// This is a three-dimensional data structure:
	// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
	// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
	// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
	VindexValues []sqltypes.PlanValue

	// Table specifies the table for the insert.
	Table *vindexes.Table

	// Generate is only set for inserts where a sequence must be generated.
	Generate *Generate

	// Prefix, Mid and Suffix are for sharded insert plans.
	Prefix string
	Mid    []string
	Suffix string

	// Option to override the standard behavior and allow a multi-shard insert
	// to use single round trip autocommit.
	//
	// This is a clear violation of the SQL semantics since it means the statement
	// is not atomic in the presence of PK conflicts on one shard and not another.
	// However some application use cases would prefer that the statement partially
	// succeed in order to get the performance benefits of autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	// Insert does not take inputs
	noInputs
}

// NewQueryInsert creates an Insert with a query string.
func NewQueryInsert(opcode InsertOpcode, keyspace *vindexes.Keyspace, query string) *Insert {
	return &Insert{
		Opcode:   opcode,
		Keyspace: keyspace,
		Query:    query,
	}
}

// NewSimpleInsert creates an Insert for a Table.
func NewSimpleInsert(opcode InsertOpcode, table *vindexes.Table, keyspace *vindexes.Keyspace) *Insert {
	return &Insert{
		Opcode:   opcode,
		Table:    table,
		Keyspace: keyspace,
	}
}

// NewInsert creates a new Insert.
func NewInsert(opcode InsertOpcode, keyspace *vindexes.Keyspace, vindexValues []sqltypes.PlanValue, table *vindexes.Table, prefix string, mid []string, suffix string) *Insert {
	return &Insert{
		Opcode:       opcode,
		Keyspace:     keyspace,
		VindexValues: vindexValues,
		Table:        table,
		Prefix:       prefix,
		Mid:          mid,
		Suffix:       suffix,
	}
}

// MarshalJSON serializes the Insert into a JSON representation.
// It's used for testing and diagnostics.
func (ins *Insert) MarshalJSON() ([]byte, error) {
	var tname string
	if ins.Table != nil {
		tname = ins.Table.Name.String()
	}
	marshalInsert := struct {
		Opcode               InsertOpcode
		Keyspace             *vindexes.Keyspace   `json:",omitempty"`
		Query                string               `json:",omitempty"`
		Values               []sqltypes.PlanValue `json:",omitempty"`
		Table                string               `json:",omitempty"`
		Generate             *Generate            `json:",omitempty"`
		Prefix               string               `json:",omitempty"`
		Mid                  []string             `json:",omitempty"`
		Suffix               string               `json:",omitempty"`
		MultiShardAutocommit bool                 `json:",omitempty"`
		QueryTimeout         int                  `json:",omitempty"`
	}{
		Opcode:               ins.Opcode,
		Keyspace:             ins.Keyspace,
		Query:                ins.Query,
		Values:               ins.VindexValues,
		Table:                tname,
		Generate:             ins.Generate,
		Prefix:               ins.Prefix,
		Mid:                  ins.Mid,
		Suffix:               ins.Suffix,
		MultiShardAutocommit: ins.MultiShardAutocommit,
		QueryTimeout:         ins.QueryTimeout,
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
// for the Insert primitive.
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

// RouteType returns a description of the query routing type used by the primitive
func (ins *Insert) RouteType() string {
	return insName[ins.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ins *Insert) GetKeyspaceName() string {
	return ins.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (ins *Insert) GetTableName() string {
	if ins.Table != nil {
		return ins.Table.Name.String()
	}
	return ""
}

// Execute performs a non-streaming exec.
func (ins *Insert) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if ins.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(ins.QueryTimeout) * time.Millisecond)
		defer cancel()
	}

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
func (ins *Insert) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", ins.Query)
}

// GetFields fetches the field info.
func (ins *Insert) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unreachable code for %q", ins.Query)
}

func (ins *Insert) execInsertUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.processGenerate(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}

	rss, _, err := vcursor.ResolveDestinations(ins.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}
	result, err := execShard(vcursor, ins.Query, bindVars, rss[0], true, true /* canAutocommit */)
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
	rss, queries, err := ins.getInsertShardedRoute(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}

	autocommit := (len(rss) == 1 || ins.MultiShardAutocommit) && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(rss, queries, true /* isDML */, autocommit)
	if errs != nil {
		return nil, vterrors.Wrap(vterrors.Aggregate(errs), "execInsertSharded")
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
		rss, _, err := vcursor.ResolveDestinations(ins.Generate.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
		if err != nil {
			return 0, vterrors.Wrap(err, "processGenerate")
		}
		if len(rss) != 1 {
			return 0, vterrors.Wrapf(err, "processGenerate len(rss)=%v", len(rss))
		}
		bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
		qr, err := vcursor.ExecuteStandalone(ins.Generate.Query, bindVars, rss[0])
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
func (ins *Insert) getInsertShardedRoute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
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
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: supplied vindex column values don't match vschema: %v %v", vColValues, ins.Table.ColumnVindexes[vIdx].Columns)
		}
		for colIdx, colValues := range vColValues.Values {
			rowsResolvedValues, err := colValues.ResolveList(bindVars)
			if err != nil {
				return nil, nil, vterrors.Wrap(err, "getInsertShardedRoute")
			}
			// This is the first iteration: allocate for transpose.
			if colIdx == 0 {
				if len(rowsResolvedValues) == 0 {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: rowcount is zero for inserts: %v", rowsResolvedValues)
				}
				if rowCount == 0 {
					rowCount = len(rowsResolvedValues)
				}
				if rowCount != len(rowsResolvedValues) {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: uneven row values for inserts: %d %d", rowCount, len(rowsResolvedValues))
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
	// id is returned as nil, which is used later to drop the corresponding rows.
	keyspaceIDs, err := ins.processPrimary(vcursor, vindexRowsValues[0], ins.Table.ColumnVindexes[0])
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}

	for vIdx := 1; vIdx < len(ins.Table.ColumnVindexes); vIdx++ {
		colVindex := ins.Table.ColumnVindexes[vIdx]
		var err error
		if colVindex.Owned {
			err = ins.processOwned(vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		} else {
			err = ins.processUnowned(vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		}
		if err != nil {
			return nil, nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
	}

	// Build 3-d bindvars. Skip rows with nil keyspace ids in case
	// we're executing an insert ignore.
	for vIdx, colVindex := range ins.Table.ColumnVindexes {
		for rowNum, rowColumnKeys := range vindexRowsValues[vIdx] {
			if keyspaceIDs[rowNum] == nil {
				// InsertShardedIgnore: skip the row.
				continue
			}
			for colIdx, vindexKey := range rowColumnKeys {
				col := colVindex.Columns[colIdx]
				bindVars[insertVarName(col, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
			}
		}
	}

	// We need to know the keyspace ids and the Mids associated with
	// each RSS.  So we pass the ksid indexes in as ids, and get them back
	// as values. We also skip nil KeyspaceIds, no need to resolve them.
	var indexes []*querypb.Value
	var destinations []key.Destination
	for i, ksid := range keyspaceIDs {
		if ksid != nil {
			indexes = append(indexes, &querypb.Value{
				Value: strconv.AppendInt(nil, int64(i), 10),
			})
			destinations = append(destinations, key.DestinationKeyspaceID(ksid))
		}
	}
	if len(destinations) == 0 {
		// In this case, all we have is nil KeyspaceIds, we don't do
		// anything at all.
		return nil, nil, nil
	}

	rss, indexesPerRss, err := vcursor.ResolveDestinations(ins.Keyspace.Name, indexes, destinations)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		var mids []string
		for _, indexValue := range indexesPerRss[i] {
			index, _ := strconv.ParseInt(string(indexValue.Value), 0, 64)
			if keyspaceIDs[index] != nil {
				mids = append(mids, ins.Mid[index])
			}
		}
		rewritten := ins.Prefix + strings.Join(mids, ",") + ins.Suffix
		queries[i] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bindVars,
		}
	}

	return rss, queries, nil
}

// processPrimary maps the primary vindex values to the keyspace ids.
func (ins *Insert) processPrimary(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex) ([][]byte, error) {
	destinations, err := vindexes.Map(colVindex.Vindex, vcursor, vindexColumnsKeys)
	if err != nil {
		return nil, err
	}

	keyspaceIDs := make([][]byte, len(destinations))
	for i, destination := range destinations {
		switch d := destination.(type) {
		case key.DestinationKeyspaceID:
			// This is a single keyspace id, we're good.
			keyspaceIDs[i] = d
		case key.DestinationNone:
			// No valid keyspace id, we may return an error.
			if ins.Opcode != InsertShardedIgnore {
				return nil, fmt.Errorf("could not map %v to a keyspace id", vindexColumnsKeys[i])
			}
		default:
			return nil, fmt.Errorf("could not map %v to a unique keyspace id: %v", vindexColumnsKeys[i], destination)
		}
	}

	return keyspaceIDs, nil
}

// processOwned creates vindex entries for the values of an owned column.
func (ins *Insert) processOwned(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, ksids [][]byte) error {
	if ins.Opcode == InsertSharded {
		return colVindex.Vindex.(vindexes.Lookup).Create(vcursor, vindexColumnsKeys, ksids, false /* ignoreMode */)
	}

	// InsertShardedIgnore
	var createIndexes []int
	var createKeys [][]sqltypes.Value
	var createKsids [][]byte

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		if ksids[rowNum] == nil {
			continue
		}
		createIndexes = append(createIndexes, rowNum)
		createKeys = append(createKeys, rowColumnKeys)
		createKsids = append(createKsids, ksids[rowNum])
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
	verified, err := vindexes.Verify(colVindex.Vindex, vcursor, createKeys, createKsids)
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
func (ins *Insert) processUnowned(vcursor VCursor, vindexColumnsKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, ksids [][]byte) error {
	var reverseIndexes []int
	var reverseKsids [][]byte
	var verifyIndexes []int
	var verifyKeys [][]sqltypes.Value
	var verifyKsids [][]byte

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		// Right now, we only validate against the first column of a colvindex.
		if ksids[rowNum] == nil {
			continue
		}
		// Perform reverse map only for non-multi-column vindexes.
		_, isMulti := colVindex.Vindex.(vindexes.MultiColumn)
		if rowColumnKeys[0].IsNull() && !isMulti {
			reverseIndexes = append(reverseIndexes, rowNum)
			reverseKsids = append(reverseKsids, ksids[rowNum])
		} else {
			verifyIndexes = append(verifyIndexes, rowNum)
			verifyKeys = append(verifyKeys, rowColumnKeys)
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
			// Fill the first column with the reverse-mapped value.
			vindexColumnsKeys[reverseIndexes[i]][0] = reverseKey
		}
	}

	if verifyKsids != nil {
		// If values were supplied, we validate against keyspace id.
		verified, err := vindexes.Verify(colVindex.Vindex, vcursor, verifyKeys, verifyKsids)
		if err != nil {
			return err
		}

		var mismatchVindexKeys [][]sqltypes.Value
		for i, v := range verified {
			rowNum := verifyIndexes[i]
			if !v {
				if ins.Opcode != InsertShardedIgnore {
					mismatchVindexKeys = append(mismatchVindexKeys, vindexColumnsKeys[rowNum])
					continue
				}
				// InsertShardedIgnore: skip the row.
				ksids[verifyIndexes[i]] = nil
			}
		}
		if len(mismatchVindexKeys) > 0 {
			return fmt.Errorf("values %v for column %v does not map to keyspace ids", mismatchVindexKeys, colVindex.Columns)
		}
	}
	return nil
}

func insertVarName(col sqlparser.ColIdent, rowNum int) string {
	return "_" + col.CompliantName() + strconv.Itoa(rowNum)
}
