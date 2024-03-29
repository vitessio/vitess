/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	InsertCommon struct {
		// Insert needs tx handling
		txNeeded

		// Opcode is the execution opcode.
		Opcode InsertOpcode

		// Keyspace specifies the keyspace to send the query to.
		Keyspace *vindexes.Keyspace

		// Ignore is for INSERT IGNORE and INSERT...ON DUPLICATE KEY constructs
		// for sharded cases.
		Ignore bool

		// TableName is the name of the table on which row will be inserted.
		TableName string

		// Option to override the standard behavior and allow a multi-shard insert
		// to use single round trip autocommit.
		//
		// This is a clear violation of the SQL semantics since it means the statement
		// is not atomic in the presence of PK conflicts on one shard and not another.
		// However, some application use cases would prefer that the statement partially
		// succeed in order to get the performance benefits of autocommit.
		MultiShardAutocommit bool

		// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
		QueryTimeout int

		// ForceNonStreaming is true when the insert table and select table are same.
		// This will avoid locking by the select table.
		ForceNonStreaming bool

		PreventAutoCommit bool

		// Generate is only set for inserts where a sequence must be generated.
		Generate *Generate

		// ColVindexes are the vindexes that will use the VindexValues
		ColVindexes []*vindexes.ColumnVindex

		// Prefix, Suffix are for sharded insert plans.
		Prefix string
		Suffix sqlparser.OnDup
	}

	ksID = []byte

	// Generate represents the instruction to generate
	// a value from a sequence.
	Generate struct {
		Keyspace *vindexes.Keyspace
		Query    string
		// Values are the supplied values for the column, which
		// will be stored as a list within the expression. New
		// values will be generated based on how many were not
		// supplied (NULL).
		Values evalengine.Expr
		// Insert using Select, offset for auto increment column
		Offset int
	}

	// InsertOpcode is a number representing the opcode
	// for the Insert primitive.
	InsertOpcode int
)

const nextValBV = "n"

const (
	// InsertUnsharded is for routing an insert statement
	// to an unsharded keyspace.
	InsertUnsharded = InsertOpcode(iota)
	// InsertSharded is for routing an insert statement
	// to individual shards. Requires: A list of Values, one
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	InsertSharded
)

var insName = map[InsertOpcode]string{
	InsertUnsharded: "InsertUnsharded",
	InsertSharded:   "InsertSharded",
}

// String returns the opcode
func (code InsertOpcode) String() string {
	return strings.ReplaceAll(insName[code], "Insert", "")
}

// MarshalJSON serializes the InsertOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code InsertOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(insName[code])
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ic *InsertCommon) GetKeyspaceName() string {
	return ic.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (ic *InsertCommon) GetTableName() string {
	return ic.TableName
}

// GetFields fetches the field info.
func (ic *InsertCommon) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unexpected fields call for insert query")
}

func (ins *InsertCommon) executeUnshardedTableQuery(ctx context.Context, vcursor VCursor, loggingPrimitive Primitive, bindVars map[string]*querypb.BindVariable, query string, insertID uint64) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, ins.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.VT09022(rss)
	}
	err = allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}
	qr, err := execShard(ctx, loggingPrimitive, vcursor, query, bindVars, rss[0], true, !ins.PreventAutoCommit /* canAutocommit */)
	if err != nil {
		return nil, err
	}

	// If processGenerateFromValues generated new values, it supersedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		qr.InsertID = insertID
	}
	return qr, nil
}

func (ins *InsertCommon) processVindexes(ctx context.Context, vcursor VCursor, vindexRowsValues [][]sqltypes.Row) ([]ksID, error) {
	colVindexes := ins.ColVindexes
	keyspaceIDs, err := ins.processPrimary(ctx, vcursor, vindexRowsValues[0], colVindexes[0])
	if err != nil {
		return nil, err
	}

	for vIdx := 1; vIdx < len(colVindexes); vIdx++ {
		colVindex := colVindexes[vIdx]
		if colVindex.Owned {
			err = ins.processOwned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		} else {
			err = ins.processUnowned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		}
		if err != nil {
			return nil, err
		}
	}
	return keyspaceIDs, nil
}

// processPrimary maps the primary vindex values to the keyspace ids.
func (ic *InsertCommon) processPrimary(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex) ([]ksID, error) {
	destinations, err := vindexes.Map(ctx, colVindex.Vindex, vcursor, vindexColumnsKeys)
	if err != nil {
		return nil, err
	}

	keyspaceIDs := make([]ksID, len(destinations))
	for i, destination := range destinations {
		switch d := destination.(type) {
		case key.DestinationKeyspaceID:
			// This is a single keyspace id, we're good.
			keyspaceIDs[i] = d
		case key.DestinationNone:
			// Not a valid keyspace id, so we cannot determine which shard this row belongs to.
			// We have to return an error.
			return nil, vterrors.VT09023(vindexColumnsKeys[i])
		default:
			return nil, vterrors.VT09024(vindexColumnsKeys[i], destination)
		}
	}

	return keyspaceIDs, nil
}

// processOwned creates vindex entries for the values of an owned column.
func (ic *InsertCommon) processOwned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
	if !ic.Ignore {
		return colVindex.Vindex.(vindexes.Lookup).Create(ctx, vcursor, vindexColumnsKeys, ksids, false /* ignoreMode */)
	}

	// InsertIgnore
	var createIndexes []int
	var createKeys []sqltypes.Row
	var createKsids []ksID

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

	err := colVindex.Vindex.(vindexes.Lookup).Create(ctx, vcursor, createKeys, createKsids, true)
	if err != nil {
		return err
	}
	// After creation, verify that the keys map to the keyspace ids. If not, remove
	// those that don't map.
	verified, err := vindexes.Verify(ctx, colVindex.Vindex, vcursor, createKeys, createKsids)
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
func (ic *InsertCommon) processUnowned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
	var reverseIndexes []int
	var reverseKsids []ksID

	var verifyIndexes []int
	var verifyKeys []sqltypes.Row
	var verifyKsids []ksID

	// Check if this VIndex is reversible or not.
	reversibleVindex, isReversible := colVindex.Vindex.(vindexes.Reversible)

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		// If we weren't able to determine a keyspace id from the primary VIndex, skip this row
		if ksids[rowNum] == nil {
			continue
		}

		if rowColumnKeys[0].IsNull() {
			// If the value of the column is `NULL`, but this is a reversible VIndex,
			// we will try to generate the value from the keyspace id generated by the primary VIndex.
			if isReversible {
				reverseIndexes = append(reverseIndexes, rowNum)
				reverseKsids = append(reverseKsids, ksids[rowNum])
			}

			// Otherwise, don't do anything. Whether `NULL` is a valid value for this column will be
			// handled by MySQL.
		} else {
			// If a value for this column was specified, the keyspace id values from the
			// secondary VIndex need to be verified against the keyspace id from the primary VIndex
			verifyIndexes = append(verifyIndexes, rowNum)
			verifyKeys = append(verifyKeys, rowColumnKeys)
			verifyKsids = append(verifyKsids, ksids[rowNum])
		}
	}

	// Reverse map values for secondary VIndex columns from the primary VIndex's keyspace id.
	if reverseKsids != nil {
		reverseKeys, err := reversibleVindex.ReverseMap(vcursor, reverseKsids)
		if err != nil {
			return err
		}

		for i, reverseKey := range reverseKeys {
			// Fill the first column with the reverse-mapped value.
			vindexColumnsKeys[reverseIndexes[i]][0] = reverseKey
		}
	}

	// Verify that the keyspace ids generated by the primary and secondary VIndexes match
	if verifyIndexes != nil {
		// If values were supplied, we validate against keyspace id.
		verified, err := vindexes.Verify(ctx, colVindex.Vindex, vcursor, verifyKeys, verifyKsids)
		if err != nil {
			return err
		}

		var mismatchVindexKeys []sqltypes.Row
		for i, v := range verified {
			rowNum := verifyIndexes[i]
			if !v {
				if !ic.Ignore {
					mismatchVindexKeys = append(mismatchVindexKeys, vindexColumnsKeys[rowNum])
					continue
				}

				// Skip the whole row if this is a `INSERT IGNORE` or `INSERT ... ON DUPLICATE KEY ...` statement
				// but the keyspace ids didn't match.
				ksids[verifyIndexes[i]] = nil
			}
		}

		if mismatchVindexKeys != nil {
			return fmt.Errorf("values %v for column %v does not map to keyspace ids", mismatchVindexKeys, colVindex.Columns)
		}
	}

	return nil
}

// processGenerateFromSelect generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ic *InsertCommon) processGenerateFromSelect(
	ctx context.Context,
	vcursor VCursor,
	loggingPrimitive Primitive,
	rows []sqltypes.Row,
) (insertID int64, err error) {
	if ic.Generate == nil {
		return 0, nil
	}
	var count int64
	offset := ic.Generate.Offset
	genColPresent := offset < len(rows[0])
	if genColPresent {
		for _, row := range rows {
			if shouldGenerate(row[offset], evalengine.ParseSQLMode(vcursor.SQLMode())) {
				count++
			}
		}
	} else {
		count = int64(len(rows))
	}

	if count == 0 {
		return 0, nil
	}

	insertID, err = ic.execGenerate(ctx, vcursor, loggingPrimitive, count)
	if err != nil {
		return 0, err
	}

	used := insertID
	for idx, val := range rows {
		if genColPresent {
			if shouldGenerate(val[offset], evalengine.ParseSQLMode(vcursor.SQLMode())) {
				val[offset] = sqltypes.NewInt64(used)
				used++
			}
		} else {
			rows[idx] = append(val, sqltypes.NewInt64(used))
			used++
		}
	}

	return insertID, nil
}

// processGenerateFromValues generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ic *InsertCommon) processGenerateFromValues(
	ctx context.Context,
	vcursor VCursor,
	loggingPrimitive Primitive,
	bindVars map[string]*querypb.BindVariable,
) (insertID int64, err error) {
	if ic.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	resolved, err := env.Evaluate(ic.Generate.Values)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	values := resolved.TupleValues()
	for _, val := range values {
		if shouldGenerate(val, evalengine.ParseSQLMode(vcursor.SQLMode())) {
			count++
		}
	}

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		insertID, err = ic.execGenerate(ctx, vcursor, loggingPrimitive, count)
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range values {
		if shouldGenerate(v, evalengine.ParseSQLMode(vcursor.SQLMode())) {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.Int64BindVariable(cur)
			cur++
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.ValueBindVariable(v)
		}
	}
	return insertID, nil
}

func (ic *InsertCommon) execGenerate(ctx context.Context, vcursor VCursor, loggingPrimitive Primitive, count int64) (int64, error) {
	// If generation is needed, generate the requested number of values (as one call).
	rss, _, err := vcursor.ResolveDestinations(ctx, ic.Generate.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return 0, err
	}
	if len(rss) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "auto sequence generation can happen through single shard only, it is getting routed to %d shards", len(rss))
	}
	bindVars := map[string]*querypb.BindVariable{nextValBV: sqltypes.Int64BindVariable(count)}
	qr, err := vcursor.ExecuteStandalone(ctx, loggingPrimitive, ic.Generate.Query, bindVars, rss[0])
	if err != nil {
		return 0, err
	}
	// If no rows are returned, it's an internal error, and the code
	// must panic, which will be caught and reported.
	return qr.Rows[0][0].ToCastInt64()
}

// shouldGenerate determines if a sequence value should be generated for a given value
func shouldGenerate(v sqltypes.Value, sqlmode evalengine.SQLMode) bool {
	if v.IsNull() {
		return true
	}

	// Unless the NO_AUTO_VALUE_ON_ZERO sql mode is active in mysql, it also
	// treats 0 as a value that should generate a new sequence.
	value, err := evalengine.CoerceTo(v, evalengine.NewType(sqltypes.Uint64, collations.CollationBinaryID), sqlmode)
	if err != nil {
		return false
	}

	id, err := value.ToCastUint64()
	if err != nil {
		return false
	}

	return id == 0
}
