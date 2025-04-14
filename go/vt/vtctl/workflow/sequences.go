/*
Copyright 2025 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	sqlCreateSequenceTable   = "create table if not exists %a (id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence'"
	sqlGetCurrentSequenceVal = "select next_id from %a.%a where id = 0"
)

// sequenceMetadata contains all of the relevant metadata for a sequence that
// is being used by a table involved in a vreplication workflow.
type sequenceMetadata struct {
	// The name of the sequence table.
	backingTableName string
	// The keyspace where the backing table lives.
	backingTableKeyspace string
	// The dbName in use by the keyspace where the backing table lives.
	backingTableDBName string
	// The name of the table using the sequence.
	usingTableName string
	// The dbName in use by the keyspace where the using table lives.
	usingTableDBName string
	// The using table definition.
	usingTableDefinition *vschemapb.Table

	// escaped values
	escaped                       bool
	usingCol, usingDB, usingTable string
	backingDB, backingTable       string
}

func (sm *sequenceMetadata) escapeValues() error {
	if sm.escaped {
		return nil
	}
	usingCol := ""
	var err error
	if sm.usingTableDefinition != nil && sm.usingTableDefinition.AutoIncrement != nil {
		usingCol, err = sqlescape.EnsureEscaped(sm.usingTableDefinition.AutoIncrement.Column)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid column name %s specified for sequence in table %s: %v",
				sm.usingTableDefinition.AutoIncrement.Column, sm.usingTableName, err)
		}
	}
	sm.usingCol = usingCol
	usingDB, err := sqlescape.EnsureEscaped(sm.usingTableDBName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid database name %s specified for sequence in table %s: %v",
			sm.usingTableDBName, sm.usingTableName, err)
	}
	sm.usingDB = usingDB
	usingTable, err := sqlescape.EnsureEscaped(sm.usingTableName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name %s specified for sequence: %v",
			sm.usingTableName, err)
	}
	sm.usingTable = usingTable
	backingDB, err := sqlescape.EnsureEscaped(sm.backingTableDBName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid database name %s specified for sequence backing table: %v",
			sm.backingTableDBName, err)
	}
	sm.backingDB = backingDB
	backingTable, err := sqlescape.EnsureEscaped(sm.backingTableName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name %s specified for sequence backing table: %v",
			sm.backingTableName, err)
	}
	sm.backingTable = backingTable
	sm.escaped = true
	return nil
}

func (ts *trafficSwitcher) getMaxSequenceValues(ctx context.Context, sequences map[string]*sequenceMetadata) (map[string]int64, error) {
	var sequencesMetadata []*tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata
	for _, seq := range sequences {
		if err := seq.escapeValues(); err != nil {
			return nil, err
		}
		sequencesMetadata = append(sequencesMetadata, &tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata{
			BackingTableName:        seq.backingTableName,
			UsingColEscaped:         seq.usingCol,
			UsingTableNameEscaped:   seq.usingTable,
			UsingTableDbNameEscaped: seq.usingDB,
		})
	}

	var mu sync.Mutex
	maxValuesByBackingTable := make(map[string]int64, len(sequences))
	setMaxSequenceValue := func(maxValues map[string]int64) {
		mu.Lock()
		defer mu.Unlock()
		for _, sm := range sequences {
			if maxValuesByBackingTable[sm.backingTableName] < maxValues[sm.backingTableName] {
				maxValuesByBackingTable[sm.backingTableName] = maxValues[sm.backingTableName]
			}
		}
	}
	errs := ts.ForAllTargets(func(target *MigrationTarget) error {
		primary := target.GetPrimary()
		if primary == nil || primary.GetAlias() == nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no primary tablet found for target shard %s/%s",
				ts.targetKeyspace, target.GetShard().ShardName())
		}
		resp, terr := ts.ws.tmc.GetMaxValueForSequences(ctx, primary.Tablet, &tabletmanagerdatapb.GetMaxValueForSequencesRequest{
			Sequences: sequencesMetadata,
		})
		if terr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"failed to get the max used sequence values on tablet %s in order to initialize the backing sequence table: %v",
				topoproto.TabletAliasString(primary.Alias), terr)
		}
		setMaxSequenceValue(resp.MaxValuesBySequenceTable)
		return nil
	})
	return maxValuesByBackingTable, errs
}

func (ts *trafficSwitcher) getCurrentSequenceValues(ctx context.Context, sequences map[string]*sequenceMetadata) (map[string]int64, error) {
	currentValues := make(map[string]int64, len(sequences))
	mu := sync.Mutex{}
	initGroup, gctx := errgroup.WithContext(ctx)
	for _, sm := range sequences {
		initGroup.Go(func() error {
			currentVal, err := ts.getCurrentSequenceValue(gctx, sm)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			currentValues[sm.backingTableName] = currentVal
			return nil
		})
	}
	errs := initGroup.Wait()
	if errs != nil {
		return nil, errs
	}
	return currentValues, nil
}

func (ts *trafficSwitcher) getCurrentSequenceValue(ctx context.Context, seq *sequenceMetadata) (int64, error) {
	if err := seq.escapeValues(); err != nil {
		return 0, err
	}
	sequenceShard, ierr := ts.TopoServer().GetOnlyShard(ctx, seq.backingTableKeyspace)
	if ierr != nil || sequenceShard == nil || sequenceShard.PrimaryAlias == nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
			seq.backingTableKeyspace, ierr)
	}
	sequenceTablet, ierr := ts.TopoServer().GetTablet(ctx, sequenceShard.PrimaryAlias)
	if ierr != nil || sequenceTablet == nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
			seq.backingTableKeyspace, ierr)
	}
	if sequenceTablet.DbNameOverride != "" {
		seq.backingTableDBName = sequenceTablet.DbNameOverride
	}
	query := sqlparser.BuildParsedQuery(sqlGetCurrentSequenceVal,
		seq.backingDB,
		seq.backingTable,
	)
	qr, ierr := ts.ws.tmc.ExecuteFetchAsApp(ctx, sequenceTablet.Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
		Query:   []byte(query.Query),
		MaxRows: 1,
	})
	if ierr != nil || len(qr.Rows) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the current value for the sequence table %s.%s: %v",
			seq.backingTableDBName, seq.backingTableName, ierr)
	}
	rawVal := sqltypes.Proto3ToResult(qr).Rows[0][0]
	if rawVal.IsNull() {
		return 0, nil
	}
	currentVal, err := rawVal.ToInt64()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to convert the current value for the sequence table %s.%s: %v",
			seq.backingTableDBName, seq.backingTableName, err)
	}
	return currentVal, nil
}

func (ts *trafficSwitcher) updateSequenceValues(ctx context.Context, sequences []*sequenceMetadata, maxValues map[string]int64) error {
	sequencesByShard := map[string][]*tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata{}
	for _, seq := range sequences {
		maxValue := maxValues[seq.backingTableName]
		if maxValue == 0 {
			continue
		}
		sequenceShard, ierr := ts.TopoServer().GetOnlyShard(ctx, seq.backingTableKeyspace)
		if ierr != nil || sequenceShard == nil || sequenceShard.PrimaryAlias == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
				seq.backingTableKeyspace, ierr)
		}
		tabletAliasStr := topoproto.TabletAliasString(sequenceShard.PrimaryAlias)
		sequencesByShard[tabletAliasStr] = append(sequencesByShard[tabletAliasStr], &tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata{
			BackingTableName:   seq.backingTableName,
			BackingTableDbName: seq.backingTableDBName,
			MaxValue:           maxValue,
		})
	}
	for tabletAliasStr, sequencesMetadata := range sequencesByShard {
		tabletAlias, err := topoproto.ParseTabletAlias(tabletAliasStr)
		if err != nil {
			// This should be impossible.
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the parse tablet alias %s: %v", tabletAlias, err)
		}
		sequenceTablet, ierr := ts.TopoServer().GetTablet(ctx, tabletAlias)
		if ierr != nil || sequenceTablet == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the primary tablet for keyspace %s: %v",
				sequenceTablet.Keyspace, ierr)
		}
		_, err = ts.TabletManagerClient().UpdateSequenceTables(ctx, sequenceTablet.Tablet, &tabletmanagerdatapb.UpdateSequenceTablesRequest{
			Sequences: sequencesMetadata,
		})
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to initialize the backing sequence tables on tablet %s: %v", topoproto.TabletAliasString(tabletAlias), err)
		}
	}
	return nil
}

// initializeTargetSequences initializes the backing sequence tables
// using a map keyed by the backing sequence table name.
//
// The backing tables must have already been created, unless a default
// global keyspace exists for the trafficSwitcher -- in which case we
// will create the backing table there if needed.

// This function will then ensure that the next value is set to a value
// greater than any currently stored in the using table on the target
// keyspace. If the backing table is updated to a new higher value then
// it will also tell the primary tablet serving the sequence to
// refresh/reset its cache to be sure that it does not provide a value
// that is less than the current max.
func (ts *trafficSwitcher) initializeTargetSequences(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata) error {
	maxValues, err := ts.getMaxSequenceValues(ctx, sequencesByBackingTable)
	if err != nil {
		return err
	}
	sequences := maps.Values(sequencesByBackingTable)
	if err := ts.updateSequenceValues(ctx, sequences, maxValues); err != nil {
		return err
	}
	return nil
}

// isSequenceParticipating checks to see if the target keyspace has any tables with a sequence
func (ts *trafficSwitcher) isSequenceParticipating(ctx context.Context) (bool, error) {
	vschema, err := ts.TopoServer().GetVSchema(ctx, ts.targetKeyspace)
	if err != nil {
		return false, err
	}
	if vschema == nil || len(vschema.Tables) == 0 {
		return false, nil
	}
	sequenceFound := false
	for _, table := range ts.Tables() {
		vs, ok := vschema.Tables[table]
		if !ok || vs == nil {
			continue
		}
		if vs.Type == vindexes.TypeSequence {
			sequenceFound = true
			break
		}
	}
	return sequenceFound, nil
}

// getTargetSequenceMetadata returns a map of sequence metadata keyed by the
// backing sequence table name. If the target keyspace has no tables
// defined that use sequences for auto_increment generation then a nil
// map will be returned.
func (ts *trafficSwitcher) getTargetSequenceMetadata(ctx context.Context) (map[string]*sequenceMetadata, error) {
	vschema, err := ts.TopoServer().GetVSchema(ctx, ts.targetKeyspace)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for target keyspace %s: %v",
			ts.targetKeyspace, err)
	}
	if vschema == nil || len(vschema.Tables) == 0 { // Nothing to do
		return nil, nil
	}

	sequencesByBackingTable, backingTablesFound, err := ts.findSequenceUsageInKeyspace(vschema.Keyspace)
	if err != nil {
		return nil, err
	}
	// If all of the sequence tables were defined using qualified table
	// names then we don't need to search for them in other keyspaces.
	if len(sequencesByBackingTable) == 0 || backingTablesFound {
		return sequencesByBackingTable, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Now we need to locate the backing sequence table(s) which will
	// be in another unsharded keyspace.
	smMu := sync.Mutex{}
	tableCount := len(sequencesByBackingTable)
	tablesFound := make(map[string]struct{}) // Used to short circuit the search
	// Define the function used to search each keyspace.
	searchKeyspace := func(sctx context.Context, done chan struct{}, keyspace string) error {
		kvs, kerr := ts.TopoServer().GetVSchema(sctx, keyspace)
		if kerr != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for keyspace %s: %v",
				keyspace, kerr)
		}
		if kvs == nil || kvs.Sharded || len(kvs.Tables) == 0 {
			return nil
		}
		for tableName, tableDef := range kvs.Tables {
			// The table name can be escaped in the vschema definition.
			unescapedTableName, err := sqlescape.UnescapeID(tableName)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %q in keyspace %s: %v",
					tableName, keyspace, err)
			}
			select {
			case <-sctx.Done():
				return sctx.Err()
			case <-done: // We've found everything we need in other goroutines
				return nil
			default:
			}
			if complete := func() bool {
				smMu.Lock() // Prevent concurrent access to the map
				defer smMu.Unlock()
				sm := sequencesByBackingTable[unescapedTableName]
				if tableDef != nil && tableDef.Type == vindexes.TypeSequence &&
					sm != nil && unescapedTableName == sm.backingTableName {
					tablesFound[tableName] = struct{}{} // This is also protected by the mutex
					sm.backingTableKeyspace = keyspace
					// Set the default keyspace name. We will later check to
					// see if the tablet we send requests to is using a dbname
					// override and use that if it is.
					sm.backingTableDBName = "vt_" + keyspace
					if len(tablesFound) == tableCount { // Short circuit the search
						select {
						case <-done: // It's already been closed
							return true
						default:
							close(done) // Mark the search as completed
							return true
						}
					}
				}
				return false
			}(); complete {
				return nil
			}
		}
		return nil
	}
	keyspaces, err := ts.TopoServer().GetKeyspaces(ctx)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get keyspaces: %v", err)
	}
	searchGroup, gctx := errgroup.WithContext(ctx)
	searchCompleted := make(chan struct{})
	for _, keyspace := range keyspaces {
		// The keyspace name could be escaped so we need to unescape it.
		ks, err := sqlescape.UnescapeID(keyspace)
		if err != nil { // Should never happen
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid keyspace name %q: %v", keyspace, err)
		}
		searchGroup.Go(func() error {
			return searchKeyspace(gctx, searchCompleted, ks)
		})
	}
	if err := searchGroup.Wait(); err != nil {
		return nil, err
	}

	if len(tablesFound) != tableCount {
		// Try and create the missing backing sequence tables if we can.
		if err := ts.createMissingSequenceTables(ctx, sequencesByBackingTable, tablesFound); err != nil {
			return nil, err
		}
	}

	return sequencesByBackingTable, nil
}

// createMissingSequenceTables will create the backing sequence tables for those that
// could not be found in any current keyspace.
func (ts *trafficSwitcher) createMissingSequenceTables(ctx context.Context, sequencesByBackingTable map[string]*sequenceMetadata, tablesFound map[string]struct{}) error {
	globalKeyspace := ts.options.GetGlobalKeyspace()
	if globalKeyspace == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to locate all of the backing sequence tables being used and no global-keyspace was provided to auto create them in: %s",
			strings.Join(maps.Keys(sequencesByBackingTable), ","))
	}
	shards, err := ts.ws.ts.GetShardNames(ctx, globalKeyspace)
	if err != nil {
		return err
	}
	if len(shards) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "global-keyspace %s is not unsharded", globalKeyspace)
	}
	globalVSchema, err := ts.ws.ts.GetVSchema(ctx, globalKeyspace)
	if err != nil {
		return err
	}
	shard, err := ts.ws.ts.GetShard(ctx, globalKeyspace, shards[0])
	if err != nil {
		return err
	}
	if shard.PrimaryAlias == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "global-keyspace %s does not currently have a primary tablet",
			globalKeyspace)
	}
	primary, err := ts.ws.ts.GetTablet(ctx, shard.PrimaryAlias)
	if err != nil {
		return err
	}
	updatedGlobalVSchema := false
	for tableName, sequenceMetadata := range sequencesByBackingTable {
		if _, ok := tablesFound[tableName]; !ok {
			// Create the backing table.

			if err := ts.createSequenceTable(ctx, tableName, primary); err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
					"failed to create the backing sequence table %s in the global-keyspace %s: %v",
					tableName, globalKeyspace, err)
			}
			if bt := globalVSchema.Tables[sequenceMetadata.backingTableName]; bt == nil {
				if globalVSchema.Tables == nil {
					globalVSchema.Tables = make(map[string]*vschemapb.Table)
				}
				globalVSchema.Tables[tableName] = &vschemapb.Table{
					Type: vindexes.TypeSequence,
				}
				updatedGlobalVSchema = true
				sequenceMetadata.backingTableDBName = "vt_" + globalKeyspace // This will be overridden later if needed
				sequenceMetadata.backingTableKeyspace = globalKeyspace
			}
		}
	}
	if updatedGlobalVSchema {
		err = ts.ws.ts.SaveVSchema(ctx, globalVSchema)
		if err != nil {
			return vterrors.Wrapf(err, "failed to update vschema in the global-keyspace %s", globalKeyspace)
		}
	}
	return nil
}

func (ts *trafficSwitcher) createSequenceTable(ctx context.Context, tableName string, primary *topo.TabletInfo) error {
	escapedTableName, err := sqlescape.EnsureEscaped(tableName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %s: %v",
			tableName, err)
	}
	stmt := sqlparser.BuildParsedQuery(sqlCreateSequenceTable, escapedTableName)
	_, err = ts.ws.tmc.ApplySchema(ctx, primary.Tablet, &tmutils.SchemaChange{
		SQL:                     stmt.Query,
		Force:                   false,
		AllowReplication:        true,
		SQLMode:                 vreplication.SQLMode,
		DisableForeignKeyChecks: true,
	})
	return err
}

// findSequenceUsageInKeyspace searches the keyspace's vschema for usage
// of sequences. It returns a map of sequence metadata keyed by the backing
// sequence table name -- if any usage is found -- along with a boolean to
// indicate if all of the backing sequence tables were defined using
// qualified table names (so we know where they all live) along with an
// error if any is seen.
func (ts *trafficSwitcher) findSequenceUsageInKeyspace(vschema *vschemapb.Keyspace) (map[string]*sequenceMetadata, bool, error) {
	allFullyQualified := true
	targets := maps.Values(ts.Targets())
	if len(targets) == 0 || targets[0].GetPrimary() == nil { // This should never happen
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no primary tablet found for target keyspace %s", ts.targetKeyspace)
	}
	targetDBName := targets[0].GetPrimary().DbName()
	sequencesByBackingTable := make(map[string]*sequenceMetadata)

	for _, table := range ts.tables {
		seqTable, ok := vschema.Tables[table]
		if !ok || seqTable.GetAutoIncrement().GetSequence() == "" {
			continue
		}
		// Be sure that the table name is unescaped as it can be escaped
		// in the vschema.
		unescapedTable, err := sqlescape.UnescapeID(table)
		if err != nil {
			return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid table name %q defined in the sequence table %+v: %v",
				table, seqTable, err)
		}
		sm := &sequenceMetadata{
			usingTableName:   unescapedTable,
			usingTableDBName: targetDBName,
		}
		// If the sequence table is fully qualified in the vschema then
		// we don't need to find it later.
		if strings.Contains(seqTable.AutoIncrement.Sequence, ".") {
			keyspace, tableName, found := strings.Cut(seqTable.AutoIncrement.Sequence, ".")
			if !found {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name %q defined in the %s keyspace",
					seqTable.AutoIncrement.Sequence, ts.targetKeyspace)
			}
			// Unescape the table name and keyspace name as they may be escaped in the
			// vschema definition if they e.g. contain dashes.
			if keyspace, err = sqlescape.UnescapeID(keyspace); err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid keyspace in qualified sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			if tableName, err = sqlescape.UnescapeID(tableName); err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid qualified sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			sm.backingTableKeyspace = keyspace
			sm.backingTableName = tableName
			// Update the definition with the unescaped values.
			seqTable.AutoIncrement.Sequence = fmt.Sprintf("%s.%s", keyspace, tableName)
			// Set the default keyspace name. We will later check to
			// see if the tablet we send requests to is using a dbname
			// override and use that if it is.
			sm.backingTableDBName = "vt_" + keyspace
		} else {
			sm.backingTableName, err = sqlescape.UnescapeID(seqTable.AutoIncrement.Sequence)
			if err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence table name %q defined in sequence table %+v: %v",
					seqTable.AutoIncrement.Sequence, seqTable, err)
			}
			seqTable.AutoIncrement.Sequence = sm.backingTableName
			allFullyQualified = false
		}
		// The column names can be escaped in the vschema definition.
		for i := range seqTable.ColumnVindexes {
			var (
				unescapedColumn string
				err             error
			)
			if len(seqTable.ColumnVindexes[i].Columns) > 0 {
				for n := range seqTable.ColumnVindexes[i].Columns {
					unescapedColumn, err = sqlescape.UnescapeID(seqTable.ColumnVindexes[i].Columns[n])
					seqTable.ColumnVindexes[i].Columns[n] = unescapedColumn
				}
			} else {
				// This is the legacy vschema definition.
				unescapedColumn, err = sqlescape.UnescapeID(seqTable.ColumnVindexes[i].Column)
				seqTable.ColumnVindexes[i].Column = unescapedColumn
			}
			if err != nil {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence column vindex name %q defined in sequence table %+v: %v",
					seqTable.ColumnVindexes[i].Column, seqTable, err)
			}
		}
		unescapedAutoIncCol, err := sqlescape.UnescapeID(seqTable.AutoIncrement.Column)
		if err != nil {
			return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid auto-increment column name %q defined in sequence table %+v: %v",
				seqTable.AutoIncrement.Column, seqTable, err)
		}
		seqTable.AutoIncrement.Column = unescapedAutoIncCol
		sm.usingTableDefinition = seqTable
		sequencesByBackingTable[sm.backingTableName] = sm
	}

	return sequencesByBackingTable, allFullyQualified, nil
}

func (ts *trafficSwitcher) mustResetSequences(ctx context.Context) (bool, error) {
	switch ts.workflowType {
	case binlogdatapb.VReplicationWorkflowType_Migrate,
		binlogdatapb.VReplicationWorkflowType_MoveTables:
		return ts.isSequenceParticipating(ctx)
	default:
		return false, nil
	}
}

func (ts *trafficSwitcher) resetSequences(ctx context.Context) error {
	var err error
	mustReset := false
	if mustReset, err = ts.mustResetSequences(ctx); err != nil {
		return err
	}
	if !mustReset {
		return nil
	}
	return ts.ForAllSources(func(source *MigrationSource) error {
		ts.Logger().Infof("Resetting sequences for source shard %s.%s on tablet %s",
			source.GetShard().Keyspace(), source.GetShard().ShardName(), topoproto.TabletAliasString(source.GetPrimary().GetAlias()))
		return ts.TabletManagerClient().ResetSequences(ctx, source.GetPrimary().Tablet, ts.Tables())
	})
}
