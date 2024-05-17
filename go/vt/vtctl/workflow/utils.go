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

package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const reverseSuffix = "_reverse"

func getTablesInKeyspace(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace string) ([]string, error) {
	shards, err := ts.GetServingShards(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("keyspace %s has no shards", keyspace)
	}
	primary := shards[0].PrimaryAlias
	if primary == nil {
		return nil, fmt.Errorf("shard does not have a primary: %v", shards[0].ShardName())
	}
	allTables := []string{"/.*/"}

	ti, err := ts.GetTablet(ctx, primary)
	if err != nil {
		return nil, err
	}
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: allTables}
	schema, err := tmc.GetSchema(ctx, ti.Tablet, req)
	if err != nil {
		return nil, err
	}
	log.Infof("got table schemas: %+v from source primary %v.", schema, primary)

	var sourceTables []string
	for _, td := range schema.TableDefinitions {
		sourceTables = append(sourceTables, td.Name)
	}
	return sourceTables, nil
}

// validateNewWorkflow ensures that the specified workflow doesn't already exist
// in the keyspace.
func validateNewWorkflow(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace, workflow string) error {
	allshards, err := ts.FindAllShardsInKeyspace(ctx, keyspace, nil)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, si := range allshards {
		if si.PrimaryAlias == nil {
			allErrors.RecordError(fmt.Errorf("shard has no primary: %v", si.ShardName()))
			continue
		}
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()

			primary, err := ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.GetTablet"))
				return
			}
			res, err := tmc.ReadVReplicationWorkflows(ctx, primary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{})
			if err != nil {
				allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.ReadVReplicationWorkflows"))
				return
			}
			if res == nil {
				// There are no workflows on this tablet.
				return
			}
			for _, wf := range res.Workflows {
				if wf.Workflow == workflow {
					allErrors.RecordError(fmt.Errorf("workflow %s already exists in keyspace %s on tablet %v", workflow, keyspace, primary.Alias))
					return
				}
			}
		}(si)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

// createDefaultShardRoutingRules creates a reverse routing rule for
// each shard in a new partial keyspace migration workflow that does
// not already have an existing routing rule in place.
func createDefaultShardRoutingRules(ctx context.Context, ms *vtctldatapb.MaterializeSettings, ts *topo.Server) error {
	srr, err := topotools.GetShardRoutingRules(ctx, ts)
	if err != nil {
		return err
	}
	allShards, err := ts.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return err
	}
	changed := false
	for _, si := range allShards {
		fromSource := fmt.Sprintf("%s.%s", ms.SourceKeyspace, si.ShardName())
		fromTarget := fmt.Sprintf("%s.%s", ms.TargetKeyspace, si.ShardName())
		if srr[fromSource] == "" && srr[fromTarget] == "" {
			srr[fromTarget] = ms.SourceKeyspace
			changed = true
			log.Infof("Added default shard routing rule from %q to %q", fromTarget, fromSource)
		}
	}
	if changed {
		if err := topotools.SaveShardRoutingRules(ctx, ts, srr); err != nil {
			return err
		}
		if err := ts.RebuildSrvVSchema(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

func stripTableConstraints(ddl string, parser *sqlparser.Parser) (string, error) {
	ast, err := parser.ParseStrictDDL(ddl)
	if err != nil {
		return "", err
	}

	stripConstraints := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.DDLStatement:
			if node.GetTableSpec() != nil {
				node.GetTableSpec().Constraints = nil
			}
		}
		return true
	}

	noConstraintAST := sqlparser.Rewrite(ast, stripConstraints, nil)
	newDDL := sqlparser.String(noConstraintAST)

	return newDDL, nil
}

func stripTableForeignKeys(ddl string, parser *sqlparser.Parser) (string, error) {
	ast, err := parser.ParseStrictDDL(ddl)
	if err != nil {
		return "", err
	}

	stripFKConstraints := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.DDLStatement:
			if node.GetTableSpec() != nil {
				var noFKConstraints []*sqlparser.ConstraintDefinition
				for _, constraint := range node.GetTableSpec().Constraints {
					if constraint.Details != nil {
						if _, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); !ok {
							noFKConstraints = append(noFKConstraints, constraint)
						}
					}
				}
				node.GetTableSpec().Constraints = noFKConstraints
			}
		}
		return true
	}

	noFKConstraintAST := sqlparser.Rewrite(ast, stripFKConstraints, nil)
	newDDL := sqlparser.String(noFKConstraintAST)
	return newDDL, nil
}

func stripAutoIncrement(ddl string, parser *sqlparser.Parser) (string, error) {
	newDDL, err := parser.ParseStrictDDL(ddl)
	if err != nil {
		return "", err
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColumnDefinition:
			if node.Type.Options.Autoincrement {
				node.Type.Options.Autoincrement = false
			}
		}
		return true, nil
	}, newDDL)

	return sqlparser.String(newDDL), nil
}

func getSourceTableDDLs(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, shards []*topo.ShardInfo) (map[string]string, error) {
	sourceDDLs := make(map[string]string)
	allTables := []string{"/.*/"}

	sourcePrimary := shards[0].PrimaryAlias
	if sourcePrimary == nil {
		return nil, fmt.Errorf("shard must have a primary for copying schema: %v", shards[0].ShardName())
	}

	ti, err := ts.GetTablet(ctx, sourcePrimary)
	if err != nil {
		return nil, err
	}
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: allTables}
	sourceSchema, err := tmc.GetSchema(ctx, ti.Tablet, req)
	if err != nil {
		return nil, err
	}

	for _, td := range sourceSchema.TableDefinitions {
		sourceDDLs[td.Name] = td.Schema
	}
	return sourceDDLs, nil
}

func forAllShards(shards []*topo.ShardInfo, f func(*topo.ShardInfo) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range shards {
		wg.Add(1)
		go func(target *topo.ShardInfo) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func matchColInSelect(col sqlparser.IdentifierCI, sel *sqlparser.Select) (*sqlparser.ColName, error) {
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			return &sqlparser.ColName{Name: col}, nil
		case *sqlparser.AliasedExpr:
			match := selExpr.As
			if match.IsEmpty() {
				if colExpr, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					match = colExpr.Name
				} else {
					// Cannot match against a complex expression.
					continue
				}
			}
			if match.Equal(col) {
				colExpr, ok := selExpr.Expr.(*sqlparser.ColName)
				if !ok {
					return nil, fmt.Errorf("vindex column cannot be a complex expression: %v", sqlparser.String(selExpr))
				}
				return colExpr, nil
			}
		default:
			return nil, fmt.Errorf("unsupported select expression: %v", sqlparser.String(selExpr))
		}
	}
	return nil, fmt.Errorf("could not find vindex column %v", sqlparser.String(col))
}

func shouldInclude(table string, excludes []string) bool {
	// We filter out internal tables elsewhere when processing SchemaDefinition
	// structures built from the GetSchema database related API calls. In this
	// case, however, the table list comes from the user via the -tables flag
	// so we need to filter out internal table names here in case a user has
	// explicitly specified some.
	// This could happen if there's some automated tooling that creates the list of
	// tables to explicitly specify.
	// But given that this should never be done in practice, we ignore the request.
	if schema.IsInternalOperationTableName(table) {
		return false
	}
	for _, t := range excludes {
		if t == table {
			return false
		}
	}
	return true
}

// getMigrationID produces a reproducible hash based on the input parameters.
func getMigrationID(targetKeyspace string, shardTablets []string) (int64, error) {
	sort.Strings(shardTablets)
	hasher := fnv.New64()
	hasher.Write([]byte(targetKeyspace))
	for _, str := range shardTablets {
		hasher.Write([]byte(str))
	}
	// Convert to int64 after dropping the highest bit.
	return int64(hasher.Sum64() & math.MaxInt64), nil
}

// BuildTargets collects MigrationTargets and other metadata (see TargetInfo)
// from a workflow in the target keyspace.
//
// It returns ErrNoStreams if there are no targets found for the workflow.
func BuildTargets(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, targetKeyspace string, workflow string) (*TargetInfo, error) {
	targetShards, err := ts.FindAllShardsInKeyspace(ctx, targetKeyspace, nil)
	if err != nil {
		return nil, err
	}

	var (
		frozen          bool
		optCells        string
		optTabletTypes  string
		targets         = make(map[string]*MigrationTarget, len(targetShards))
		workflowType    binlogdatapb.VReplicationWorkflowType
		workflowSubType binlogdatapb.VReplicationWorkflowSubType
		options         vtctldatapb.WorkflowOptions
	)

	// We check all shards in the target keyspace. Not all of them may have a
	// stream. For example, if we're splitting -80 to [-40,40-80], only those
	// two target shards will have vreplication streams, and the other shards in
	// the target keyspace will not.
	for targetShardName, targetShard := range targetShards {
		if targetShard.PrimaryAlias == nil {
			// This can happen if bad inputs are given.
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "shard %v/%v doesn't have a primary set", targetKeyspace, targetShard)
		}

		primary, err := ts.GetTablet(ctx, targetShard.PrimaryAlias)
		if err != nil {
			return nil, err
		}

		wf, err := tmc.ReadVReplicationWorkflow(ctx, primary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: workflow,
		})
		if err != nil {
			return nil, err
		}

		if wf == nil || len(wf.Streams) < 1 {
			continue
		}

		target := &MigrationTarget{
			si:      targetShard,
			primary: primary,
			Sources: make(map[int32]*binlogdatapb.BinlogSource),
		}

		optCells = wf.Cells
		optTabletTypes = topoproto.MakeStringTypeCSV(wf.TabletTypes)
		workflowType = wf.WorkflowType
		workflowSubType = wf.WorkflowSubType
		optionsJSON := wf.GetOptions()
		if optionsJSON != "" {
			if err := json.Unmarshal([]byte(optionsJSON), &options); err != nil {
				log.Errorf("failed to unmarshal options: %v %s", err, optionsJSON)
				return nil, err
			}
		}

		for _, stream := range wf.Streams {
			if stream.Message == Frozen {
				frozen = true
			}
			target.Sources[stream.Id] = stream.Bls
		}

		targets[targetShardName] = target
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w in keyspace %s for %s", ErrNoStreams, targetKeyspace, workflow)
	}

	return &TargetInfo{
		Targets:         targets,
		Frozen:          frozen,
		OptCells:        optCells,
		OptTabletTypes:  optTabletTypes,
		WorkflowType:    workflowType,
		WorkflowSubType: workflowSubType,
		Options:         &options,
	}, nil
}

func getSourceAndTargetKeyRanges(sourceShards, targetShards []string) (*topodatapb.KeyRange, *topodatapb.KeyRange, error) {
	if len(sourceShards) == 0 || len(targetShards) == 0 {
		return nil, nil, fmt.Errorf("either source or target shards are missing")
	}

	getKeyRange := func(shard string) (*topodatapb.KeyRange, error) {
		krs, err := key.ParseShardingSpec(shard)
		if err != nil {
			return nil, err
		}
		return krs[0], nil
	}

	// Happily string sorting of shards also sorts them in the ascending order of key
	// ranges in vitess.
	sort.Strings(sourceShards)
	sort.Strings(targetShards)
	getFullKeyRange := func(shards []string) (*topodatapb.KeyRange, error) {
		// Expect sorted shards.
		kr1, err := getKeyRange(sourceShards[0])
		if err != nil {
			return nil, err
		}
		kr2, err := getKeyRange(sourceShards[len(sourceShards)-1])
		if err != nil {
			return nil, err
		}
		return &topodatapb.KeyRange{
			Start: kr1.Start,
			End:   kr2.End,
		}, nil
	}

	skr, err := getFullKeyRange(sourceShards)
	if err != nil {
		return nil, nil, err
	}
	tkr, err := getFullKeyRange(targetShards)
	if err != nil {
		return nil, nil, err
	}

	return skr, tkr, nil
}

// CompareShards compares the list of shards in a workflow with the shards in
// that keyspace according to the topo. It returns an error if they do not match.
//
// This function is used to validate MoveTables workflows.
//
// (TODO|@ajm188): This function is temporarily-exported until *wrangler.trafficSwitcher
// has been fully moved over to this package. Once that refactor is finished,
// this function should be unexported. Consequently, YOU SHOULD NOT DEPEND ON
// THIS FUNCTION EXTERNALLY.
func CompareShards(ctx context.Context, keyspace string, shards []*topo.ShardInfo, ts *topo.Server) error {
	shardSet := sets.New[string]()
	for _, si := range shards {
		shardSet.Insert(si.ShardName())
	}

	topoShards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	topoShardSet := sets.New[string](topoShards...)
	if !shardSet.Equal(topoShardSet) {
		wfExtra := shardSet.Difference(topoShardSet)
		topoExtra := topoShardSet.Difference(shardSet)

		var rec concurrency.AllErrorRecorder
		if wfExtra.Len() > 0 {
			wfExtraSorted := sets.List(wfExtra)
			rec.RecordError(fmt.Errorf("switch command shards not in topo: %v", wfExtraSorted))
		}

		if topoExtra.Len() > 0 {
			topoExtraSorted := sets.List(topoExtra)
			rec.RecordError(fmt.Errorf("topo shards not in switch command: %v", topoExtraSorted))
		}

		return fmt.Errorf("mismatched shards for keyspace %s: %s", keyspace, strings.Join(rec.ErrorStrings(), "; "))
	}

	return nil
}

// HashStreams produces a stable hash based on the target keyspace and migration
// targets.
func HashStreams(targetKeyspace string, targets map[string]*MigrationTarget) int64 {
	var expanded []string
	for shard, target := range targets {
		for uid := range target.Sources {
			expanded = append(expanded, fmt.Sprintf("%s:%d", shard, uid))
		}
	}

	sort.Strings(expanded)

	hasher := fnv.New64()
	hasher.Write([]byte(targetKeyspace))

	for _, s := range expanded {
		hasher.Write([]byte(s))
	}

	// Convert to int64 after dropping the highest bit.
	return int64(hasher.Sum64() & math.MaxInt64)
}

func doValidateWorkflowHasCompleted(ctx context.Context, ts *trafficSwitcher) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	if ts.MigrationType() == binlogdatapb.MigrationType_SHARDS {
		_ = ts.ForAllSources(func(source *MigrationSource) error {
			wg.Add(1)
			if source.GetShard().IsPrimaryServing {
				rec.RecordError(fmt.Errorf(fmt.Sprintf("Shard %s is still serving", source.GetShard().ShardName())))
			}
			wg.Done()
			return nil
		})
	} else {
		_ = ts.ForAllTargets(func(target *MigrationTarget) error {
			wg.Add(1)
			defer wg.Done()
			res, err := ts.ws.tmc.ReadVReplicationWorkflow(ctx, target.GetPrimary().Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
				Workflow: ts.WorkflowName(),
			})
			if err != nil {
				rec.RecordError(err)
				return nil
			}
			for _, stream := range res.Streams {
				if stream.Message != Frozen {
					rec.RecordError(fmt.Errorf("vreplication streams are not frozen on tablet %d", target.GetPrimary().Alias.Uid))
					return nil
				}
			}
			return nil
		})
	}
	wg.Wait()

	if !ts.keepRoutingRules {
		// Check if table is routable.
		if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
			rules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
			if err != nil {
				rec.RecordError(fmt.Errorf("could not get RoutingRules"))
			}
			for fromTable, toTables := range rules {
				for _, toTable := range toTables {
					for _, table := range ts.Tables() {
						if toTable == fmt.Sprintf("%s.%s", ts.SourceKeyspaceName(), table) {
							rec.RecordError(fmt.Errorf("routing still exists from keyspace %s table %s to %s", ts.SourceKeyspaceName(), table, fromTable))
						}
					}
				}
			}
		}
	}
	if rec.HasErrors() {
		return fmt.Errorf("%s", strings.Join(rec.ErrorStrings(), "\n"))
	}
	return nil

}

// ReverseWorkflowName returns the "reversed" name of a workflow. For a
// "forward" workflow, this is the workflow name with "_reverse" appended, and
// for a "reversed" workflow, this is the workflow name with the "_reverse"
// suffix removed.
func ReverseWorkflowName(workflow string) string {
	if strings.HasSuffix(workflow, reverseSuffix) {
		return workflow[:len(workflow)-len(reverseSuffix)]
	}

	return workflow + reverseSuffix
}

// Straight copy-paste of encodeString from wrangler/keyspace.go. I want to make
// this public, but it doesn't belong in package workflow. Maybe package sqltypes,
// or maybe package sqlescape?
func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

func getRenameFileName(tableName string) string {
	return fmt.Sprintf(renameTableTemplate, tableName)
}

func parseTabletTypes(tabletTypes []topodatapb.TabletType) (hasReplica, hasRdonly, hasPrimary bool, err error) {
	for _, tabletType := range tabletTypes {
		switch {
		case tabletType == topodatapb.TabletType_REPLICA:
			hasReplica = true
		case tabletType == topodatapb.TabletType_RDONLY:
			hasRdonly = true
		case tabletType == topodatapb.TabletType_PRIMARY:
			hasPrimary = true
		default:
			return false, false, false, fmt.Errorf("invalid tablet type passed %s", tabletType)
		}
	}
	return hasReplica, hasRdonly, hasPrimary, nil
}

func areTabletsAvailableToStreamFrom(ctx context.Context, req *vtctldatapb.WorkflowSwitchTrafficRequest, ts *trafficSwitcher, keyspace string, shards []*topo.ShardInfo) error {
	// We use the value from the workflow for the TabletPicker.
	tabletTypesStr := ts.optTabletTypes
	cells := req.Cells
	// If no cells were provided in the command then use the value from the workflow.
	if len(cells) == 0 && ts.optCells != "" {
		cells = strings.Split(strings.TrimSpace(ts.optCells), ",")
	}

	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(cells []string, keyspace string, shard *topo.ShardInfo) {
			defer wg.Done()
			if cells == nil {
				cells = append(cells, shard.PrimaryAlias.Cell)
			}
			tp, err := discovery.NewTabletPicker(ctx, ts.ws.ts, cells, shard.PrimaryAlias.Cell, keyspace, shard.ShardName(), tabletTypesStr, discovery.TabletPickerOptions{})
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			tablets := tp.GetMatchingTablets(ctx)
			if len(tablets) == 0 {
				allErrors.RecordError(fmt.Errorf("no tablet found to source data in keyspace %s, shard %s", keyspace, shard.ShardName()))
				return
			}
		}(cells, keyspace, shard)
	}

	wg.Wait()
	if allErrors.HasErrors() {
		log.Errorf("%s", allErrors.Error())
		return allErrors.Error()
	}
	return nil
}

// LegacyBuildTargets collects MigrationTargets and other metadata (see TargetInfo)
// from a workflow in the target keyspace. It uses VReplicationExec to get the workflow
// details rather than the new TabletManager ReadVReplicationWorkflow RPC. This is
// being used to slowly transition all of the older code, including unit tests, over to
// the new RPC and limit the impact of the new implementation to vtctldclient. You can see
// how the unit tests were being migrated here: https://gist.github.com/mattlord/738c12befe951f8d09304ff7fdc47c46
//
// New callers should instead use the new BuildTargets function.
//
// It returns ErrNoStreams if there are no targets found for the workflow.
func LegacyBuildTargets(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, targetKeyspace string, workflow string,
	targetShards []string) (*TargetInfo, error) {

	var (
		frozen          bool
		optCells        string
		optTabletTypes  string
		targets         = make(map[string]*MigrationTarget, len(targetShards))
		workflowType    binlogdatapb.VReplicationWorkflowType
		workflowSubType binlogdatapb.VReplicationWorkflowSubType
	)

	getVReplicationWorkflowType := func(row sqltypes.RowNamedValues) binlogdatapb.VReplicationWorkflowType {
		i, _ := row["workflow_type"].ToInt32()
		return binlogdatapb.VReplicationWorkflowType(i)
	}

	getVReplicationWorkflowSubType := func(row sqltypes.RowNamedValues) binlogdatapb.VReplicationWorkflowSubType {
		i, _ := row["workflow_sub_type"].ToInt32()
		return binlogdatapb.VReplicationWorkflowSubType(i)
	}

	// We check all shards in the target keyspace. Not all of them may have a
	// stream. For example, if we're splitting -80 to [-40,40-80], only those
	// two target shards will have vreplication streams, and the other shards in
	// the target keyspace will not.
	for _, targetShard := range targetShards {
		si, err := ts.GetShard(ctx, targetKeyspace, targetShard)
		if err != nil {
			return nil, err
		}

		if si.PrimaryAlias == nil {
			// This can happen if bad inputs are given.
			return nil, fmt.Errorf("shard %v/%v doesn't have a primary set", targetKeyspace, targetShard)
		}

		primary, err := ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return nil, err
		}

		// NB: changing the whitespace of this query breaks tests for now.
		// (TODO:@ajm188) extend FakeDBClient to be less whitespace-sensitive on
		// expected queries.
		query := fmt.Sprintf("select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(primary.DbName()))
		p3qr, err := tmc.VReplicationExec(ctx, primary.Tablet, query)
		if err != nil {
			return nil, err
		}

		if len(p3qr.Rows) < 1 {
			continue
		}

		target := &MigrationTarget{
			si:      si,
			primary: primary,
			Sources: make(map[int32]*binlogdatapb.BinlogSource),
		}

		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Named().Rows {
			id, err := row["id"].ToInt32()
			if err != nil {
				return nil, err
			}

			var bls binlogdatapb.BinlogSource
			rowBytes, err := row["source"].ToBytes()
			if err != nil {
				return nil, err
			}
			if err := prototext.Unmarshal(rowBytes, &bls); err != nil {
				return nil, err
			}

			if row["message"].ToString() == Frozen {
				frozen = true
			}

			target.Sources[id] = &bls
			optCells = row["cell"].ToString()
			optTabletTypes = row["tablet_types"].ToString()

			workflowType = getVReplicationWorkflowType(row)
			workflowSubType = getVReplicationWorkflowSubType(row)

		}

		targets[targetShard] = target
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w in keyspace %s for %s", ErrNoStreams, targetKeyspace, workflow)
	}

	return &TargetInfo{
		Targets:         targets,
		Frozen:          frozen,
		OptCells:        optCells,
		OptTabletTypes:  optTabletTypes,
		WorkflowType:    workflowType,
		WorkflowSubType: workflowSubType,
	}, nil
}

func addFilter(sel *sqlparser.Select, filter sqlparser.Expr) {
	if sel.Where != nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: &sqlparser.AndExpr{
				Left:  filter,
				Right: sel.Where.Expr,
			},
		}
	} else {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: filter,
		}
	}
}

func getTenantClause(vrOptions *vtctldatapb.WorkflowOptions,
	targetVSchema *vindexes.KeyspaceSchema, parser *sqlparser.Parser) (*sqlparser.Expr, error) {
	if vrOptions.TenantId == "" {
		return nil, nil
	}
	if targetVSchema == nil || targetVSchema.MultiTenantSpec == nil {
		return nil, fmt.Errorf("target keyspace not defined, or it does not have multi-tenant spec")
	}
	tenantColumnName := targetVSchema.MultiTenantSpec.TenantIdColumnName
	tenantColumnType := targetVSchema.MultiTenantSpec.TenantIdColumnType
	if tenantColumnName == "" {
		return nil, fmt.Errorf("tenant column name not defined in multi-tenant spec")
	}

	var tenantId string
	switch tenantColumnType {
	case querypb.Type_INT64:
		_, err := strconv.Atoi(vrOptions.TenantId)
		if err != nil {
			return nil, fmt.Errorf("tenant id is not a valid int: %s", vrOptions.TenantId)
		}
		tenantId = vrOptions.TenantId
	case querypb.Type_VARCHAR:
		tenantId = fmt.Sprintf("'%s'", vrOptions.TenantId)
	default:
		return nil, fmt.Errorf("unsupported tenant column type: %s", tenantColumnType)
	}

	stmt, err := parser.Parse(fmt.Sprintf("select * from t where %s = %s", tenantColumnName, tenantId))
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("error getting select: %s", tenantId)
	}
	return &sel.Where.Expr, nil
}

func changeKeyspaceRouting(ctx context.Context, ts *topo.Server, tabletTypes []topodatapb.TabletType,
	sourceKeyspace, targetKeyspace, reason string) error {
	routes := make(map[string]string)
	for _, tabletType := range tabletTypes {
		suffix := getTabletTypeSuffix(tabletType)
		routes[sourceKeyspace+suffix] = targetKeyspace
	}
	if err := updateKeyspaceRoutingRules(ctx, ts, reason, routes); err != nil {
		return err
	}
	return ts.RebuildSrvVSchema(ctx, nil)
}

// updateKeyspaceRoutingRules updates the keyspace routing rules for the (effective) source
// keyspace to the target keyspace.
func updateKeyspaceRoutingRules(ctx context.Context, ts *topo.Server, reason string, routes map[string]string) error {
	update := func() error {
		return topotools.UpdateKeyspaceRoutingRules(ctx, ts, reason,
			func(ctx context.Context, rules *map[string]string) error {
				for fromKeyspace, toKeyspace := range routes {
					(*rules)[fromKeyspace] = toKeyspace
				}
				return nil
			})
	}
	err := update()
	if err == nil {
		return nil
	}
	// If we were racing with another caller to create the initial routing rules, then
	// we can immediately retry the operation.
	if !topo.IsErrType(err, topo.NodeExists) {
		return err
	}
	return update()
}

func validateTenantId(dataType querypb.Type, value string) error {
	switch dataType {
	case querypb.Type_INT64:
		_, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("value %s is not a valid int", value)
		}
	case querypb.Type_VARCHAR:
	// no validation needed
	default:
		return fmt.Errorf("unsupported data type: %s", dataType)
	}
	return nil
}

func updateKeyspaceRoutingState(ctx context.Context, ts *topo.Server, sourceKeyspace, targetKeyspace string, state *State) error {
	// For multi-tenant migrations, we only support switching traffic to all cells at once
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return err
	}

	rules, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	if err != nil {
		return err
	}
	hasSwitched := func(tabletTypePrefix string) bool {
		ks, ok := rules[sourceKeyspace+tabletTypePrefix]
		return ok && ks == targetKeyspace
	}
	rdonlySwitched := hasSwitched(rdonlyTabletSuffix)
	replicaSwitched := hasSwitched(replicaTabletSuffix)
	primarySwitched := hasSwitched(primaryTabletSuffix)
	if rdonlySwitched {
		state.RdonlyCellsSwitched = cells
		state.RdonlyCellsNotSwitched = nil
	} else {
		state.RdonlyCellsNotSwitched = cells
		state.RdonlyCellsSwitched = nil
	}
	if replicaSwitched {
		state.ReplicaCellsSwitched = cells
		state.ReplicaCellsNotSwitched = nil
	} else {
		state.ReplicaCellsNotSwitched = cells
		state.ReplicaCellsSwitched = nil
	}
	state.WritesSwitched = primarySwitched
	return nil
}

func getTabletTypeSuffix(tabletType topodatapb.TabletType) string {
	switch tabletType {
	case topodatapb.TabletType_REPLICA:
		return replicaTabletSuffix
	case topodatapb.TabletType_RDONLY:
		return rdonlyTabletSuffix
	case topodatapb.TabletType_PRIMARY:
		return primaryTabletSuffix
	}
	return ""
}
