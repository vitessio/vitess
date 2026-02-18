/*
Copyright 2026 The Vitess Authors.

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
	"log/slog"
	"strings"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// resolveViewsOptions holds the options to resolve views for a MoveTables create request.
type resolveViewsOptions struct {
	// req is the MoveTables create request.
	req *vtctldatapb.MoveTablesCreateRequest

	// sourceViews contains the definitions of the views that exist in the source keyspace.
	sourceViews map[string]*tabletmanagerdatapb.TableDefinition

	// skipViewValidation skips validating that views reference tables also being moved.
	skipViewValidation bool

	// sourceTables contains the definitions of the tables that exist in the source keyspace. Used to
	// verify that all tables referenced by views are included in the MoveTables.
	sourceTables map[string]*tabletmanagerdatapb.TableDefinition

	// targetTables contains the table names that will be available on the target after the
	// MoveTables completes. This includes tables being moved and tables already on the target. Used
	// to verify that all tables referenced by views will be available.
	targetTables []string
}

// resolveViews returns the set of views the MoveTables workflow should move based on the request. Returns an error if
// all the views given in `IncludeViews` were excluded by `ExcludeViews`, or if a table that a view references is not included
// in the MoveTables.
func (s *Server) resolveViews(opts resolveViewsOptions) ([]string, error) {
	includedViews := make(map[string]*tabletmanagerdatapb.TableDefinition)

	// If a specific set of views was requested, validate that they exist in the source
	// keyspace and use those.
	if len(opts.req.IncludeViews) > 0 {
		if err := includeViews(opts, includedViews); err != nil {
			return nil, err
		}
	} else if opts.req.AllViews {
		includedViews = opts.sourceViews
	} else {
		// No request to move views, ignore.
		return nil, nil
	}

	// If a specific set of views was requested to be excluded, validate them and filter them out.
	if len(opts.req.ExcludeViews) > 0 {
		if err := excludeViews(opts, includedViews); err != nil {
			return nil, err
		}
	}

	if opts.skipViewValidation {
		return maps.Keys(includedViews), nil
	}

	// Validate that all the tables that the views reference are included in the MoveTables or
	// already exist on the target.
	if err := s.validateViewReferences(includedViews, opts.targetTables); err != nil {
		return nil, err
	}

	return maps.Keys(includedViews), nil
}

// validateViewReferences checks that all tables referenced by the given views are present
// in targetTables.
func (s *Server) validateViewReferences(views map[string]*tabletmanagerdatapb.TableDefinition, targetTables []string) error {
	targetTableSet := make(map[string]struct{}, len(targetTables))
	for _, table := range targetTables {
		targetTableSet[table] = struct{}{}
	}

	errMsg := strings.Builder{}
	for viewName, viewDefinition := range views {
		// The view definition contains {{.DatabaseName}} placeholders. Fill them with a dummy value
		// so we can parse the DDL to extract table references.
		ddl, err := fillStringTemplate(viewDefinition.Schema, map[string]string{"DatabaseName": "db"})
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to fill template for view %q: %v", viewName, err)
		}

		// Parse the view's definition.
		statement, err := s.env.Parser().ParseStrictDDL(ddl)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to parse view %q definition: %v", viewName, err)
		}

		createViewStatement, ok := statement.(*sqlparser.CreateView)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to assert view %q statement as CreateView", viewName)
		}

		// Get all the tables the view references.
		tableNames, _ := schemadiff.GetViewDependentTableNames(createViewStatement)

		// Validate each table referenced is also being moved.
		var missingTables []string
		for _, tableName := range tableNames {
			if _, alsoMoved := targetTableSet[tableName]; !alsoMoved {
				missingTables = append(missingTables, tableName)
			}
		}

		// If we found any missing tables for this view, add it to the error message.
		if len(missingTables) > 0 {
			fmt.Fprintf(&errMsg, "view %q is missing table(s) %q", viewName, strings.Join(missingTables, ", "))
		}
	}

	// If the error message contains anything, it means we found views that are missing their tables.
	if errMsg.Len() > 0 {
		return fmt.Errorf("%w: %q", errViewMissingTable, errMsg.String())
	}

	return nil
}

// includeViews adds the views in IncludeViews to views.
func includeViews(opts resolveViewsOptions, views map[string]*tabletmanagerdatapb.TableDefinition) error {
	var missingViews []string

	for _, viewName := range opts.req.IncludeViews {
		viewDefinition, exists := opts.sourceViews[viewName]
		if !exists {
			missingViews = append(missingViews, viewName)
			continue
		}

		// Include this view.
		views[viewName] = viewDefinition
	}

	// Return an error if we found any views that don't exist in the source keyspace.
	if len(missingViews) > 0 {
		return fmt.Errorf("%w %q: %q", errViewsNotFound, opts.req.SourceKeyspace, strings.Join(missingViews, ","))
	}

	return nil
}

// excludeViews excludes the views in ExcludeViews from views.
func excludeViews(opts resolveViewsOptions, views map[string]*tabletmanagerdatapb.TableDefinition) error {
	var missingViews []string

	for _, viewName := range opts.req.ExcludeViews {
		_, exists := opts.sourceViews[viewName]
		if !exists {
			missingViews = append(missingViews, viewName)
			continue
		}

		// Exclude this view.
		delete(views, viewName)
	}

	// Return an error if we found any views that don't exist in the source keyspace.
	if len(missingViews) > 0 {
		return fmt.Errorf("%w %q: %q", errViewsNotFound, opts.req.SourceKeyspace, strings.Join(missingViews, ","))
	}

	// Make sure we didn't exclude all views.
	if len(views) == 0 {
		return errNoViewsToMove
	}

	return nil
}

// deployViews creates the materializer's views on the target keyspace.
func (mz *materializer) deployViews(ctx context.Context, views []string) error {
	if len(views) == 0 {
		return nil
	}

	// Collect all the DDLs from the source.
	sourceDDLMap, err := getSourceDDLs(ctx, mz.sourceTs, mz.tmc, mz.sourceShards, true)
	if err != nil {
		return fmt.Errorf("deploy views: failed to get source ddls: %w", err)
	}

	// Create the views on each target shard.
	return forAllShards(mz.targetShards, func(target *topo.ShardInfo) error {
		// Fetch existing views so we can skip them if they already exist on the target.
		targetViewSet, err := mz.getTargetViewSet(ctx, target)
		if err != nil {
			return fmt.Errorf("deploy views: failed to get target view set: %w", err)
		}

		// Get the primary tablet's info to run the DDLs on it.
		targetTablet, err := mz.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return fmt.Errorf("deploy views: failed to get target tablet: %w", err)
		}

		// Collect the views that don't already exist on the target.
		viewsToCreate := make(map[string]struct{})
		for _, view := range views {
			if _, exists := targetViewSet[view]; exists {
				continue
			}

			viewsToCreate[view] = struct{}{}
		}

		if len(viewsToCreate) == 0 {
			log.Info("deploy views: all views already created")
			return nil
		}

		sql, err := mz.buildViewsDDL(sourceDDLMap, viewsToCreate, targetTablet.DbName())
		if err != nil {
			return fmt.Errorf("deploy views: failed to build views ddl: %w", err)
		}

		log.Info("deploy views: applying schema", slog.String("sql", sql))

		// Apply the DDLs on the tablet.
		sc := &tmutils.SchemaChange{SQL: sql, Force: false, AllowReplication: true, SQLMode: vreplication.SQLMode}
		_, err = mz.tmc.ApplySchema(ctx, targetTablet.Tablet, sc)
		if err != nil {
			return fmt.Errorf("deploy views: failed to apply schema: %w", err)
		}

		return nil
	})
}

// getTargetViewSet returns the current views on the given target.
func (mz *materializer) getTargetViewSet(ctx context.Context, target *topo.ShardInfo) (map[string]struct{}, error) {
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{"/.*/"}, IncludeViews: true}
	targetSchema, err := schematools.GetSchema(ctx, mz.ts, mz.tmc, target.PrimaryAlias, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for target: %w", err)
	}

	targetViews := make(map[string]struct{})
	for _, td := range targetSchema.TableDefinitions {
		// Filter out non-views
		if td.Type != tmutils.TableView {
			continue
		}

		targetViews[td.Name] = struct{}{}
	}

	return targetViews, nil
}

// buildViewsDDL returns the view DDL to apply on the target database. The source DDLs contain
// {{.DatabaseName}} placeholders which are replaced with the target database name. Views are
// sorted in dependency order.
func (mz *materializer) buildViewsDDL(sourceDDLMap map[string]string, views map[string]struct{}, dbName string) (string, error) {
	// Replace {{.DatabaseName}} placeholders with the actual target database name.
	sourceDDLs := make([]string, 0, len(sourceDDLMap))
	for _, ddl := range sourceDDLMap {
		ddl, err := fillStringTemplate(ddl, map[string]string{"DatabaseName": dbName})
		if err != nil {
			return "", fmt.Errorf("failed to fill string template: %w", err)
		}

		sourceDDLs = append(sourceDDLs, ddl)
	}

	// We need to sort the views in dependency order, as one view may be reliant on another.
	env := schemadiff.NewEnv(mz.env, mz.env.CollationEnv().DefaultConnectionCharset())
	schema, err := schemadiff.NewSchemaFromQueries(env, sourceDDLs)
	if err != nil {
		return "", fmt.Errorf("failed to create schema from queries: %w", err)
	}
	orderedViews := schema.Views()

	// Collect the DDLs (in dependency order) for the views that we should create.
	var ddls []string
	for _, view := range orderedViews {
		name := view.Name()
		if _, shouldCreate := views[name]; !shouldCreate {
			continue
		}

		ddls = append(ddls, view.Create().CanonicalStatementString())
	}

	return strings.Join(ddls, ";\n"), nil
}
