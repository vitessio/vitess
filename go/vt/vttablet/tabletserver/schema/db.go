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

package schema

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

const (
	// insertTableIntoSchemaEngineTables inserts a record in the datastore for the schema-engine tables.
	insertTableIntoSchemaEngineTables = `INSERT INTO %s.tables(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, CREATE_TIME)
values (database(), :table_name, :create_statement, :create_time)`

	// deleteFromSchemaEngineTablesTable removes the tables from the table that have been modified.
	deleteFromSchemaEngineTablesTable = `DELETE FROM %s.tables WHERE TABLE_SCHEMA = database() AND TABLE_NAME IN ::tableNames`

	// readTableCreateTimes reads the tables create times
	readTableCreateTimes = "SELECT TABLE_NAME, CREATE_TIME FROM %s.`tables`"

	// fetchUpdatedTables queries fetches information about updated tables
	fetchUpdatedTables = `select table_name, create_statement from %s.tables where table_schema = database() and table_name in ::tableNames`

	// fetchTables queries fetches all information about tables
	fetchTables = `select table_name, create_statement from %s.tables where table_schema = database()`

	// detectViewChange query detects if there is any view change from previous copy.
	detectViewChange = `
SELECT distinct table_name
FROM (
	SELECT table_name, view_definition
	FROM information_schema.views
	WHERE table_schema = database()

	UNION ALL

	SELECT table_name, view_definition
	FROM %s.views
	WHERE table_schema = database()
) _inner
GROUP BY table_name, view_definition
HAVING COUNT(*) = 1
`

	// insertViewIntoSchemaEngineViews using information_schema.views.
	insertViewIntoSchemaEngineViews = `INSERT INTO %s.views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION)
values (database(), :view_name, :create_statement, :view_definition)`

	// deleteFromSchemaEngineViewsTable removes the views from the table that have been modified.
	deleteFromSchemaEngineViewsTable = `DELETE FROM %s.views WHERE TABLE_SCHEMA = database() AND TABLE_NAME IN ::viewNames`

	// fetchViewDefinitions retrieves view definition from information_schema.views table.
	fetchViewDefinitions = `select table_name, view_definition from information_schema.views
where table_schema = database() and table_name in ::viewNames`

	// fetchCreateStatement retrieves create statement.
	fetchCreateStatement = `show create table %s`

	// fetchUpdatedViews queries fetches information about updated views
	fetchUpdatedViews = `select table_name, create_statement from %s.views where table_schema = database() and table_name in ::viewNames`

	// fetchViews queries fetches all views
	fetchViews = `select table_name, create_statement from %s.views where table_schema = database()`

	// fetchUpdatedTablesAndViews queries fetches information about updated tables and views
	fetchUpdatedTablesAndViews = `select table_name, create_statement from %s.tables where table_schema = database() and table_name in ::tableNames union select table_name, create_statement from %s.views where table_schema = database() and table_name in ::tableNames`

	// fetchTablesAndViews queries fetches all information about tables and views
	fetchTablesAndViews = `select table_name, create_statement from %s.tables where table_schema = database() union select table_name, create_statement from %s.views where table_schema = database()`
)

// reloadTablesDataInDB reloads teh tables information we have stored in our database we use for schema-tracking.
func reloadTablesDataInDB(ctx context.Context, conn *connpool.DBConn, tables []*Table, droppedTables []string) error {
	// No need to do anything if we have no tables to refresh or drop.
	if len(tables) == 0 && len(droppedTables) == 0 {
		return nil
	}

	// Delete all the tables that are dropped or modified.
	tableNamesToDelete := droppedTables
	for _, table := range tables {
		tableNamesToDelete = append(tableNamesToDelete, table.Name.String())
	}
	tablesBV, err := sqltypes.BuildBindVariable(tableNamesToDelete)
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{"tableNames": tablesBV}

	// Get the create statements for all the tables that are modified.
	var createStatements []string
	for _, table := range tables {
		cs, err := getCreateStatement(ctx, conn, sqlparser.String(table.Name))
		if err != nil {
			return err
		}
		createStatements = append(createStatements, cs)
	}

	// Generate the queries to delete and insert table data.
	clearTableParsedQuery, err := generateFullQuery(deleteFromSchemaEngineTablesTable)
	if err != nil {
		return err
	}
	clearTableQuery, err := clearTableParsedQuery.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}

	insertTablesParsedQuery, err := generateFullQuery(insertTableIntoSchemaEngineTables)
	if err != nil {
		return err
	}

	// Reload the tables in a transaction.
	_, err = conn.Exec(ctx, "begin", 1, false)
	if err != nil {
		return err
	}
	defer conn.Exec(ctx, "rollback", 1, false)

	_, err = conn.Exec(ctx, clearTableQuery, 1, false)
	if err != nil {
		return err
	}

	for idx, table := range tables {
		bv["table_name"] = sqltypes.StringBindVariable(table.Name.String())
		bv["create_statement"] = sqltypes.StringBindVariable(createStatements[idx])
		bv["create_time"] = sqltypes.Int64BindVariable(table.CreateTime)
		insertTableQuery, err := insertTablesParsedQuery.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}
		_, err = conn.Exec(ctx, insertTableQuery, 1, false)
		if err != nil {
			return err
		}
	}

	_, err = conn.Exec(ctx, "commit", 1, false)
	return err
}

// generateFullQuery generates the full query from the query as a string.
func generateFullQuery(query string) (*sqlparser.ParsedQuery, error) {
	stmt, err := sqlparser.Parse(
		sqlparser.BuildParsedQuery(query, sidecardb.GetIdentifier(), sidecardb.GetIdentifier()).Query)
	if err != nil {
		return nil, err
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.Format(buf)
	return buf.ParsedQuery(), nil
}

// reloadViewsDataInDB reloads teh views information we have stored in our database we use for schema-tracking.
func reloadViewsDataInDB(ctx context.Context, conn *connpool.DBConn, views []*Table, droppedViews []string) error {
	// No need to do anything if we have no views to refresh or drop.
	if len(views) == 0 && len(droppedViews) == 0 {
		return nil
	}

	// Delete all the views that are dropped or modified.
	viewNamesToDelete := droppedViews
	for _, view := range views {
		viewNamesToDelete = append(viewNamesToDelete, view.Name.String())
	}
	viewsBV, err := sqltypes.BuildBindVariable(viewNamesToDelete)
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{"viewNames": viewsBV}

	// Get the create statements for all the views that are modified.
	var createStatements []string
	for _, view := range views {
		cs, err := getCreateStatement(ctx, conn, sqlparser.String(view.Name))
		if err != nil {
			return err
		}
		createStatements = append(createStatements, cs)
	}

	// Get the view definitions for all the views that are modified.
	// We only need to run this if we have any views to reload.
	viewDefinitions := make(map[string]string)
	if len(views) > 0 {
		err = getViewDefinition(ctx, conn, bv,
			func(qr *sqltypes.Result) error {
				for _, row := range qr.Rows {
					viewDefinitions[row[0].ToString()] = row[1].ToString()
				}
				return nil
			},
			func() *sqltypes.Result { return &sqltypes.Result{} },
			1000,
		)
		if err != nil {
			return err
		}
	}

	// Generate the queries to delete and insert view data.
	clearViewParsedQuery, err := generateFullQuery(deleteFromSchemaEngineViewsTable)
	if err != nil {
		return err
	}
	clearViewQuery, err := clearViewParsedQuery.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}

	insertViewsParsedQuery, err := generateFullQuery(insertViewIntoSchemaEngineViews)
	if err != nil {
		return err
	}

	// Reload the views in a transaction.
	_, err = conn.Exec(ctx, "begin", 1, false)
	if err != nil {
		return err
	}
	defer conn.Exec(ctx, "rollback", 1, false)

	_, err = conn.Exec(ctx, clearViewQuery, 1, false)
	if err != nil {
		return err
	}

	for idx, view := range views {
		bv["view_name"] = sqltypes.StringBindVariable(view.Name.String())
		bv["create_statement"] = sqltypes.StringBindVariable(createStatements[idx])
		bv["view_definition"] = sqltypes.StringBindVariable(viewDefinitions[view.Name.String()])
		insertViewQuery, err := insertViewsParsedQuery.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}
		_, err = conn.Exec(ctx, insertViewQuery, 1, false)
		if err != nil {
			return err
		}
	}

	_, err = conn.Exec(ctx, "commit", 1, false)
	return err
}

// getViewDefinition gets the viewDefinition for the given views.
func getViewDefinition(ctx context.Context, conn *connpool.DBConn, bv map[string]*querypb.BindVariable, callback func(qr *sqltypes.Result) error, alloc func() *sqltypes.Result, bufferSize int) error {
	viewsDefParsedQuery, err := generateFullQuery(fetchViewDefinitions)
	if err != nil {
		return err
	}
	viewsDefQuery, err := viewsDefParsedQuery.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	return conn.Stream(ctx, viewsDefQuery, callback, alloc, bufferSize, 0)
}

// getCreateStatement gets the create-statement for the given view/table.
func getCreateStatement(ctx context.Context, conn *connpool.DBConn, tableName string) (string, error) {
	res, err := conn.Exec(ctx, sqlparser.BuildParsedQuery(fetchCreateStatement, tableName).Query, 1, false)
	if err != nil {
		return "", err
	}
	return res.Rows[0][1].ToString(), nil
}

// getChangedViewNames gets the list of views that have their definitions changed.
func getChangedViewNames(ctx context.Context, conn *connpool.DBConn, isServingPrimary bool) (map[string]any, error) {
	/* Retrieve changed views */
	views := make(map[string]any)
	if !isServingPrimary {
		return views, nil
	}
	callback := func(qr *sqltypes.Result) error {
		for _, row := range qr.Rows {
			view := row[0].ToString()
			views[view] = true
		}
		return nil
	}
	alloc := func() *sqltypes.Result { return &sqltypes.Result{} }
	bufferSize := 1000

	viewChangeQuery := sqlparser.BuildParsedQuery(detectViewChange, sidecardb.GetIdentifier()).Query
	err := conn.Stream(ctx, viewChangeQuery, callback, alloc, bufferSize, 0)
	if err != nil {
		return nil, err
	}

	return views, nil
}

// getMismatchedTableNames gets the tables that do not align with the tables information we have in the cache.
func (se *Engine) getMismatchedTableNames(ctx context.Context, conn *connpool.DBConn, isServingPrimary bool) (map[string]any, error) {
	tablesMismatched := make(map[string]any)
	if !isServingPrimary {
		return tablesMismatched, nil
	}
	tablesFound := make(map[string]bool)
	callback := func(qr *sqltypes.Result) error {
		// For each row we check 2 things —
		// 1. If a table exists in our database, but not in the cache, then it could have been dropped.
		// 2. If the table's create time in our database doesn't match that in our cache, then it could have been altered.
		for _, row := range qr.Rows {
			tableName := row[0].ToString()
			createTime, _ := row[1].ToInt64()
			tablesFound[tableName] = true
			table, isFound := se.tables[tableName]
			if !isFound || table.CreateTime != createTime {
				tablesMismatched[tableName] = true
			}
		}
		return nil
	}
	alloc := func() *sqltypes.Result { return &sqltypes.Result{} }
	bufferSize := 1000
	readTableCreateTimesQuery := sqlparser.BuildParsedQuery(readTableCreateTimes, sidecardb.GetIdentifier()).Query
	err := conn.Stream(ctx, readTableCreateTimesQuery, callback, alloc, bufferSize, 0)
	if err != nil {
		return nil, err
	}

	// Finally, we also check for tables that exist only in the cache, because these tables would have been created.
	for tableName := range se.tables {
		if se.tables[tableName].Type == View {
			continue
		}
		// Explicitly ignore dual because schema-engine stores this in its list of tables.
		if !tablesFound[tableName] && tableName != "dual" {
			tablesMismatched[tableName] = true
		}
	}

	return tablesMismatched, nil
}

// reloadDataInDB reloads the schema tracking data in the database
func reloadDataInDB(ctx context.Context, conn *connpool.DBConn, altered []*Table, created []*Table, dropped []*Table) error {
	// tablesToReload and viewsToReload stores the tables and views that need reloading and storing in our MySQL database.
	var tablesToReload, viewsToReload []*Table
	// droppedTables, droppedViews stores the list of tables and views we need to delete, respectively.
	var droppedTables []string
	var droppedViews []string

	for _, table := range append(created, altered...) {
		if table.Type == View {
			viewsToReload = append(viewsToReload, table)
		} else {
			tablesToReload = append(tablesToReload, table)
		}
	}

	for _, table := range dropped {
		tableName := table.Name.String()
		if table.Type == View {
			droppedViews = append(droppedViews, tableName)
		} else {
			droppedTables = append(droppedTables, tableName)
		}
	}

	if err := reloadTablesDataInDB(ctx, conn, tablesToReload, droppedTables); err != nil {
		return err
	}
	if err := reloadViewsDataInDB(ctx, conn, viewsToReload, droppedViews); err != nil {
		return err
	}
	return nil
}

// GetFetchViewQuery gets the fetch query to run for getting the listed views. If no views are provided, then all the views are fetched.
func GetFetchViewQuery(viewNames []string) (string, error) {
	if len(viewNames) == 0 {
		parsedQuery, err := generateFullQuery(fetchViews)
		if err != nil {
			return "", err
		}
		return parsedQuery.Query, nil
	}

	viewsBV, err := sqltypes.BuildBindVariable(viewNames)
	if err != nil {
		return "", err
	}
	bv := map[string]*querypb.BindVariable{"viewNames": viewsBV}

	parsedQuery, err := generateFullQuery(fetchUpdatedViews)
	if err != nil {
		return "", err
	}
	return parsedQuery.GenerateQuery(bv, nil)
}

// GetFetchTableQuery gets the fetch query to run for getting the listed tables. If no tables are provided, then all the tables are fetched.
func GetFetchTableQuery(tableNames []string) (string, error) {
	if len(tableNames) == 0 {
		parsedQuery, err := generateFullQuery(fetchTables)
		if err != nil {
			return "", err
		}
		return parsedQuery.Query, nil
	}

	tablesBV, err := sqltypes.BuildBindVariable(tableNames)
	if err != nil {
		return "", err
	}
	bv := map[string]*querypb.BindVariable{"tableNames": tablesBV}

	parsedQuery, err := generateFullQuery(fetchUpdatedTables)
	if err != nil {
		return "", err
	}
	return parsedQuery.GenerateQuery(bv, nil)
}

// GetFetchTableAndViewsQuery gets the fetch query to run for getting the listed tables and views. If no table names are provided, then all the tables and views are fetched.
func GetFetchTableAndViewsQuery(tableNames []string) (string, error) {
	if len(tableNames) == 0 {
		parsedQuery, err := generateFullQuery(fetchTablesAndViews)
		if err != nil {
			return "", err
		}
		return parsedQuery.Query, nil
	}

	tablesBV, err := sqltypes.BuildBindVariable(tableNames)
	if err != nil {
		return "", err
	}
	bv := map[string]*querypb.BindVariable{"tableNames": tablesBV}

	parsedQuery, err := generateFullQuery(fetchUpdatedTablesAndViews)
	if err != nil {
		return "", err
	}
	return parsedQuery.GenerateQuery(bv, nil)
}
