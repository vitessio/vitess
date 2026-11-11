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

package tabletserver

import (
	"errors"
	"strings"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type viewKey struct {
	schema, name string
}

// viewDef is a view definition as read from MySQL. A view whose definition
// cannot be read or parsed is unresolved.
type viewDef struct {
	body       sqlparser.TableStatement
	columns    sqlparser.Columns
	unresolved bool
}

// viewExpander resolves the views a statement names. MySQL runs a view's body
// as the tablet's own MySQL user, so table ACL must check the tables the body
// reads, and the statement sent to MySQL must run the body that was checked
// rather than name the view again, as the view can be redefined in between.
// Each definition is read once per execution and every reference to the view
// is replaced by that definition as a derived table.
type viewExpander struct {
	qre  *QueryExecutor
	conn *connpool.PooledConn

	// views holds the views among the classified names; a name that is not a
	// view is in classified only.
	views      map[viewKey]*viewDef
	classified map[viewKey]bool
	checked    map[viewKey]bool
	expanding  map[viewKey]bool

	permissions  []planbuilder.Permission
	undetermined bool
	rewritten    bool
}

// checkViewAccess applies strict table ACL to the views the statement names:
// the caller must hold the statement's role on the view's name, which
// checkPermissions has already checked, and READER on every table the view
// reads, transitively. A view that is written through, or whose definition
// cannot be resolved, leaves the statement's table set undetermined. On
// success the plan is replaced by one for the statement with its views
// expanded.
func (qre *QueryExecutor) checkViewAccess(callerID *querypb.VTGateCallerID) error {
	if len(qre.plan.Permissions) == 0 || qre.plan.Original == "" {
		return nil
	}
	stmt, err := qre.tsv.env.Parser().Parse(qre.plan.Original)
	if err != nil {
		return err
	}

	ve := &viewExpander{
		qre:        qre,
		views:      map[viewKey]*viewDef{},
		classified: map[viewKey]bool{},
		checked:    map[viewKey]bool{},
		expanding:  map[viewKey]bool{},
	}
	defer ve.close()

	switch stmt := stmt.(type) {
	case *sqlparser.CreateView, *sqlparser.AlterView:
		// The stored body keeps naming the views it reads; a query of the new
		// view expands them when it runs. They are checked here so that only
		// a caller who may read what a view returns can define one over it.
		err = ve.expand(stmt, false)
	case *sqlparser.RenameTable:
		// Renaming a view onto a name serves its rows under that name.
		err = ve.expandRenamed(stmt)
	default:
		err = ve.expand(stmt, true)
	}
	if err != nil {
		return err
	}

	for _, perm := range ve.permissions {
		if err := qre.checkAccess(tableacl.Authorized(perm.TableName, perm.Role), perm.TableName, callerID); err != nil {
			return err
		}
	}
	if ve.undetermined {
		return qre.checkUndeterminedTableAccess(tableacl.AuthorizedForAllTables(), callerID)
	}
	if !ve.rewritten {
		return nil
	}
	return qre.replan(stmt)
}

// replan replaces the plan with one built the same way for stmt.
func (qre *QueryExecutor) replan(stmt sqlparser.Statement) error {
	qe := qre.tsv.qe
	tables := qe.schema.Load().tables
	dbName := qe.env.Config().DB.DBName
	var (
		splan *planbuilder.Plan
		err   error
	)
	if qre.plan.streaming {
		splan, err = planbuilder.BuildStreaming(qe.env.Environment(), stmt, tables, dbName)
	} else {
		splan, err = planbuilder.Build(qe.env.Environment(), stmt, tables, dbName, qre.plan.noRowsLimit)
	}
	if err != nil {
		return err
	}
	if splan.PlanID != qre.plan.PlanID || splan.FullQuery == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] expanding the views of a %s statement produced a %s plan", qre.plan.PlanID.String(), splan.PlanID.String())
	}
	plan := &TabletPlan{
		Plan:        splan,
		Original:    qre.plan.Original,
		Rules:       qre.plan.Rules,
		streaming:   qre.plan.streaming,
		noRowsLimit: qre.plan.noRowsLimit,
	}
	plan.buildAuthorized()
	qre.plan = plan
	return nil
}

func (ve *viewExpander) close() {
	if ve.conn != nil {
		ve.conn.Recycle()
	}
}

func (ve *viewExpander) key(name sqlparser.TableName) viewKey {
	key := viewKey{schema: name.Qualifier.String(), name: name.Name.String()}
	if key.schema == "" {
		key.schema = ve.qre.tsv.config.DB.DBName
	}
	return key
}

// expand checks the views among the table expressions of stmt and, if
// rewrite is set, replaces each of them with its body.
func (ve *viewExpander) expand(stmt sqlparser.Statement, rewrite bool) error {
	refs := planbuilder.BuildTableRefs(stmt)
	names := make([]sqlparser.TableName, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Expr.Expr.(sqlparser.TableName))
	}
	if err := ve.classify(names); err != nil {
		return err
	}
	for i, ref := range refs {
		key := ve.key(names[i])
		if _, isView := ve.views[key]; !isView {
			continue
		}
		if ref.Role != tableacl.READER {
			// A write through a view goes to whatever table the view names
			// when MySQL runs it, which cannot be pinned to a checked body.
			ve.undetermined = true
			continue
		}
		body, err := ve.check(key)
		if err != nil {
			return err
		}
		if rewrite && body != nil {
			if ref.Expr.As.IsEmpty() {
				ref.Expr.As = names[i].Name
			}
			if columns := ve.views[key].columns; len(columns) > 0 {
				ref.Expr.Columns = columns
			}
			ref.Expr.Expr = &sqlparser.DerivedTable{Select: body}
			ve.rewritten = true
		}
	}
	return nil
}

func (ve *viewExpander) expandRenamed(stmt *sqlparser.RenameTable) error {
	names := make([]sqlparser.TableName, 0, len(stmt.TablePairs))
	for _, pair := range stmt.TablePairs {
		names = append(names, pair.FromTable)
	}
	if err := ve.classify(names); err != nil {
		return err
	}
	for _, name := range names {
		if _, isView := ve.views[ve.key(name)]; isView {
			if _, err := ve.check(ve.key(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// check records the permissions of the view's body and expands the views the
// body reads. It returns a copy of the expanded body, or nil if the view
// cannot be resolved.
func (ve *viewExpander) check(key viewKey) (sqlparser.TableStatement, error) {
	def := ve.views[key]
	if def.unresolved || ve.expanding[key] {
		ve.undetermined = true
		return nil, nil
	}
	body := sqlparser.Clone(def.body)
	if !ve.checked[key] {
		ve.checked[key] = true
		perms, _ := planbuilder.BuildPermissions(body)
		ve.permissions = append(ve.permissions, perms...)
	}
	ve.expanding[key] = true
	defer delete(ve.expanding, key)
	if err := ve.expand(body, true); err != nil {
		return nil, err
	}
	return body, nil
}

// classify finds which of names are views. information_schema lists the
// views MySQL has now; the schema engine also knows views whose definition
// MySQL may hide. The definition of each is then read with SHOW CREATE TABLE,
// which is the definition the statement runs.
func (ve *viewExpander) classify(names []sqlparser.TableName) error {
	bySchema := map[string][]string{}
	for _, name := range names {
		key := ve.key(name)
		if ve.classified[key] {
			continue
		}
		ve.classified[key] = true
		bySchema[key.schema] = append(bySchema[key.schema], key.name)
	}
	if len(bySchema) == 0 {
		return nil
	}
	dbName := ve.qre.tsv.config.DB.DBName
	tables := ve.qre.tsv.qe.schema.Load().tables
	for schemaName, tableNames := range bySchema {
		listed, err := ve.listViews(schemaName, tableNames)
		if err != nil {
			return err
		}
		for _, name := range tableNames {
			definition, isListed := listed[strings.ToLower(name)]
			isView := isListed
			if schemaName == dbName {
				if table := tables[name]; table != nil && table.Type == schema.View {
					isView = true
				}
			}
			if !isView {
				continue
			}
			key := viewKey{schema: schemaName, name: name}
			if isListed && definition == "" {
				// MySQL hides the definition of a view the account may not see.
				ve.views[key] = &viewDef{unresolved: true}
				continue
			}
			def, err := ve.readView(key)
			if err != nil {
				return err
			}
			if def != nil {
				ve.views[key] = def
			}
		}
	}
	return nil
}

// listViews returns the listed definition of each of names that MySQL lists
// as a view in schemaName, keyed by lowercased name: whether a name matches
// in any case is MySQL's decision, and a match is then read by its own name.
func (ve *viewExpander) listViews(schemaName string, names []string) (map[string]string, error) {
	namesBV, err := sqltypes.BuildBindVariable(names)
	if err != nil {
		return nil, err
	}
	query, err := sqlparser.ParseAndBind("select table_name, view_definition from information_schema.views where table_schema = %a and table_name in %a",
		sqltypes.StringBindVariable(schemaName), namesBV)
	if err != nil {
		return nil, err
	}
	qr, err := ve.exec(query)
	if err != nil {
		return nil, err
	}
	listed := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		if len(row) < 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected information_schema.views row: %v", row)
		}
		listed[strings.ToLower(row[0].ToString())] = row[1].ToString()
	}
	return listed, nil
}

// readView reads a view's definition. It returns nil if MySQL reports a base
// table under the name.
func (ve *viewExpander) readView(key viewKey) (*viewDef, error) {
	name := sqlparser.TableName{Name: sqlparser.NewIdentifierCS(key.name)}
	if key.schema != ve.qre.tsv.config.DB.DBName {
		name.Qualifier = sqlparser.NewIdentifierCS(key.schema)
	}
	qr, err := ve.exec("show create table " + sqlparser.String(name))
	if err != nil {
		var sqlErr *sqlerror.SQLError
		if errors.As(err, &sqlErr) && !sqlerror.IsConnErr(err) {
			return &viewDef{unresolved: true}, nil
		}
		return nil, err
	}
	if len(qr.Fields) >= 2 && qr.Fields[1].Name == "Create Table" {
		return nil, nil
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) < 2 {
		return &viewDef{unresolved: true}, nil
	}
	stmt, err := ve.qre.tsv.env.Parser().Parse(qr.Rows[0][1].ToString())
	if err != nil {
		return &viewDef{unresolved: true}, nil
	}
	create, ok := stmt.(*sqlparser.CreateView)
	if !ok || create.Select == nil {
		return &viewDef{unresolved: true}, nil
	}
	return &viewDef{body: create.Select, columns: create.Columns}, nil
}

func (ve *viewExpander) exec(query string) (*sqltypes.Result, error) {
	if ve.conn == nil {
		conn, err := ve.qre.tsv.qe.conns.Get(ve.qre.ctx, nil)
		if err != nil {
			return nil, err
		}
		ve.conn = conn
	}
	return ve.conn.Conn.Exec(ve.qre.ctx, query, 10000, true)
}
