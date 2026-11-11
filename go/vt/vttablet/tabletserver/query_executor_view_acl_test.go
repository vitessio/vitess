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
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/vterrors"
	eschema "vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// view bodies and the rows they expose. The stand-in below applies the view
// rules from the MySQL 8.4 manual (the server version Vitess reports): a
// statement that names a view is executed as that view's current SELECT, and
// a view may read another view (CREATE VIEW, "Selecting from the view
// selects, in effect, using the SELECT statement"; View Processing
// Algorithms, MERGE). SQL SECURITY defaults to DEFINER, so MySQL checks the
// view body against the definer, and the tablet's vt_app / vt_dba accounts
// hold SELECT, INSERT, UPDATE, and DELETE on *.* (config/init_db.sql). The
// stand-in therefore returns the current base table's row whenever the
// statement Vitess sends still names the view.
const (
	viewPublic   = "public_name"
	viewNested   = "nested_name"
	viewOpaque   = "opaque_name"
	tablePrivate = "private_a"
	tableOther   = "private_b"
	tableOpen    = "writable_ok"

	secretPrivate = "row-from-private-a"
	secretOther   = "row-from-private-b"
)

// TestQueryExecutorStrictTableACLView is the spec for strict table ACL on
// views. The public caller may read and write the view name and writable_ok,
// and may administer spawned. The owner may also read private_a. Neither may
// read private_b. public_name currently selects private_a; nested_name
// selects public_name; opaque_name is a view whose definition MySQL will not
// return.
func TestQueryExecutorStrictTableACLView(t *testing.T) {
	aclName := fmt.Sprintf("simpleacl-view-%d", rand.Int64())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)
	require.NoError(t, tableacl.InitFromProto(&tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "private_a",
			TableNamesOrPrefixes: []string{tablePrivate},
			Readers:              []string{"owner", "attacker"},
			Writers:              []string{"owner", "attacker"},
			Admins:               []string{"attacker"},
		}, {
			Name:                 "private_b",
			TableNamesOrPrefixes: []string{tableOther},
			Readers:              []string{"attacker"},
			Writers:              []string{"attacker"},
			Admins:               []string{"attacker"},
		}, {
			Name:                 "view",
			TableNamesOrPrefixes: []string{viewPublic, viewNested, viewOpaque},
			Readers:              []string{"public", "owner"},
			Writers:              []string{"public", "owner"},
			Admins:               []string{"attacker"},
		}, {
			Name:                 "open",
			TableNamesOrPrefixes: []string{tableOpen, "spawned"},
			Readers:              []string{"public", "owner"},
			Writers:              []string{"public", "owner"},
			Admins:               []string{"public", "owner", "attacker"},
		}},
	}))

	db := setUpQueryExecutorTest(t)
	t.Cleanup(db.Close)
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			mysql.BaseShowTablesRow(tablePrivate, false, ""),
			mysql.BaseShowTablesRow(tableOther, false, ""),
			mysql.BaseShowTablesRow(tableOpen, false, ""),
			mysql.BaseShowTablesRow(viewPublic, true, ""),
			mysql.BaseShowTablesRow(viewNested, true, ""),
			mysql.BaseShowTablesRow(viewOpaque, true, ""),
		},
	})
	secretFields := &sqltypes.Result{Fields: []*querypb.Field{{Name: "secret", Type: sqltypes.VarChar}}}
	db.MockQueriesForTable(tablePrivate, secretFields)
	// The schema reload after DDL compares view definitions. An empty probe
	// means "nothing changed"; the stand-in answers real definition reads itself.
	db.AddQueryPattern(`SELECT distinct table_name.*HAVING COUNT\(\*\) = 1`, &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("table_name", "varchar"),
	})
	db.MockQueriesForTable(tableOther, secretFields)
	db.MockQueriesForTable(tableOpen, secretFields)

	backend := &viewBackend{
		db:     db,
		parser: sqlparser.NewTestParser(),
		body: map[string]string{
			viewPublic: "select secret from " + tablePrivate,
			viewNested: "select secret from " + viewPublic,
			viewOpaque: "",
		},
		secret: map[string]string{
			tablePrivate: secretPrivate,
			tableOther:   secretOther,
			tableOpen:    "row-from-writable-ok",
		},
	}
	db.Handler = backend

	ctx := t.Context()
	tsv := newTestTabletServer(ctx, enableStrictTableACL, db)
	t.Cleanup(tsv.StopService)

	loaded := tsv.qe.schema.Load().tables
	require.NotNil(t, loaded[viewPublic], "schema engine must load the view")
	require.Equal(t, eschema.View, loaded[viewPublic].Type)
	require.Equal(t, eschema.View, loaded[viewNested].Type)
	require.Equal(t, eschema.View, loaded[viewOpaque].Type)
	require.Equal(t, eschema.NoType, loaded[tablePrivate].Type)

	publicCtx := callerid.NewContext(ctx, nil, &querypb.VTGateCallerID{Username: "public"})
	ownerCtx := callerid.NewContext(ctx, nil, &querypb.VTGateCallerID{Username: "owner"})

	// Every shape that reaches MySQL still naming the view, plus a view whose
	// definition cannot be read. Each must be denied before MySQL runs it.
	denied := []struct {
		name   string
		query  string
		stream bool
		inTx   bool
	}{
		{"select", "select * from " + viewPublic, false, false},
		{"select stream", "select * from " + viewPublic, true, false},
		{"update", "update " + viewPublic + " set secret = 'changed'", false, false},
		{"delete", "delete from " + viewPublic, false, false},
		{"insert", "insert into " + viewPublic + "(secret) values ('changed')", false, false},
		{"replace", "replace into " + viewPublic + "(secret) values ('changed')", false, false},
		{"insert select", "insert into " + tableOpen + "(secret) select secret from " + viewPublic, false, false},
		{"join", "select * from " + tableOpen + " join " + viewPublic, false, false},
		{"subquery", "select * from " + tableOpen + " where secret in (select secret from " + viewPublic + ")", false, false},
		{"union", "select * from " + viewPublic + " union select * from " + tableOpen, false, false},
		{"cte", "with c as (select * from " + viewPublic + ") select * from c", false, false},
		{"update join", "update " + viewPublic + " join " + tableOpen + " set " + viewPublic + ".secret = " + tableOpen + ".secret", false, false},
		{"explain", "explain select * from " + viewPublic, false, false},
		{"explain analyze", "explain analyze select * from " + viewPublic, false, false},
		// SET runs on the reserved or transaction connection. Outside one, the
		// executor rejects it before MySQL for a reason other than table ACL.
		{"set subquery", "set @v = (select secret from " + viewPublic + " limit 1)", false, true},
		{"create table as select", "create table spawned as select * from " + viewPublic, false, false},
		{"nested view", "select * from " + viewNested, false, false},
		{"unresolved view", "select * from " + viewOpaque, false, false},
	}
	for _, tc := range denied {
		t.Run(tc.name, func(t *testing.T) {
			before := backend.snapshot()
			var (
				got *sqltypes.Result
				err error
			)
			connID := int64(0)
			if tc.inTx {
				connID = newTransaction(tsv, &querypb.ExecuteOptions{})
				t.Cleanup(func() {
					_, _ = tsv.Rollback(ctx, tsv.sm.Target(), connID)
				})
			}
			if tc.stream {
				qre := newTestQueryExecutorStreaming(publicCtx, tsv, tc.query, connID)
				err = qre.Stream(func(*sqltypes.Result) error { return nil })
			} else {
				qre := newTestQueryExecutor(publicCtx, tsv, tc.query, connID)
				got, err = qre.Execute()
			}
			require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err),
				"query %s err=%v result=%v mysql=%v", tc.query, err, got, backend.since(before))
			require.Nil(t, got)
			require.Empty(t, backend.since(before), "MySQL ran %s before the ACL refused it: %v", tc.query, backend.since(before))
		})
	}

	// The owner may read private_a, so a select of the view is allowed only
	// while the view still reads private_a, and the rows are private_a's.
	t.Run("reader of the underlying table", func(t *testing.T) {
		backend.setBody(viewPublic, "select secret from "+tablePrivate)
		before := backend.snapshot()
		qre := newTestQueryExecutor(ownerCtx, tsv, "select * from "+viewPublic, 0)
		got, err := qre.Execute()
		require.NoError(t, err)
		require.Equal(t, []string{secretPrivate}, secretsOf(got))
		require.NotContains(t, backend.since(before), secretOther)
	})

	// The plan for this text was cached above. Changing the live view to
	// private_b must not return private_b to an owner who cannot read it:
	// authorizing private_a and then sending the view name lets MySQL run the
	// new body.
	t.Run("view body changed after the plan was built", func(t *testing.T) {
		backend.setBody(viewPublic, "select secret from "+tableOther)
		t.Cleanup(func() { backend.setBody(viewPublic, "select secret from "+tablePrivate) })
		before := backend.snapshot()
		qre := newTestQueryExecutor(ownerCtx, tsv, "select * from "+viewPublic, 0)
		got, err := qre.Execute()
		disclosed := backend.since(before)
		require.NotContains(t, disclosed, secretOther, "MySQL ran the swapped view body: %v result=%v err=%v", disclosed, got, err)
		if err == nil {
			require.Equal(t, []string{secretPrivate}, secretsOf(got))
		} else {
			require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "err=%v result=%v", err, got)
			require.Nil(t, got)
		}
	})

	// A definition read and the statement that follows are the check and the
	// use. The first read still describes private_a; MySQL's next read of the
	// view sees private_b. Returning private_b means the authorized body was
	// not the body that ran.
	t.Run("view body changed between the definition read and the statement", func(t *testing.T) {
		backend.setBody(viewPublic, "select secret from "+tablePrivate)
		backend.armSwap(viewPublic, "select secret from "+tableOther)
		t.Cleanup(func() {
			backend.clearSwap(viewPublic)
			backend.setBody(viewPublic, "select secret from "+tablePrivate)
		})
		query := "select * from " + viewPublic + " where 1 = 1"
		before := backend.snapshot()
		qre := newTestQueryExecutor(ownerCtx, tsv, query, 0)
		got, err := qre.Execute()
		disclosed := backend.since(before)
		require.NotContains(t, disclosed, secretOther, "definition check and MySQL used different bodies: %v result=%v err=%v", disclosed, got, err)
		if err == nil {
			require.Equal(t, []string{secretPrivate}, secretsOf(got))
		}
	})
}

func secretsOf(result *sqltypes.Result) []string {
	if result == nil {
		return nil
	}
	out := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		out = append(out, row[0].ToString())
	}
	return out
}

// viewBackend is the MySQL stand-in for view execution. Definition reads
// (SHOW CREATE TABLE, and a SELECT of view_definition from
// information_schema.views that is not the schema engine's change probe)
// return the current body. An armed swap takes effect after that read and
// before the next statement, which is the window between an ACL check and
// MySQL running the view.
type viewBackend struct {
	db     *fakesqldb.DB
	parser *sqlparser.Parser

	mu       sync.Mutex
	body     map[string]string
	swapTo   map[string]string
	secret   map[string]string
	executed []string
}

func (v *viewBackend) snapshot() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.executed)
}

func (v *viewBackend) since(n int) []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]string, len(v.executed[n:]))
	copy(out, v.executed[n:])
	return out
}

func (v *viewBackend) setBody(view, body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.body[view] = body
}

func (v *viewBackend) armSwap(view, body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.swapTo == nil {
		v.swapTo = map[string]string{}
	}
	v.swapTo[view] = body
}

func (v *viewBackend) clearSwap(view string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.swapTo, view)
}

func (v *viewBackend) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	if result, ok, err := v.answer(query); ok {
		if err != nil {
			return err
		}
		return callback(result)
	}
	return v.db.HandleQuery(c, query, callback)
}

func (v *viewBackend) answer(query string) (*sqltypes.Result, bool, error) {
	lower := strings.ToLower(strings.TrimSpace(query))
	if strings.Contains(lower, "where 1 != 1") || strings.Contains(lower, "having count(*)") || strings.Contains(lower, "union all") {
		return nil, false, nil
	}
	if strings.HasPrefix(lower, "show create table") {
		return v.showCreate(lower)
	}
	if strings.Contains(lower, "information_schema.views") && strings.Contains(lower, "view_definition") {
		return v.viewDefinitions()
	}

	stmt, err := v.parser.Parse(query)
	if err != nil {
		return nil, false, nil
	}
	tables := statementTables(stmt)
	if len(tables) == 0 {
		return nil, false, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	var bases []string
	viewUsed := false
	for _, name := range tables {
		if _, isView := v.body[name]; !isView {
			bases = append(bases, name)
			continue
		}
		viewUsed = true
		resolved, unresolved := v.resolveLocked(name, map[string]bool{})
		if unresolved {
			v.executed = append(v.executed, "unresolved:"+name)
			return nil, true, fmt.Errorf("view %s has no definition", name)
		}
		bases = append(bases, resolved...)
	}
	private := false
	for _, base := range bases {
		if base == tablePrivate || base == tableOther {
			private = true
			v.executed = append(v.executed, v.secret[base])
		}
	}
	// A statement that does not name a view and does not touch a private
	// table belongs to the rest of the fake database.
	if !viewUsed && !private {
		return nil, false, nil
	}
	return v.rowsLocked(bases), true, nil
}

func (v *viewBackend) showCreate(lower string) (*sqltypes.Result, bool, error) {
	name := strings.Trim(lower[len("show create table"):], " `\t")
	v.mu.Lock()
	defer v.mu.Unlock()
	body, isView := v.body[name]
	if !isView {
		return nil, false, nil
	}
	if body == "" {
		return nil, true, fmt.Errorf("definition unavailable for %s", name)
	}
	create := fmt.Sprintf("create view %s as %s", name, body)
	v.swapLocked(name)
	return sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("View|Create View|character_set_client|collation_connection", "varchar|varchar|varchar|varchar"),
		name+"|"+create+"|utf8mb4|utf8mb4_general_ci",
	), true, nil
}

func (v *viewBackend) viewDefinitions() (*sqltypes.Result, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var rows []string
	for _, name := range []string{viewPublic, viewNested, viewOpaque} {
		body := v.body[name]
		if body == "" {
			continue
		}
		rows = append(rows, name+"|"+body)
		v.swapLocked(name)
	}
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar"), rows...), true, nil
}

func (v *viewBackend) swapLocked(name string) {
	next, ok := v.swapTo[name]
	if !ok {
		return
	}
	v.body[name] = next
	delete(v.swapTo, name)
}

func (v *viewBackend) resolveLocked(name string, seen map[string]bool) ([]string, bool) {
	if seen[name] {
		return nil, true
	}
	body, isView := v.body[name]
	if !isView {
		return []string{name}, false
	}
	if body == "" {
		return nil, true
	}
	seen[name] = true
	stmt, err := v.parser.Parse(body)
	if err != nil {
		return nil, true
	}
	var bases []string
	for _, table := range statementTables(stmt) {
		resolved, unresolved := v.resolveLocked(table, seen)
		if unresolved {
			return nil, true
		}
		bases = append(bases, resolved...)
	}
	if len(bases) == 0 {
		return nil, true
	}
	return bases, false
}

func (v *viewBackend) rowsLocked(bases []string) *sqltypes.Result {
	var rows []string
	for _, base := range bases {
		if secret, ok := v.secret[base]; ok {
			rows = append(rows, secret)
		}
	}
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields("secret", "varchar"), rows...)
}

func statementTables(stmt sqlparser.Statement) []string {
	var tables []string
	seen := map[string]bool{}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliased, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok {
			return true, nil
		}
		name, ok := aliased.Expr.(sqlparser.TableName)
		if !ok || name.Name.IsEmpty() {
			return true, nil
		}
		table := name.Name.String()
		if !seen[table] {
			seen[table] = true
			tables = append(tables, table)
		}
		return false, nil
	}, stmt)
	return tables
}
