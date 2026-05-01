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

package schema

import (
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// isPseudoName reports whether name refers to a synthetic schema-engine
// entry that does not correspond to a real MySQL table. Engine.Open seeds
// se.tables with a "dual" entry to satisfy queries like SELECT 1 FROM dual,
// but mysqld has no such table, so DROP / RENAME / CREATE involving it are
// no-ops or errors there. Engine.TableCount() already excludes dual; the
// simulator must do the same to keep the running count consistent with what
// mysqld will actually see after the batch executes.
func isPseudoName(name sqlparser.IdentifierCS) bool {
	return strings.EqualFold(name.String(), "dual")
}

var maxTableCountSetting = viperutil.Configure(
	"queryserver_config_schema_max_table_count",
	viperutil.Options[int]{
		FlagName: "queryserver-config-schema-max-table-count",
		Default:  10000,
		Dynamic:  true,
	},
)

type (
	// tableLimitCheckStep preserves execution order across parsed and
	// unparseable SQL pieces.
	tableLimitCheckStep struct {
		stmt        sqlparser.Statement
		parseFailed bool
	}
)

func init() {
	servenv.OnParseFor("vttablet", registerSchemaFlags)
	servenv.OnParseFor("vtcombo", registerSchemaFlags)
}

func registerSchemaFlags(fs *pflag.FlagSet) {
	fs.Int("queryserver-config-schema-max-table-count", maxTableCountSetting.Default(), "max number of schema objects (tables and views) that vttablet will allow to be created on the underlying MySQL instance. CREATE TABLE and CREATE VIEW statements that would put the schema object count above this limit are rejected before they reach MySQL. Increasing this limit may require additional memory in vttablet and mysqld.")
	viperutil.BindFlags(fs, maxTableCountSetting)
}

// MaxTableCount returns the current value of the max-table-count dynamic flag.
func MaxTableCount() int {
	return maxTableCountSetting.Get()
}

// SetMaxTableCount overrides the configured value at runtime. Intended for
// tests; production updates flow through Viper's normal config channels.
func SetMaxTableCount(n int) {
	maxTableCountSetting.Set(n)
}

// CheckCreateTableLimit returns an error if running the given statements
// would push the schema engine past its configured table-count limit. It
// is the unified gate used by CREATE TABLE and CREATE VIEW entry points (QueryExecutor,
// TabletManager.ApplySchema, ExecuteFetchAsDba/AllPrivs/App and friends).
//
// Behavior:
//
//   - The helper simulates CREATE TABLE / CREATE VIEW / DROP TABLE / DROP
//     VIEW effects in the order stmts are given so a batch like "DROP VIEW
//     v; CREATE TABLE new" is correctly accepted at the limit (running
//     count never exceeds the limit between statements). Views participate
//     in the count because Engine.TableCount() reflects every entry in
//     se.tables, including views.
//   - Temporary CREATE TABLEs are ignored (they are session-scoped and not
//     tracked by the schema engine). Views cannot be temporary in MySQL.
//   - A CREATE that targets an already-present name is treated as a no-op
//     (IF NOT EXISTS / OR REPLACE) or as a downstream MySQL failure: it
//     cannot change the count.
//   - parseFailures is the number of statements in the original input that
//     the caller could not parse. vttablet cannot tell whether each one
//     would have been a CREATE TABLE / CREATE VIEW, so each is treated as
//     a worst-case, trailing potential CREATE for limit-checking purposes.
//     Callers with SQL pieces should use CheckCreateTableLimitForQueries to
//     preserve the original statement order.
//   - A nil engine, an empty statement list with parseFailures == 0, or a
//     batch whose net new-object effect fits within the limit is a no-op.
//   - The synthetic "dual" entry the schema engine seeds is excluded by
//     Engine.TableCount(); the simulator likewise ignores DROP / RENAME /
//     CREATE on dual so an in-batch DROP TABLE dual cannot free a slot for
//     a CREATE that mysqld would reject the limit at.
//
// The check is best-effort under concurrency: two callers in different
// goroutines can each see the count below the limit and both proceed,
// briefly leaving the count at limit+N. Schema reload tolerates this; the
// gate's purpose is a clear operator-facing error in the common case, not
// strict admission control.
//
// TableCount is per-engine instance state; MaxTableCount is the
// process-wide Viper-managed limit, hence the asymmetric calls.
func CheckCreateTableLimit(se *Engine, stmts []sqlparser.Statement, parseFailures int) error {
	steps := make([]tableLimitCheckStep, 0, len(stmts)+parseFailures)
	for _, stmt := range stmts {
		steps = append(steps, tableLimitCheckStep{stmt: stmt})
	}
	for range parseFailures {
		steps = append(steps, tableLimitCheckStep{parseFailed: true})
	}
	return checkCreateTableLimitSteps(se, steps)
}

// CheckCreateTableLimitForQueries parses SQL pieces in order so unparseable
// statements are checked at the same point MySQL would execute them.
func CheckCreateTableLimitForQueries(se *Engine, parser *sqlparser.Parser, queries []string) error {
	steps := make([]tableLimitCheckStep, 0, len(queries))
	for _, query := range queries {
		stmt, err := parser.Parse(query)
		if err != nil {
			if queryCouldCreatePersistentTable(parser, query) {
				steps = append(steps, tableLimitCheckStep{parseFailed: true})
			}
			continue
		}
		steps = append(steps, tableLimitCheckStep{stmt: stmt})
	}
	return checkCreateTableLimitSteps(se, steps)
}

// CheckCreateTableLimitForParsedStatements lets callers reuse parse results
// while still checking parse failures against their original SQL text.
func CheckCreateTableLimitForParsedStatements(se *Engine, parser *sqlparser.Parser, queries []string, stmts []sqlparser.Statement) error {
	steps := make([]tableLimitCheckStep, 0, len(stmts))
	for i, stmt := range stmts {
		if stmt == nil {
			if i >= len(queries) || queryCouldCreatePersistentTable(parser, queries[i]) {
				steps = append(steps, tableLimitCheckStep{parseFailed: true})
			}
			continue
		}
		steps = append(steps, tableLimitCheckStep{stmt: stmt})
	}
	return checkCreateTableLimitSteps(se, steps)
}

// queryCouldCreatePersistentTable keeps parser failures from blocking
// unrelated DBA statements while preserving conservative CREATE TABLE / VIEW
// handling. CREATE VIEW allows OR REPLACE and various view attributes
// (ALGORITHM=, DEFINER=, SQL SECURITY ...) between CREATE and VIEW, so we walk
// a small window of tokens looking for TABLE or VIEW. We bail on a keyword
// that excludes both forms (TEMPORARY, INDEX, PROCEDURE, etc.).
func queryCouldCreatePersistentTable(parser *sqlparser.Parser, query string) bool {
	tokenizer := parser.NewStringTokenizer(query)
	tokens := make([]int, 0, 32)
	for len(tokens) < 32 {
		for {
			token, _ := tokenizer.Scan()
			if token != sqlparser.COMMENT {
				tokens = append(tokens, token)
				break
			}
		}
		last := tokens[len(tokens)-1]
		if last == sqlparser.LEX_ERROR || last == 0 {
			break
		}
	}
	if len(tokens) == 0 || tokens[0] != sqlparser.CREATE {
		return false
	}
	for i := 1; i < len(tokens); {
		switch tokens[i] {
		case sqlparser.TABLE, sqlparser.VIEW:
			return true
		case sqlparser.OR:
			if i+1 < len(tokens) && tokens[i+1] == sqlparser.REPLACE {
				i += 2
				continue
			}
		case sqlparser.ALGORITHM:
			i++
			if i < len(tokens) && tokens[i] == '=' {
				i++
			}
			if i < len(tokens) {
				i++
			}
			continue
		case sqlparser.DEFINER:
			i++
			if i < len(tokens) && tokens[i] == '=' {
				i++
			}
			if i < len(tokens) {
				if tokens[i] == sqlparser.CURRENT_USER {
					i++
					if i+1 < len(tokens) && tokens[i] == '(' && tokens[i+1] == ')' {
						i += 2
					}
				} else {
					i++
					if i < len(tokens) && tokens[i] == '@' {
						i++
						if i < len(tokens) {
							i++
						}
					}
				}
			}
			continue
		case sqlparser.SQL:
			if i+2 < len(tokens) && tokens[i+1] == sqlparser.SECURITY {
				i += 3
				continue
			}
		case sqlparser.TEMPORARY,
			sqlparser.TRIGGER,
			sqlparser.PROCEDURE,
			sqlparser.FUNCTION,
			sqlparser.EVENT,
			sqlparser.INDEX,
			sqlparser.DATABASE,
			sqlparser.SCHEMA,
			sqlparser.USER,
			sqlparser.ROLE,
			sqlparser.UNIQUE,
			sqlparser.FULLTEXT,
			sqlparser.SPATIAL,
			sqlparser.LEX_ERROR,
			0:
			return false
		}
		i++
	}
	return false
}

// checkCreateTableLimitSteps applies the limit check to an already ordered
// stream of parsed statements and worst-case parse failures.
func checkCreateTableLimitSteps(se *Engine, steps []tableLimitCheckStep) error {
	if se == nil {
		return nil
	}
	limit := MaxTableCount()
	running := se.TableCount()

	// pendingState tracks the hypothetical existence of tables the batch
	// has already touched. true = present, false = dropped. Tables not in
	// the map fall back to the engine's view (se.GetTable).
	pendingState := map[string]bool{}
	isPresent := func(name sqlparser.IdentifierCS) bool {
		if v, ok := pendingState[name.String()]; ok {
			return v
		}
		return se.GetTable(name) != nil
	}
	createObject := func(kind string, name sqlparser.TableName) error {
		if isPseudoName(name.Name) || isPresent(name.Name) {
			return nil
		}
		running++
		pendingState[name.Name.String()] = true
		if running > limit {
			return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"cannot create %s %q: schema engine table limit of %d reached. "+
					"Increase --queryserver-config-schema-max-table-count and ensure "+
					"vttablet and mysqld have enough memory for a larger schema.",
				kind, sqlparser.String(name), limit)
		}
		return nil
	}
	dropObjects := func(names sqlparser.TableNames) {
		for _, n := range names {
			if isPseudoName(n.Name) || !isPresent(n.Name) {
				continue
			}
			running--
			pendingState[n.Name.String()] = false
		}
	}
	renameTable := func(fromTable, toTable sqlparser.TableName) {
		if isPseudoName(fromTable.Name) || isPseudoName(toTable.Name) {
			return
		}
		if !isPresent(fromTable.Name) || isPresent(toTable.Name) {
			return
		}
		pendingState[fromTable.Name.String()] = false
		pendingState[toTable.Name.String()] = true
	}

	for _, step := range steps {
		if step.parseFailed {
			running++
			if running > limit {
				return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
					"schema engine table limit of %d would be exceeded by this batch (running count: %d, unparseable statement treated as potential CREATE TABLE/VIEW). "+
						"Increase --queryserver-config-schema-max-table-count, free space by dropping unused tables, or rephrase the unparseable statement.",
					limit, running)
			}
			continue
		}

		switch s := step.stmt.(type) {
		case *sqlparser.CreateTable:
			if s.Temp {
				continue
			}
			if err := createObject("table", s.Table); err != nil {
				return err
			}
		case *sqlparser.CreateView:
			if err := createObject("view", s.ViewName); err != nil {
				return err
			}
		case *sqlparser.DropTable:
			if s.Temp {
				continue
			}
			dropObjects(s.FromTables)
		case *sqlparser.DropView:
			dropObjects(s.FromTables)
		case *sqlparser.RenameTable:
			for _, pair := range s.TablePairs {
				renameTable(pair.FromTable, pair.ToTable)
			}
		case *sqlparser.AlterTable:
			for _, toTable := range s.GetToTables() {
				renameTable(s.Table, toTable)
			}
		}
	}
	return nil
}
