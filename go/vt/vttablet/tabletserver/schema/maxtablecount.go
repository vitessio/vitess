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
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

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
	fs.Int("queryserver-config-schema-max-table-count", maxTableCountSetting.Default(), "max number of tables that vttablet will allow to be created on the underlying MySQL instance. CREATE TABLE statements that would put the table count above this limit are rejected before they reach MySQL. Increasing this limit may require additional memory in vttablet and mysqld.")
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
// is the unified gate used by all CREATE TABLE entry points (QueryExecutor,
// TabletManager.ApplySchema, ExecuteFetchAsDba/AllPrivs/App and friends).
//
// Behavior:
//
//   - The helper simulates DROP TABLE and CREATE TABLE effects in the order
//     stmts are given so a batch like "DROP TABLE old; CREATE TABLE new" is
//     correctly accepted at the limit (running count never exceeds the
//     limit between statements).
//   - Temporary CREATE TABLEs are ignored (they are session-scoped and not
//     tracked by the schema engine).
//   - A CREATE TABLE that targets an already-present table is treated as a
//     no-op (IF NOT EXISTS) or as a downstream MySQL failure: it cannot
//     change the count.
//   - parseFailures is the number of statements in the original input that
//     the caller could not parse. vttablet cannot tell whether each one
//     would have been a CREATE TABLE, so each is treated as a worst-case,
//     trailing potential CREATE TABLE for limit-checking purposes. Callers
//     with SQL pieces should use CheckCreateTableLimitForQueries to preserve
//     the original statement order.
//   - A nil engine, an empty statement list with parseFailures == 0, or a
//     batch whose net new-table effect fits within the limit is a no-op.
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
// unrelated DBA statements while preserving conservative CREATE TABLE handling.
func queryCouldCreatePersistentTable(parser *sqlparser.Parser, query string) bool {
	tokenizer := parser.NewStringTokenizer(query)
	nextToken := func() int {
		for {
			token, _ := tokenizer.Scan()
			if token != sqlparser.COMMENT {
				return token
			}
		}
	}
	if nextToken() != sqlparser.CREATE {
		return false
	}
	token := nextToken()
	if token == sqlparser.TEMPORARY {
		return false
	}
	return token == sqlparser.TABLE
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
	createTable := func(tableName sqlparser.TableName) error {
		if isPresent(tableName.Name) {
			return nil
		}
		running++
		pendingState[tableName.Name.String()] = true
		if running > limit {
			return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"cannot create table %q: schema engine table limit of %d reached. "+
					"Increase --queryserver-config-schema-max-table-count and ensure "+
					"vttablet and mysqld have enough memory for a larger schema.",
				sqlparser.String(tableName), limit)
		}
		return nil
	}
	renameTable := func(fromTable, toTable sqlparser.TableName) {
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
					"schema engine table limit of %d would be exceeded by this batch (running count: %d, unparseable statement treated as potential CREATE TABLE). "+
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
			if err := createTable(s.Table); err != nil {
				return err
			}
		case *sqlparser.DropTable:
			if s.Temp {
				continue
			}
			for _, tbl := range s.FromTables {
				if !isPresent(tbl.Name) {
					continue
				}
				running--
				pendingState[tbl.Name.String()] = false
			}
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
