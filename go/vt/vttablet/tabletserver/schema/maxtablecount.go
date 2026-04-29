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
//     would have been a CREATE TABLE, so each is treated as a worst-case
//     potential CREATE TABLE for limit-checking purposes.
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

	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *sqlparser.CreateTable:
			if s.Temp {
				continue
			}
			if isPresent(s.Table.Name) {
				continue
			}
			running++
			pendingState[s.Table.Name.String()] = true
			if running > limit {
				return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
					"cannot create table %q: schema engine table limit of %d reached. "+
						"Increase --queryserver-config-schema-max-table-count and ensure "+
						"vttablet and mysqld have enough memory for a larger schema.",
					sqlparser.String(s.Table), limit)
			}
		case *sqlparser.DropTable:
			for _, tbl := range s.FromTables {
				if !isPresent(tbl.Name) {
					continue
				}
				running--
				pendingState[tbl.Name.String()] = false
			}
		}
	}

	// Worst-case accounting for unparseable statements: each could be a
	// CREATE TABLE that mysqld accepts. Treat them as potential creations
	// added on top of the simulated running count.
	if parseFailures > 0 && running+parseFailures > limit {
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
			"schema engine table limit of %d would be exceeded by this batch (running count: %d, %d unparseable statement(s) treated as potential CREATE TABLEs). "+
				"Increase --queryserver-config-schema-max-table-count, free space by dropping unused tables, or rephrase the unparseable statement(s).",
			limit, running, parseFailures)
	}
	return nil
}
