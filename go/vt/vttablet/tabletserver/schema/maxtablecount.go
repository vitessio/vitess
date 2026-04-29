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

// RejectIfAtLimitWithUnparseable is a defense-in-depth gate for paths that
// receive raw SQL strings (e.g. TabletManager's ExecuteFetchAsDba and
// ApplySchema flows). When some part of the request could not be parsed by
// vttablet, we cannot tell whether it contained a brand-new CREATE TABLE.
// If the engine is already at or above its configured limit, fail closed:
// allowing the request through would be the only way to silently bypass the
// limit. When the engine has headroom, parse failures are tolerated as
// before so parser quirks don't disrupt unrelated operations.
func RejectIfAtLimitWithUnparseable(se *Engine, hadParseFailure bool) error {
	if se == nil || !hadParseFailure {
		return nil
	}
	if count, limit := se.TableCount(), MaxTableCount(); count >= limit {
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
			"schema engine table limit of %d reached and the request contains a statement that vttablet cannot parse; refusing to bypass the limit. "+
				"Increase --queryserver-config-schema-max-table-count, free space by dropping unused tables, or rephrase the unparseable statement.",
			limit)
	}
	return nil
}

// CheckCreateTableLimit returns an error if running the given statements as
// a batch would push the schema engine past its configured table-count
// limit. It counts brand-new non-temporary CREATE TABLE targets across the
// whole batch (deduped by name, skipping ones that already exist in the
// engine), so a batch with two new CREATEs is correctly rejected when
// adding both would exceed the limit, not just when each one individually
// would.
//
// Statements that are not a CREATE TABLE, target an existing table, or
// create a temporary table do not contribute to the count. Passing zero
// statements or a nil engine is a no-op.
//
// The check is best-effort under concurrency: two callers in different
// goroutines can each see the count below the limit and both proceed to
// create tables, briefly leaving the count at limit+N. Schema reload
// tolerates this — the gate's purpose is to give a clear error in the
// common case, not to enforce strict admission.
//
// TableCount is per-engine instance state; MaxTableCount is the
// process-wide Viper-managed limit, hence the asymmetric calls.
func CheckCreateTableLimit(se *Engine, stmts ...sqlparser.Statement) error {
	if se == nil || len(stmts) == 0 {
		return nil
	}
	var newNames []string
	seen := make(map[string]struct{})
	for _, stmt := range stmts {
		create, ok := stmt.(*sqlparser.CreateTable)
		if !ok {
			continue
		}
		// Temporary tables are session-scoped and not tracked in the
		// schema engine, so they don't contribute to the count.
		if create.Temp {
			continue
		}
		// If the target table is already in the engine's schema, this
		// CREATE is either a no-op (with IF NOT EXISTS) or will fail
		// downstream in MySQL (without IF NOT EXISTS). Either way the
		// count cannot increase.
		if se.GetTable(create.Table.Name) != nil {
			continue
		}
		// Dedupe by the engine's storage key (unqualified case-sensitive
		// table name) so the same table referenced with and without a
		// schema qualifier in the same batch is counted once. Keep the
		// fully-formatted name only for the error message.
		key := create.Table.Name.String()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		newNames = append(newNames, sqlparser.String(create.Table))
	}
	if len(newNames) == 0 {
		return nil
	}
	count, limit := se.TableCount(), MaxTableCount()
	if count+len(newNames) <= limit {
		return nil
	}
	if len(newNames) == 1 {
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
			"cannot create table %q: schema engine table limit of %d reached. "+
				"Increase --queryserver-config-schema-max-table-count and ensure "+
				"vttablet and mysqld have enough memory for a larger schema.",
			newNames[0], limit)
	}
	return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
		"cannot create %d new tables in this batch (first: %q): schema engine table limit of %d would be exceeded (current count: %d). "+
			"Increase --queryserver-config-schema-max-table-count and ensure "+
			"vttablet and mysqld have enough memory for a larger schema.",
		len(newNames), newNames[0], limit, count)
}
