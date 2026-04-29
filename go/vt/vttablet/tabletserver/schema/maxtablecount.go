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

// CheckCreateTableLimit returns an error if running stmt would push the
// schema engine past its configured table-count limit. It is a no-op for
// statements that are not a non-temporary CREATE TABLE for a brand-new
// table, and for nil engines (which can occur in tests or unconfigured
// callers).
//
// The check is best-effort under concurrency: two callers can each see the
// count below the limit and both create tables, briefly leaving the count
// at limit+1. Schema reload tolerates this — the gate's purpose is to give
// a clear error in the common single-client case rather than to enforce
// strict admission.
//
// TableCount is per-engine instance state; MaxTableCount is the
// process-wide Viper-managed limit, hence the asymmetric calls.
func CheckCreateTableLimit(se *Engine, stmt sqlparser.Statement) error {
	if se == nil {
		return nil
	}
	create, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil
	}
	// Temporary tables are session-scoped and not tracked in the schema
	// engine, so they don't contribute to the count.
	if create.Temp {
		return nil
	}
	// If the target table is already in the engine's schema, this CREATE is
	// either a no-op (with IF NOT EXISTS) or will fail downstream in MySQL
	// (without IF NOT EXISTS). Either way the count cannot increase.
	if se.GetTable(create.Table.Name) != nil {
		return nil
	}
	if count, limit := se.TableCount(), MaxTableCount(); count >= limit {
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
			"cannot create table %q: schema engine table limit of %d reached. "+
				"Increase --queryserver-config-schema-max-table-count and ensure "+
				"vttablet and mysqld have enough memory for a larger schema.",
			sqlparser.String(create.Table), limit)
	}
	return nil
}
