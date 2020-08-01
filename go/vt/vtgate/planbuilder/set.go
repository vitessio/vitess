/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type (
	planFunc = func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error)

	expressionConverter struct {
		tabletExpressions []*sqlparser.SetExpr
	}

	setting struct {
		name    string
		boolean bool
	}
)

var sysVarPlanningFunc = map[string]planFunc{}

var notSupported = []setting{
	{name: "audit_log_read_buffer_size"},
	{name: "auto_increment_increment"},
	{name: "auto_increment_offset"},
	{name: "binlog_direct_non_transactional_updates"},
	{name: "binlog_row_image"},
	{name: "binlog_rows_query_log_events"},
	{name: "innodb_ft_enable_stopword"},
	{name: "innodb_ft_user_stopword_table"},
	{name: "max_points_in_geometry"},
	{name: "max_sp_recursion_depth"},
	{name: "myisam_repair_threads"},
	{name: "myisam_sort_buffer_size"},
	{name: "myisam_stats_method"},
	{name: "ndb_allow_copying_alter_table"},
	{name: "ndb_autoincrement_prefetch_sz"},
	{name: "ndb_blob_read_batch_bytes"},
	{name: "ndb_blob_write_batch_bytes"},
	{name: "ndb_deferred_constraints"},
	{name: "ndb_force_send"},
	{name: "ndb_fully_replicated"},
	{name: "ndb_index_stat_enable"},
	{name: "ndb_index_stat_option"},
	{name: "ndb_join_pushdown"},
	{name: "ndb_log_bin"},
	{name: "ndb_log_exclusive_reads"},
	{name: "ndb_row_checksum"},
	{name: "ndb_use_exact_count"},
	{name: "ndb_use_transactions"},
	{name: "ndbinfo_max_bytes"},
	{name: "ndbinfo_max_rows"},
	{name: "ndbinfo_show_hidden"},
	{name: "ndbinfo_table_prefix"},
	{name: "old_alter_table"},
	{name: "preload_buffer_size"},
	{name: "rbr_exec_mode"},
	{name: "sql_log_off"},
	{name: "thread_pool_high_priority_connection"},
	{name: "thread_pool_prio_kickup_timer"},
	{name: "transaction_write_set_extraction"},
}

var ignoreThese = []setting{
	{name: "big_tables", boolean: true},
	{name: "bulk_insert_buffer_size"},
	{name: "debug"},
	{name: "default_storage_engine"},
	{name: "default_tmp_storage_engine"},
	{name: "innodb_strict_mode", boolean: true},
	{name: "innodb_support_xa", boolean: true},
	{name: "innodb_table_locks", boolean: true},
	{name: "innodb_tmpdir"},
	{name: "join_buffer_size"},
	{name: "keep_files_on_create", boolean: true},
	{name: "lc_messages"},
	{name: "long_query_time"},
	{name: "low_priority_updates", boolean: true},
	{name: "max_delayed_threads"},
	{name: "max_insert_delayed_threads"},
	{name: "multi_range_count"},
	{name: "net_buffer_length"},
	{name: "new", boolean: true},
	{name: "query_cache_type"},
	{name: "query_cache_wlock_invalidate", boolean: true},
	{name: "query_prealloc_size"},
	{name: "sql_buffer_result", boolean: true},
	{name: "transaction_alloc_block_size"},
	{name: "wait_timeout"},
}

var useReservedConn = []setting{
	{name: "default_week_format"},
	{name: "end_markers_in_json", boolean: true},
	{name: "eq_range_index_dive_limit"},
	{name: "explicit_defaults_for_timestamp"},
	{name: "foreign_key_checks", boolean: true},
	{name: "group_concat_max_len"},
	{name: "max_heap_table_size"},
	{name: "max_seeks_for_key"},
	{name: "max_tmp_tables"},
	{name: "min_examined_row_limit"},
	{name: "old_passwords"},
	{name: "optimizer_prune_level"},
	{name: "optimizer_search_depth"},
	{name: "optimizer_switch"},
	{name: "optimizer_trace"},
	{name: "optimizer_trace_features"},
	{name: "optimizer_trace_limit"},
	{name: "optimizer_trace_max_mem_size"},
	{name: "transaction_isolation"},
	{name: "tx_isolation"},
	{name: "optimizer_trace_offset"},
	{name: "parser_max_mem_size"},
	{name: "profiling", boolean: true},
	{name: "profiling_history_size"},
	{name: "query_alloc_block_size"},
	{name: "range_alloc_block_size"},
	{name: "range_optimizer_max_mem_size"},
	{name: "read_buffer_size"},
	{name: "read_rnd_buffer_size"},
	{name: "show_create_table_verbosity", boolean: true},
	{name: "show_old_temporals", boolean: true},
	{name: "sort_buffer_size"},
	{name: "sql_big_selects", boolean: true},
	{name: "sql_mode"},
	{name: "sql_notes", boolean: true},
	{name: "sql_quote_show_create", boolean: true},
	{name: "sql_safe_updates", boolean: true},
	{name: "sql_warnings", boolean: true},
	{name: "tmp_table_size"},
	{name: "transaction_prealloc_size"},
	{name: "unique_checks", boolean: true},
	{name: "updatable_views_with_limit", boolean: true},
}

// TODO: Most of these settings should be moved into SysSetOpAware, and change Vitess behaviour.
// Until then, SET statements against these settings are allowed
// as long as they have the same value as the underlying database
var checkAndIgnore = []setting{
	{name: "binlog_format"},
	{name: "block_encryption_mode"},
	{name: "character_set_client"},
	{name: "character_set_connection"},
	{name: "character_set_database"},
	{name: "character_set_filesystem"},
	{name: "character_set_results"},
	{name: "character_set_server"},
	{name: "collation_connection"},
	{name: "collation_database"},
	{name: "collation_server"},
	{name: "completion_type"},
	{name: "div_precision_increment"},
	{name: "innodb_lock_wait_timeout"},
	{name: "interactive_timeout"},
	{name: "lc_time_names"},
	{name: "lock_wait_timeout"},
	{name: "max_allowed_packet"},
	{name: "max_error_count"},
	{name: "max_execution_time"},
	{name: "max_join_size"},
	{name: "max_length_for_sort_data"},
	{name: "max_sort_length"},
	{name: "max_user_connections"},
	{name: "net_read_timeout"},
	{name: "net_retry_count"},
	{name: "net_write_timeout"},
	{name: "session_track_gtids"},
	{name: "session_track_schema", boolean: true},
	{name: "session_track_state_change", boolean: true},
	{name: "session_track_system_variables"},
	{name: "session_track_transaction_info"},
	{name: "sql_auto_is_null", boolean: true},
	{name: "time_zone"},
	{name: "version_tokens_session"},
}

func init() {
	forSettings(ignoreThese, buildSetOpIgnore)
	forSettings(useReservedConn, buildSetOpVarSet)
	forSettings(checkAndIgnore, buildSetOpCheckAndIgnore)
	forSettings(notSupported, buildNotSupported)
}

func forSettings(settings []setting, f func(bool) planFunc) {
	for _, setting := range settings {
		if _, alreadyExists := sysVarPlanningFunc[setting.name]; alreadyExists {
			panic("bug in set plan init - " + setting.name + " aleady configured")
		}
		sysVarPlanningFunc[setting.name] = f(setting.boolean)
	}
}

func buildSetPlan(stmt *sqlparser.Set, vschema ContextVSchema) (engine.Primitive, error) {
	var setOps []engine.SetOp
	var setOp engine.SetOp
	var err error

	ec := new(expressionConverter)

	for _, expr := range stmt.Exprs {
		switch expr.Scope {
		case sqlparser.GlobalStr:
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported in set: global")
			// AST struct has been prepared before getting here, so no scope here means that
			// we have a UDV. If the original query didn't explicitly specify the scope, it
			// would have been explictly set to sqlparser.SessionStr before reaching this
			// phase of planning
		case "":
			evalExpr, err := ec.convert(expr)
			if err != nil {
				return nil, err
			}
			setOp = &engine.UserDefinedVariable{
				Name: expr.Name.Lowered(),
				Expr: evalExpr,
			}

			setOps = append(setOps, setOp)
		case sqlparser.SessionStr:
			planFunc, ok := sysVarPlanningFunc[expr.Name.Lowered()]
			if !ok {
				return nil, ErrPlanNotSupported
			}
			setOp, err = planFunc(expr, vschema)
			if err != nil {
				return nil, err
			}
			setOps = append(setOps, setOp)
		default:
			return nil, ErrPlanNotSupported
		}
	}

	input, err := ec.source(vschema)
	if err != nil {
		return nil, err
	}

	return &engine.Set{
		Ops:   setOps,
		Input: input,
	}, nil
}

func (spb *expressionConverter) convert(setExpr *sqlparser.SetExpr) (evalengine.Expr, error) {
	astExpr := setExpr.Expr
	evalExpr, err := sqlparser.Convert(astExpr)
	if err != nil {
		if err != sqlparser.ExprNotSupported {
			return nil, err
		}
		// We have an expression that we can't handle at the vtgate level
		if !expressionOkToDelegateToTablet(astExpr) {
			// Uh-oh - this expression is not even safe to delegate to the tablet. Give up.
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "expression not supported for SET: %s", sqlparser.String(astExpr))
		}
		evalExpr = &evalengine.Column{Offset: len(spb.tabletExpressions)}
		spb.tabletExpressions = append(spb.tabletExpressions, setExpr)
	}
	return evalExpr, nil
}

func (spb *expressionConverter) source(vschema ContextVSchema) (engine.Primitive, error) {
	if len(spb.tabletExpressions) == 0 {
		return &engine.SingleRow{}, nil
	}
	ks, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	var expr []string
	for _, e := range spb.tabletExpressions {
		expr = append(expr, sqlparser.String(e.Expr))
	}
	query := fmt.Sprintf("select %s from dual", strings.Join(expr, ","))

	primitive := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	return primitive, nil
}

func buildNotSupported(bool) func(*sqlparser.SetExpr, ContextVSchema) (engine.SetOp, error) {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema) (engine.SetOp, error) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s: system setting is not supported", expr.Name)
	}
}

func buildSetOpIgnore(boolean bool) func(*sqlparser.SetExpr, ContextVSchema) (engine.SetOp, error) {
	return func(expr *sqlparser.SetExpr, _ ContextVSchema) (engine.SetOp, error) {
		return &engine.SysVarIgnore{
			Name: expr.Name.Lowered(),
			Expr: extractValue(expr, boolean),
		}, nil
	}
}

func buildSetOpCheckAndIgnore(boolean bool) func(*sqlparser.SetExpr, ContextVSchema) (engine.SetOp, error) {
	return func(expr *sqlparser.SetExpr, schema ContextVSchema) (engine.SetOp, error) {
		keyspace, dest, err := resolveDestination(schema)
		if err != nil {
			return nil, err
		}

		return &engine.SysVarCheckAndIgnore{
			Name:              expr.Name.Lowered(),
			Keyspace:          keyspace,
			TargetDestination: dest,
			Expr:              extractValue(expr, boolean),
		}, nil
	}
}

func expressionOkToDelegateToTablet(e sqlparser.Expr) bool {
	valid := true
	sqlparser.Rewrite(e, nil, func(cursor *sqlparser.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *sqlparser.Subquery, *sqlparser.TimestampFuncExpr, *sqlparser.CurTimeFuncExpr:
			valid = false
			return false
		case *sqlparser.FuncExpr:
			_, ok := validFuncs[n.Name.Lowered()]
			valid = ok
			return ok
		}
		return true
	})
	return valid
}

func buildSetOpVarSet(boolean bool) func(*sqlparser.SetExpr, ContextVSchema) (engine.SetOp, error) {
	return func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}

		return &engine.SysVarSet{
			Name:              expr.Name.Lowered(),
			Keyspace:          ks,
			TargetDestination: vschema.Destination(),
			Expr:              extractValue(expr, boolean),
		}, nil
	}
}

func resolveDestination(vschema ContextVSchema) (*vindexes.Keyspace, key.Destination, error) {
	keyspace, err := vschema.AnyKeyspace()
	if err != nil {
		return nil, nil, err
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}
	return keyspace, dest, nil
}

func extractValue(expr *sqlparser.SetExpr, boolean bool) string {
	switch node := expr.Expr.(type) {
	case *sqlparser.SQLVal:
		if node.Type == sqlparser.StrVal && boolean {
			switch strings.ToLower(string(node.Val)) {
			case "on":
				return "1"
			case "off":
				return "0"
			}
		}
	case *sqlparser.ColName:
		// this is a little of a hack. it's used when the setting is not a normal expression, but rather
		// an enumeration, such as utf8, utf8mb4, etc
		if node.Name.AtCount() == sqlparser.NoAt {
			switch node.Name.Lowered() {
			case "on":
				return "1"
			case "off":
				return "0"
			}
			return fmt.Sprintf("'%s'", sqlparser.String(expr.Expr))
		}
	}

	return sqlparser.String(expr.Expr)
}

// whitelist of functions knows to be safe to pass through to mysql for evaluation
// this list tries to not include functions that might return different results on different tablets
var validFuncs = map[string]interface{}{
	"if":               nil,
	"ifnull":           nil,
	"nullif":           nil,
	"abs":              nil,
	"acos":             nil,
	"asin":             nil,
	"atan2":            nil,
	"atan":             nil,
	"ceil":             nil,
	"ceiling":          nil,
	"conv":             nil,
	"cos":              nil,
	"cot":              nil,
	"crc32":            nil,
	"degrees":          nil,
	"div":              nil,
	"exp":              nil,
	"floor":            nil,
	"ln":               nil,
	"log":              nil,
	"log10":            nil,
	"log2":             nil,
	"mod":              nil,
	"pi":               nil,
	"pow":              nil,
	"power":            nil,
	"radians":          nil,
	"rand":             nil,
	"round":            nil,
	"sign":             nil,
	"sin":              nil,
	"sqrt":             nil,
	"tan":              nil,
	"truncate":         nil,
	"adddate":          nil,
	"addtime":          nil,
	"convert_tz":       nil,
	"date":             nil,
	"date_add":         nil,
	"date_format":      nil,
	"date_sub":         nil,
	"datediff":         nil,
	"day":              nil,
	"dayname":          nil,
	"dayofmonth":       nil,
	"dayofweek":        nil,
	"dayofyear":        nil,
	"extract":          nil,
	"from_days":        nil,
	"from_unixtime":    nil,
	"get_format":       nil,
	"hour":             nil,
	"last_day":         nil,
	"makedate":         nil,
	"maketime":         nil,
	"microsecond":      nil,
	"minute":           nil,
	"month":            nil,
	"monthname":        nil,
	"period_add":       nil,
	"period_diff":      nil,
	"quarter":          nil,
	"sec_to_time":      nil,
	"second":           nil,
	"str_to_date":      nil,
	"subdate":          nil,
	"subtime":          nil,
	"time_format":      nil,
	"time_to_sec":      nil,
	"timediff":         nil,
	"timestampadd":     nil,
	"timestampdiff":    nil,
	"to_days":          nil,
	"to_seconds":       nil,
	"week":             nil,
	"weekday":          nil,
	"weekofyear":       nil,
	"year":             nil,
	"yearweek":         nil,
	"ascii":            nil,
	"bin":              nil,
	"bit_length":       nil,
	"char":             nil,
	"char_length":      nil,
	"character_length": nil,
	"concat":           nil,
	"concat_ws":        nil,
	"elt":              nil,
	"export_set":       nil,
	"field":            nil,
	"find_in_set":      nil,
	"format":           nil,
	"from_base64":      nil,
	"hex":              nil,
	"insert":           nil,
	"instr":            nil,
	"lcase":            nil,
	"left":             nil,
	"length":           nil,
	"load_file":        nil,
	"locate":           nil,
	"lower":            nil,
	"lpad":             nil,
	"ltrim":            nil,
	"make_set":         nil,
	"mid":              nil,
	"oct":              nil,
	"octet_length":     nil,
	"ord":              nil,
	"position":         nil,
	"quote":            nil,
	"repeat":           nil,
	"replace":          nil,
	"reverse":          nil,
	"right":            nil,
	"rpad":             nil,
	"rtrim":            nil,
	"soundex":          nil,
	"space":            nil,
	"strcmp":           nil,
	"substr":           nil,
	"substring":        nil,
	"substring_index":  nil,
	"to_base64":        nil,
	"trim":             nil,
	"ucase":            nil,
	"unhex":            nil,
	"upper":            nil,
	"weight_string":    nil,
}
