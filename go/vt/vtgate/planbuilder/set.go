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

type planFunc = func(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error)

var sysVarPlanningFunc = map[string]planFunc{}

var notSupported = []string{
	"audit_log_read_buffer_size",
	"auto_increment_increment",
	"auto_increment_offset",
	"binlog_direct_non_transactional_updates",
	"binlog_row_image",
	"binlog_rows_query_log_events",
	"innodb_ft_enable_stopword",
	"innodb_ft_user_stopword_table",
	"max_points_in_geometry",
	"max_sp_recursion_depth",
	"myisam_repair_threads",
	"myisam_sort_buffer_size",
	"myisam_stats_method",
	"ndb_allow_copying_alter_table",
	"ndb_autoincrement_prefetch_sz",
	"ndb_blob_read_batch_bytes",
	"ndb_blob_write_batch_bytes",
	"ndb_deferred_constraints",
	"ndb_force_send",
	"ndb_fully_replicated",
	"ndb_index_stat_enable",
	"ndb_index_stat_option",
	"ndb_join_pushdown",
	"ndb_log_bin",
	"ndb_log_exclusive_reads",
	"ndb_row_checksum",
	"ndb_use_exact_count",
	"ndb_use_transactions",
	"ndbinfo_max_bytes",
	"ndbinfo_max_rows",
	"ndbinfo_show_hidden",
	"ndbinfo_table_prefix",
	"old_alter_table",
	"preload_buffer_size",
	"rbr_exec_mode",
	"sql_log_off",
	"thread_pool_high_priority_connection",
	"thread_pool_prio_kickup_timer",
	"transaction_write_set_extraction",
}

var ignoreThese = []string{
	"big_tables",
	"bulk_insert_buffer_size",
	"debug",
	"default_storage_engine",
	"default_tmp_storage_engine",
	"innodb_strict_mode",
	"innodb_support_xa",
	"innodb_table_locks",
	"innodb_tmpdir",
	"join_buffer_size",
	"keep_files_on_create",
	"lc_messages",
	"long_query_time",
	"low_priority_updates",
	"max_delayed_threads",
	"max_insert_delayed_threads",
	"multi_range_count",
	"net_buffer_length",
	"new",
	"query_cache_type",
	"query_cache_wlock_invalidate",
	"query_prealloc_size",
	"sql_buffer_result",
	"transaction_alloc_block_size",
	"wait_timeout",
}

var useReservedConn = []string{
	"default_week_format",
	"end_markers_in_json",
	"eq_range_index_dive_limit",
	"explicit_defaults_for_timestamp",
	"foreign_key_checks",
	"group_concat_max_len",
	"max_heap_table_size",
	"max_seeks_for_key",
	"max_tmp_tables",
	"min_examined_row_limit",
	"old_passwords",
	"optimizer_prune_level",
	"optimizer_search_depth",
	"optimizer_switch",
	"optimizer_trace",
	"optimizer_trace_features",
	"optimizer_trace_limit",
	"optimizer_trace_max_mem_size",
	"transaction_isolation",
	"tx_isolation",
	"optimizer_trace_offset",
	"parser_max_mem_size",
	"profiling",
	"profiling_history_size",
	"query_alloc_block_size",
	"range_alloc_block_size",
	"range_optimizer_max_mem_size",
	"read_buffer_size",
	"read_rnd_buffer_size",
	"show_create_table_verbosity",
	"show_old_temporals",
	"sort_buffer_size",
	"sql_big_selects",
	"sql_mode",
	"sql_notes",
	"sql_quote_show_create",
	"sql_safe_updates",
	"sql_warnings",
	"tmp_table_size",
	"transaction_prealloc_size",
	"unique_checks",
	"updatable_views_with_limit",
}

// TODO: Most of these settings should be moved into SysSetOpAware, and change Vitess behaviour.
// Until then, SET statements against these settings are allowed
// as long as they have the same value as the underlying database
var checkAndIgnore = []string{
	"binlog_format",
	"block_encryption_mode",
	"character_set_client",
	"character_set_connection",
	"character_set_database",
	"character_set_filesystem",
	"character_set_server",
	"collation_connection",
	"collation_database",
	"collation_server",
	"completion_type",
	"div_precision_increment",
	"innodb_lock_wait_timeout",
	"interactive_timeout",
	"lc_time_names",
	"lock_wait_timeout",
	"max_allowed_packet",
	"max_error_count",
	"max_execution_time",
	"max_join_size",
	"max_length_for_sort_data",
	"max_sort_length",
	"max_user_connections",
	"net_read_timeout",
	"net_retry_count",
	"net_write_timeout",
	"session_track_gtids",
	"session_track_schema",
	"session_track_state_change",
	"session_track_system_variables",
	"session_track_transaction_info",
	"sql_auto_is_null",
	"time_zone",
	"version_tokens_session",
}

func init() {
	forSettings(ignoreThese, buildSetOpIgnore)
	forSettings(useReservedConn, buildSetOpVarSet)
	forSettings(checkAndIgnore, buildSetOpCheckAndIgnore)
	forSettings(notSupported, buildNotSupported)
}

func forSettings(settings []string, f planFunc) {
	for _, setting := range settings {
		if _, alreadyExists := sysVarPlanningFunc[setting]; alreadyExists {
			panic("bug in set plan init - " + setting + " aleady configured")
		}
		sysVarPlanningFunc[setting] = f
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

type expressionConverter struct {
	tabletExpressions []*sqlparser.SetExpr
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

func buildNotSupported(e *sqlparser.SetExpr, _ ContextVSchema) (engine.SetOp, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s: system setting is not supported", e.Name)
}

func buildSetOpIgnore(expr *sqlparser.SetExpr, _ ContextVSchema) (engine.SetOp, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", expr.Expr)

	return &engine.SysVarIgnore{
		Name: expr.Name.Lowered(),
		Expr: buf.String(),
	}, nil
}

func buildSetOpCheckAndIgnore(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
	keyspace, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	return &engine.SysVarCheckAndIgnore{
		Name:              expr.Name.Lowered(),
		Keyspace:          keyspace,
		TargetDestination: dest,
		Expr:              sqlparser.String(expr.Expr),
	}, nil
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

func buildSetOpVarSet(expr *sqlparser.SetExpr, vschema ContextVSchema) (engine.SetOp, error) {
	ks, err := vschema.AnyKeyspace()
	if err != nil {
		return nil, err
	}

	return &engine.SysVarSet{
		Name:              expr.Name.Lowered(),
		Keyspace:          ks,
		TargetDestination: vschema.Destination(),
		Expr:              sqlparser.String(expr.Expr),
	}, nil
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
