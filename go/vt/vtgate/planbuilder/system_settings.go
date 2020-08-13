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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func init() {
	forSettings(ignoreThese, buildSetOpIgnore)
	forSettings(useReservedConn, buildSetOpVarSet)
	forSettings(checkAndIgnore, buildSetOpCheckAndIgnore)
	forSettings(notSupported, buildNotSupported)
	forSettings(vitessAware, buildSetOpVitessAware)
}

func forSettings(settings []setting, f func(s setting) planFunc) {
	for _, setting := range settings {
		if _, alreadyExists := sysVarPlanningFunc[setting.name]; alreadyExists {
			panic("bug in set plan init - " + setting.name + " aleady configured")
		}
		sysVarPlanningFunc[setting.name] = f(setting)
	}
}

var (
	ON  = evalengine.NewLiteralInt(1)
	OFF = evalengine.NewLiteralInt(0)

	vitessAware = []setting{
		{name: engine.Autocommit, boolean: true, defaultValue: ON},
		{name: engine.ClientFoundRows, boolean: true, defaultValue: OFF},
		{name: engine.SkipQueryPlanCache, boolean: true, defaultValue: OFF},
		{name: engine.TransactionReadOnly, boolean: true, defaultValue: OFF},
		{name: engine.TxReadOnly, boolean: true, defaultValue: OFF},
		{name: engine.SQLSelectLimit, defaultValue: OFF},
		{name: engine.TransactionMode, identifierAsString: true, defaultValue: evalengine.NewLiteralString([]byte("MULTI"))},
		{name: engine.Workload, identifierAsString: true, defaultValue: evalengine.NewLiteralString([]byte("UNSPECIFIED"))},
		{name: engine.Charset, identifierAsString: true, defaultValue: evalengine.NewLiteralString([]byte("utf8"))},
		{name: engine.Names, identifierAsString: true, defaultValue: evalengine.NewLiteralString([]byte("utf8"))},
	}

	notSupported = []setting{
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

	ignoreThese = []setting{
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

	useReservedConn = []setting{
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
	checkAndIgnore = []setting{
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
)
