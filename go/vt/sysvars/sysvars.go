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

package sysvars

// This information lives here, because it's needed from the vtgate planbuilder, the vtgate engine,
// and the AST rewriter, that happens to live in sqlparser.

// SystemVariable is a system variable that Vitess handles in queries such as:
// select @@sql_mode
// set skip_query_plan_cache = true
type SystemVariable struct {
	// IsBoolean is used to signal necessary type coercion so that strings
	// and numbers can be evaluated to a boolean value
	IsBoolean bool

	// IdentifierAsString allows identifiers (a.k.a. ColName) from the AST to be handled as if they are strings.
	// SET transaction_mode = two_pc => SET transaction_mode = 'two_pc'
	IdentifierAsString bool

	// Default is the default value, if none is given
	Default string

	Name string

	SupportSetVar bool
}

// System Settings
var (
	on      = "1"
	off     = "0"
	utf8mb4 = "'utf8mb4'"

	Autocommit                  = SystemVariable{Name: "autocommit", IsBoolean: true, Default: on}
	Charset                     = SystemVariable{Name: "charset", Default: utf8mb4, IdentifierAsString: true}
	ClientFoundRows             = SystemVariable{Name: "client_found_rows", IsBoolean: true, Default: off}
	SessionEnableSystemSettings = SystemVariable{Name: "enable_system_settings", IsBoolean: true, Default: on}
	Names                       = SystemVariable{Name: "names", Default: utf8mb4, IdentifierAsString: true}
	SessionUUID                 = SystemVariable{Name: "session_uuid", IdentifierAsString: true}
	SkipQueryPlanCache          = SystemVariable{Name: "skip_query_plan_cache", IsBoolean: true, Default: off}
	Socket                      = SystemVariable{Name: "socket", Default: off}
	SQLSelectLimit              = SystemVariable{Name: "sql_select_limit", Default: off, SupportSetVar: true}
	TransactionMode             = SystemVariable{Name: "transaction_mode", IdentifierAsString: true}
	TransactionReadOnly         = SystemVariable{Name: "transaction_read_only", IsBoolean: true, Default: off}
	TxReadOnly                  = SystemVariable{Name: "tx_read_only", IsBoolean: true, Default: off}
	Workload                    = SystemVariable{Name: "workload", IdentifierAsString: true}

	// Online DDL
	DDLStrategy    = SystemVariable{Name: "ddl_strategy", IdentifierAsString: true}
	Version        = SystemVariable{Name: "version"}
	VersionComment = SystemVariable{Name: "version_comment"}

	// Read After Write settings
	ReadAfterWriteGTID    = SystemVariable{Name: "read_after_write_gtid"}
	ReadAfterWriteTimeOut = SystemVariable{Name: "read_after_write_timeout"}
	SessionTrackGTIDs     = SystemVariable{Name: "session_track_gtids", IdentifierAsString: true}

	VitessAware = []SystemVariable{
		Autocommit,
		ClientFoundRows,
		SkipQueryPlanCache,
		TxReadOnly,
		TransactionReadOnly,
		SQLSelectLimit,
		TransactionMode,
		DDLStrategy,
		Workload,
		Charset,
		Names,
		SessionUUID,
		SessionEnableSystemSettings,
		ReadAfterWriteGTID,
		ReadAfterWriteTimeOut,
		SessionTrackGTIDs,
	}

	ReadOnly = []SystemVariable{
		Socket,
		Version,
		VersionComment,
	}

	IgnoreThese = []SystemVariable{
		{Name: "big_tables", IsBoolean: true, SupportSetVar: true},
		{Name: "bulk_insert_buffer_size", SupportSetVar: true},
		{Name: "debug"},
		{Name: "default_storage_engine"},
		{Name: "default_tmp_storage_engine", SupportSetVar: true},
		{Name: "innodb_strict_mode", IsBoolean: true},
		{Name: "innodb_support_xa", IsBoolean: true},
		{Name: "innodb_table_locks", IsBoolean: true},
		{Name: "innodb_tmpdir"},
		{Name: "join_buffer_size", SupportSetVar: true},
		{Name: "keep_files_on_create", IsBoolean: true},
		{Name: "lc_messages"},
		{Name: "long_query_time"},
		{Name: "low_priority_updates", IsBoolean: true},
		{Name: "max_delayed_threads"},
		{Name: "max_insert_delayed_threads"},
		{Name: "multi_range_count"},
		{Name: "net_buffer_length"},
		{Name: "new", IsBoolean: true},
		{Name: "query_cache_type"},
		{Name: "query_cache_wlock_invalidate", IsBoolean: true},
		{Name: "query_prealloc_size"},
		{Name: "sql_buffer_result", IsBoolean: true, SupportSetVar: true},
		{Name: "transaction_alloc_block_size"},
		{Name: "wait_timeout"},
	}

	NotSupported = []SystemVariable{
		{Name: "audit_log_read_buffer_size"},
		{Name: "auto_increment_increment", SupportSetVar: true},
		{Name: "auto_increment_offset", SupportSetVar: true},
		{Name: "binlog_direct_non_transactional_updates"},
		{Name: "binlog_row_image"},
		{Name: "binlog_rows_query_log_events"},
		{Name: "innodb_ft_enable_stopword"},
		{Name: "innodb_ft_user_stopword_table"},
		{Name: "max_points_in_geometry", SupportSetVar: true},
		{Name: "max_sp_recursion_depth"},
		{Name: "myisam_repair_threads"},
		{Name: "myisam_sort_buffer_size"},
		{Name: "myisam_stats_method"},
		{Name: "ndb_allow_copying_alter_table"},
		{Name: "ndb_autoincrement_prefetch_sz"},
		{Name: "ndb_blob_read_batch_bytes"},
		{Name: "ndb_blob_write_batch_bytes"},
		{Name: "ndb_deferred_constraints"},
		{Name: "ndb_force_send"},
		{Name: "ndb_fully_replicated"},
		{Name: "ndb_index_stat_enable"},
		{Name: "ndb_index_stat_option"},
		{Name: "ndb_join_pushdown"},
		{Name: "ndb_log_bin"},
		{Name: "ndb_log_exclusive_reads"},
		{Name: "ndb_row_checksum"},
		{Name: "ndb_use_exact_count"},
		{Name: "ndb_use_transactions"},
		{Name: "ndbinfo_max_bytes"},
		{Name: "ndbinfo_max_rows"},
		{Name: "ndbinfo_show_hidden"},
		{Name: "ndbinfo_table_prefix"},
		{Name: "old_alter_table"},
		{Name: "preload_buffer_size"},
		{Name: "rbr_exec_mode"},
		{Name: "sql_log_off"},
		{Name: "thread_pool_high_priority_connection"},
		{Name: "thread_pool_prio_kickup_timer"},
		{Name: "transaction_write_set_extraction"},
	}
	UseReservedConn = []SystemVariable{
		{Name: "default_week_format"},
		{Name: "end_markers_in_json", IsBoolean: true, SupportSetVar: true},
		{Name: "eq_range_index_dive_limit", SupportSetVar: true},
		{Name: "explicit_defaults_for_timestamp"},
		{Name: "foreign_key_checks", IsBoolean: true, SupportSetVar: true},
		{Name: "group_concat_max_len", SupportSetVar: true},
		{Name: "information_schema_stats_expiry"},
		{Name: "max_heap_table_size", SupportSetVar: true},
		{Name: "max_seeks_for_key", SupportSetVar: true},
		{Name: "max_tmp_tables"},
		{Name: "min_examined_row_limit"},
		{Name: "old_passwords"},
		{Name: "optimizer_prune_level", SupportSetVar: true},
		{Name: "optimizer_search_depth", SupportSetVar: true},
		{Name: "optimizer_switch", SupportSetVar: true},
		{Name: "optimizer_trace"},
		{Name: "optimizer_trace_features"},
		{Name: "optimizer_trace_limit"},
		{Name: "optimizer_trace_max_mem_size"},
		{Name: "transaction_isolation"},
		{Name: "tx_isolation"},
		{Name: "optimizer_trace_offset"},
		{Name: "parser_max_mem_size"},
		{Name: "profiling", IsBoolean: true},
		{Name: "profiling_history_size"},
		{Name: "query_alloc_block_size"},
		{Name: "range_alloc_block_size", SupportSetVar: true},
		{Name: "range_optimizer_max_mem_size", SupportSetVar: true},
		{Name: "read_buffer_size", SupportSetVar: true},
		{Name: "read_rnd_buffer_size", SupportSetVar: true},
		{Name: "show_create_table_verbosity", IsBoolean: true},
		{Name: "show_old_temporals", IsBoolean: true},
		{Name: "sort_buffer_size", SupportSetVar: true},
		{Name: "sql_big_selects", IsBoolean: true, SupportSetVar: true},
		{Name: "sql_mode", SupportSetVar: true},
		{Name: "sql_notes", IsBoolean: true},
		{Name: "sql_quote_show_create", IsBoolean: true},
		{Name: "sql_safe_updates", IsBoolean: true, SupportSetVar: true},
		{Name: "sql_warnings", IsBoolean: true},
		{Name: "time_zone"},
		{Name: "tmp_table_size", SupportSetVar: true},
		{Name: "transaction_prealloc_size"},
		{Name: "unique_checks", IsBoolean: true, SupportSetVar: true},
		{Name: "updatable_views_with_limit", IsBoolean: true, SupportSetVar: true},
	}
	CheckAndIgnore = []SystemVariable{
		// TODO: Most of these settings should be moved into SysSetOpAware, and change Vitess behaviour.
		// Until then, SET statements against these settings are allowed
		// as long as they have the same value as the underlying database
		{Name: "binlog_format"},
		{Name: "block_encryption_mode"},
		{Name: "character_set_client"},
		{Name: "character_set_connection"},
		{Name: "character_set_database"},
		{Name: "character_set_filesystem"},
		{Name: "character_set_results"},
		{Name: "character_set_server"},
		{Name: "collation_connection"},
		{Name: "collation_database"},
		{Name: "collation_server"},
		{Name: "completion_type"},
		{Name: "div_precision_increment", SupportSetVar: true},
		{Name: "innodb_lock_wait_timeout"},
		{Name: "interactive_timeout"},
		{Name: "lc_time_names"},
		{Name: "lock_wait_timeout", SupportSetVar: true},
		{Name: "max_allowed_packet"},
		{Name: "max_error_count", SupportSetVar: true},
		{Name: "max_execution_time", SupportSetVar: true},
		{Name: "max_join_size", SupportSetVar: true},
		{Name: "max_length_for_sort_data", SupportSetVar: true},
		{Name: "max_sort_length", SupportSetVar: true},
		{Name: "max_user_connections"},
		{Name: "net_read_timeout"},
		{Name: "net_retry_count"},
		{Name: "net_write_timeout"},
		{Name: "session_track_schema", IsBoolean: true},
		{Name: "session_track_state_change", IsBoolean: true},
		{Name: "session_track_system_variables"},
		{Name: "session_track_transaction_info"},
		{Name: "sql_auto_is_null", IsBoolean: true, SupportSetVar: true},
		{Name: "version_tokens_session"},
	}
)

// GetInterestingVariables is used to return all the variables that may be listed in a SHOW VARIABLES command.
func GetInterestingVariables() []string {
	var res []string
	// Add all the vitess aware variables
	for _, variable := range VitessAware {
		res = append(res, variable.Name)
	}
	// Also add version and version comment
	res = append(res, Version.Name)
	res = append(res, VersionComment.Name)
	res = append(res, Socket.Name)

	for _, variable := range UseReservedConn {
		if variable.SupportSetVar {
			res = append(res, variable.Name)
		}
	}
	return res
}
