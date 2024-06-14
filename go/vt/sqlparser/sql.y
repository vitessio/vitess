/*
Copyright 2019 The Vitess Authors.

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

%{
package sqlparser

import "fmt"
import "strings"
//import "runtime/debug"

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func incNesting(yylex interface{}) bool {
  yylex.(*Tokenizer).nesting++
  if yylex.(*Tokenizer).nesting == 200 {
    return true
  }
  return false
}

func decNesting(yylex interface{}) {
  yylex.(*Tokenizer).nesting--
}

func statementSeen(yylex interface{}) {
  if yylex.(*Tokenizer).stopAfterFirstStmt {
    yylex.(*Tokenizer).stopped = true
  }
}

func yyPosition(yylex interface{}) int {
  return yylex.(*Tokenizer).Position
}

func yyOldPosition(yylex interface{}) int {
  return yylex.(*Tokenizer).OldPosition
}

func yySpecialCommentMode(yylex interface{}) bool {
  tkn := yylex.(*Tokenizer)
  return tkn.specialComment != nil
}

%}

%union {
  empty         struct{}
  statement     Statement
  selStmt       SelectStatement
  ddl           *DDL
  ddls          []*DDL
  ins           *Insert
  byt           byte
  bytes         []byte
  bytes2        [][]byte
  str           string
  int           int
  strs          []string
  selectExprs   SelectExprs
  selectExpr    SelectExpr
  columns       Columns
  statements    Statements
  partitions    Partitions
  variables	Variables
  into		*Into
  colName       *ColName
  tableExprs    TableExprs
  tableExpr     TableExpr
  subquery      *Subquery
  simpleTableExpr  SimpleTableExpr
  joinCondition JoinCondition
  triggerName   TriggerName
  tableName     TableName
  tableNames    TableNames
  procedureName ProcedureName
  indexHints    *IndexHints
  asOf          *AsOf
  expr          Expr
  exprs         Exprs
  boolVal       BoolVal
  boolean       bool
  sqlVal        *SQLVal
  colTuple      ColTuple
  values        Values
  aliasedvalues AliasedValues
  valTuple      ValTuple
  whens         []*When
  when          *When
  orderBy       OrderBy
  order         *Order
  limit         *Limit
  assignExprs   AssignmentExprs
  assignExpr    *AssignmentExpr
  setVarExprs   SetVarExprs
  setVarExpr    *SetVarExpr
  setScope      SetScope
  colIdent      ColIdent
  colIdents     []ColIdent
  tableIdent    TableIdent
  convertType   *ConvertType
  aliasedTableName *AliasedTableExpr
  TableSpec  *TableSpec
  columnType    ColumnType
  queryOpts     QueryOpts
  JSONTableSpec *JSONTableSpec
  JSONTableColDef *JSONTableColDef
  JSONTableColOpts JSONTableColOpts
  columnOrder   *ColumnOrder
  triggerOrder  *TriggerOrder
  colKeyOpt     ColumnKeyOption
  optVal        Expr
  LengthScaleOption LengthScaleOption
  columnDefinition *ColumnDefinition
  indexDefinition *IndexDefinition
  indexInfo     *IndexInfo
  indexOption   *IndexOption
  indexOptions  []*IndexOption
  flushOption   *FlushOption
  replicationOption *ReplicationOption
  replicationOptions []*ReplicationOption
  indexColumn   *IndexColumn
  indexColumns  []*IndexColumn
  constraintDefinition *ConstraintDefinition
  constraintInfo ConstraintInfo
  tableOption   *TableOption
  tableOptions  []*TableOption
  partOption    *PartitionOption
  subpart       *SubPartition
  ReferenceAction ReferenceAction
  partDefs      []*PartitionDefinition
  partDef       *PartitionDefinition
  partSpec      *PartitionSpec
  partSpecs     []*PartitionSpec
  viewSpec      *ViewSpec
  viewCheckOption ViewCheckOption
  showFilter    *ShowFilter
  frame         *Frame
  frameExtent   *FrameExtent
  frameBound    *FrameBound
  caseStatementCases []CaseStatementCase
  caseStatementCase CaseStatementCase
  ifStatementConditions []IfStatementCondition
  ifStatementCondition IfStatementCondition
  signalInfo SignalInfo
  signalInfos []SignalInfo
  signalConditionItemName SignalConditionItemName
  declareHandlerAction DeclareHandlerAction
  declareHandlerCondition DeclareHandlerCondition
  declareHandlerConditions []DeclareHandlerCondition
  procedureParam ProcedureParam
  procedureParams []ProcedureParam
  characteristic Characteristic
  characteristics []Characteristic
  charsetCollate *CharsetAndCollate
  charsetCollates []*CharsetAndCollate
  Fields *Fields
  Lines	*Lines
  EnclosedBy *EnclosedBy
  tableAndLockType *TableAndLockType
  tableAndLockTypes TableAndLockTypes
  lockType LockType
  accountName AccountName
  accountNames []AccountName
  accountRenames []AccountRename
  authentication *Authentication
  accountWithAuth AccountWithAuth
  accountsWithAuth []AccountWithAuth
  tlsOptionItem TLSOptionItem
  tlsOptionItems []TLSOptionItem
  accountLimitItem AccountLimitItem
  accountLimitItems []AccountLimitItem
  passLockItem PassLockItem
  passLockItems []PassLockItem
  grantPrivilege Privilege
  grantPrivileges []Privilege
  grantObjectType GrantObjectType
  privilegeLevel PrivilegeLevel
  grantAssumption *GrantUserAssumption
  with *With
  window Window
  over *Over
  windowDef *WindowDef
  eventName EventName
  eventScheduleSpec *EventScheduleSpec
  eventScheduleTimeSpec *EventScheduleTimeSpec
  eventStatus EventStatus
  eventOnCompletion EventOnCompletion
  intervalExprs []IntervalExpr
  separator Separator
  srsAttr *SrsAttribute
}

// These precedence rules are there to handle shift-reduce conflicts.

// STRING_TYPE_PREFIX_NON_KEYWORD is used to resolve shift-reduce conflicts occuring due to column_name symbol and
// being able to use keywords like DATE and TIME as prefixes to strings to denote their type. The shift-reduce conflict occurrs because
// after seeing one of these non-reserved keywords, if we see a STRING, then we can either shift to use the STRING typed rule in literal or
// reduce the non-reserved keyword into column_name and eventually use a rule from simple_expr.
// The way to fix this conflict is to give shifting higher precedence than reducing.
// Adding no precedence also works, since shifting is the default, but it reports some conflicts
// Precedence is also assined to shifting on STRING.
// We also need to add a lower precedence to reducing the grammar symbol to non-reserved keywords.
// In order to ensure lower precedence of reduction, this rule has to come before the precedence declaration of STRING.
// This precedence should not be used anywhere else other than with non-reserved-keywords that are also used for type-casting a STRING.
%nonassoc <str> STRING_TYPE_PREFIX_NON_KEYWORD

%token LEX_ERROR

// Special tokens
%token <bytes> FOR_SYSTEM_TIME
%token <bytes> FOR_VERSION

%left <bytes> EXCEPT
%left <bytes> UNION
%left <bytes> INTERSECT
%token <bytes> SELECT STREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR CALL
%token <bytes> ALL DISTINCT AS EXISTS ASC DESC DUPLICATE DEFAULT SET LOCK UNLOCK KEYS OF
%token <bytes> OUTFILE DUMPFILE DATA LOAD LINES TERMINATED ESCAPED ENCLOSED OPTIONALLY STARTING
%right <bytes> UNIQUE KEY
%token <bytes> SYSTEM_TIME CONTAINED VERSION VERSIONS
%token <bytes> VALUES LAST_INSERT_ID SQL_CALC_FOUND_ROWS
%token <bytes> NEXT VALUE SHARE MODE
%token <bytes> SQL_NO_CACHE SQL_CACHE
%left <bytes> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <bytes> ON USING
%token <empty> '(' ',' ')' '@' ':'
%nonassoc <bytes> STRING
%token <bytes> ID HEX INTEGRAL FLOAT HEXNUM VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD BIT_LITERAL
%token <bytes> NULL TRUE FALSE OFF
%right <bytes> INTO

// Precedence dictated by mysql. But the vitess grammar is simplified.
// Some of these operators don't conflict in our situation. Nevertheless,
// it's better to have these listed in the correct order. Also, we don't
// support all operators yet.
%left <bytes> OR
%left <bytes> XOR
%left <bytes> AND
%right <bytes> NOT '!'
%left <bytes> BETWEEN CASE WHEN THEN ELSE ELSEIF END
%left <bytes> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN ASSIGNMENT_OP
%nonassoc  UNBOUNDED // ideally should have same precedence as IDENT
%nonassoc ID NULL PARTITION RANGE ROWS GROUPS PRECEDING FOLLOWING
%left <bytes> '|'
%left <bytes> '&'
%left <bytes> SHIFT_LEFT SHIFT_RIGHT
%left <bytes> '+' '-'
%left <bytes> '*' '/' DIV '%' MOD
%left <bytes> '^'
%right <bytes> '~' UNARY
%left <bytes> COLLATE
%right <bytes> BINARY UNDERSCORE_ARMSCII8 UNDERSCORE_ASCII UNDERSCORE_BIG5 UNDERSCORE_BINARY UNDERSCORE_CP1250
%right <bytes> UNDERSCORE_CP1251 UNDERSCORE_CP1256 UNDERSCORE_CP1257 UNDERSCORE_CP850 UNDERSCORE_CP852 UNDERSCORE_CP866
%right <bytes> UNDERSCORE_CP932 UNDERSCORE_DEC8 UNDERSCORE_EUCJPMS UNDERSCORE_EUCKR UNDERSCORE_GB18030 UNDERSCORE_GB2312
%right <bytes> UNDERSCORE_GBK UNDERSCORE_GEOSTD8 UNDERSCORE_GREEK UNDERSCORE_HEBREW UNDERSCORE_HP8 UNDERSCORE_KEYBCS2
%right <bytes> UNDERSCORE_KOI8R UNDERSCORE_KOI8U UNDERSCORE_LATIN1 UNDERSCORE_LATIN2 UNDERSCORE_LATIN5 UNDERSCORE_LATIN7
%right <bytes> UNDERSCORE_MACCE UNDERSCORE_MACROMAN UNDERSCORE_SJIS UNDERSCORE_SWE7 UNDERSCORE_TIS620 UNDERSCORE_UCS2
%right <bytes> UNDERSCORE_UJIS UNDERSCORE_UTF16 UNDERSCORE_UTF16LE UNDERSCORE_UTF32 UNDERSCORE_UTF8 UNDERSCORE_UTF8MB3 UNDERSCORE_UTF8MB4
%right <bytes> INTERVAL
%nonassoc <bytes> '.'

// There is no need to define precedence for the JSON
// operators because the syntax is restricted enough that
// they don't cause conflicts.
%token <empty> JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP

// DDL Tokens
%token <bytes> CREATE ALTER DROP RENAME ANALYZE ADD MODIFY CHANGE
%token <bytes> SCHEMA TABLE INDEX INDEXES VIEW TO IGNORE IF PRIMARY COLUMN SPATIAL FULLTEXT KEY_BLOCK_SIZE CHECK
%token <bytes> ACTION CASCADE CONSTRAINT FOREIGN NO REFERENCES RESTRICT
%token <bytes> FIRST AFTER LAST
%token <bytes> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE FORMAT EXTENDED
%token <bytes> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER TRIGGERS FUNCTION
%token <bytes> STATUS VARIABLES WARNINGS ERRORS KILL CONNECTION
%token <bytes> SEQUENCE ENABLE DISABLE
%token <bytes> EACH ROW BEFORE FOLLOWS PRECEDES DEFINER INVOKER
%token <bytes> INOUT OUT DETERMINISTIC CONTAINS READS MODIFIES SQL SECURITY TEMPORARY ALGORITHM MERGE TEMPTABLE UNDEFINED
%token <bytes> EVENT EVENTS SCHEDULE EVERY STARTS ENDS COMPLETION PRESERVE CASCADED
%token <bytes> INSTANT INPLACE COPY
%token <bytes> DISCARD IMPORT
%token <bytes> SHARED EXCLUSIVE
%token <bytes> WITHOUT VALIDATION
%token <bytes> COALESCE EXCHANGE REBUILD REMOVE PARTITIONING

// SIGNAL Tokens
%token <bytes> CLASS_ORIGIN SUBCLASS_ORIGIN MESSAGE_TEXT MYSQL_ERRNO CONSTRAINT_CATALOG CONSTRAINT_SCHEMA
%token <bytes> CONSTRAINT_NAME CATALOG_NAME SCHEMA_NAME TABLE_NAME COLUMN_NAME CURSOR_NAME SIGNAL RESIGNAL SQLSTATE

// Stored Procedure Tokens
%token <bytes> DECLARE CONDITION CURSOR CONTINUE EXIT UNDO HANDLER FOUND SQLWARNING SQLEXCEPTION FETCH OPEN CLOSE
%token <bytes> LOOP LEAVE ITERATE REPEAT UNTIL WHILE DO RETURN

// Permissions Tokens
%token <bytes> USER IDENTIFIED ROLE REUSE GRANT GRANTS REVOKE NONE ATTRIBUTE RANDOM PASSWORD INITIAL AUTHENTICATION
%token <bytes> SSL X509 CIPHER ISSUER SUBJECT ACCOUNT EXPIRE NEVER OPTION OPTIONAL ADMIN PRIVILEGES
%token <bytes> MAX_QUERIES_PER_HOUR MAX_UPDATES_PER_HOUR MAX_CONNECTIONS_PER_HOUR MAX_USER_CONNECTIONS FLUSH
%token <bytes> FAILED_LOGIN_ATTEMPTS PASSWORD_LOCK_TIME UNBOUNDED REQUIRE PROXY ROUTINE TABLESPACE CLIENT SLAVE
%token <bytes> EXECUTE FILE RELOAD REPLICATION SHUTDOWN SUPER USAGE LOGS ENGINE ERROR GENERAL HOSTS
%token <bytes> OPTIMIZER_COSTS RELAY SLOW USER_RESOURCES NO_WRITE_TO_BINLOG CHANNEL

// Dynamic Privilege Tokens
%token <bytes> APPLICATION_PASSWORD_ADMIN AUDIT_ABORT_EXEMPT AUDIT_ADMIN AUTHENTICATION_POLICY_ADMIN BACKUP_ADMIN
%token <bytes> BINLOG_ADMIN BINLOG_ENCRYPTION_ADMIN CLONE_ADMIN CONNECTION_ADMIN ENCRYPTION_KEY_ADMIN FIREWALL_ADMIN
%token <bytes> FIREWALL_EXEMPT FIREWALL_USER FLUSH_OPTIMIZER_COSTS FLUSH_STATUS FLUSH_TABLES FLUSH_USER_RESOURCES
%token <bytes> GROUP_REPLICATION_ADMIN GROUP_REPLICATION_STREAM INNODB_REDO_LOG_ARCHIVE INNODB_REDO_LOG_ENABLE
%token <bytes> NDB_STORED_USER PASSWORDLESS_USER_ADMIN PERSIST_RO_VARIABLES_ADMIN REPLICATION_APPLIER
%token <bytes> REPLICATION_SLAVE_ADMIN RESOURCE_GROUP_ADMIN RESOURCE_GROUP_USER ROLE_ADMIN SENSITIVE_VARIABLES_OBSERVER
%token <bytes> SESSION_VARIABLES_ADMIN SET_USER_ID SHOW_ROUTINE SKIP_QUERY_REWRITE SYSTEM_VARIABLES_ADMIN
%token <bytes> TABLE_ENCRYPTION_ADMIN TP_CONNECTION_ADMIN VERSION_TOKEN_ADMIN XA_RECOVER_ADMIN

// Replication Tokens
%token <bytes> REPLICA REPLICAS SOURCE STOP RESET FILTER LOG MASTER
%token <bytes> SOURCE_HOST SOURCE_USER SOURCE_PASSWORD SOURCE_PORT SOURCE_CONNECT_RETRY SOURCE_RETRY_COUNT SOURCE_AUTO_POSITION
%token <bytes> REPLICATE_DO_TABLE REPLICATE_IGNORE_TABLE

// Transaction Tokens
%token <bytes> BEGIN START TRANSACTION COMMIT ROLLBACK SAVEPOINT WORK RELEASE CHAIN

// Type Tokens
%token <bytes> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM SERIAL INT1 INT2 INT3 INT4 INT8
%token <bytes> REAL DOUBLE FLOAT_TYPE DECIMAL NUMERIC DEC FIXED PRECISION
%token <bytes> TIME TIMESTAMP DATETIME
%token <bytes> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR NVARCHAR NATIONAL VARYING VARCHARACTER
%token <bytes> TEXT TINYTEXT MEDIUMTEXT LONGTEXT LONG
%token <bytes> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON ENUM
%token <bytes> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON

// Lock tokens
%token <bytes> LOCAL LOW_PRIORITY SKIP LOCKED

// Type Modifiers
%token <bytes> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL SRID

// Supported SHOW tokens
%token <bytes> COLLATION DATABASES SCHEMAS TABLES FULL PROCESSLIST COLUMNS FIELDS ENGINES PLUGINS

// SET tokens
%token <bytes> NAMES CHARSET GLOBAL SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE
%token <bytes> ENCRYPTION

// Functions
%token <bytes> CURRENT_TIMESTAMP NOW DATABASE CURRENT_DATE CURRENT_USER
%token <bytes> CURRENT_TIME LOCALTIME LOCALTIMESTAMP
%token <bytes> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <bytes> REPLACE
%token <bytes> CONVERT CAST POSITION
%token <bytes> SUBSTR SUBSTRING
%token <bytes> TRIM LEADING TRAILING BOTH
%token <bytes> GROUP_CONCAT SEPARATOR
%token <bytes> TIMESTAMPADD TIMESTAMPDIFF EXTRACT

// Window functions
%token <bytes> OVER WINDOW GROUPING GROUPS
%token <bytes> CURRENT ROWS RANGE
%token <bytes> AVG BIT_AND BIT_OR BIT_XOR COUNT JSON_ARRAYAGG JSON_OBJECTAGG MAX MIN STDDEV_POP STDDEV STD STDDEV_SAMP
%token <bytes> SUM VAR_POP VARIANCE VAR_SAMP CUME_DIST DENSE_RANK FIRST_VALUE LAG LAST_VALUE LEAD NTH_VALUE NTILE
%token <bytes> ROW_NUMBER PERCENT_RANK RANK

// Table functions
%token <bytes> DUAL JSON_TABLE PATH

// Table options
%token <bytes> AVG_ROW_LENGTH CHECKSUM TABLE_CHECKSUM COMPRESSION DIRECTORY DELAY_KEY_WRITE ENGINE_ATTRIBUTE INSERT_METHOD MAX_ROWS
%token <bytes> MIN_ROWS PACK_KEYS ROW_FORMAT SECONDARY_ENGINE SECONDARY_ENGINE_ATTRIBUTE STATS_AUTO_RECALC STATS_PERSISTENT
%token <bytes> STATS_SAMPLE_PAGES STORAGE DISK MEMORY DYNAMIC COMPRESSED REDUNDANT
%token <bytes> COMPACT LIST HASH PARTITIONS SUBPARTITION SUBPARTITIONS

// Prepared statements
%token <bytes> PREPARE DEALLOCATE

// Match
%token <bytes> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION

// Time Unit Tokens
%token <bytes> MICROSECOND SECOND MINUTE HOUR DAY WEEK MONTH QUARTER YEAR
%token <bytes> SECOND_MICROSECOND
%token <bytes> MINUTE_MICROSECOND MINUTE_SECOND
%token <bytes> HOUR_MICROSECOND HOUR_SECOND HOUR_MINUTE
%token <bytes> DAY_MICROSECOND DAY_SECOND DAY_MINUTE DAY_HOUR
%token <bytes> YEAR_MONTH

// Spatial Reference System Tokens
%token <bytes> NAME SYSTEM

// MySQL reserved words that are currently unused.
%token <bytes> ACCESSIBLE ASENSITIVE
%token <bytes> CUBE
%token <bytes> DELAYED DISTINCTROW
%token <bytes> EMPTY
%token <bytes> FLOAT4 FLOAT8
%token <bytes> GET
%token <bytes> HIGH_PRIORITY
%token <bytes> INSENSITIVE IO_AFTER_GTIDS IO_BEFORE_GTIDS LINEAR
%token <bytes> MASTER_BIND MASTER_SSL_VERIFY_SERVER_CERT MIDDLEINT
%token <bytes> PURGE
%token <bytes> READ_WRITE RLIKE
%token <bytes> SENSITIVE SPECIFIC SQL_BIG_RESULT SQL_SMALL_RESULT

%token <bytes> UNUSED DESCRIPTION LATERAL MEMBER RECURSIVE
%token <bytes> BUCKETS CLONE COMPONENT DEFINITION ENFORCED NOT_ENFORCED EXCLUDE FOLLOWING GEOMCOLLECTION GET_MASTER_PUBLIC_KEY HISTOGRAM HISTORY
%token <bytes> INACTIVE INVISIBLE MASTER_COMPRESSION_ALGORITHMS MASTER_PUBLIC_KEY_PATH MASTER_TLS_CIPHERSUITES MASTER_ZSTD_COMPRESSION_LEVEL
%token <bytes> NESTED NETWORK_NAMESPACE NOWAIT NULLS OJ OLD ORDINALITY ORGANIZATION OTHERS PERSIST PERSIST_ONLY PRECEDING PRIVILEGE_CHECKS_USER PROCESS
%token <bytes> REFERENCE REQUIRE_ROW_FORMAT RESOURCE RESPECT RESTART RETAIN SECONDARY SECONDARY_LOAD SECONDARY_UNLOAD
%token <bytes> THREAD_PRIORITY TIES VCPU VISIBLE INFILE

// MySQL unreserved keywords that are currently unused
%token <bytes> ACTIVE AGGREGATE ANY ARRAY ASCII AT AUTOEXTEND_SIZE

// Generated Columns
%token <bytes> GENERATED ALWAYS STORED VIRTUAL

// TODO: categorize/organize these somehow later
%token <bytes> NVAR PASSWORD_LOCK

%type <statement> command
%type <selStmt> create_query_expression create_query_select_expression select_statement with_select select_or_set_op base_select base_select_no_cte select_statement_with_no_trailing_into values_select_statement
%type <selStmt> set_op intersect_stmt union_except_lhs union_except_rhs
%type <statement> stream_statement insert_statement update_statement delete_statement set_statement trigger_body
%type <statement> create_statement rename_statement drop_statement truncate_statement call_statement
%type <statement> trigger_begin_end_block statement_list_statement case_statement if_statement signal_statement
%type <statement> begin_end_block declare_statement resignal_statement open_statement close_statement fetch_statement
%type <statement> loop_statement leave_statement iterate_statement repeat_statement while_statement return_statement
%type <statement> savepoint_statement rollback_savepoint_statement release_savepoint_statement
%type <statement> lock_statement unlock_statement kill_statement grant_statement revoke_statement flush_statement replication_statement
%type <statements> statement_list
%type <caseStatementCases> case_statement_case_list
%type <caseStatementCase> case_statement_case
%type <ifStatementConditions> elseif_list
%type <ifStatementCondition> elseif_list_item
%type <strs> fetch_variable_list
%type <signalInfo> signal_information_item
%type <signalInfos> signal_information_item_list
%type <signalConditionItemName> signal_information_name
%type <declareHandlerCondition> declare_handler_condition
%type <declareHandlerConditions> declare_handler_condition_list
%type <declareHandlerAction> declare_handler_action
%type <bytes> signal_condition_value
%type <bytes> char_or_character
%type <str> trigger_time trigger_event
%type <boolean> with_or_without
%type <statement> alter_statement alter_table_statement alter_database_statement alter_event_statement alter_user_statement
%type <ddl> create_table_prefix rename_list alter_table_statement_part alter_table_options
%type <ddls> alter_table_statement_list
%type <statement> analyze_statement analyze_opt show_statement use_statement prepare_statement execute_statement deallocate_statement
%type <statement> describe_statement explain_statement explainable_statement
%type <statement> begin_statement commit_statement rollback_statement start_transaction_statement load_statement
%type <bytes> work_opt no_opt chain_opt release_opt index_name_opt no_first_last
%type <bytes2> comment_opt comment_list
%type <str> distinct_opt union_op intersect_op except_op insert_or_replace
%type <str> match_option format_opt
%type <separator> separator_opt
%type <expr> like_escape_opt
%type <selectExprs> select_expression_list argument_expression_list argument_expression_list_opt
%type <selectExpr> select_expression argument_expression
%type <expr> expression naked_like group_by
%type <tableExprs> table_references cte_list from_opt
%type <with> with_clause with_clause_opt
%type <tableExpr> table_reference table_function table_factor join_table common_table_expression
%type <simpleTableExpr> values_statement subquery_or_values
%type <subquery> subquery
%type <joinCondition> join_condition join_condition_opt on_expression_opt
%type <tableNames> table_name_list delete_table_list view_name_list
%type <str> inner_join outer_join straight_join natural_join
%type <triggerName> trigger_name
%type <tableName> table_name load_into_table_name into_table_name delete_table_name
%type <aliasedTableName> aliased_table_name aliased_table_options
%type <asOf> as_of_clause as_of_point_clause between_times between_versions all_times all_versions
%type <procedureName> procedure_name
%type <eventName> event_name rename_event_name_opt
%type <indexHints> index_hint_list
%type <expr> where_expression_opt
%type <expr> condition
%type <boolVal> boolean_value
%type <boolean> all_opt enforced_opt
%type <str> compare
%type <ins> insert_data insert_data_alias insert_data_select insert_data_values
%type <expr> value value_expression num_val as_of_opt limit_val integral_or_interval_expr timestamp_value
%type <bytes> time_unit non_microsecond_time_unit
%type <expr> function_call_keyword function_call_nonkeyword function_call_generic function_call_conflict
%type <expr> func_datetime_prec_opt function_call_window function_call_aggregate_with_window function_call_on_update
%type <str> is_suffix
%type <colTuple> col_tuple
%type <exprs> expression_list group_by_list partition_by_opt
%type <values> tuple_list row_list

%type <valTuple> row_tuple tuple_or_empty
%type <expr> tuple_expression
%type <colName> column_name
%type <whens> when_expression_list
%type <when> when_expression
%type <expr> expression_opt else_expression_opt
%type <exprs> group_by_opt
%type <expr> having_opt having
%type <orderBy> order_by_opt order_list
%type <columnOrder> column_order_opt
%type <triggerOrder> trigger_order_opt
%type <order> order
%type <over> over over_opt
%type <window> window window_opt
%type <windowDef> window_definition window_spec
%type <frame> frame_opt
%type <frameExtent> frame_extent
%type <frameBound> frame_bound
%type <int> lexer_position lexer_old_position
%type <boolean> special_comment_mode
%type <str> asc_desc_opt
%type <limit> limit_opt
%type <str> lock_opt
%type <colIdent> ins_column
%type <columns> ins_column_list ins_column_list_opt column_list paren_column_list column_list_opt
%type <partitions> opt_partition_clause partition_list
%type <variables> variable_list
%type <strs> system_variable_list
%type <str> system_variable
%type <into> into_opt
%type <assignExprs> on_dup_opt assignment_list
%type <setVarExprs> set_list transaction_chars
%type <bytes> charset_or_character_set
%type <assignExpr> assignment_expression
%type <setVarExpr> set_expression set_expression_assignment transaction_char
%type <setScope> set_scope_primary set_scope_secondary
%type <str> isolation_level
%type <bytes> for_from
%type <str> ignore_opt default_opt
%type <str> from_database_opt columns_or_fields
%type <boolean> full_opt
%type <showFilter> like_or_where_opt
%type <byt> exists_opt not_exists_opt temp_opt
%type <queryOpts> query_opts
%type <str> key_type key_type_opt
%type <str> flush_type flush_type_opt
%type <empty> to_or_as_opt to_or_as /*to_opt*/ as_opt column_opt
%type <str> algorithm_view_opt algorithm_part_opt definer_opt security_opt
%type <viewSpec> view_opts
%type <viewCheckOption> opt_with_check_option
%type <bytes> reserved_keyword qualified_column_name_safe_reserved_keyword non_reserved_keyword column_name_safe_keyword function_call_keywords non_reserved_keyword2 non_reserved_keyword3 all_non_reserved id_or_non_reserved
%type <colIdent> sql_id reserved_sql_id col_alias as_ci_opt using_opt existing_window_name_opt
%type <colIdents> reserved_sql_id_list
%type <expr> charset_value
%type <tableIdent> table_id reserved_table_id table_alias
%type <str> charset underscore_charsets
%type <str> show_session_or_global
%type <convertType> convert_type
%type <columnType> column_type  column_type_options
%type <columnType> int_type decimal_type numeric_type time_type char_type spatial_type
%type <sqlVal> char_length_opt length_opt column_comment ignore_number_opt comment_keyword_opt
%type <optVal> column_default on_update
%type <str> charset_opt character_set collate_opt collate table_option_collate
%type <boolean> default_keyword_opt
%type <charsetCollate> charset_default_opt collate_default_opt encryption_default_opt
%type <charsetCollates> creation_option creation_option_opt
%type <boolVal> stored_opt
%type <boolVal> signed_or_unsigned_opt zero_fill_opt
%type <LengthScaleOption> float_length_opt decimal_length_opt
%type <boolVal> auto_increment local_opt optionally_opt
%type <colKeyOpt> column_key
%type <strs> enum_values
%type <columnDefinition> column_definition column_definition_for_create
%type <indexDefinition> index_definition
%type <constraintDefinition> foreign_key_definition check_constraint_definition
%type <str> index_or_key indexes_or_keys index_or_key_opt
%type <str> from_or_in show_database_opt
%type <str> name_opt
%type <str> equal_opt assignment_op
%type <TableSpec> table_spec table_column_list
%type <str> table_opt_value row_fmt_opt
%type <partOption> partition_option_opt partition_option linear_partition_opt
%type <subpart> subpartition_opt
%type <boolean> linear_opt
%type <str> range_or_list
%type <sqlVal> partition_num_opt subpartition_num_opt
%type <indexInfo> index_info
%type <indexColumn> index_column
%type <indexColumns> index_column_list
%type <indexOption> index_option
%type <indexOptions> index_option_list index_option_list_opt
%type <tableOption> table_option
%type <tableOptions> table_option_list
%type <flushOption> flush_option
%type <boolean> flush_tables_read_lock_opt
%type <replicationOptions> replication_option_list replication_filter_option_list
%type <replicationOption> replication_option replication_filter_option
%type <str> relay_logs_attribute
%type <constraintInfo> foreign_key_details check_constraint_info
%type <partDefs> partition_definitions partition_definitions_opt
%type <partDef> partition_definition
%type <partSpec> partition_operation
%type <partSpecs> partition_operation_list_opt partition_operation_list
%type <ReferenceAction> fk_reference_action fk_on_delete fk_on_update drop_statement_action
%type <str> pk_name_opt constraint_symbol_opt infile_opt ignore_or_replace_opt
%type <exprs> call_param_list_opt
%type <procedureParams> proc_param_list_opt proc_param_list
%type <procedureParam> proc_param
%type <characteristics> characteristic_list_opt characteristic_list
%type <characteristic> characteristic
%type <Fields> fields_opt
%type <Lines> lines_opt
%type <EnclosedBy> enclosed_by_opt
%type <sqlVal> terminated_by_opt starting_by_opt escaped_by_opt
%type <tableAndLockTypes> lock_table_list
%type <tableAndLockType> lock_table
%type <lockType> lock_type
%type <accountName> account_name role_name
%type <accountNames> account_name_list role_name_list default_role_opt
%type <accountRenames> rename_user_list
%type <str> account_name_str user_comment_attribute
%type <accountWithAuth> account_with_auth
%type <accountsWithAuth> account_with_auth_list
%type <authentication> authentication authentication_initial
%type <eventScheduleSpec> event_schedule
%type <eventScheduleTimeSpec> event_starts_opt event_ends_opt
%type <eventStatus> event_status_opt
%type <eventOnCompletion> event_on_completion_preserve_opt
%type <intervalExprs> event_schedule_intervals_opt
%type <tlsOptionItem> tls_option_item
%type <tlsOptionItems> tls_options tls_option_item_list
%type <accountLimitItem> account_limit_item
%type <accountLimitItems> account_limits account_limit_item_list
%type <passLockItem> pass_lock_item
%type <passLockItems> pass_lock_options pass_lock_item_list
%type <grantPrivilege> grant_privilege
%type <grantPrivileges> grant_privilege_list
%type <bytes> dynamic_privilege
%type <strs> grant_privilege_columns_opt grant_privilege_column_list
%type <grantObjectType> grant_object_type
%type <privilegeLevel> grant_privilege_level
%type <grantAssumption> grant_assumption
%type <boolean> with_grant_opt with_admin_opt
%type <bytes> any_identifier
%type <str> any_identifier_list
%type <statement> create_spatial_ref_sys
%type <srsAttr> srs_attribute

%type <tableExpr> json_table
%type <JSONTableSpec> json_table_column_list
%type <JSONTableColDef> json_table_column_definition
%type <JSONTableColOpts> json_table_column_options
%type <expr> val_on_empty val_on_error

%type <bytes> coericble_to_integral

%start any_command

%%

any_command:
  command
  {
    setParseTree(yylex, $1)
  }
| command ';'
  {
    setParseTree(yylex, $1)
    statementSeen(yylex)
  }

command:
  select_statement
  {
    $$ = $1
  }
| values_select_statement
  {
    $$ = $1
  }
| stream_statement
| insert_statement
| update_statement
| delete_statement
| set_statement
| create_statement
| alter_statement
| rename_statement
| drop_statement
| truncate_statement
| analyze_statement
| prepare_statement
| execute_statement
| deallocate_statement
| show_statement
| use_statement
| begin_statement
| commit_statement
| rollback_statement
| explain_statement
| describe_statement
| signal_statement
| resignal_statement
| call_statement
| load_statement
| savepoint_statement
| rollback_savepoint_statement
| release_savepoint_statement
| lock_statement
| unlock_statement
| kill_statement
| grant_statement
| revoke_statement
| replication_statement
| flush_statement
| /*empty*/
{
  setParseTree(yylex, nil)
}

load_statement:
  LOAD DATA local_opt infile_opt ignore_or_replace_opt load_into_table_name opt_partition_clause charset_opt fields_opt lines_opt ignore_number_opt column_list_opt
  {
    $$ = &Load{Local: $3, Infile: $4, IgnoreOrReplace: $5, Table: $6, Partition: $7, Charset: $8, Fields: $9, Lines: $10, IgnoreNum: $11, Columns: $12}
  }

select_statement:
  with_select order_by_opt limit_opt lock_opt into_opt
  {
    $1.SetOrderBy($2)
    $1.SetLimit($3)
    $1.SetLock($4)
    if err := $1.SetInto($5); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    $$ = $1
  }
| SELECT comment_opt query_opts NEXT num_val for_from table_name
  {
    $$ = &Select{
    	Comments: Comments($2),
    	QueryOpts: $3,
    	SelectExprs: SelectExprs{Nextval{Expr: $5}},
    	From: TableExprs{&AliasedTableExpr{Expr: $7}},
    }
  }

values_select_statement:
  values_statement order_by_opt limit_opt
  {
    $$ = &Select{
    	SelectExprs: SelectExprs{&StarExpr{}},
    	From: TableExprs{&AliasedTableExpr{Expr: $1}},
    	OrderBy: $2,
    	Limit: $3,
    }
  }
| openb values_select_statement closeb
  {
    $$ = $2
  }

select_statement_with_no_trailing_into:
  select_statement
  {
    if $1.GetInto() != nil {
      yylex.Error(fmt.Errorf("INTO clause is not allowed").Error())
      return 1
    }
    $$ = $1
  }

stream_statement:
  STREAM comment_opt select_expression FROM table_name
  {
    $$ = &Stream{Comments: Comments($2), SelectExpr: $3, Table: $5}
  }

// select_or_set_op is an unparenthesized SELECT without an order by clause or UNION/INTERSECT/EXCEPT operation
// TODO: select_or_set_op should also include openb select_statement_with_no_trailing_into closeb; so it should just use base_select
select_or_set_op:
  base_select_no_cte
  {
    $$ = $1
  }
| set_op
  {
    $$ = $1
  }

// set_op is a UNION/INTERSECT/EXCEPT operation
set_op:
  intersect_stmt
  {
    $$ = $1
  }
| union_except_lhs union_op union_except_rhs
  {
    $$ = &SetOp{Type: $2, Left: $1, Right: $3}
  }
| union_except_lhs except_op union_except_rhs
  {
    $$ = &SetOp{Type: $2, Left: $1, Right: $3}
  }

// intersect_stmt is an INTERSECT operation
// in order to enforce its higher precedence over UNION/EXCEPT, it is defined as a terminal rule
// this way, base_select and other intersect_stmt are greedily paired up
intersect_stmt:
  base_select intersect_op base_select
  {
    $$ = &SetOp{Type: $2, Left: $1, Right: $3}
  }
| intersect_stmt intersect_op base_select
  {
    $$ = &SetOp{Type: $2, Left: $1, Right: $3}
  }

// base_select is either a simple SELECT or a SELECT wrapped in parentheses
base_select:
  base_select_no_cte
  {
    if $1.GetInto() != nil {
      yylex.Error(fmt.Errorf("INTO clause is not allowed").Error())
      return 1
    }
    $$ = $1
  }
| openb select_statement_with_no_trailing_into closeb
  {
    $$ = &ParenSelect{Select: $2}
  }

// union_except_lhs is either a simple select or a set_op
// this allows further nesting of UNION/INTERSECT/EXCEPT
union_except_lhs:
  base_select
  {
    $$ = $1
  }
| set_op
  {
    $$ = $1
  }

// union_except_rhs is either a simple SELECT or an intersect_stmt
// this is a terminal rule, to prevent further nesting of set_op
union_except_rhs:
  base_select
  {
    $$ = $1
  }
| intersect_stmt
  {
    $$ = $1
  }

with_select:
  select_or_set_op
  {
    $$ = $1
  }
| WITH with_clause select_or_set_op
  {
    $3.SetWith($2)
    $$ = $3
  }

with_clause:
  RECURSIVE cte_list
  {
    $$ = &With{Ctes: $2, Recursive: true}
  }
| cte_list {
    $$ = &With{Ctes: $1, Recursive: false}
}

base_select_no_cte:
  SELECT comment_opt query_opts select_expression_list into_opt from_opt where_expression_opt group_by_opt having_opt window_opt
  {
    $$ = &Select{
    	Comments: Comments($2),
    	QueryOpts: $3,
	SelectExprs: $4,
	Into: $5,
	From: $6,
	Where: NewWhere(WhereStr, $7),
	GroupBy: GroupBy($8),
	Having: NewWhere(HavingStr, $9),
	Window: $10,
    }
  }
| TABLE table_reference
  {
    $$ = &Select{SelectExprs: SelectExprs{&StarExpr{}}, From: TableExprs{$2}}
  }

from_opt:
  {
    $$ = nil
  }
| FROM DUAL
  {
    $$ = nil
  }
| FROM table_references
  {
    $$ = $2
  }

// They may appear either before from-clause or at the end of a query. This causes shift/reduce conflict when INTO
// token is used and has two cases it can be interpreted as depending on whether the query is table-less or other
// clauses (WHERE, GROUP BY, HAVING, WINDOW) are present.
into_opt:
%prec INTO
  {
    $$ = nil
  }
| INTO variable_list
  {
    $$ = &Into{Variables: $2}
  }
| INTO DUMPFILE STRING
  {
    $$ = &Into{Dumpfile: string($3)}
  }
| INTO OUTFILE STRING charset_opt fields_opt lines_opt
  {
    $$ = &Into{Outfile: string($3), Charset: $4, Fields: $5, Lines: $6}
  }

variable_list:
  sql_id
  {
    $$ = Variables{$1}
  }
| variable_list ',' sql_id
  {
    $$ = append($$, $3)
  }

with_clause_opt:
  {
    $$ = nil
  }
| WITH with_clause
  {
    $$ = $2
  }

cte_list:
  common_table_expression
  {
    $$ = TableExprs{$1}
  }
| cte_list ',' common_table_expression
  {
    $$ = append($1, $3)
  }

common_table_expression:
  table_alias ins_column_list_opt AS subquery_or_values
  {
    $$ = &CommonTableExpr{&AliasedTableExpr{Expr:$4, As: $1}, $2}
  }

insert_statement:
  with_clause_opt insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause insert_data_alias on_dup_opt
  {
    // insert_data returns a *Insert pre-filled with Columns & Values
    ins := $7
    ins.Action = $2
    ins.Comments = $3
    ins.Ignore = $4
    ins.Table = $5
    ins.Partitions = $6
    ins.OnDup = OnDup($8)
    ins.With = $1
    $$ = ins
  }
| with_clause_opt insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause insert_data_select on_dup_opt
  {
    // insert_data returns a *Insert pre-filled with Columns & Values
    ins := $7
    ins.Action = $2
    ins.Comments = $3
    ins.Ignore = $4
    ins.Table = $5
    ins.Partitions = $6
    ins.OnDup = OnDup($8)
    ins.With = $1
    $$ = ins
  }
| with_clause_opt insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause SET assignment_list on_dup_opt
  {
    cols := make(Columns, 0, len($8))
    vals := make(ValTuple, 0, len($9))
    for _, updateList := range $8 {
      cols = append(cols, updateList.Name.Name)
      vals = append(vals, updateList.Expr)
    }
    $$ = &Insert{Action: $2, Comments: Comments($3), Ignore: $4, Table: $5, Partitions: $6, Columns: cols, Rows: &AliasedValues{Values: Values{vals}}, OnDup: OnDup($9), With: $1}
  }

insert_or_replace:
  INSERT
  {
    $$ = InsertStr
  }
| REPLACE
  {
    $$ = ReplaceStr
  }

update_statement:
  with_clause_opt UPDATE comment_opt ignore_opt table_references SET assignment_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{Comments: Comments($3), Ignore: $4, TableExprs: $5, Exprs: $7, Where: NewWhere(WhereStr, $8), OrderBy: $9, Limit: $10, With: $1}
  }

delete_statement:
  with_clause_opt DELETE comment_opt FROM table_name opt_partition_clause where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{Comments: Comments($3), TableExprs: TableExprs{&AliasedTableExpr{Expr:$5}}, Partitions: $6, Where: NewWhere(WhereStr, $7), OrderBy: $8, Limit: $9, With: $1}
  }
| with_clause_opt DELETE comment_opt FROM table_name_list USING table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($3), Targets: $5, TableExprs: $7, Where: NewWhere(WhereStr, $8), With: $1}
  }
| with_clause_opt DELETE comment_opt table_name_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($3), Targets: $4, TableExprs: $6, Where: NewWhere(WhereStr, $7), With: $1}
  }
| with_clause_opt DELETE comment_opt delete_table_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($3), Targets: $4, TableExprs: $6, Where: NewWhere(WhereStr, $7), With: $1}
  }

from_or_using:
  FROM {}
| USING {}

view_name_list:
  table_name
  {
    $$ = TableNames{$1.ToViewName()}
  }
| view_name_list ',' table_name
  {
    $$ = append($$, $3.ToViewName())
  }

table_name_list:
  table_name
  {
    $$ = TableNames{$1}
  }
| table_name_list ',' table_name
  {
    $$ = append($$, $3)
  }

delete_table_list:
  delete_table_name
  {
    $$ = TableNames{$1}
  }
| delete_table_list ',' delete_table_name
  {
    $$ = append($$, $3)
  }

opt_partition_clause:
  {
    $$ = nil
  }
| PARTITION openb partition_list closeb
  {
  $$ = $3
  }

set_statement:
  SET comment_opt set_list
  {
    $$ = &Set{Comments: Comments($2), Exprs: $3}
  }
| SET comment_opt TRANSACTION transaction_chars
  {
    for i := 0; i < len($4); i++ {
      $4[i].Scope = SetScope_None
    }
    $$ = &Set{Comments: Comments($2), Exprs: $4}
  }
| SET comment_opt set_scope_primary TRANSACTION transaction_chars
  {
    for i := 0; i < len($5); i++ {
      $5[i].Scope = $3
    }
    $$ = &Set{Comments: Comments($2), Exprs: $5}
  }

transaction_chars:
  transaction_char
  {
    $$ = SetVarExprs{$1}
  }
| transaction_chars ',' transaction_char
  {
    $$ = append($$, $3)
  }

transaction_char:
  ISOLATION LEVEL isolation_level
  {
    $$ = &SetVarExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte($3))}
  }
| READ WRITE
  {
    $$ = &SetVarExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte(TxReadWrite))}
  }
| READ ONLY
  {
    $$ = &SetVarExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte(TxReadOnly))}
  }

isolation_level:
  REPEATABLE READ
  {
    $$ = IsolationLevelRepeatableRead
  }
| READ COMMITTED
  {
    $$ = IsolationLevelReadCommitted
  }
| READ UNCOMMITTED
  {
    $$ = IsolationLevelReadUncommitted
  }
| SERIALIZABLE
  {
    $$ = IsolationLevelSerializable
  }

lexer_position:
  {
    $$ = yyPosition(yylex)
  }

lexer_old_position:
  {
    $$ = yyOldPosition(yylex)
  }

special_comment_mode:
  {
    $$ = yySpecialCommentMode(yylex)
  }

create_statement:
  create_table_prefix table_spec
  {
    $1.TableSpec = $2
    if len($1.TableSpec.Constraints) > 0 {
      $1.ConstraintAction = AddStr
    }
    $$ = $1
  }
  // TODO: Allow for table specs to be parsed here
| create_table_prefix AS create_query_expression
  {
    $1.OptSelect = &OptSelect{Select: $3}
    $$ = $1
  }
  // Currently, only unparenthesized SELECT expressions
  // are permitted for `CREATE AS` if `AS` is ommitted.
  // This is done to avoid ambiguity when parsing
  // > CREATE TABLE AS (...
| create_table_prefix create_query_select_expression
   {
     $1.OptSelect = &OptSelect{Select: $2}
     $$ = $1
   }
| create_table_prefix LIKE table_name
  {
    $1.OptLike = &OptLike{LikeTable: $3}
    $$ = $1
  }
| CREATE key_type_opt INDEX sql_id using_opt ON table_name '(' index_column_list ')' index_option_list_opt
  {
    // For consistency, we always return AlterTable for any ALTER TABLE-equivalent statements
    ddl := &DDL{Action: AlterStr, Table: $7, IndexSpec: &IndexSpec{Action: CreateStr, ToName: $4, Using: $5, Type: $2, Columns: $9, Options: $11}}
    $$ = &AlterTable{Table: $7, Statements: []*DDL{ddl}}
  }
| CREATE view_opts VIEW table_name ins_column_list_opt AS lexer_position special_comment_mode select_statement_with_no_trailing_into lexer_position opt_with_check_option
  {
    $2.ViewName = $4.ToViewName()
    $2.ViewExpr = $9
    $2.Columns = $5
    $2.CheckOption = $11
    $$ = &DDL{Action: CreateStr, ViewSpec: $2, SpecialCommentMode: $8, SubStatementPositionStart: $7, SubStatementPositionEnd: $10 - 1}
  }
| CREATE OR REPLACE view_opts VIEW table_name ins_column_list_opt AS lexer_position special_comment_mode select_statement_with_no_trailing_into lexer_position opt_with_check_option
  {
    $4.ViewName = $6.ToViewName()
    $4.ViewExpr = $11
    $4.Columns = $7
    $4.CheckOption = $13
    $$ = &DDL{Action: CreateStr, ViewSpec: $4,  SpecialCommentMode: $10, SubStatementPositionStart: $9, SubStatementPositionEnd: $12 - 1, OrReplace: true}
  }
| CREATE DATABASE not_exists_opt ID creation_option_opt
  {
    var ne bool
    if $3 != 0 {
      ne = true
    }
    $$ = &DBDDL{Action: CreateStr, SchemaOrDatabase: "database", DBName: string($4), IfNotExists: ne, CharsetCollate: $5}
  }
| CREATE SCHEMA not_exists_opt ID creation_option_opt
  {
    var ne bool
    if $3 != 0 {
      ne = true
    }
    $$ = &DBDDL{Action: CreateStr, SchemaOrDatabase: "schema", DBName: string($4), IfNotExists: ne, CharsetCollate: $5}
  }
| CREATE definer_opt TRIGGER trigger_name trigger_time trigger_event ON table_name FOR EACH ROW trigger_order_opt lexer_position special_comment_mode trigger_body lexer_position
  {
    $$ = &DDL{Action: CreateStr, Table: $8, TriggerSpec: &TriggerSpec{TrigName: $4, Definer: $2, Time: $5, Event: $6, Order: $12, Body: $15}, SpecialCommentMode: $14, SubStatementPositionStart: $13, SubStatementPositionEnd: $16 - 1}
  }
| CREATE definer_opt PROCEDURE procedure_name '(' proc_param_list_opt ')' characteristic_list_opt lexer_old_position special_comment_mode statement_list_statement lexer_position
  {
    $$ = &DDL{Action: CreateStr, ProcedureSpec: &ProcedureSpec{ProcName: $4, Definer: $2, Params: $6, Characteristics: $8, Body: $11}, SpecialCommentMode: $10, SubStatementPositionStart: $9, SubStatementPositionEnd: $12 - 1}
  }
| CREATE USER not_exists_opt account_with_auth_list default_role_opt tls_options account_limits pass_lock_options user_comment_attribute
  {
    var notExists bool
    if $3 != 0 {
      notExists = true
    }
    tlsOptions, err := NewTLSOptions($6)
    if err != nil {
      yylex.Error(err.Error())
      return 1
    }
    accountLimits, err := NewAccountLimits($7)
    if err != nil {
      yylex.Error(err.Error())
      return 1
    }
    passwordOptions, locked := NewPasswordOptionsWithLock($8)
    $$ = &CreateUser{IfNotExists: notExists, Users: $4, DefaultRoles: $5, TLSOptions: tlsOptions, AccountLimits: accountLimits, PasswordOptions: passwordOptions, Locked: locked, Attribute: $9}
  }
| CREATE ROLE not_exists_opt role_name_list
  {
    var notExists bool
    if $3 != 0 {
      notExists = true
    }
    $$ = &CreateRole{IfNotExists: notExists, Roles: $4}
  }
| CREATE definer_opt EVENT not_exists_opt event_name ON SCHEDULE event_schedule event_on_completion_preserve_opt event_status_opt comment_keyword_opt DO lexer_position statement_list_statement lexer_position
  {
    var notExists bool
    if $4 != 0 {
      notExists = true
    }
    $$ = &DDL{Action: CreateStr, EventSpec: &EventSpec{EventName: $5, Definer: $2, IfNotExists: notExists, OnSchedule: $8, OnCompletionPreserve: $9, Status: $10, Comment: $11, Body: $14}, SubStatementPositionStart: $13, SubStatementPositionEnd: $15 - 1}
  }
| create_spatial_ref_sys
  {
    $$ = $1
  }

create_spatial_ref_sys:
  CREATE SPATIAL REFERENCE SYSTEM INTEGRAL srs_attribute
  {
    $$ = &CreateSpatialRefSys{SRID: NewIntVal($5), SrsAttr: $6}
  }
| CREATE SPATIAL REFERENCE SYSTEM IF NOT EXISTS INTEGRAL srs_attribute
  {
    $$ = &CreateSpatialRefSys{SRID: NewIntVal($8), IfNotExists: true, SrsAttr: $9}
  }
| CREATE OR REPLACE SPATIAL REFERENCE SYSTEM INTEGRAL srs_attribute
  {
    $$ = &CreateSpatialRefSys{SRID: NewIntVal($7), OrReplace: true, SrsAttr: $8}
  }

srs_attribute:
  {
    $$ = &SrsAttribute{}
  }
| srs_attribute NAME STRING
  {
    if $1.Name != "" {
      yylex.Error("multiple definitions of attribute name")
      return 1
    }
    $1.Name = string($3)
    $$ = $1
  }
| srs_attribute DEFINITION STRING
  {
    if $1.Definition != "" {
      yylex.Error("multiple definitions of attribute definition")
      return 1
    }
    $1.Definition = string($3)
    $$ = $1
  }
| srs_attribute ORGANIZATION STRING IDENTIFIED BY INTEGRAL
  {
    if $1.Organization != "" {
      yylex.Error("multiple definitions of attribute organization")
      return 1
    }
    $1.Organization = string($3)
    $1.OrgID = NewIntVal($6)
    $$ = $1
  }
| srs_attribute DESCRIPTION STRING
  {
    if $1.Description != "" {
      yylex.Error("multiple definitions of attribute description")
      return 1
    }
    $1.Description = string($3)
    $$ = $1
  }

opt_with_check_option:
  /* EMPTY */
  {
    $$ = ViewCheckOptionUnspecified
  }
| WITH CHECK OPTION
  {
    $$ = ViewCheckOptionCascaded
  }
| WITH CASCADED CHECK OPTION
  {
    $$ = ViewCheckOptionCascaded
  }
| WITH LOCAL CHECK OPTION
  {
    $$ = ViewCheckOptionLocal
  }

default_role_opt:
  {
    $$ = nil
  }
| DEFAULT ROLE role_name_list
  {
    $$ = $3
  }

tls_options:
  {
    $$ = nil
  }
| REQUIRE NONE
  {
    $$ = nil
  }
| REQUIRE tls_option_item_list
  {
    $$ = $2
  }

tls_option_item_list:
  tls_option_item
  {
    $$ = []TLSOptionItem{$1}
  }
| tls_option_item_list AND tls_option_item
  {
    $$ = append($1, $3)
  }

tls_option_item:
  SSL
  {
    $$ = TLSOptionItem{TLSOptionItemType: TLSOptionItemType_SSL, ItemData: ""}
  }
| X509
  {
    $$ = TLSOptionItem{TLSOptionItemType: TLSOptionItemType_X509, ItemData: ""}
  }
| CIPHER STRING
  {
    $$ = TLSOptionItem{TLSOptionItemType: TLSOptionItemType_Cipher, ItemData: string($2)}
  }
| ISSUER STRING
  {
    $$ = TLSOptionItem{TLSOptionItemType: TLSOptionItemType_Issuer, ItemData: string($2)}
  }
| SUBJECT STRING
  {
    $$ = TLSOptionItem{TLSOptionItemType: TLSOptionItemType_Subject, ItemData: string($2)}
  }

account_limits:
  {
    $$ = nil
  }
| WITH account_limit_item_list
  {
    $$ = $2
  }

account_limit_item_list:
  account_limit_item
  {
    $$ = []AccountLimitItem{$1}
  }
| account_limit_item_list account_limit_item
  {
    $$ = append($1, $2)
  }

account_limit_item:
  MAX_QUERIES_PER_HOUR INTEGRAL
  {
    $$ = AccountLimitItem{AccountLimitItemType: AccountLimitItemType_Queries_PH, Count: NewIntVal($2)}
  }
| MAX_UPDATES_PER_HOUR INTEGRAL
  {
    $$ = AccountLimitItem{AccountLimitItemType: AccountLimitItemType_Updates_PH, Count: NewIntVal($2)}
  }
| MAX_CONNECTIONS_PER_HOUR INTEGRAL
  {
    $$ = AccountLimitItem{AccountLimitItemType: AccountLimitItemType_Connections_PH, Count: NewIntVal($2)}
  }
| MAX_USER_CONNECTIONS INTEGRAL
  {
    $$ = AccountLimitItem{AccountLimitItemType: AccountLimitItemType_Connections, Count: NewIntVal($2)}
  }

pass_lock_options:
  {
    $$ = nil
  }
| pass_lock_item_list
  {
    $$ = $1
  }

pass_lock_item_list:
  pass_lock_item
  {
    $$ = []PassLockItem{$1}
  }
| pass_lock_item_list pass_lock_item
  {
    $$ = append($1, $2)
  }

pass_lock_item:
  PASSWORD EXPIRE DEFAULT
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassExpireDefault, Value: nil}
  }
| PASSWORD EXPIRE NEVER
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassExpireNever, Value: nil}
  }
| PASSWORD EXPIRE INTERVAL INTEGRAL DAY
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassExpireInterval, Value: NewIntVal($4)}
  }
| PASSWORD HISTORY DEFAULT
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassHistory, Value: nil}
  }
| PASSWORD HISTORY INTEGRAL
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassHistory, Value: NewIntVal($3)}
  }
| PASSWORD REUSE INTERVAL DEFAULT
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassReuseInterval, Value: nil}
  }
| PASSWORD REUSE INTERVAL INTEGRAL DAY
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassReuseInterval, Value: NewIntVal($4)}
  }
| PASSWORD REQUIRE CURRENT DEFAULT
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassReqCurrentDefault, Value: nil}
  }
| PASSWORD REQUIRE CURRENT OPTIONAL
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassReqCurrentOptional, Value: nil}
  }
| FAILED_LOGIN_ATTEMPTS INTEGRAL
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassFailedLogins, Value: NewIntVal($2)}
  }
| PASSWORD_LOCK_TIME INTEGRAL
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassLockTime, Value: NewIntVal($2)}
  }
| PASSWORD_LOCK_TIME UNBOUNDED
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_PassLockTime, Value: nil}
  }
| ACCOUNT LOCK
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_AccountLock, Value: nil}
  }
| ACCOUNT UNLOCK
  {
    $$ = PassLockItem{PassLockItemType: PassLockItemType_AccountUnlock, Value: nil}
  }

user_comment_attribute:
  {
    $$ = ""
  }
| COMMENT_KEYWORD STRING
  {
    comment := string($2)
    $$ = `{"comment": "`+escapeDoubleQuotes(comment)+`"}`
  }
| ATTRIBUTE STRING
  {
    $$ = string($2)
  }

grant_statement:
  GRANT ALL ON grant_object_type grant_privilege_level TO account_name_list with_grant_opt grant_assumption
  {
    allPriv := []Privilege{Privilege{Type: PrivilegeType_All, Columns: nil}}
    $$ = &GrantPrivilege{Privileges: allPriv, ObjectType: $4, PrivilegeLevel: $5, To: $7, WithGrantOption: $8, As: $9}
  }
| GRANT ALL PRIVILEGES ON grant_object_type grant_privilege_level TO account_name_list with_grant_opt grant_assumption
    {
      allPriv := []Privilege{Privilege{Type: PrivilegeType_All, Columns: nil}}
      $$ = &GrantPrivilege{Privileges: allPriv, ObjectType: $5, PrivilegeLevel: $6, To: $8, WithGrantOption: $9, As: $10}
    }
| GRANT grant_privilege_list ON grant_object_type grant_privilege_level TO account_name_list with_grant_opt grant_assumption
  {
    $$ = &GrantPrivilege{Privileges: $2, ObjectType: $4, PrivilegeLevel: $5, To: $7, WithGrantOption: $8, As: $9}
  }
| GRANT role_name_list TO account_name_list with_admin_opt
  {
    $$ = &GrantRole{Roles: $2, To: $4, WithAdminOption: $5}
  }
| GRANT PROXY ON account_name TO account_name_list with_grant_opt
  {
    $$ = &GrantProxy{On: $4, To: $6, WithGrantOption: $7}
  }

revoke_statement:
  REVOKE ALL ON grant_object_type grant_privilege_level FROM account_name_list
  {
    allPriv := []Privilege{Privilege{Type: PrivilegeType_All, Columns: nil}}
    $$ = &RevokePrivilege{Privileges: allPriv, ObjectType: $4, PrivilegeLevel: $5, From: $7}
  }
| REVOKE grant_privilege_list ON grant_object_type grant_privilege_level FROM account_name_list
  {
    $$ = &RevokePrivilege{Privileges: $2, ObjectType: $4, PrivilegeLevel: $5, From: $7}
  }
| REVOKE ALL ',' GRANT OPTION FROM account_name_list
  {
    $$ = &RevokeAllPrivileges{From: $7}
  }
| REVOKE ALL PRIVILEGES ',' GRANT OPTION FROM account_name_list
  {
    $$ = &RevokeAllPrivileges{From: $8}
  }
| REVOKE role_name_list FROM account_name_list
  {
    $$ = &RevokeRole{Roles: $2, From: $4}
  }
| REVOKE PROXY ON account_name FROM account_name_list
  {
    $$ = &RevokeProxy{On: $4, From: $6}
  }

grant_privilege:
  ALTER grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Alter, Columns: $2}
  }
| ALTER ROUTINE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_AlterRoutine, Columns: $3}
  }
| CREATE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Create, Columns: $2}
  }
| CREATE ROLE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateRole, Columns: $3}
  }
| CREATE ROUTINE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateRoutine, Columns: $3}
  }
| CREATE TABLESPACE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateTablespace, Columns: $3}
  }
| CREATE TEMPORARY TABLES grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateTemporaryTables, Columns: $4}
  }
| CREATE USER grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateUser, Columns: $3}
  }
| CREATE VIEW grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_CreateView, Columns: $3}
  }
| DELETE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Delete, Columns: $2}
  }
| DROP grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Drop, Columns: $2}
  }
| DROP ROLE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_DropRole, Columns: $3}
  }
| EVENT grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Event, Columns: $2}
  }
| EXECUTE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Execute, Columns: $2}
  }
| FILE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_File, Columns: $2}
  }
| GRANT OPTION grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_GrantOption, Columns: $3}
  }
| INDEX grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Index, Columns: $2}
  }
| INSERT grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Insert, Columns: $2}
  }
| LOCK TABLES grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_LockTables, Columns: $3}
  }
| PROCESS grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Process, Columns: $2}
  }
| REFERENCES grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_References, Columns: $2}
  }
| RELOAD grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Reload, Columns: $2}
  }
| REPLICATION CLIENT grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_ReplicationClient, Columns: $3}
  }
| REPLICATION SLAVE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_ReplicationSlave, Columns: $3}
  }
| SELECT grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Select, Columns: $2}
  }
| SHOW DATABASES grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_ShowDatabases, Columns: $3}
  }
| SHOW VIEW grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_ShowView, Columns: $3}
  }
| SHUTDOWN grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Shutdown, Columns: $2}
  }
| SUPER grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Super, Columns: $2}
  }
| TRIGGER grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Trigger, Columns: $2}
  }
| UPDATE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Update, Columns: $2}
  }
| USAGE grant_privilege_columns_opt
  {
    $$ = Privilege{Type: PrivilegeType_Usage, Columns: $2}
  }
| dynamic_privilege
  {
    $$ = Privilege{Type: PrivilegeType_Dynamic, DynamicName: strings.ToLower(string($1))}
  }

grant_privilege_list:
  grant_privilege
  {
    $$ = []Privilege{$1}
  }
| grant_privilege_list ',' grant_privilege
  {
    $$ = append($1, $3)
  }

dynamic_privilege:
  APPLICATION_PASSWORD_ADMIN
| AUDIT_ABORT_EXEMPT
| AUDIT_ADMIN
| AUTHENTICATION_POLICY_ADMIN
| BACKUP_ADMIN
| BINLOG_ADMIN
| BINLOG_ENCRYPTION_ADMIN
| CLONE_ADMIN
| CONNECTION_ADMIN
| ENCRYPTION_KEY_ADMIN
| FIREWALL_ADMIN
| FIREWALL_EXEMPT
| FIREWALL_USER
| FLUSH_OPTIMIZER_COSTS
| FLUSH_STATUS
| FLUSH_TABLES
| FLUSH_USER_RESOURCES
| GROUP_REPLICATION_ADMIN
| GROUP_REPLICATION_STREAM
| INNODB_REDO_LOG_ARCHIVE
| INNODB_REDO_LOG_ENABLE
| NDB_STORED_USER
| PASSWORDLESS_USER_ADMIN
| PERSIST_RO_VARIABLES_ADMIN
| REPLICATION_APPLIER
| REPLICATION_SLAVE_ADMIN
| RESOURCE_GROUP_ADMIN
| RESOURCE_GROUP_USER
| ROLE_ADMIN
| SENSITIVE_VARIABLES_OBSERVER
| SESSION_VARIABLES_ADMIN
| SET_USER_ID
| SHOW_ROUTINE
| SKIP_QUERY_REWRITE
| SYSTEM_VARIABLES_ADMIN
| TABLE_ENCRYPTION_ADMIN
| TP_CONNECTION_ADMIN
| VERSION_TOKEN_ADMIN
| XA_RECOVER_ADMIN

grant_privilege_columns_opt:
  {
    $$ = nil
  }
| '(' grant_privilege_column_list ')'
  {
    $$ = $2
  }

grant_privilege_column_list:
  sql_id
  {
    $$ = []string{$1.String()}
  }
| grant_privilege_column_list ',' sql_id
  {
    $$ = append($1, $3.String())
  }

grant_object_type:
  {
    $$ = GrantObjectType_Any
  }
| TABLE
  {
    $$ = GrantObjectType_Table
  }
| FUNCTION
  {
    $$ = GrantObjectType_Function
  }
| PROCEDURE
  {
    $$ = GrantObjectType_Procedure
  }

grant_privilege_level:
  '*'
  {
    $$ = PrivilegeLevel{Database: "", TableRoutine: "*"}
  }
| '*' '.' '*'
  {
    $$ = PrivilegeLevel{Database: "*", TableRoutine: "*"}
  }
| sql_id
  {
    $$ = PrivilegeLevel{Database: "", TableRoutine: $1.String()}
  }
| sql_id '.' '*'
  {
    $$ = PrivilegeLevel{Database: $1.String(), TableRoutine: "*"}
  }
| sql_id '.' sql_id
  {
    $$ = PrivilegeLevel{Database: $1.String(), TableRoutine: $3.String()}
  }

grant_assumption:
  {
    $$ = nil
  }
| AS account_name
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_Default, User: $2, Roles: nil}
  }
| AS account_name WITH ROLE DEFAULT
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_Default, User: $2, Roles: nil}
  }
| AS account_name WITH ROLE NONE
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_None, User: $2, Roles: nil}
  }
| AS account_name WITH ROLE ALL
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_All, User: $2, Roles: nil}
  }
| AS account_name WITH ROLE ALL EXCEPT role_name_list
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_AllExcept, User: $2, Roles: $7}
  }
| AS account_name WITH ROLE role_name_list
  {
    $$ = &GrantUserAssumption{Type: GrantUserAssumptionType_Roles, User: $2, Roles: $5}
  }

with_grant_opt:
  {
    $$ = false
  }
| WITH GRANT OPTION
  {
    $$ = true
  }

with_admin_opt:
  {
    $$ = false
  }
| WITH ADMIN OPTION
  {
    $$ = true
  }

// TODO: Implement IGNORE, REPLACE, VALUES, and TABLE
create_query_select_expression:
  base_select_no_cte order_by_opt limit_opt lock_opt
  {
    if $1.GetInto() != nil {
      yylex.Error(fmt.Errorf("INTO clause is not allowed").Error())
      return 1
    }
    $1.SetOrderBy($2)
    $1.SetLimit($3)
    $1.SetLock($4)
    $$ = $1
  }

create_query_expression:
  select_or_set_op order_by_opt limit_opt lock_opt
  {
    if $1.GetInto() != nil {
      yylex.Error(fmt.Errorf("INTO clause is not allowed").Error())
      return 1
    }
    $1.SetOrderBy($2)
    $1.SetLimit($3)
    $1.SetLock($4)
    $$ = $1
  }

proc_param_list_opt:
  {
    $$ = nil
  }
| proc_param_list
  {
    $$ = $1
  }

proc_param_list:
  proc_param
  {
    $$ = []ProcedureParam{$1}
  }
| proc_param_list ',' proc_param
  {
    $$ = append($$, $3)
  }

proc_param:
  sql_id column_type
  {
    $$ = ProcedureParam{Direction: ProcedureParamDirection_In, Name: $1.String(), Type: $2}
  }
| IN sql_id column_type
  {
    $$ = ProcedureParam{Direction: ProcedureParamDirection_In, Name: $2.String(), Type: $3}
  }
| INOUT sql_id column_type
  {
    $$ = ProcedureParam{Direction: ProcedureParamDirection_Inout, Name: $2.String(), Type: $3}
  }
| OUT sql_id column_type
  {
    $$ = ProcedureParam{Direction: ProcedureParamDirection_Out, Name: $2.String(), Type: $3}
  }

characteristic_list_opt:
  {
    $$ = nil
  }
| characteristic_list
  {
    $$ = $1
  }

characteristic_list:
  characteristic
  {
    $$ = []Characteristic{$1}
  }
| characteristic_list characteristic
  {
    $$ = append($$, $2)
  }

characteristic:
  COMMENT_KEYWORD STRING
  {
    $$ = Characteristic{Type: CharacteristicValue_Comment, Comment: string($2)}
  }
| LANGUAGE SQL
  {
    $$ = Characteristic{Type: CharacteristicValue_LanguageSql}
  }
| NOT DETERMINISTIC
  {
    $$ = Characteristic{Type: CharacteristicValue_NotDeterministic}
  }
| DETERMINISTIC
  {
    $$ = Characteristic{Type: CharacteristicValue_Deterministic}
  }
| CONTAINS SQL
  {
    $$ = Characteristic{Type: CharacteristicValue_ContainsSql}
  }
| NO SQL
  {
    $$ = Characteristic{Type: CharacteristicValue_NoSql}
  }
| READS SQL DATA
  {
    $$ = Characteristic{Type: CharacteristicValue_ReadsSqlData}
  }
| MODIFIES SQL DATA
  {
    $$ = Characteristic{Type: CharacteristicValue_ModifiesSqlData}
  }
| SQL SECURITY DEFINER
  {
    $$ = Characteristic{Type: CharacteristicValue_SqlSecurityDefiner}
  }
| SQL SECURITY INVOKER
  {
    $$ = Characteristic{Type: CharacteristicValue_SqlSecurityInvoker}
  }

begin_end_block:
  BEGIN END
  {
    $$ = &BeginEndBlock{Label: ""}
  }
| ID ':' BEGIN END
  {
    $$ = &BeginEndBlock{Label: string($1)}
  }
| ID ':' BEGIN END ID
  {
    label := string($1)
      if label != string($5) {
        yylex.Error("End-label "+string($5)+" without match")
        return 1
      }
    $$ = &BeginEndBlock{Label: label}
  }
| BEGIN statement_list ';' END
  {
    $$ = &BeginEndBlock{Label: "", Statements: $2}
  }
| ID ':' BEGIN statement_list ';' END
  {
    $$ = &BeginEndBlock{Label: string($1), Statements: $4}
  }
| ID ':' BEGIN statement_list ';' END ID
  {
    label := string($1)
      if label != string($7) {
        yylex.Error("End-label "+string($7)+" without match")
        return 1
      }
    $$ = &BeginEndBlock{Label: label, Statements: $4}
  }

view_opts:
  definer_opt security_opt
  {
    $$ = &ViewSpec{Algorithm: "", Definer: $1, Security: $2}
  }
| algorithm_view_opt definer_opt security_opt
  {
    $$ = &ViewSpec{Algorithm: $1, Definer: $2, Security: $3}
  }

algorithm_view_opt:
  ALGORITHM '=' UNDEFINED
  {
    $$ = string($3)
  }
| ALGORITHM '=' MERGE
  {
    $$ = string($3)
  }
| ALGORITHM '=' TEMPTABLE
  {
    $$ = string($3)
  }

definer_opt:
  {
    $$ = ""
  }
| DEFINER '=' account_name
  {
    $$ = $3.String()
  }

security_opt:
  {
    $$ = ""
  }
| SQL SECURITY DEFINER
  {
    $$ = string($3)
  }
| SQL SECURITY INVOKER
  {
    $$ = string($3)
  }

account_name_str:
  STRING
  {
    $$ = string($1)
  }
| ID
  {
    $$ = string($1)
  }
| non_reserved_keyword
  {
    $$ = string($1)
  }

account_name:
  account_name_str '@' account_name_str
  {
    anyHost := false
    if $3 == "%" {
      anyHost = true
    }
    $$ = AccountName{Name: $1, Host: $3, AnyHost: anyHost}
  }
| account_name_str '@'
  {
    $$ = AccountName{Name: $1, Host: "", AnyHost: false}
  }
| account_name_str
  {
    $$ = AccountName{Name: $1, Host: "", AnyHost: true}
  }

account_name_list:
  account_name
  {
    $$ = []AccountName{$1}
  }
| account_name_list ',' account_name
  {
    $$ = append($1, $3)
  }

role_name:
  account_name_str '@' account_name_str
  {
    if len($1) == 0 {
      yylex.Error("the anonymous user is not a valid role name")
      return 1
    }
    $$ = AccountName{Name: $1, Host: $3, AnyHost: false}
  }
| account_name_str '@'
  {
    if len($1) == 0 {
      yylex.Error("the anonymous user is not a valid role name")
      return 1
    }
    $$ = AccountName{Name: $1, Host: "", AnyHost: false}
  }
| account_name_str
  {
    if len($1) == 0 {
      yylex.Error("the anonymous user is not a valid role name")
      return 1
    }
    $$ = AccountName{Name: $1, Host: "", AnyHost: true}
  }

role_name_list:
  role_name
  {
    $$ = []AccountName{$1}
  }
| role_name_list ',' role_name
  {
    $$ = append($1, $3)
  }

account_with_auth:
  account_name
  {
    $$ = AccountWithAuth{AccountName: $1}
  }
| account_name authentication
  {
    $$ = AccountWithAuth{AccountName: $1, Auth1: $2}
  }
| account_name authentication INITIAL AUTHENTICATION authentication_initial
  {
    $$ = AccountWithAuth{AccountName: $1, Auth1: $2, AuthInitial: $5}
  }
| account_name authentication AND authentication
  {
    $$ = AccountWithAuth{AccountName: $1, Auth1: $2, Auth2: $4}
  }
| account_name authentication AND authentication AND authentication
  {
    $$ = AccountWithAuth{AccountName: $1, Auth1: $2, Auth2: $4, Auth3: $6}
  }

authentication:
  IDENTIFIED BY RANDOM PASSWORD
  {
    $$ = &Authentication{RandomPassword: true}
  }
| IDENTIFIED BY STRING
  {
    $$ = &Authentication{Password: string($3)}
  }
| IDENTIFIED WITH ID
  {
    $$ = &Authentication{Plugin: string($3)}
  }
| IDENTIFIED WITH ID BY RANDOM PASSWORD
  {
    $$ = &Authentication{Plugin: string($3), RandomPassword: true}
  }
| IDENTIFIED WITH ID BY STRING
  {
    $$ = &Authentication{Plugin: string($3), Password: string($5)}
  }
| IDENTIFIED WITH ID AS STRING
  {
    $$ = &Authentication{Plugin: string($3), Identity: string($5)}
  }
| IDENTIFIED WITH STRING AS STRING
  {
    $$ = &Authentication{Plugin: string($3), Identity: string($5)}
  }

authentication_initial:
  IDENTIFIED BY RANDOM PASSWORD
  {
    $$ = &Authentication{RandomPassword: true}
  }
| IDENTIFIED BY STRING
  {
    $$ = &Authentication{Password: string($3)}
  }
| IDENTIFIED WITH ID AS STRING
  {
    $$ = &Authentication{Plugin: string($3), Identity: string($5)}
  }

account_with_auth_list:
  account_with_auth
  {
    $$ = []AccountWithAuth{$1}
  }
| account_with_auth_list ',' account_with_auth
  {
    $$ = append($1, $3)
  }

event_name:
  sql_id
  {
    $$ = EventName{Name: $1}
  }
| table_id '.' sql_id
  {
    $$ = EventName{Qualifier: $1, Name: $3}
  }

event_schedule:
  AT timestamp_value event_schedule_intervals_opt
  {
    $$ = &EventScheduleSpec{At: &EventScheduleTimeSpec{EventTimestamp: $2, EventIntervals: $3}}
  }
| EVERY value non_microsecond_time_unit event_starts_opt event_ends_opt
  {
    $$ = &EventScheduleSpec{EveryInterval: IntervalExpr{Expr: $2, Unit: string($3)}, Starts: $4, Ends: $5}
  }

event_schedule_intervals_opt:
  {
    $$ = []IntervalExpr{}
  }
| event_schedule_intervals_opt '+' INTERVAL value non_microsecond_time_unit
  {
    $$ = append($1, IntervalExpr{Expr: $4, Unit: string($5)})
  }

event_starts_opt:
  {
    $$ = nil
  }
| STARTS timestamp_value event_schedule_intervals_opt
  {
    $$ = &EventScheduleTimeSpec{EventTimestamp: $2, EventIntervals: $3}
  }

event_ends_opt:
  {
    $$ = nil
  }
| ENDS timestamp_value event_schedule_intervals_opt
  {
    $$ = &EventScheduleTimeSpec{EventTimestamp: $2, EventIntervals: $3}
  }

event_on_completion_preserve_opt:
  {
    $$ = EventOnCompletion_Undefined
  }
| ON COMPLETION PRESERVE
  {
    $$ = EventOnCompletion_Preserve
  }
| ON COMPLETION NOT PRESERVE
  {
    $$ = EventOnCompletion_NotPreserve
  }

event_status_opt:
  {
    $$ = EventStatus_Undefined
  }
| ENABLE
  {
    $$ = EventStatus_Enable
  }
| DISABLE
  {
    $$ = EventStatus_Disable
  }
| DISABLE ON SLAVE
  {
    $$ = EventStatus_DisableOnSlave
  }

comment_keyword_opt:
  {
    $$ = nil
  }
| COMMENT_KEYWORD STRING
  {
    $$ = NewStrVal($2)
  }

timestamp_value:
  value
  {
    $$ = $1
  }
| function_call_nonkeyword
  {
    $$ = $1
  }

trigger_time:
  BEFORE
  {
    $$ = BeforeStr
  }
| AFTER
  {
    $$ = AfterStr
  }

trigger_event:
  INSERT
  {
    $$ = InsertStr
  }
| UPDATE
  {
    $$ = UpdateStr
  }
| DELETE
  {
    $$ = DeleteStr
  }

trigger_order_opt:
  {
    $$ = nil
  }
| FOLLOWS ID
  {
    $$ = &TriggerOrder{PrecedesOrFollows: FollowsStr, OtherTriggerName: string($2)}
  }
| PRECEDES ID
  {
    $$ = &TriggerOrder{PrecedesOrFollows: PrecedesStr, OtherTriggerName: string($2)}
  }

trigger_body:
  trigger_begin_end_block
  {
    $$ = $1
  }
| set_statement
| call_statement
| insert_statement
| update_statement
| delete_statement

trigger_begin_end_block:
  BEGIN statement_list ';' END
  {
    $$ = &BeginEndBlock{Statements: $2}
  }
| BEGIN END
  {
    $$ = &BeginEndBlock{}
  }

case_statement:
  CASE expression case_statement_case_list END CASE
  {
    $$ = &CaseStatement{Expr: $2, Cases: $3}
  }
| CASE expression case_statement_case_list ELSE statement_list ';' END CASE
  {
    $$ = &CaseStatement{Expr: $2, Cases: $3, Else: $5}
  }
| CASE case_statement_case_list END CASE
  {
    $$ = &CaseStatement{Expr: nil, Cases: $2}
  }
| CASE case_statement_case_list ELSE statement_list ';' END CASE
  {
    $$ = &CaseStatement{Expr: nil, Cases: $2, Else: $4}
  }

case_statement_case_list:
  case_statement_case
  {
    $$ = []CaseStatementCase{$1}
  }
| case_statement_case_list case_statement_case
  {
    $$ = append($$, $2)
  }

case_statement_case:
  WHEN expression THEN statement_list ';'
  {
    $$ = CaseStatementCase{Case: $2, Statements: $4}
  }

if_statement:
  IF expression THEN statement_list ';' END IF
  {
    conds := []IfStatementCondition{IfStatementCondition{Expr: $2, Statements: $4}}
    $$ = &IfStatement{Conditions: conds}
  }
| IF expression THEN statement_list ';' ELSE statement_list ';' END IF
  {
    conds := []IfStatementCondition{IfStatementCondition{Expr: $2, Statements: $4}}
    $$ = &IfStatement{Conditions: conds, Else: $7}
  }
| IF expression THEN statement_list ';' elseif_list END IF
  {
    conds := $6
    conds = append([]IfStatementCondition{IfStatementCondition{Expr: $2, Statements: $4}}, conds...)
    $$ = &IfStatement{Conditions: conds}
  }
| IF expression THEN statement_list ';' elseif_list ELSE statement_list ';' END IF
  {
    conds := $6
    conds = append([]IfStatementCondition{IfStatementCondition{Expr: $2, Statements: $4}}, conds...)
    $$ = &IfStatement{Conditions: conds, Else: $8}
  }

elseif_list:
  elseif_list_item
  {
    $$ = []IfStatementCondition{$1}
  }
| elseif_list elseif_list_item
  {
    $$ = append($$, $2)
  }

elseif_list_item:
  ELSEIF expression THEN statement_list ';'
  {
    $$ = IfStatementCondition{Expr: $2, Statements: $4}
  }

declare_statement:
  DECLARE ID CONDITION FOR signal_condition_value
  {
    $$ = &Declare{Condition: &DeclareCondition{Name: string($2), SqlStateValue: string($5)}}
  }
| DECLARE ID CONDITION FOR INTEGRAL
  {
    $$ = &Declare{Condition: &DeclareCondition{Name: string($2), MysqlErrorCode: NewIntVal($5)}}
  }
| DECLARE ID CURSOR FOR select_statement_with_no_trailing_into
  {
    $$ = &Declare{Cursor: &DeclareCursor{Name: string($2), SelectStmt: $5}}
  }
| DECLARE declare_handler_action HANDLER FOR declare_handler_condition_list statement_list_statement
  {
    $$ = &Declare{Handler: &DeclareHandler{Action: $2, ConditionValues: $5, Statement: $6}}
  }
| DECLARE reserved_sql_id_list column_type charset_opt collate_opt
  {
    $3.Charset = $4
    $3.Collate = $5
    $$ = &Declare{Variables: &DeclareVariables{Names: $2, VarType: $3}}
  }
| DECLARE reserved_sql_id_list column_type charset_opt collate_opt DEFAULT value_expression
  {
    $3.Charset = $4
    $3.Collate = $5
    $3.Default = $7
    $$ = &Declare{Variables: &DeclareVariables{Names: $2, VarType: $3}}
  }

declare_handler_action:
  CONTINUE
  {
    $$ = DeclareHandlerAction_Continue
  }
| EXIT
  {
    $$ = DeclareHandlerAction_Exit
  }
| UNDO
  {
    $$ = DeclareHandlerAction_Undo
  }

declare_handler_condition_list:
  declare_handler_condition
  {
    $$ = []DeclareHandlerCondition{$1}
  }
| declare_handler_condition_list ',' declare_handler_condition
  {
    $$ = append($$, $3)
  }

declare_handler_condition:
  INTEGRAL
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_MysqlErrorCode, MysqlErrorCode: NewIntVal($1)}
  }
| signal_condition_value
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_SqlState, String: string($1)}
  }
| SQLWARNING
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_SqlWarning}
  }
| NOT FOUND
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_NotFound}
  }
| SQLEXCEPTION
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_SqlException}
  }
| ID
  {
    $$ = DeclareHandlerCondition{ValueType: DeclareHandlerCondition_ConditionName, String: string($1)}
  }

open_statement:
  OPEN ID
  {
    $$ = &OpenCursor{Name: string($2)}
  }

close_statement:
  CLOSE ID
  {
    $$ = &CloseCursor{Name: string($2)}
  }

fetch_statement:
  FETCH fetch_next_from_opt ID INTO fetch_variable_list
  {
    $$ = &FetchCursor{Name: string($3), Variables: $5}
  }

fetch_next_from_opt:
  {}
| FROM
| NEXT FROM
  {}

fetch_variable_list:
  ID
  {
    $$ = []string{string($1)}
  }
| fetch_variable_list ',' ID
  {
    $$ = append($$, string($3))
  }

loop_statement:
  LOOP statement_list ';' END LOOP
  {
    $$ = &Loop{Label: "", Statements: $2}
  }
| ID ':' LOOP statement_list ';' END LOOP
  {
    $$ = &Loop{Label: string($1), Statements: $4}
  }
| ID ':' LOOP statement_list ';' END LOOP ID
  {
    label := string($1)
    if label != string($8) {
      yylex.Error("End-label "+string($8)+" without match")
      return 1
    }
    $$ = &Loop{Label: label, Statements: $4}
  }

repeat_statement:
  REPEAT statement_list ';' UNTIL expression END REPEAT
  {
    $$ = &Repeat{Label: "", Statements: $2, Condition: $5}
  }
| ID ':' REPEAT statement_list ';' UNTIL expression END REPEAT
  {
    $$ = &Repeat{Label: string($1), Statements: $4, Condition: $7}
  }
| ID ':' REPEAT statement_list ';' UNTIL expression END REPEAT ID
  {
    label := string($1)
    if label != string($10) {
      yylex.Error("End-label "+string($10)+" without match")
      return 1
    }
    $$ = &Repeat{Label: label, Statements: $4, Condition: $7}
  }

while_statement:
  WHILE expression DO statement_list ';' END WHILE
  {
    $$ = &While{Label: "", Condition: $2, Statements: $4}
  }
| ID ':' WHILE expression DO statement_list ';' END WHILE
  {
    $$ = &While{Label: string($1), Condition: $4, Statements: $6}
  }
| ID ':' WHILE expression DO statement_list ';' END WHILE ID
  {
    label := string($1)
    if label != string($10) {
      yylex.Error("End-label "+string($10)+" without match")
      return 1
    }
    $$ = &While{Label: label, Condition: $4, Statements: $6}
  }

leave_statement:
  LEAVE ID
  {
    $$ = &Leave{Label: string($2)}
  }

iterate_statement:
  ITERATE ID
  {
    $$ = &Iterate{Label: string($2)}
  }

return_statement:
  RETURN value_expression
  {
    $$ = &Return{Expr: $2}
  }

signal_statement:
  SIGNAL signal_condition_value
  {
    $$ = &Signal{SqlStateValue: string($2)}
  }
| SIGNAL signal_condition_value SET signal_information_item_list
  {
    $$ = &Signal{SqlStateValue: string($2), Info: $4}
  }
| SIGNAL ID
  {
    $$ = &Signal{ConditionName: string($2)}
  }
| SIGNAL ID SET signal_information_item_list
  {
    $$ = &Signal{ConditionName: string($2), Info: $4}
  }

signal_condition_value:
  SQLSTATE STRING
  {
    $$ = $2
  }
| SQLSTATE VALUE STRING
  {
    $$ = $3
  }

signal_information_item_list:
  signal_information_item
  {
    $$ = []SignalInfo{$1}
  }
| signal_information_item_list ',' signal_information_item
  {
    $$ = append($$, $3)
  }

signal_information_item:
  signal_information_name '=' value
  {
    $$ = SignalInfo{ConditionItemName: $1, Value: $3.(*SQLVal)}
  }
| signal_information_name '=' sql_id
  {
    $$ = SignalInfo{ConditionItemName: $1, Value: &ColName{Name: $3}}
  }

signal_information_name:
  CLASS_ORIGIN
  {
    $$ = SignalConditionItemName_ClassOrigin
  }
| SUBCLASS_ORIGIN
  {
    $$ = SignalConditionItemName_SubclassOrigin
  }
| MESSAGE_TEXT
  {
    $$ = SignalConditionItemName_MessageText
  }
| MYSQL_ERRNO
  {
    $$ = SignalConditionItemName_MysqlErrno
  }
| CONSTRAINT_CATALOG
  {
    $$ = SignalConditionItemName_ConstraintCatalog
  }
| CONSTRAINT_SCHEMA
  {
    $$ = SignalConditionItemName_ConstraintSchema
  }
| CONSTRAINT_NAME
  {
    $$ = SignalConditionItemName_ConstraintName
  }
| CATALOG_NAME
  {
    $$ = SignalConditionItemName_CatalogName
  }
| SCHEMA_NAME
  {
    $$ = SignalConditionItemName_SchemaName
  }
| TABLE_NAME
  {
    $$ = SignalConditionItemName_TableName
  }
| COLUMN_NAME
  {
    $$ = SignalConditionItemName_ColumnName
  }
| CURSOR_NAME
  {
    $$ = SignalConditionItemName_CursorName
  }

resignal_statement:
  RESIGNAL
  {
    $$ = &Resignal{}
  }
| RESIGNAL signal_condition_value
  {
    $$ = &Resignal{Signal{SqlStateValue: string($2)}}
  }
| RESIGNAL signal_condition_value SET signal_information_item_list
  {
    $$ = &Resignal{Signal{SqlStateValue: string($2), Info: $4}}
  }
| RESIGNAL SET signal_information_item_list
  {
    $$ = &Resignal{Signal{Info: $3}}
  }
| RESIGNAL ID
  {
    $$ = &Resignal{Signal{ConditionName: string($2)}}
  }
| RESIGNAL ID SET signal_information_item_list
  {
    $$ = &Resignal{Signal{ConditionName: string($2), Info: $4}}
  }

call_statement:
  CALL procedure_name call_param_list_opt as_of_opt
  {
    $$ = &Call{ProcName: $2, Params: $3, AsOf: $4}
  }

call_param_list_opt:
  {
    $$ = nil
  }
| '(' ')'
  {
    $$ = nil
  }
| '(' expression_list ')'
  {
    $$ = $2
  }

statement_list:
  statement_list_statement
  {
    $$ = Statements{$1}
  }
| statement_list ';' statement_list_statement
  {
    $$ = append($$, $3)
  }

statement_list_statement:
  select_statement
  {
    $$ = $1
  }
| insert_statement
| update_statement
| delete_statement
| set_statement
| create_statement
| alter_statement
| rename_statement
| drop_statement
| case_statement
| if_statement
| truncate_statement
| analyze_statement
| prepare_statement
| execute_statement
| deallocate_statement
| show_statement
| start_transaction_statement
| commit_statement
| rollback_statement
| explain_statement
| describe_statement
| declare_statement
| open_statement
| close_statement
| loop_statement
| repeat_statement
| while_statement
| leave_statement
| iterate_statement
| fetch_statement
| signal_statement
| resignal_statement
| return_statement
| call_statement
| savepoint_statement
| rollback_savepoint_statement
| release_savepoint_statement
| grant_statement
| revoke_statement
| begin_end_block
| flush_statement

create_table_prefix:
  CREATE temp_opt TABLE not_exists_opt table_name
  {
    var ne bool
    if $4 != 0 {
      ne = true
    }

    var neTemp bool
    if $2 != 0 {
      neTemp = true
    }

    $$ = &DDL{Action: CreateStr, Table: $5, IfNotExists: ne, Temporary: neTemp}
  }
| CREATE temp_opt TABLE not_exists_opt FORMAT
  {
    var ne bool
    if $4 != 0 {
      ne = true
    }

    var neTemp bool
    if $2 != 0 {
      neTemp = true
    }

    $$ = &DDL{Action: CreateStr, Table: TableName{Name: NewTableIdent(string($5))}, IfNotExists: ne, Temporary: neTemp}
  }

table_spec:
  '(' table_column_list ')' table_option_list partition_option_opt
  {
    $$ = $2
    for _, opt := range $4 {
      $$.AddTableOption(opt)
    }
    $$.PartitionOpt = $5
  }

table_column_list:
  column_definition_for_create
  {
    $$ = &TableSpec{}
    $$.AddColumn($1)
  }
| check_constraint_definition
  {
    $$ = &TableSpec{}
    $$.AddConstraint($1)
  }
| table_column_list ',' column_definition_for_create
  {
    $$.AddColumn($3)
  }
| table_column_list ',' index_definition
  {
    $$.AddIndex($3)
  }
| table_column_list ',' foreign_key_definition
  {
    $$.AddConstraint($3)
  }
| table_column_list ',' check_constraint_definition
  {
    $$.AddConstraint($3)
  }

column_definition:
  ID column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }
| all_non_reserved column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }

column_definition_for_create:
  sql_id column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: $1, Type: $2}
  }
| column_name_safe_keyword column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }
| non_reserved_keyword2 column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }
| non_reserved_keyword3 column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }
| ESCAPE column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }
| function_call_keywords column_type column_type_options
  {
    if err := $2.merge($3); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }

stored_opt:
  {
    $$ = BoolVal(false)
  }
| VIRTUAL
  {
    $$ = BoolVal(false)
  }
| STORED
  {
    $$ = BoolVal(true)
  }

column_type_options:
  {
    $$ = ColumnType{}
  }
| column_type_options INVISIBLE
  {
    $$ = $1
  }
| column_type_options NULL
  {
    opt := ColumnType{Null: BoolVal(true), NotNull: BoolVal(false), sawnull: true}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options NOT NULL
  {
    opt := ColumnType{Null: BoolVal(false), NotNull: BoolVal(true), sawnull: true}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options character_set
  {
    opt := ColumnType{Charset: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options collate
  {
    opt := ColumnType{Collate: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options BINARY
  {
    opt := ColumnType{BinaryCollate: true}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options column_default
  {
    opt := ColumnType{Default: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options on_update
  {
    opt := ColumnType{OnUpdate: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options auto_increment
  {
    opt := ColumnType{Autoincrement: $2, sawai: true}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options column_key
  {
    opt := ColumnType{KeyOpt: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options column_comment
  {
    opt := ColumnType{Comment: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options AS openb value_expression closeb stored_opt
  {
    opt := ColumnType{GeneratedExpr: &ParenExpr{$4}, Stored: $6}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options GENERATED ALWAYS AS openb value_expression closeb stored_opt
  {
    opt := ColumnType{GeneratedExpr: &ParenExpr{$6}, Stored: $8}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options SRID INTEGRAL
  {
    opt := ColumnType{SRID: NewIntVal($3)}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options REFERENCES table_name '(' column_list ')'
  // TODO: This still needs support for "ON DELETE" and "ON UPDATE"
  {
    opt := ColumnType{ForeignKeyDef: &ForeignKeyDefinition{ReferencedTable: $3, ReferencedColumns: $5}}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }
| column_type_options check_constraint_definition
  {
    opt := ColumnType{Constraint: $2}
    if err := $1.merge(opt); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = $1
  }

column_type:
  numeric_type signed_or_unsigned_opt zero_fill_opt
  {
    $$ = $1
    $$.Unsigned = $2
    $$.Zerofill = $3
  }
| char_type
| time_type
| spatial_type

numeric_type:
  int_type length_opt
  {
    $$ = $1
    $$.Length = $2
  }
| decimal_type
  {
    $$ = $1
  }

int_type:
  BIT
  {
    $$ = ColumnType{Type: string($1)}
  }
| BOOL
  {
    $$ = ColumnType{Type: string($1)}
  }
| BOOLEAN
  {
    $$ = ColumnType{Type: string($1)}
  }
| TINYINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| SMALLINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| MEDIUMINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| INT
  {
    $$ = ColumnType{Type: string($1)}
  }
| INTEGER
  {
    $$ = ColumnType{Type: string($1)}
  }
| BIGINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| SERIAL
  {
    $$ = ColumnType{Type: "bigint", Unsigned: true, NotNull: true, Autoincrement: true, KeyOpt: colKeyUnique}
  }
| INT1
  {
    $$ = ColumnType{Type: "tinyint"}
  }
| INT2
  {
    $$ = ColumnType{Type: "smallint"}
  }
| INT3
  {
    $$ = ColumnType{Type: "mediumint"}
  }
| INT4
  {
    $$ = ColumnType{Type: "int"}
  }
| INT8
  {
    $$ = ColumnType{Type: "bigint"}
  }

decimal_type:
REAL float_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DOUBLE float_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DOUBLE PRECISION float_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2)}
    $$.Length = $3.Length
    $$.Scale = $3.Scale
  }
| FLOAT_TYPE decimal_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DECIMAL decimal_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| NUMERIC decimal_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DEC decimal_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| FIXED decimal_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }

time_type:
  DATE
  {
    $$ = ColumnType{Type: string($1)}
  }
| TIME length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| TIMESTAMP length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| DATETIME length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| YEAR
  {
    $$ = ColumnType{Type: string($1)}
  }

char_type:
  char_or_character char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| NATIONAL char_or_character char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| NCHAR char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| NCHAR VARCHAR char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| NCHAR VARYING char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| VARCHAR char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| CHAR VARYING char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| CHARACTER VARYING char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| NVARCHAR char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| NATIONAL VARCHAR char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2), Length: $3}
  }
| NATIONAL char_or_character VARYING char_length_opt
  {
    $$ = ColumnType{Type: string($1) + " " + string($2) + " " + string($3), Length: $4}
  }
| BINARY char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| VARBINARY char_length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| TEXT
  {
    $$ = ColumnType{Type: string($1)}
  }
| TINYTEXT
  {
    $$ = ColumnType{Type: string($1)}
  }
| MEDIUMTEXT
  {
    $$ = ColumnType{Type: string($1)}
  }
| LONGTEXT
  {
    $$ = ColumnType{Type: string($1)}
  }
| LONG
  {
    $$ = ColumnType{Type: string($1)}
  }
| LONG VARCHAR
  {
    $$ = ColumnType{Type: string($1) + " " + string($2)}
  }
| BLOB
  {
    $$ = ColumnType{Type: string($1)}
  }
| TINYBLOB
  {
    $$ = ColumnType{Type: string($1)}
  }
| MEDIUMBLOB
  {
    $$ = ColumnType{Type: string($1)}
  }
| LONGBLOB
  {
    $$ = ColumnType{Type: string($1)}
  }
| JSON
  {
    $$ = ColumnType{Type: string($1)}
  }
| ENUM '(' enum_values ')'
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3}
  }
// need set_values / SetValues ?
| SET '(' enum_values ')'
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3}
  }

spatial_type:
  GEOMETRY
  {
    $$ = ColumnType{Type: string($1)}
  }
| POINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| LINESTRING
  {
    $$ = ColumnType{Type: string($1)}
  }
| POLYGON
  {
    $$ = ColumnType{Type: string($1)}
  }
| GEOMETRYCOLLECTION
  {
    $$ = ColumnType{Type: string($1)}
  }
| MULTIPOINT
  {
    $$ = ColumnType{Type: string($1)}
  }
| MULTILINESTRING
  {
    $$ = ColumnType{Type: string($1)}
  }
| MULTIPOLYGON
  {
    $$ = ColumnType{Type: string($1)}
  }

enum_values:
  STRING
  {
    $$ = make([]string, 0, 4)
    $$ = append($$, string($1))
  }
| enum_values ',' STRING
  {
    $$ = append($1, string($3))
  }

length_opt:
  {
    $$ = nil
  }
| '(' INTEGRAL ')'
  {
    $$ = NewIntVal($2)
  }

char_length_opt:
  {
    $$ = nil
  }
| '(' INTEGRAL ')'
  {
    $$ = NewIntVal($2)
  }
| '(' MAX ')'
  {
    $$ = NewValArg($2)
  }

float_length_opt:
  {
    $$ = LengthScaleOption{}
  }
| '(' INTEGRAL ',' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntVal($2),
        Scale: NewIntVal($4),
    }
  }

decimal_length_opt:
  {
    $$ = LengthScaleOption{}
  }
| '(' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntVal($2),
    }
  }
| '(' INTEGRAL ',' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntVal($2),
        Scale: NewIntVal($4),
    }
  }

signed_or_unsigned_opt:
  {
    $$ = BoolVal(false)
  }
| SIGNED
  {
   $$ = BoolVal(false)
  }
| UNSIGNED
  {
    $$ = BoolVal(true)
  }

zero_fill_opt:
  {
    $$ = BoolVal(false)
  }
| ZEROFILL
  {
    $$ = BoolVal(true)
  }

column_default:
  DEFAULT value
  {
    $$ = $2
  }
| DEFAULT '-' value
  {
    if num, ok := $3.(*SQLVal); ok && num.Type == IntVal {
      // Handle double negative
      if num.Val[0] == '-' {
        num.Val = num.Val[1:]
        $$ = num
      } else {
        $$ = NewIntVal(append([]byte("-"), num.Val...))
      }
    } else {
      $$ = &UnaryExpr{Operator: UMinusStr, Expr: $3}
    }
  }
| DEFAULT underscore_charsets STRING
  {
    $$ = &UnaryExpr{Operator: $2, Expr: NewStrVal($3)}
  }
| DEFAULT boolean_value
  {
    $$ = $2
  }
| DEFAULT function_call_nonkeyword
  {
    $$ = $2
  }
| DEFAULT openb value_expression closeb
  {
    $$ = &ParenExpr{$3}
  }

on_update:
  ON UPDATE function_call_on_update
  {
    $$ = $3
  }

auto_increment:
  AUTO_INCREMENT
  {
    $$ = BoolVal(true)
  }

charset_opt:
  {
    $$ = ""
  }
| character_set
  {
    $$ = $1
  }

character_set:
  CHARACTER SET ID
  {
    $$ = string($3)
  }
| CHARACTER SET BINARY
  {
    $$ = string($3)
  }
| CHARSET ID
  {
    $$ = string($2)
  }
| CHARSET BINARY
  {
    $$ = string($2)
  }


collate_opt:
  {
    $$ = ""
  }
| collate
  {
    $$ = $1
  }

collate:
  COLLATE ID
  {
    $$ = string($2)
  }
| COLLATE STRING
  {
    $$ = string($2)
  }
| COLLATE BINARY
  {
    $$ = string($2)
  }

default_keyword_opt:
  {
    $$ = false
  }
| DEFAULT
  {
    $$ = true
  }

creation_option_opt:
  {
    $$ = nil
  }
| creation_option
  {
    $$ = $1
  }

creation_option:
  charset_default_opt
  {
    $$ = []*CharsetAndCollate{$1}
  }
| collate_default_opt
  {
    $$ = []*CharsetAndCollate{$1}
  }
| encryption_default_opt
  {
    $$ = []*CharsetAndCollate{$1}
  }
| creation_option collate_default_opt
  {
    $$ = append($1,$2)
  }
| creation_option charset_default_opt
  {
    $$ = append($1,$2)
  }
| creation_option encryption_default_opt
  {
    $$ = append($1,$2)
  }

charset_default_opt:
  default_keyword_opt CHARACTER SET equal_opt ID
  {
    $$ = &CharsetAndCollate{Type: string($2) + " " + string($3), Value: string($5), IsDefault: $1}
  }
| default_keyword_opt CHARACTER SET equal_opt STRING
  {
    $$ = &CharsetAndCollate{Type: string($2) + " " + string($3), Value: string($5), IsDefault: $1}
  }
| default_keyword_opt CHARACTER SET equal_opt BINARY
  {
    $$ = &CharsetAndCollate{Type: string($2) + " " + string($3), Value: string($5), IsDefault: $1}
  }
| default_keyword_opt CHARSET equal_opt ID
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }
| default_keyword_opt CHARSET equal_opt STRING
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }
| default_keyword_opt CHARSET equal_opt BINARY
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }

collate_default_opt:
  default_keyword_opt COLLATE equal_opt ID
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }
| default_keyword_opt COLLATE equal_opt STRING
    {
      $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
    }
| default_keyword_opt COLLATE equal_opt BINARY
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }

encryption_default_opt:
  default_keyword_opt ENCRYPTION equal_opt STRING
  {
    $$ = &CharsetAndCollate{Type: string($2), Value: string($4), IsDefault: $1}
  }

column_key:
  PRIMARY KEY
  {
    $$ = colKeyPrimary
  }
| KEY
  {
    $$ = colKey
  }
| UNIQUE KEY
  {
    $$ = colKeyUniqueKey
  }
| UNIQUE
  {
    $$ = colKeyUnique
  }
| FULLTEXT KEY
  {
    $$ = colKeyFulltextKey
  }

column_comment:
  COMMENT_KEYWORD STRING
  {
    $$ = NewStrVal($2)
  }

flush_statement:
  FLUSH flush_type_opt flush_option
  {
    $$ = &Flush{Type: $2, Option: $3}
  }

flush_option:
  BINARY LOGS
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2)}
  }
| ENGINE LOGS
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2)}
  }
| ERROR LOGS
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2)}
  }
| GENERAL LOGS
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2)}
  }
| HOSTS
  {
    $$ = &FlushOption{Name: string($1)}
  }
| LOGS
  {
    $$ = &FlushOption{Name: string($1)}
  }
| PRIVILEGES
  {
    $$ = &FlushOption{Name: string($1)}
  }
| OPTIMIZER_COSTS
  {
    $$ = &FlushOption{Name: string($1)}
  }
| RELAY LOGS relay_logs_attribute
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2), Channel: $3}
  }
| SLOW LOGS
  {
    $$ = &FlushOption{Name: string($1) + " " +  string($2)}
  }
| STATUS
  {
    $$ = &FlushOption{Name: string($1)}
  }
| USER_RESOURCES
  {
    $$ = &FlushOption{Name: string($1)}
  }
| TABLE table_name_list flush_tables_read_lock_opt
  {
    $$ = &FlushOption{Name: string($1), Tables: $2, ReadLock: $3}
  }
| TABLES table_name_list flush_tables_read_lock_opt
  {
    $$ = &FlushOption{Name: string($1), Tables: $2, ReadLock: $3}
  }

flush_tables_read_lock_opt:
  {$$ = false}
| WITH READ LOCK
  {$$ = true}

relay_logs_attribute:
  { $$ = "" }
| FOR CHANNEL STRING
  { $$ = string($3) }

flush_type:
  NO_WRITE_TO_BINLOG
  { $$ = string($1) }
| LOCAL
  { $$ = string($1) }

flush_type_opt:
  { $$ = "" }
| flush_type
  { $$ = $1 }

replication_statement:
  CHANGE REPLICATION SOURCE TO replication_option_list
  {
    $$ = &ChangeReplicationSource{Options: $5}
  }
| CHANGE REPLICATION FILTER replication_filter_option_list
  {
    $$ = &ChangeReplicationFilter{Options: $4}
  }
| START REPLICA
  {
    $$ = &StartReplica{}
  }
| STOP REPLICA
  {
    $$ = &StopReplica{}
  }
| RESET REPLICA all_opt
  {
    $$ = &ResetReplica{All: $3}
  }

all_opt:
  { $$ = false }
| ALL
  { $$ = true }


replication_option_list:
  replication_option
  {
    $$ = []*ReplicationOption{$1}
  }
| replication_option_list ',' replication_option
 {
   $$ = append($$, $3)
 }

replication_option:
  SOURCE_HOST '=' STRING
  {
    $$ = &ReplicationOption{Name: string($1), Value: string($3)}
  }
| SOURCE_USER '=' STRING
  {
    $$ = &ReplicationOption{Name: string($1), Value: string($3)}
  }
| SOURCE_PASSWORD '=' STRING
  {
    $$ = &ReplicationOption{Name: string($1), Value: string($3)}
  }
| SOURCE_PORT '=' INTEGRAL
  {
    $$ = &ReplicationOption{Name: string($1), Value: mustAtoi(yylex, string($3))}
  }
| SOURCE_CONNECT_RETRY '=' INTEGRAL
  {
    $$ = &ReplicationOption{Name: string($1), Value: mustAtoi(yylex, string($3))}
  }
| SOURCE_RETRY_COUNT '=' INTEGRAL
  {
    $$ = &ReplicationOption{Name: string($1), Value: mustAtoi(yylex, string($3))}
  }
| SOURCE_AUTO_POSITION '=' INTEGRAL
  {
    $$ = &ReplicationOption{Name: string($1), Value: mustAtoi(yylex, string($3))}
  }

replication_filter_option_list:
  replication_filter_option
  {
    $$ = []*ReplicationOption{$1}
  }
| replication_filter_option_list ',' replication_filter_option
 {
   $$ = append($$, $3)
 }

replication_filter_option:
  REPLICATE_DO_TABLE '=' '(' table_name_list ')'
  {
    $$ = &ReplicationOption{Name: string($1), Value: $4}
  }
| REPLICATE_IGNORE_TABLE '=' '(' table_name_list ')'
  {
    $$ = &ReplicationOption{Name: string($1), Value: $4}
  }

index_definition:
  index_info '(' index_column_list ')' index_option_list
  {
    $$ = &IndexDefinition{Info: $1, Columns: $3, Options: $5}
  }
| index_info '(' index_column_list ')'
  {
    $$ = &IndexDefinition{Info: $1, Columns: $3}
  }

index_option_list_opt:
  {
    $$ = nil
  }
| index_option_list
  {
    $$ = $1
  }

index_option_list:
  index_option
  {
    $$ = []*IndexOption{$1}
  }
| index_option_list index_option
  {
    $$ = append($$, $2)
  }

index_option:
  USING any_identifier // TODO: should be restricted
  {
    $$ = &IndexOption{Name: string($1), Using: string($2)}
  }
| KEY_BLOCK_SIZE equal_opt INTEGRAL
  {
    // should not be string
    $$ = &IndexOption{Name: string($1), Value: NewIntVal($3)}
  }
| COMMENT_KEYWORD STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrVal($2)}
  }
| ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrVal($3)}
  }
| SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrVal($3)}
  }
| VISIBLE
  {
    $$ = &IndexOption{Name: string($1), Value: nil}
  }
| INVISIBLE
  {
    $$ = &IndexOption{Name: string($1), Value: nil}
  }

equal_opt:
  /* empty */
  {
    $$ = ""
  }
| assignment_op
  {
    $$ = string($1)
  }

assignment_op:
  '='
  {
    $$ = string($1)
  }
| ASSIGNMENT_OP
  {
    $$ = ":="
  }

index_info:
  // A name may be specified for a primary key, but it is ignored since the primary
  // key is always named 'PRIMARY'
  PRIMARY KEY name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
  }
| CONSTRAINT name_opt PRIMARY KEY name_opt
  {
    $$ = &IndexInfo{Type: string($3) + " " + string($4), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
  }
| SPATIAL index_or_key name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Spatial: true, Unique: false}
  }
| FULLTEXT index_or_key_opt name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Fulltext: true}
  }
| CONSTRAINT name_opt UNIQUE index_or_key_opt name_opt
  {
    var name string
    name = $2
    if name == "" {
      name = $5
    }
    $$ = &IndexInfo{Type: string($3) + " " + string($4), Name: NewColIdent(name), Unique: true}
  }
| UNIQUE index_or_key name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Unique: true}
  }
| UNIQUE name_opt
  {
    $$ = &IndexInfo{Type: string($1), Name: NewColIdent($2), Unique: true}
  }
| index_or_key name_opt
  {
    $$ = &IndexInfo{Type: string($1), Name: NewColIdent($2), Unique: false}
  }

indexes_or_keys:
  INDEX
  {
    $$ = string($1)
  }
| INDEXES
  {
    $$ = string($1)
  }
| KEYS
  {
    $$ = string($1)
  }

index_or_key:
  INDEX
  {
    $$ = string($1)
  }
| KEY
  {
    $$ = string($1)
  }

index_or_key_opt:
  {
    $$ = ""
  }
| index_or_key
  {
    $$ = $1
  }

name_opt:
  {
    $$ = ""
  }
| ID
  {
    $$ = string($1)
  }
| non_reserved_keyword
  {
    $$ = string($1)
  }

index_column_list:
  index_column
  {
    $$ = []*IndexColumn{$1}
  }
| index_column_list ',' index_column
  {
    $$ = append($$, $3)
  }

index_column:
  ID length_opt asc_desc_opt
  {
      $$ = &IndexColumn{Column: NewColIdent(string($1)), Length: $2, Order: $3}
  }
| all_non_reserved length_opt asc_desc_opt
  {
      $$ = &IndexColumn{Column: NewColIdent(string($1)), Length: $2, Order: $3}
  }

foreign_key_definition:
  CONSTRAINT id_or_non_reserved foreign_key_details
  {
    $$ = &ConstraintDefinition{Name: string($2), Details: $3}
  }
| CONSTRAINT foreign_key_details
  {
    $$ = &ConstraintDefinition{Details: $2}
  }
| foreign_key_details
  {
    $$ = &ConstraintDefinition{Details: $1}
  }

foreign_key_details:
  FOREIGN KEY index_name_opt '(' column_list ')' REFERENCES table_name '(' column_list ')'
  {
    $$ = &ForeignKeyDefinition{Source: $5, ReferencedTable: $8, ReferencedColumns: $10, Index: string($3)}
  }
| FOREIGN KEY index_name_opt '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_delete
  {
    $$ = &ForeignKeyDefinition{Source: $5, ReferencedTable: $8, ReferencedColumns: $10, OnDelete: $12, Index: string($3)}
  }
| FOREIGN KEY index_name_opt '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_update
  {
    $$ = &ForeignKeyDefinition{Source: $5, ReferencedTable: $8, ReferencedColumns: $10, OnUpdate: $12, Index: string($3)}
  }
| FOREIGN KEY index_name_opt '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_delete fk_on_update
  {
    $$ = &ForeignKeyDefinition{Source: $5, ReferencedTable: $8, ReferencedColumns: $10, OnDelete: $12, OnUpdate: $13, Index: string($3)}
  }
| FOREIGN KEY index_name_opt '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_update fk_on_delete
  {
    $$ = &ForeignKeyDefinition{Source: $5, ReferencedTable: $8, ReferencedColumns: $10, OnDelete: $13, OnUpdate: $12, Index: string($3)}
  }

index_name_opt:
  {
    $$ = nil
  }
| id_or_non_reserved
  {
    $$ = $1
  }

check_constraint_definition:
  CONSTRAINT id_or_non_reserved check_constraint_info
  {
    $$ = &ConstraintDefinition{Name: string($2), Details: $3}
  }
| CONSTRAINT check_constraint_info
  {
    $$ = &ConstraintDefinition{Details: $2}
  }
|  check_constraint_info
  {
    $$ = &ConstraintDefinition{Details: $1}
  }

check_constraint_info:
  CHECK '(' expression ')' enforced_opt
  {
    $$ = &CheckConstraintDefinition{Expr: $3, Enforced: $5}
  }

from_or_in:
  FROM
  {
    $$ = string($1)
  }
| IN
  {
    $$ = string($1)
  }

show_database_opt:
  {
    $$ = ""
  }
| FROM ID
  {
    $$ = string($2)
  }
| IN ID
  {
    $$ = string($2)
  }

fk_on_delete:
  ON DELETE fk_reference_action
  {
    $$ = $3
  }

fk_on_update:
  ON UPDATE fk_reference_action
  {
    $$ = $3
  }

fk_reference_action:
  RESTRICT
  {
    $$ = Restrict
  }
| CASCADE
  {
    $$ = Cascade
  }
| NO ACTION
  {
    $$ = NoAction
  }
| SET DEFAULT
  {
    $$ = SetDefault
  }
| SET NULL
  {
    $$ = SetNull
  }

enforced_opt:
  {
    $$ = true
  }
| ENFORCED
  {
    $$ = true
  }
| NOT_ENFORCED
  {
    $$ = false
  }

table_option_list:
  {
    $$ = nil
  }
| table_option_list table_option
  {
    $$ = append($1, $2)
  }
| table_option_list ',' table_option
  {
    $$ = append($1, $3)
  }

table_option:
  AUTOEXTEND_SIZE equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| AUTO_INCREMENT equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| AVG_ROW_LENGTH equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| default_keyword_opt CHARSET equal_opt charset
  {
    $$ = &TableOption{Name: "CHARACTER SET", Value: $4}
  }
| default_keyword_opt CHARACTER SET equal_opt charset
  {
    $$ = &TableOption{Name: string($2) + " " + string($3), Value: $5}
  }
| CHECKSUM equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| TABLE_CHECKSUM equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: "CHECKSUM", Value: string($3)}
  }
| default_keyword_opt COLLATE equal_opt table_option_collate
  {
    $$ = &TableOption{Name: string($2), Value: $4}
  }
| COMMENT_KEYWORD equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| COMPRESSION equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| CONNECTION equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| DATA DIRECTORY equal_opt STRING
  {
    $$ = &TableOption{Name: string($1) + " "  + string($2), Value: string($4)}
  }
| INDEX DIRECTORY equal_opt STRING
  {
    $$ = &TableOption{Name: string($1) + " "  + string($2), Value: string($4)}
  }
| DELAY_KEY_WRITE equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| ENCRYPTION equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| ENGINE equal_opt any_identifier
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| INSERT_METHOD equal_opt no_first_last
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| KEY_BLOCK_SIZE equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| MAX_ROWS equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| MIN_ROWS equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| PACK_KEYS equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| PASSWORD equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| ROW_FORMAT equal_opt row_fmt_opt
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| START TRANSACTION
  {
    $$ = &TableOption{Name: string($1) + string($2)}
  }
| SECONDARY_ENGINE equal_opt ID
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| SECONDARY_ENGINE equal_opt NULL
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| SECONDARY_ENGINE equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| STATS_AUTO_RECALC equal_opt DEFAULT
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| STATS_AUTO_RECALC equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| STATS_PERSISTENT equal_opt DEFAULT
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| STATS_PERSISTENT equal_opt coericble_to_integral
  {
    $$ = &TableOption{Name: string($1), Value: string($3)}
  }
| STATS_SAMPLE_PAGES equal_opt table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $3}
  }
| TABLESPACE table_opt_value
  {
    $$ = &TableOption{Name: string($1), Value: $2}
  }
| TABLESPACE any_identifier
  {
    $$ = &TableOption{Name: string($1), Value: string($2)}
  }
| TABLESPACE any_identifier STORAGE DISK
  {
    $$ = &TableOption{Name: string($1), Value: string($2) + " "  + string($3) + " "  + string($4)}
  }
| TABLESPACE any_identifier STORAGE MEMORY
  {
    $$ = &TableOption{Name: string($1), Value: string($2) + " "  + string($3) + " "  + string($4)}
  }
| UNION equal_opt openb any_identifier_list closeb
  {
    $$ = &TableOption{Name: string($1), Value: "(" + $4 + ")"}
  }

no_first_last:
  NO
  {
    $$ = $1
  }
| FIRST
  {
    $$ = $1
  }
| LAST
  {
    $$ = $1
  }

table_option_collate:
  any_identifier
  {
    $$ = string($1)
  }
| STRING
  {
    $$ = string($1)
  }

table_opt_value:
  STRING
  {
    $$ = "'" + string($1) + "'"
  }
| coericble_to_integral
  {
    $$ = string($1)
  }
// TODO: should be able to use non_reserved_keywords
| COMMENT_KEYWORD
  {
    $$ = string($1)
  }
| EVENT
  {
    $$ = string($1)
  }
| PASSWORD
  {
    $$ = string($1)
  }

coericble_to_integral:
  INTEGRAL
  {
     $$ = $1
  }
| HEXNUM
  {
    $$ = $1
  }
| FLOAT
  {
     $$ = $1
  }

row_fmt_opt:
  DEFAULT
  {
    $$ = string($1)
  }
| DYNAMIC
  {
    $$ = string($1)
  }
| FIXED
  {
    $$ = string($1)
  }
| COMPRESSED
  {
    $$ = string($1)
  }
| REDUNDANT
  {
    $$ = string($1)
  }
| COMPACT
  {
    $$ = string($1)
  }

any_identifier_list:
  any_identifier
  {
    $$ = string($1)
  }
| any_identifier_list ',' any_identifier
  {
    $$ = $1 + "," + string($3)
  }

any_identifier:
  ID
| non_reserved_keyword
| reserved_keyword

// TODO: partition options for table creation will parse, but do nothing for now
partition_option_opt:
  {
    $$ = nil
  }
| PARTITION BY partition_option partition_num_opt subpartition_opt partition_definitions_opt
  {
    $3.Partitions = $4
    $3.SubPartition = $5
    $3.Definitions = $6
    $$ = $3
  }

partition_option:
  linear_partition_opt
  {
    $$ = $1
  }
| range_or_list openb value_expression closeb
  {
    $$ = &PartitionOption {
    	PartitionType: string($1),
    	Expr: $3,
    }
  }
| range_or_list COLUMNS openb column_list closeb
  {
    $$ = &PartitionOption {
    	PartitionType: string($1),
    	ColList: $4,
    }
  }

linear_partition_opt:
  linear_opt HASH openb value_expression closeb
  {
    $$ = &PartitionOption {
    	IsLinear: $1,
    	PartitionType: string($2),
    	Expr: $4,
    }
  }
| linear_opt KEY algorithm_part_opt openb column_list closeb
  {
    $$ = &PartitionOption {
	IsLinear: $1,
	PartitionType: string($2),
	KeyAlgorithm: $3,
	ColList: $5,
    }
  }

linear_opt:
  {
    $$ = false
  }
| LINEAR
  {
    $$ = true
  }

algorithm_part_opt:
  {
    $$ = ""
  }
| ALGORITHM '=' INTEGRAL
  {
    $$ = string($1) + " = " + string($3)
  }

range_or_list:
  RANGE
  {
    $$ = string($1)
  }
| LIST
  {
    $$ = string($1)
  }

partition_num_opt:
  {
    $$ = nil
  }
| PARTITIONS INTEGRAL
  {
    $$ = NewIntVal($2)
  }

subpartition_opt:
  {
    $$ = nil
  }
| SUBPARTITION BY linear_opt HASH openb value_expression closeb subpartition_num_opt
  {
    $$ = &SubPartition{
    	IsLinear: $3,
    	PartitionType: string($4),
    	Expr: $6,
    	SubPartitions: $8,
    }
  }
| SUBPARTITION BY linear_opt KEY algorithm_part_opt openb value_expression closeb subpartition_num_opt
  {
    $$ = &SubPartition{
    	IsLinear: $3,
    	PartitionType: string($4),
    	KeyAlgorithm: $5,
    	Expr: $7,
    	SubPartitions: $9,
    }
  }

subpartition_num_opt:
  {
    $$ = nil
  }
| SUBPARTITIONS INTEGRAL
  {
    $$ = NewIntVal($2)
  }

constraint_symbol_opt:
  {
    $$ = ""
  }
| CONSTRAINT
  {
    $$ = ""
  }
| CONSTRAINT ID
  {
    $$ = string($2)
  }

pk_name_opt:
  {
    $$ = string("")
  }
| CONSTRAINT ID
  {
    $$ = string($2)
  }

alter_statement:
  alter_database_statement
| alter_table_statement
| alter_event_statement
| alter_user_statement

alter_database_statement:
  ALTER DATABASE ID creation_option_opt
  {
    $$ = &DBDDL{Action: AlterStr, SchemaOrDatabase: "database", DBName: string($3), CharsetCollate: $4}
  }
| ALTER DATABASE creation_option_opt
  {
    $$ = &DBDDL{Action: AlterStr, SchemaOrDatabase: "database", CharsetCollate: $3}
  }

alter_table_statement:
  ALTER ignore_opt TABLE table_name alter_table_statement_list partition_operation_list_opt
  {
    for i := 0; i < len($5); i++ {
    	if $5[i].Action == RenameStr {
    		$5[i].FromTables = append(TableNames{$4}, $5[i].FromTables...)
	} else {
		$5[i].Table = $4
	}
    }
    $$ = &AlterTable{Table: $4, Statements: $5, PartitionSpecs: $6}
  }
| ALTER ignore_opt TABLE table_name partition_operation_list
  {
    $$ = &AlterTable{Table: $4, PartitionSpecs: $5}
  }

alter_table_statement_list:
  alter_table_statement_part
  {
    $$ = []*DDL{$1}
  }
| alter_table_statement_list ',' alter_table_statement_part
  {
    $$ = append($$, $3)
  }

alter_table_statement_part:
  ADD column_opt '(' column_definition ')'
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ColumnAction: AddStr,
    	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddColumn($4)
    ddl.Column = $4.Name
    $$ = ddl
  }
| ADD column_opt column_definition column_order_opt
  {
    ddl := &DDL{
    	Action: AlterStr,
	ColumnAction: AddStr,
	TableSpec: &TableSpec{},
	ColumnOrder: $4,
    }
    ddl.TableSpec.AddColumn($3)
    ddl.Column = $3.Name
    $$ = ddl
  }
| ADD index_or_key name_opt using_opt '(' index_column_list ')' index_option_list_opt
  {
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: CreateStr,
    		ToName: NewColIdent($3),
    		Using: $4,
    		Columns: $6,
    		Options: $8,
	},
    }
  }
| ADD constraint_symbol_opt key_type index_or_key_opt name_opt using_opt '(' index_column_list ')' index_option_list_opt
  {
    idxName := $5
    if len(idxName) == 0 {
      idxName = $2
    }
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: CreateStr,
    		ToName: NewColIdent(idxName),
    		Type: $3,
    		Using: $6,
    		Columns: $8,
    		Options: $10,
	},
    }
  }
// A name may be specified for a primary key, but it is ignored since the primary
// key is always named 'PRIMARY'
| ADD pk_name_opt PRIMARY KEY name_opt '(' index_column_list ')' index_option_list_opt
  {
    ddl := &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: CreateStr,
	},
    }
    ddl.IndexSpec = &IndexSpec{
    	Action: CreateStr,
	Using: NewColIdent(""),
	ToName: NewColIdent($2),
	Type: PrimaryStr,
	Columns: $7,
	Options: $9,
    }
    $$ = ddl
  }
| ADD foreign_key_definition
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ConstraintAction: AddStr,
    	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint($2)
    $$ = ddl
  }
| ADD check_constraint_definition
  {
    ddl := &DDL{
	Action: AlterStr,
	ConstraintAction: AddStr,
	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint($2)
    $$ = ddl
  }
| DROP CONSTRAINT id_or_non_reserved
  {
    $$ = &DDL{
    	Action: AlterStr,
	ConstraintAction: DropStr,
	TableSpec: &TableSpec{
		Constraints: []*ConstraintDefinition{
	    		&ConstraintDefinition{
	    			Name: string($3),
			},
		},
	},
    }
  }
| DROP CHECK id_or_non_reserved
  {
    $$ = &DDL{
    	Action: AlterStr,
    	ConstraintAction: DropStr,
    	TableSpec: &TableSpec{
    		Constraints: []*ConstraintDefinition{
    			&ConstraintDefinition{
    				Name: string($3),
    				Details: &CheckConstraintDefinition{},
			},
		},
	},
    }
  }
| ALTER CONSTRAINT id_or_non_reserved ENFORCED
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALTER CHECK id_or_non_reserved ENFORCED
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALTER CONSTRAINT id_or_non_reserved NOT_ENFORCED
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALTER CHECK id_or_non_reserved NOT_ENFORCED
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALGORITHM equal_opt DEFAULT
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALGORITHM equal_opt INSTANT
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALGORITHM equal_opt INPLACE
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALGORITHM equal_opt COPY
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALTER column_opt sql_id SET DEFAULT value_expression
  {
    $$ = &DDL{
    	Action: AlterStr,
	DefaultSpec: &DefaultSpec{
		Action: SetStr,
		Column: $3,
		Value: $6,
	},
    }
  }
| ALTER column_opt sql_id DROP DEFAULT
  {
    $$ = &DDL{
    	Action: AlterStr,
    	DefaultSpec: &DefaultSpec{
    		Action: DropStr,
    		Column: $3,
	},
    }
  }
| ALTER INDEX id_or_non_reserved VISIBLE
  {
    $$ = &DDL{Action: AlterStr}
  }
| ALTER INDEX id_or_non_reserved INVISIBLE
  {
    $$ = &DDL{Action: AlterStr}
  }
| CHANGE column_opt ID column_definition column_order_opt
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ColumnAction: ChangeStr,
    	TableSpec: &TableSpec{},
    	Column: NewColIdent(string($3)),
    	ColumnOrder: $5,
    }
    ddl.TableSpec.AddColumn($4)
    $$ = ddl
  }
| default_keyword_opt CHARACTER SET equal_opt charset
  {
    $$ = &DDL{
    	Action: AlterStr,
    	AlterCollationSpec: &AlterCollationSpec{
    		CharacterSet: $5,
    		Collation: "",
	},
    }
  }
| default_keyword_opt CHARACTER SET equal_opt charset COLLATE equal_opt charset
  {
    $$ = &DDL{
    	Action: AlterStr,
    	AlterCollationSpec: &AlterCollationSpec{
    		CharacterSet: $5,
    		Collation: $8,
	},
    }
  }
| default_keyword_opt COLLATE equal_opt charset
  {
    $$ = &DDL{
    	Action: AlterStr,
    	AlterCollationSpec: &AlterCollationSpec{
    		CharacterSet: "",
    		Collation: $4,
	},
    }
  }
| CONVERT TO CHARACTER SET charset
  {
    $$ = &DDL{
    	Action: AlterStr,
    	AlterCollationSpec: &AlterCollationSpec{
    		CharacterSet: $5,
    		Collation: "",
	},
    }
  }
| CONVERT TO CHARACTER SET charset COLLATE charset
  {
    $$ = &DDL{
    	Action: AlterStr,
    	AlterCollationSpec: &AlterCollationSpec{
    		CharacterSet: $5,
    		Collation: $7,
	},
    }
  }
| DISABLE KEYS
  {
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: string($1),
	},
    }
  }
| ENABLE KEYS
  {
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: string($1),
	},
    }
  }
| DISCARD TABLESPACE
  {
    $$ = &DDL{Action: AlterStr}
  }
| IMPORT TABLESPACE
  {
    $$ = &DDL{Action: AlterStr}
  }
| DROP column_opt id_or_non_reserved
  {
    $$ = &DDL{
    	Action: AlterStr,
    	ColumnAction: DropStr,
    	Column: NewColIdent(string($3)),
    }
  }
| DROP index_or_key sql_id
  {
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: DropStr,
    		ToName: $3,
	},
    }
  }
| DROP PRIMARY KEY
  {
    $$ = &DDL{
    	Action: AlterStr,
	IndexSpec: &IndexSpec{
		Action: DropStr,
		Type: PrimaryStr,
	},
    }
  }
| DROP FOREIGN KEY id_or_non_reserved
  {
    ddl := &DDL{
    	Action: AlterStr,
	ConstraintAction: DropStr,
	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($4),
    	Details: &ForeignKeyDefinition{},
    })
    $$ = ddl
  }
| FORCE
  {
    $$ = &DDL{Action: AlterStr}
  }
| LOCK equal_opt DEFAULT
  {
    $$ = &DDL{Action: AlterStr}
  }
| LOCK equal_opt NONE
  {
    $$ = &DDL{Action: AlterStr}
  }
| LOCK equal_opt SHARED
  {
    $$ = &DDL{Action: AlterStr}
  }
| LOCK equal_opt EXCLUSIVE
  {
    $$ = &DDL{Action: AlterStr}
  }
| MODIFY column_opt column_definition column_order_opt
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ColumnAction: ModifyStr,
    	TableSpec: &TableSpec{},
    	ColumnOrder: $4,
    }
    ddl.TableSpec.AddColumn($3)
    ddl.Column = $3.Name
    $$ = ddl
  }
// | ORDER BY col_name [, col_name] ...
| RENAME COLUMN id_or_non_reserved to_or_as id_or_non_reserved
  {
    $$ = &DDL{
    	Action: AlterStr,
    	ColumnAction: RenameStr,
    	Column: NewColIdent(string($3)),
    	ToColumn: NewColIdent(string($5)),
    }
  }
| RENAME index_or_key sql_id TO sql_id
  {
    $$ = &DDL{
    	Action: AlterStr,
    	IndexSpec: &IndexSpec{
    		Action: RenameStr,
    		FromName: $3,
    		ToName: $5,
	},
    }
  }
| RENAME to_or_as_opt table_name
  {
    // Change this to a rename statement
    $$ = &DDL{
    	Action: RenameStr,
    	ToTables: TableNames{$3},
    }
  }
| RENAME CONSTRAINT FOREIGN KEY id_or_non_reserved TO id_or_non_reserved
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ConstraintAction: RenameStr,
    	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($5),
    	Details: &ForeignKeyDefinition{},
    })
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($7),
    	Details: &ForeignKeyDefinition{},
    })
    $$ = ddl
  }
| RENAME CONSTRAINT CHECK id_or_non_reserved TO id_or_non_reserved
  {
    ddl := &DDL{
    	Action: AlterStr,
    	ConstraintAction: RenameStr,
    	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($4),
    	Details: &CheckConstraintDefinition{},
    })
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
 	Name: string($6),
 	Details: &CheckConstraintDefinition{},
    })
    $$ = ddl
  }
| RENAME CONSTRAINT id_or_non_reserved TO id_or_non_reserved
  {
    ddl := &DDL{
    	Action: AlterStr,
	ConstraintAction: RenameStr,
	TableSpec: &TableSpec{},
    }
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($3),
    })
    ddl.TableSpec.AddConstraint(&ConstraintDefinition{
    	Name: string($5),
    })
    $$ = ddl
  }
| with_or_without VALIDATION
  {
    $$ = &DDL{
    	Action: AlterStr,
    }
  }
| alter_table_options
  {
    $$ = $1
  }

// TODO: these are the same as table_option_list, but we need to have the return type be different
alter_table_options:
  AUTOEXTEND_SIZE equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| AUTO_INCREMENT equal_opt expression
  {
    $$ = &DDL{Action: AlterStr, AutoIncSpec: &AutoIncSpec{Value: $3}}
  }
| AVG_ROW_LENGTH equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| CHECKSUM equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| TABLE_CHECKSUM equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| COMMENT_KEYWORD equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| COMPRESSION equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| CONNECTION equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| DATA DIRECTORY equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| INDEX DIRECTORY equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| DELAY_KEY_WRITE equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| ENCRYPTION equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| ENGINE equal_opt any_identifier
  {
    $$ = &DDL{Action: AlterStr}
  }
| ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| INSERT_METHOD equal_opt no_first_last
  {
    $$ = &DDL{Action: AlterStr}
  }
| KEY_BLOCK_SIZE equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| MAX_ROWS equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| MIN_ROWS equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| PACK_KEYS equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| PASSWORD equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| ROW_FORMAT equal_opt row_fmt_opt
  {
    $$ = &DDL{Action: AlterStr}
  }
| SECONDARY_ENGINE equal_opt ID
  {
    $$ = &DDL{Action: AlterStr}
  }
| SECONDARY_ENGINE equal_opt NULL
  {
    $$ = &DDL{Action: AlterStr}
  }
| SECONDARY_ENGINE equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &DDL{Action: AlterStr}
  }
| STATS_AUTO_RECALC equal_opt DEFAULT
  {
    $$ = &DDL{Action: AlterStr}
  }
| STATS_AUTO_RECALC equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| STATS_PERSISTENT equal_opt DEFAULT
  {
    $$ = &DDL{Action: AlterStr}
  }
| STATS_PERSISTENT equal_opt coericble_to_integral
  {
    $$ = &DDL{Action: AlterStr}
  }
| STATS_SAMPLE_PAGES equal_opt table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| TABLESPACE table_opt_value
  {
    $$ = &DDL{Action: AlterStr}
  }
| TABLESPACE any_identifier
  {
    $$ = &DDL{Action: AlterStr}
  }
| TABLESPACE any_identifier STORAGE DISK
  {
    $$ = &DDL{Action: AlterStr}
  }
| TABLESPACE any_identifier STORAGE MEMORY
  {
    $$ = &DDL{Action: AlterStr}
  }
| UNION equal_opt openb any_identifier_list closeb
  {
    $$ = &DDL{Action: AlterStr}
  }

with_or_without:
  WITH
  {
    $$ = true
  }
| WITHOUT
  {
    $$ = false
  }

id_or_non_reserved:
  ID
| all_non_reserved

alter_user_statement:
  ALTER USER exists_opt account_name authentication
  {
    var ifExists bool
    if $3 != 0 {
      ifExists = true
    }
    $$ = &DDL{Action: AlterStr, User: $4, Authentication: $5, IfExists: ifExists}
  }

column_order_opt:
  {
    $$ = nil
  }
| FIRST
  {
    $$ = &ColumnOrder{First: true}
  }
| AFTER ID
  {
    $$ = &ColumnOrder{AfterColumn: NewColIdent(string($2))}
  }

column_opt:
  { }
| COLUMN
  { }

partition_operation_list_opt:
  {
    $$ = nil
  }
| partition_operation_list
  {
    $$ = $1
  }

partition_operation_list:
  partition_operation
  {
    $$ = []*PartitionSpec{$1}
  }
| partition_operation_list partition_operation
  {
    $$ = append($1, $2)
  }

partition_operation:
  ADD PARTITION openb partition_definitions closeb
  {
    $$ = &PartitionSpec{Action: AddStr, Definitions: $4}
  }
| DROP PARTITION partition_list
  {
    $$ = &PartitionSpec{Action: DropStr, Names: $3}
  }
| DISCARD PARTITION partition_list TABLESPACE
  {
    $$ = &PartitionSpec{Action: DiscardStr, Names: $3}
  }
| DISCARD PARTITION ALL TABLESPACE
  {
    $$ = &PartitionSpec{Action: DiscardStr, IsAll: true}
  }
| IMPORT PARTITION partition_list TABLESPACE
  {
    $$ = &PartitionSpec{Action: ImportStr, Names: $3}
  }
| IMPORT PARTITION ALL TABLESPACE
  {
    $$ = &PartitionSpec{Action: ImportStr, IsAll: true}
  }
| TRUNCATE PARTITION partition_list TABLESPACE
  {
    $$ = &PartitionSpec{Action: TruncateStr, Names: $3}
  }
| TRUNCATE PARTITION ALL TABLESPACE
  {
    $$ = &PartitionSpec{Action: TruncateStr, IsAll: true}
  }
| COALESCE PARTITION INTEGRAL
  {
    $$ = &PartitionSpec{Action: CoalesceStr, Number: NewIntVal($3)}
  }
| REORGANIZE PARTITION partition_list INTO openb partition_definitions closeb
  {
    $$ = &PartitionSpec{Action: ReorganizeStr, Names: $3, Definitions: $6}
  }
| EXCHANGE PARTITION sql_id WITH TABLE table_name
  {
    $$ = &PartitionSpec{Action: ExchangeStr, Names: Partitions{$3}, TableName: $6}
  }
| EXCHANGE PARTITION sql_id WITH TABLE table_name with_or_without VALIDATION
  {
    $$ = &PartitionSpec{Action: ExchangeStr, Names: Partitions{$3}, TableName: $6, WithValidation: $7}
  }
| ANALYZE PARTITION partition_list
  {
    $$ = &PartitionSpec{Action: AnalyzeStr, Names: $3}
  }
| ANALYZE PARTITION ALL
  {
    $$ = &PartitionSpec{Action: AnalyzeStr, IsAll: true}
  }
// The parser confuses CHECK with column_definitions
//| CHECK PARTITION partition_list
//  {
//    $$ = &PartitionSpec{Action: CheckStr, Names: $3}
//  }
//| CHECK PARTITION ALL
//  {
//    $$ = &PartitionSpec{Action: CheckStr, IsAll: true}
//  }
| OPTIMIZE PARTITION partition_list
  {
    $$ = &PartitionSpec{Action: OptimizeStr, Names: $3}
  }
| OPTIMIZE PARTITION ALL
  {
    $$ = &PartitionSpec{Action: OptimizeStr, IsAll: true}
  }
| REBUILD PARTITION partition_list
  {
    $$ = &PartitionSpec{Action: RebuildStr, Names: $3}
  }
| REBUILD PARTITION ALL
  {
    $$ = &PartitionSpec{Action: RebuildStr, IsAll: true}
  }
| REPAIR PARTITION partition_list
  {
    $$ = &PartitionSpec{Action: RepairStr, Names: $3}
  }
| REPAIR PARTITION ALL
  {
    $$ = &PartitionSpec{Action: RepairStr, IsAll: true}
  }
| REMOVE PARTITIONING
  {
    $$ = &PartitionSpec{Action: RemoveStr}
  }

partition_definitions_opt:
  {
    $$ = nil
  }
| openb partition_definitions closeb
  {
    $$ = $2
  }

partition_definitions:
  partition_definition
  {
    $$ = []*PartitionDefinition{$1}
  }
| partition_definitions ',' partition_definition
  {
    $$ = append($1, $3)
  }

partition_definition:
  PARTITION sql_id VALUES LESS THAN openb value_expression closeb
  {
    $$ = &PartitionDefinition{Name: $2, Limit: $7}
  }
| PARTITION sql_id VALUES LESS THAN openb MAXVALUE closeb
  {
    $$ = &PartitionDefinition{Name: $2, Maxvalue: true}
  }

alter_event_statement:
  ALTER definer_opt EVENT event_name event_on_completion_preserve_opt rename_event_name_opt event_status_opt comment_keyword_opt
  {
    es := &EventSpec{EventName: $4, Definer: $2, OnCompletionPreserve: $5, RenameName: $6, Status: $7, Comment: $8}
    if err := es.ValidateAlterEvent(); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $$ = &DDL{Action: AlterStr, EventSpec: es}
  }
| ALTER definer_opt EVENT event_name ON SCHEDULE event_schedule event_on_completion_preserve_opt rename_event_name_opt event_status_opt comment_keyword_opt
  {
    $$ = &DDL{Action: AlterStr, EventSpec: &EventSpec{EventName: $4, Definer: $2, OnSchedule: $7, OnCompletionPreserve: $8, RenameName: $9, Status: $10, Comment: $11}}
  }
| ALTER definer_opt EVENT event_name event_on_completion_preserve_opt rename_event_name_opt event_status_opt comment_keyword_opt DO lexer_position statement_list_statement lexer_position
  {
    $$ = &DDL{Action: AlterStr, EventSpec: &EventSpec{EventName: $4, Definer: $2, OnCompletionPreserve: $5, RenameName: $6, Status: $7, Comment: $8, Body: $11}, SubStatementPositionStart: $10, SubStatementPositionEnd: $12 - 1}
  }
| ALTER definer_opt EVENT event_name ON SCHEDULE event_schedule event_on_completion_preserve_opt rename_event_name_opt event_status_opt comment_keyword_opt DO lexer_position statement_list_statement lexer_position
  {
    $$ = &DDL{Action: AlterStr, EventSpec: &EventSpec{EventName: $4, Definer: $2, OnSchedule: $7, OnCompletionPreserve: $8, RenameName: $9, Status: $10, Comment: $11, Body: $14}, SubStatementPositionStart: $13, SubStatementPositionEnd: $15 - 1}
  }

rename_event_name_opt:
  {
    $$ = EventName{}
  }
| RENAME TO event_name
  {
    $$ = $3
  }

rename_statement:
  RENAME TABLE rename_list
  {
    $$ = $3
  }
| RENAME USER rename_user_list
  {
    $$ = &RenameUser{Accounts: $3}
  }

rename_list:
  table_name TO table_name
  {
    $$ = &DDL{Action: RenameStr, FromTables: TableNames{$1}, ToTables: TableNames{$3}}
  }
| rename_list ',' table_name TO table_name
  {
    $$ = $1
    $$.FromTables = append($$.FromTables, $3)
    $$.ToTables = append($$.ToTables, $5)
  }

rename_user_list:
  account_name TO account_name
  {
    $$ = []AccountRename{{From: $1, To: $3}}
  }
| rename_user_list ',' account_name TO account_name
  {
    $$ = append($1, AccountRename{From: $3, To: $5})
  }

drop_statement:
  DROP TABLE exists_opt table_name_list drop_statement_action
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, FromTables: $4, IfExists: exists}
  }
| DROP INDEX sql_id ON table_name
  {
    // For consistency, we always use a AlterTable for ALTER TABLE equivalent statements
    ddl := &DDL{Action: AlterStr, Table: $5, IndexSpec: &IndexSpec{Action: DropStr, ToName: $3}}
    $$ = &AlterTable{Table: $5, Statements: []*DDL{ddl}}
  }
| DROP VIEW exists_opt view_name_list
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, FromViews: $4, IfExists: exists}
  }
| DROP DATABASE exists_opt ID
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DBDDL{Action: DropStr, SchemaOrDatabase: "database", DBName: string($4), IfExists: exists}
  }
| DROP SCHEMA exists_opt ID
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DBDDL{Action: DropStr, SchemaOrDatabase: "schema", DBName: string($4), IfExists: exists}
  }
| DROP TRIGGER exists_opt trigger_name
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, TriggerSpec: &TriggerSpec{TrigName: $4}, IfExists: exists}
  }
| DROP PROCEDURE exists_opt procedure_name
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, ProcedureSpec: &ProcedureSpec{ProcName: $4}, IfExists: exists}
  }
| DROP USER exists_opt account_name_list
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DropUser{IfExists: exists, AccountNames: $4}
  }
| DROP ROLE exists_opt role_name_list
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DropRole{IfExists: exists, Roles: $4}
  }
| DROP EVENT exists_opt event_name
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, EventSpec: &EventSpec{EventName: $4}, IfExists: exists}
  }

drop_statement_action:
  {

  }
| RESTRICT
  {
    $$ = Restrict
  }
| CASCADE
  {
    $$ = Cascade
  }

truncate_statement:
  TRUNCATE TABLE table_name
  {
    $$ = &DDL{Action: TruncateStr, Table: $3}
  }
| TRUNCATE table_name
  {
    $$ = &DDL{Action: TruncateStr, Table: $2}
  }

analyze_statement:
  ANALYZE TABLE analyze_opt
  {
    $$ = $3
  }

analyze_opt:
  table_name UPDATE HISTOGRAM ON paren_column_list USING DATA value_expression
  {
    $$ = &Analyze{Tables: []TableName{$1}, Action: UpdateStr, Columns: $5, Using: $8}
  }
| table_name DROP HISTOGRAM ON paren_column_list
  {
    $$ = &Analyze{Tables: []TableName{$1}, Action: DropStr, Columns: $5}
  }
| table_name_list
  {
    $$ = &Analyze{Tables: $1}
  }

all_non_reserved:
  non_reserved_keyword
| non_reserved_keyword2
| non_reserved_keyword3
| column_name_safe_keyword

prepare_statement:
  PREPARE ID FROM STRING
  {
    $$ = &Prepare{Name: string($2), Expr: string($4)}
  }
| PREPARE all_non_reserved FROM STRING
  {
    $$ = &Prepare{Name: string($2), Expr: string($4)}
  }
| PREPARE ID FROM system_variable
  {
    $$ = &Prepare{Name: string($2), Expr: string($4)}
  }
| PREPARE all_non_reserved FROM system_variable
  {
    $$ = &Prepare{Name: string($2), Expr: string($4)}
  }

system_variable_list:
  system_variable
  {
    $$ = []string{$1}
  }
| system_variable_list ',' system_variable
  {
    $$ = append($1, $3)
  }

// TODO: ensure these start with '@'
system_variable:
  ID
  {
    $$ = string($1)
  }
| all_non_reserved
  {
    $$ = string($1)
  }

execute_statement:
  EXECUTE ID
  {
    $$ = &Execute{Name: string($2)}
  }
| EXECUTE non_reserved_keyword
  {
    $$ = &Execute{Name: string($2)}
  }
| EXECUTE ID USING system_variable_list
  {
    $$ = &Execute{Name: string($2), VarList: $4}
  }
| EXECUTE non_reserved_keyword USING system_variable_list
  {
    $$ = &Execute{Name: string($2), VarList: $4}
  }

deallocate_statement:
  DEALLOCATE PREPARE ID
  {
    $$ = &Deallocate{Name: string($3)}
  }
| DEALLOCATE PREPARE all_non_reserved
  {
    $$ = &Deallocate{Name: string($3)}
  }
| DROP PREPARE ID
  {
    $$ = &Deallocate{Name: string($3)}
  }
| DROP PREPARE all_non_reserved
  {
    $$ = &Deallocate{Name: string($3)}
  }

show_statement:
  SHOW BINARY ID /* SHOW BINARY LOGS */
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
/* SHOW CHARACTER SET and SHOW CHARSET are equivalent */
| SHOW CHARACTER SET like_or_where_opt
  {
    $$ = &Show{Type: CharsetStr, Filter: $4}
  }
| SHOW CHARSET like_or_where_opt
  {
    $$ = &Show{Type: string($2), Filter: $3}
  }
| SHOW CREATE DATABASE not_exists_opt ID
  {
    $$ = &Show{Type: string($2) + " " + string($3), IfNotExists: $4 == 1, Database: string($5)}
  }
| SHOW CREATE SCHEMA not_exists_opt ID
  {
    $$ = &Show{Type: string($2) + " " + string($3), IfNotExists: $4 == 1, Database: string($5)}
  }
| SHOW CREATE TABLE table_name as_of_opt
  {
    showTablesOpt := &ShowTablesOpt{AsOf:$5}
    $$ = &Show{Type: CreateTableStr, Table: $4, ShowTablesOpt: showTablesOpt}
  }
| SHOW CREATE PROCEDURE table_name
  {
    $$ = &Show{Type: CreateProcedureStr, Table: $4}
  }
| SHOW CREATE TRIGGER table_name
  {
    $$ = &Show{Type: CreateTriggerStr, Table: $4}
  }
| SHOW CREATE VIEW table_name
  {
    $$ = &Show{Type: string($2) + " " + string($3), Table: $4}
  }
| SHOW CREATE EVENT table_name
  {
    $$ = &Show{Type: CreateEventStr, Table: $4}
  }
| SHOW DATABASES like_or_where_opt
  {
    $$ = &Show{Type: string($2), Filter: $3}
  }
| SHOW SCHEMAS
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW ENGINES
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW indexes_or_keys from_or_in table_name show_database_opt where_expression_opt
  {
    $$ = &Show{Type: IndexStr, Table: $4, Database: $5, ShowIndexFilterOpt: $6}
  }
| SHOW PLUGINS
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW PROCEDURE STATUS like_or_where_opt
  {
    $$ = &Show{Type: string($2) + " " + string($3), Filter: $4}
  }
| SHOW REPLICA STATUS
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW FUNCTION STATUS like_or_where_opt
  {
    $$ = &Show{Type: string($2) + " " + string($3), Filter: $4}
  }
| SHOW show_session_or_global STATUS like_or_where_opt
  {
    $$ = &Show{Scope: $2, Type: string($3), Filter: $4}
  }
| SHOW TABLE STATUS from_database_opt like_or_where_opt
  {
    $$ = &Show{Type: string($2) + " " + string($3), Database: $4, Filter:$5}
  }
| SHOW full_opt columns_or_fields FROM table_name from_database_opt as_of_opt like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{DbName:$6, AsOf:$7, Filter:$8}
    $$ = &Show{Type: string($3), ShowTablesOpt: showTablesOpt, Table: $5, Full: $2}
  }
| SHOW full_opt TABLES from_database_opt as_of_opt like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{DbName:$4, Filter:$6, AsOf:$5}
    $$ = &Show{Type: string($3), ShowTablesOpt: showTablesOpt, Full: $2}
  }
| SHOW full_opt PROCESSLIST
  {
    $$ = &Show{Type: string($3), Full: $2}
  }
| SHOW TRIGGERS from_database_opt like_or_where_opt
  {
    $$ = &Show{Type: string($2), ShowTablesOpt: &ShowTablesOpt{DbName: $3, Filter: $4}}
  }
| SHOW show_session_or_global VARIABLES like_or_where_opt
  {
    $$ = &Show{Scope: $2, Type: string($3), Filter: $4}
  }
| SHOW COLLATION
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW COLLATION WHERE expression
  {
    $$ = &Show{Type: string($2), ShowCollationFilterOpt: $4}
  }
| SHOW COLLATION naked_like
  {
    cmp := $3.(*ComparisonExpr)
    cmp.Left = &ColName{Name: NewColIdent("collation")}
    $$ = &Show{Type: string($2), ShowCollationFilterOpt: cmp}
  }
| SHOW GRANTS
  {
    $$ = &ShowGrants{}
  }
| SHOW GRANTS FOR account_name
  {
    an := $4
    $$ = &ShowGrants{For: &an}
  }
| SHOW GRANTS FOR CURRENT_USER func_parens_opt
  {
    $$ = &ShowGrants{CurrentUser: true}
  }
| SHOW GRANTS FOR account_name USING role_name_list
  {
    an := $4
    $$ = &ShowGrants{For: &an, Using: $6}
  }
| SHOW PRIVILEGES
  {
    $$ = &ShowPrivileges{}
  }
| SHOW COUNT openb '*' closeb WARNINGS
  {
    $$ = &Show{Type: string($6), CountStar: true}
  }
| SHOW COUNT openb '*' closeb ERRORS
  {
    $$ = &Show{Type: string($6), CountStar: true}
  }
| SHOW WARNINGS limit_opt
  {
    $$ = &Show{Type: string($2), Limit: $3}
  }
| SHOW ERRORS limit_opt
  {
    $$ = &Show{Type: string($2), Limit: $3}
  }
| SHOW EVENTS from_database_opt like_or_where_opt
  {
    $$ = &Show{Type: string($2), ShowTablesOpt: &ShowTablesOpt{DbName: $3, Filter: $4}}
  }
| SHOW REPLICAS
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW BINARY LOG STATUS
  {
    $$ = &Show{Type: string($2) + " " + string($3) + " " + string($4)}
  }
| SHOW MASTER STATUS
  {
    $$ = &Show{Type: "BINARY LOG STATUS"}
  }
| SHOW BINARY LOGS
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }

naked_like:
LIKE value_expression like_escape_opt
  {
    $$ = &ComparisonExpr{Operator: LikeStr, Right: $2, Escape: $3}
  }

full_opt:
  /* empty */
  {
    $$ = false
  }
| FULL
  {
    $$ = true
  }

columns_or_fields:
  COLUMNS
  {
      $$ = string($1)
  }
| FIELDS
  {
      $$ = string($1)
  }

from_database_opt:
  /* empty */
  {
    $$ = ""
  }
| FROM table_id
  {
    $$ = $2.v
  }
| IN table_id
  {
    $$ = $2.v
  }

like_or_where_opt:
  /* empty */
  {
    $$ = nil
  }
| LIKE STRING
  {
    $$ = &ShowFilter{Like:string($2)}
  }
| LIKE underscore_charsets STRING
  {
    $$ = &ShowFilter{Like:string($3)}
  }
| WHERE expression
  {
    $$ = &ShowFilter{Filter:$2}
  }

show_session_or_global:
  /* empty */
  {
    $$ = ""
  }
| SESSION
  {
    $$ = SessionStr
  }
| GLOBAL
  {
    $$ = GlobalStr
  }

use_statement:
  USE table_id
  {
    $$ = &Use{DBName: $2}
  }
| USE table_id '/' table_id
  {
    $$ = &Use{DBName:TableIdent{v:$2.v+"/"+$4.v}}
  }
| USE
  {
    $$ = &Use{DBName:TableIdent{v:""}}
  }

work_opt:
  {
    $$ = nil
  }
| WORK
  {
    $$ = $1
  }

begin_statement:
  BEGIN work_opt
  {
    $$ = &Begin{}
  }
| start_transaction_statement
  {
    $$ = $1
  }

start_transaction_statement:
  START TRANSACTION
  {
    $$ = &Begin{}
  }
| START TRANSACTION READ WRITE
  {
    $$ = &Begin{TransactionCharacteristic: TxReadWrite}
  }
 | START TRANSACTION READ ONLY
  {
    $$ = &Begin{TransactionCharacteristic: TxReadOnly}
  }

no_opt:
  {
    $$ = nil
  }
| NO
  {
    $$ = nil
  }

chain_opt:
  {
    $$ = nil
  }
| AND no_opt CHAIN
  {
    $$ = nil
  }

release_opt:
  {
    $$ = nil
  }
| no_opt RELEASE
  {
    $$ = nil
  }

commit_statement:
  COMMIT work_opt chain_opt release_opt
  {
    $$ = &Commit{}
  }

rollback_statement:
  ROLLBACK work_opt chain_opt release_opt
  {
    $$ = &Rollback{}
  }

savepoint_statement:
  SAVEPOINT ID
  {
    $$ = &Savepoint{Identifier: string($2)}
  }

rollback_savepoint_statement:
  ROLLBACK TO ID
  {
    $$ = &RollbackSavepoint{Identifier: string($3)}
  }
| ROLLBACK WORK TO ID
  {
    $$ = &RollbackSavepoint{Identifier: string($4)}
  }
| ROLLBACK TO SAVEPOINT ID
  {
    $$ = &RollbackSavepoint{Identifier: string($4)}
  }
| ROLLBACK WORK TO SAVEPOINT ID
  {
    $$ = &RollbackSavepoint{Identifier: string($5)}
  }

release_savepoint_statement:
  RELEASE SAVEPOINT ID
  {
    $$ = &ReleaseSavepoint{Identifier: string($3)}
  }

explain_statement:
  explain_verb format_opt explainable_statement
  {
    $$ = &Explain{ExplainFormat: $2, Statement: $3}
  }
| explain_verb EXTENDED format_opt explainable_statement
  {
    $$ = &Explain{ExplainFormat: $3, Statement: $4}
  }
| explain_verb ANALYZE select_statement_with_no_trailing_into
  {
    $$ = &Explain{Analyze: true, ExplainFormat: TreeStr, Statement: $3}
  }

explainable_statement:
  select_statement
  {
    $$ = $1
  }
| delete_statement
| insert_statement
| update_statement

format_opt:
  {
    $$ = ""
  }
| FORMAT '=' ID
  {
    $$ = string($3)
  }

explain_verb:
  EXPLAIN
| DESCRIBE
| DESC

describe_statement:
  explain_verb table_name as_of_opt
  // rewrite describe table as show columns from table
  {
    showTablesOpt := &ShowTablesOpt{AsOf:$3}
    $$ = &Show{Type: "columns", Table: $2, ShowTablesOpt: showTablesOpt}
  }

comment_opt:
  {
    setAllowComments(yylex, true)
  }
  comment_list
  {
    $$ = $2
    setAllowComments(yylex, false)
  }

comment_list:
  {
    $$ = nil
  }
| comment_list COMMENT
  {
    $$ = append($1, $2)
  }

union_op:
  UNION
  {
    $$ = UnionStr
  }
| UNION ALL
  {
    $$ = UnionAllStr
  }
| UNION DISTINCT
  {
    $$ = UnionDistinctStr
  }

intersect_op:
  INTERSECT
  {
    $$ = IntersectStr
  }
| INTERSECT ALL
  {
    $$ = IntersectAllStr
  }
| INTERSECT DISTINCT
  {
    $$ = IntersectDistinctStr
  }

except_op:
  EXCEPT
  {
    $$ = ExceptStr
  }
| EXCEPT ALL
  {
    $$ = ExceptAllStr
  }
| EXCEPT DISTINCT
  {
    $$ = ExceptDistinctStr
  }

query_opts:
  {
    $$ = QueryOpts{}
  }
| query_opts ALL
  {
    opt := QueryOpts{All: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }
| query_opts DISTINCT
  {
    opt := QueryOpts{Distinct: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }
| query_opts STRAIGHT_JOIN
  {
    opt := QueryOpts{StraightJoinHint: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }
| query_opts SQL_CALC_FOUND_ROWS
  {
    opt := QueryOpts{SQLCalcFoundRows: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }
| query_opts SQL_CACHE
  {
    opt := QueryOpts{SQLCache: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }
| query_opts SQL_NO_CACHE
  {
    opt := QueryOpts{SQLNoCache: true}
    if err := $1.merge(opt); err != nil {
    	yylex.Error(err.Error())
	return 1
    }
    $$ = $1
  }

distinct_opt:
  {
    $$ = ""
  }
| ALL
  {
    $$ = AllStr
  }
| DISTINCT
  {
    $$ = DistinctStr
  }

select_expression_list:
  lexer_old_position select_expression lexer_old_position
  {
    if ae, ok := $2.(*AliasedExpr); ok {
      ae.StartParsePos = $1
      ae.EndParsePos = $3-1
    }
    $$ = SelectExprs{$2}
  }
| select_expression_list ',' lexer_old_position select_expression lexer_old_position
  {
    if ae, ok := $4.(*AliasedExpr); ok {
      ae.StartParsePos = $3
      ae.EndParsePos = $5-1
    }
    $$ = append($$, $4)
  }

// argument_expression is identical to select_expression except aliases are not allowed
argument_expression:
  '*'
  {
    $$ = &StarExpr{}
  }
| expression
  {
    $$ = &AliasedExpr{Expr: $1}
  }
| table_id '.' '*'
  {
    $$ = &StarExpr{TableName: TableName{Name: $1}}
  }
| table_id '.' reserved_table_id '.' '*'
  {
    $$ = &StarExpr{TableName: TableName{DbQualifier: $1, Name: $3}}
  }

select_expression:
  '*'
  {
    $$ = &StarExpr{}
  }
| expression as_ci_opt
  {
    $$ = &AliasedExpr{Expr: $1, As: $2}
  }
| table_id '.' '*'
  {
    $$ = &StarExpr{TableName: TableName{Name: $1}}
  }
| table_id '.' reserved_table_id '.' '*'
  {
    $$ = &StarExpr{TableName: TableName{DbQualifier: $1, Name: $3}}
  }

over:
  OVER sql_id
  {
    $$ = &Over{NameRef: $2}
  }
| OVER window_spec
  {
    $$ = (*Over)($2)
  }

window_spec:
  openb existing_window_name_opt partition_by_opt order_by_opt frame_opt closeb
  {
    $$ = &WindowDef{NameRef: $2, PartitionBy: $3, OrderBy: $4, Frame: $5}
  }

existing_window_name_opt:
  {
    $$ = ColIdent{}
  }
| ID {
    $$ = NewColIdent(string($1))
  }

partition_by_opt:
  {
    $$ = nil
  }
| PARTITION BY expression_list
  {
    $$ = $3
  }

over_opt:
  {
    $$ = nil
  }
| over
  {
    $$ = $1
  }

frame_opt:
  {
    $$ = nil
  }
| ROWS frame_extent
  {
    $$ = &Frame{Unit: RowsUnit, Extent: $2}
  }
| RANGE frame_extent
  {
    $$ = &Frame{Unit: RangeUnit, Extent: $2}
  }

// enforce PRECEDING < CURRENT ROW < FOLLOWING
frame_extent:
  BETWEEN frame_bound AND frame_bound
  {
    startBound := $2
    endBound := $4
    switch {
    case startBound.Type == UnboundedFollowing:
      yylex.Error("frame start cannot be UNBOUNDED FOLLOWING")
      return 1
    case endBound.Type == UnboundedPreceding:
      yylex.Error("frame end cannot be UNBOUNDED PRECEDING")
      return 1
    case startBound.Type == CurrentRow && endBound.Type == ExprPreceding:
      yylex.Error("frame starting from current row cannot have preceding rows")
      return 1
    case startBound.Type == ExprFollowing && endBound.Type == ExprPreceding:
      yylex.Error("frame starting from following row cannot have preceding rows")
      return 1
    case startBound.Type == ExprFollowing && endBound.Type == CurrentRow:
      yylex.Error("frame starting from following row cannot have preceding rows")
      return 1
    }
    $$ = &FrameExtent{Start: startBound, End: endBound}
  }
| frame_bound
  {
    startBound := $1
     switch {
     case startBound.Type == UnboundedFollowing:
       yylex.Error("frame start cannot be UNBOUNDED FOLLOWING")
       return 1
     case startBound.Type == ExprFollowing:
       yylex.Error("frame starting from following row cannot end with current row")
       return 1
     }
     $$ = &FrameExtent{Start: startBound}
  }

frame_bound:
UNBOUNDED PRECEDING
  {
    $$ = &FrameBound{Type: UnboundedPreceding}
  }
| UNBOUNDED FOLLOWING
  {
    $$ = &FrameBound{Type: UnboundedFollowing}
  }
| CURRENT ROW
  {
    $$ = &FrameBound{Type: CurrentRow}
  }
| integral_or_interval_expr PRECEDING
  {
    $$ = &FrameBound{
       Expr: $1,
       Type: ExprPreceding,
     }
  }
| integral_or_interval_expr FOLLOWING
  {
    $$ = &FrameBound{
       Expr: $1,
       Type: ExprFollowing,
     }
  }

window_opt:
  {
  $$ = nil
  }
| WINDOW window {
  $$ = $2
  }

window:
  window_definition
  {
    $$ = Window{$1}
  }
| window ',' window_definition {
    $$ = append($1, $3)
  }

window_definition:
  sql_id AS window_spec
  {
    def := $3
    def.Name = $1
    $$ = def
  }

time_unit:
  non_microsecond_time_unit
| MICROSECOND
| SECOND_MICROSECOND
| MINUTE_MICROSECOND
| HOUR_MICROSECOND
| DAY_MICROSECOND

non_microsecond_time_unit:
  YEAR
| QUARTER
| MONTH
| DAY
| HOUR
| MINUTE
| WEEK
| SECOND
| YEAR_MONTH
| DAY_HOUR
| DAY_MINUTE
| DAY_SECOND
| HOUR_MINUTE
| HOUR_SECOND
| MINUTE_SECOND

// TODO : support prepared statements
integral_or_interval_expr:
  INTEGRAL
  {
    $$ = NewIntVal($1)
  }
| INTERVAL value time_unit
  {
    $$ = &IntervalExpr{Expr: $2, Unit: string($3)}
  }

as_ci_opt:
  {
    $$ = ColIdent{}
  }
| col_alias
  {
    $$ = $1
  }
| AS col_alias
  {
    $$ = $2
  }
| AS ESCAPE
  {
    $$ = NewColIdent(string($2))
  }

col_alias:
  ID
  {
    $$ = NewColIdent(string($1))
  }
| STRING
  {
    $$ = NewColIdent(string($1))
  }
| all_non_reserved
  {
    $$ = NewColIdent(string($1))
  }

table_references:
  table_reference
  {
    $$ = TableExprs{$1}
  }
| table_references ',' table_reference
  {
    $$ = append($$, $3)
  }

table_reference:
  table_factor
| join_table

table_factor:
  aliased_table_name
  {
    $$ = $1
  }
| subquery_or_values as_opt table_alias column_list_opt
  {
    switch n := $1.(type) {
    case *Subquery:
        n.Columns = $4
    case *ValuesStatement:
        n.Columns = $4
    }
    $$ = &AliasedTableExpr{Lateral: false, Expr:$1, As: $3}
  }
| LATERAL subquery_or_values as_opt table_alias column_list_opt
  {
    switch n := $2.(type) {
    case *Subquery:
        n.Columns = $5
    case *ValuesStatement:
        n.Columns = $5
    }
    $$ = &AliasedTableExpr{Lateral: true, Expr:$2, As: $4}
  }
| subquery_or_values
  {
    // missed alias for subquery
    yylex.Error("Every derived table must have its own alias")
    return 1
  }
| LATERAL subquery_or_values
  {
    // missed alias for subquery
    yylex.Error("Every derived table must have its own alias")
    return 1
  }
| openb table_references closeb
  {
    $$ = &ParenTableExpr{Exprs: $2}
  }
| table_function
| json_table

values_statement:
  VALUES row_list
  {
    $$ = &ValuesStatement{Rows: $2}
  }

row_list:
  row_opt row_tuple
  {
    $$ = Values{$2}
  }
| row_list ',' row_opt row_tuple
  {
    $$ = append($$, $4)
  }

row_opt:
  {}
| ROW
  {}

aliased_table_name:
  table_name aliased_table_options
  {
    $$ = $2
    $$.Expr = $1
  }
| table_name PARTITION openb partition_list closeb aliased_table_options
  {
    $$ = $6
    $$.Expr = $1
    $$.Partitions = $4
  }

// All possible combinations of qualifiers for a table alias expression, declared in a single rule to avoid
// shift/reduce conflicts.
aliased_table_options:
  index_hint_list
  {
    $$ = &AliasedTableExpr{Hints: $1}
  }
| as_opt table_alias index_hint_list
  {
    $$ = &AliasedTableExpr{As: $2, Hints: $3}
  }
| as_of_clause index_hint_list
  {
    $$ = &AliasedTableExpr{AsOf: $1, Hints: $2}
  }
| as_of_clause as_opt table_alias index_hint_list
  {
    $$ = &AliasedTableExpr{AsOf: $1, As: $3, Hints: $4}
  }

as_of_clause:
  as_of_point_clause
  {
    $$ = $1
  }
| between_times
  {
    $$ = $1
  }
| between_versions
  {
    $$ = $1
  }
| all_times
  {
    $$ = $1
  }
| all_versions
  {
    $$ = $1
  }

between_times:
  FOR_SYSTEM_TIME BETWEEN value_expression AND value_expression
  {
    $$ = &AsOf{Start: $3, End: $5, EndInclusive: true}
  }
| FOR_SYSTEM_TIME FROM value_expression TO value_expression
  {
    $$ = &AsOf{Start: $3, End: $5}
  }
| FOR_SYSTEM_TIME CONTAINED IN openb value_expression ',' value_expression closeb
  {
    $$ = &AsOf{Start: $5, End: $7, StartInclusive: true, EndInclusive: true}
  }

between_versions:
  FOR_VERSION BETWEEN value_expression AND value_expression
  {
    $$ = &AsOf{Start: $3, End: $5, EndInclusive: true}
  }
| FOR_VERSION FROM value_expression TO value_expression
  {
    $$ = &AsOf{Start: $3, End: $5}
  }
| FOR_VERSION CONTAINED IN openb value_expression ',' value_expression closeb
  {
    $$ = &AsOf{Start: $5, End: $7, StartInclusive: true, EndInclusive: true}
  }
| VERSIONS BETWEEN value_expression AND value_expression
  {
    $$ = &AsOf{Start: $3, End: $5, EndInclusive: true}
  }
| VERSIONS FROM value_expression TO value_expression
  {
    $$ = &AsOf{Start: $3, End: $5}
  }
| VERSIONS CONTAINED IN openb value_expression ',' value_expression closeb
  {
    $$ = &AsOf{Start: $5, End: $7, StartInclusive: true, EndInclusive: true}
  }

all_times:
  FOR_SYSTEM_TIME ALL
  {
    $$ = &AsOf{All: true}
  }

all_versions:
  FOR_VERSION ALL
  {
    $$ = &AsOf{All: true}
  }
| VERSIONS ALL
  {
    $$ = &AsOf{All: true}
  }

as_of_point_clause:
  AS OF value_expression
  {
    $$ = &AsOf{Time: $3}
  }
| FOR_SYSTEM_TIME AS OF value_expression
  {
    $$ = &AsOf{Time: $4}
  }
| FOR_VERSION AS OF value_expression
  {
    $$ = &AsOf{Time: $4}
  }

as_of_opt:
  {
    $$ = nil
  }
| as_of_point_clause
  {
    $$ = $1.Time
  }

paren_column_list:
  '(' column_list ')'
  {
    $$ = $2
  }
| column_list
  {
    $$ = $1
  }

column_list_opt:
  {
    $$ = nil
  }
| '(' column_list ')'
  {
    $$ = $2
  }

column_list:
  sql_id
  {
    $$ = Columns{$1}
  }
| column_list ',' sql_id
  {
    $$ = append($$, $3)
  }

partition_list:
  sql_id
  {
    $$ = Partitions{$1}
  }
| partition_list ',' sql_id
  {
    $$ = append($$, $3)
  }

table_function:
  ID openb argument_expression_list_opt closeb
  {
    $$ = &TableFuncExpr{Name: string($1), Exprs: $3}
  }
| ID openb argument_expression_list_opt closeb as_opt table_alias
  {
    $$ = &TableFuncExpr{Name: string($1), Exprs: $3, Alias: $6}
  }


// There is a grammar conflict here:
// 1: INSERT INTO a SELECT * FROM b JOIN c ON b.i = c.i
// 2: INSERT INTO a SELECT * FROM b JOIN c ON DUPLICATE KEY UPDATE a.i = 1
// When yacc encounters the ON clause, it cannot determine which way to
// resolve. The %prec override below makes the parser choose the
// first construct, which automatically makes the second construct a
// syntax error. This is the same behavior as MySQL.
join_table:
  table_reference inner_join table_factor join_condition_opt
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, Condition: $4}
  }
| table_reference straight_join table_factor on_expression_opt
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, Condition: $4}
  }
| table_reference outer_join table_reference join_condition
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, Condition: $4}
  }
| table_reference natural_join table_factor
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3}
  }

join_condition:
  ON expression
  { $$ = JoinCondition{On: $2} }
| USING '(' column_list ')'
  { $$ = JoinCondition{Using: $3} }

join_condition_opt:
%prec JOIN
  { $$ = JoinCondition{} }
| join_condition
  { $$ = $1 }

on_expression_opt:
%prec JOIN
  { $$ = JoinCondition{} }
| ON expression
  { $$ = JoinCondition{On: $2} }

table_alias:
  table_id
| column_name_safe_keyword
  {
    $$ = NewTableIdent(string($1))
  }

inner_join:
  JOIN
  {
    $$ = JoinStr
  }
| INNER JOIN
  {
    $$ = JoinStr
  }
| CROSS JOIN
  {
    $$ = JoinStr
  }

straight_join:
  STRAIGHT_JOIN
  {
    $$ = StraightJoinStr
  }

outer_join:
  LEFT JOIN
  {
    $$ = LeftJoinStr
  }
| LEFT OUTER JOIN
  {
    $$ = LeftJoinStr
  }
| RIGHT JOIN
  {
    $$ = RightJoinStr
  }
| RIGHT OUTER JOIN
  {
    $$ = RightJoinStr
  }
| FULL OUTER JOIN
  {
    $$ = FullOuterJoinStr
  }
| FULL JOIN
  {
    $$ = FullOuterJoinStr
  }

natural_join:
  NATURAL JOIN
  {
    $$ = NaturalJoinStr
  }
| NATURAL outer_join
  {
    if $2 == LeftJoinStr {
      $$ = NaturalLeftJoinStr
    } else {
      $$ = NaturalRightJoinStr
    }
  }

json_table:
  JSON_TABLE openb value_expression ',' STRING COLUMNS openb json_table_column_list closeb closeb as_opt table_alias
  {
    $8.Path = string($5)
    $$ = &JSONTableExpr{Data: $3, Spec: $8, Alias: $12}
  }

json_table_column_list:
  json_table_column_definition
  {
    $$ = &JSONTableSpec{}
    $$.AddColumn($1)
  }
| json_table_column_list ',' json_table_column_definition
  {
    $$.AddColumn($3)
  }

json_table_column_definition:
  reserved_sql_id column_type json_table_column_options
  {
    $$ = &JSONTableColDef{Name: $1, Type: $2, Opts: $3}
  }
| reserved_sql_id FOR ORDINALITY
  {
    $$ = &JSONTableColDef{Name: $1, Type: ColumnType{Type: "INTEGER", Unsigned: true, Autoincrement: true}}
  }
| NESTED STRING COLUMNS openb json_table_column_list closeb
  {
    $5.Path = string($2)
    $$ = &JSONTableColDef{Spec: $5}
  }
| NESTED PATH STRING COLUMNS openb json_table_column_list closeb
  {
    $6.Path = string($3)
    $$ = &JSONTableColDef{Spec: $6}
  }

json_table_column_options:
  PATH STRING
  {
    $$ = JSONTableColOpts{Path: string($2)}
  }
| PATH STRING val_on_empty
  {
    $$ = JSONTableColOpts{Path: string($2), ValOnEmpty: $3}
  }
| PATH STRING val_on_error
  {
    $$ = JSONTableColOpts{Path: string($2), ValOnError: $3}
  }
| PATH STRING val_on_empty val_on_error
  {
    $$ = JSONTableColOpts{Path: string($2), ValOnEmpty: $3, ValOnError: $4}
  }
| PATH STRING val_on_error val_on_empty
  {
    $$ = JSONTableColOpts{Path: string($2), ValOnEmpty: $4, ValOnError: $3}
  }
| PATH STRING ERROR ON EMPTY
  {
    $$ = JSONTableColOpts{Path: string($2), ErrorOnEmpty: true}
  }
| PATH STRING ERROR ON ERROR
  {
    $$ = JSONTableColOpts{Path: string($2), ErrorOnError: true}
  }
| PATH STRING ERROR ON EMPTY ERROR ON ERROR
  {
    $$ = JSONTableColOpts{Path: string($2), ErrorOnEmpty: true, ErrorOnError: true}
  }
| PATH STRING ERROR ON ERROR ERROR ON EMPTY
  {
    $$ = JSONTableColOpts{Path: string($2), ErrorOnEmpty: true, ErrorOnError: true}
  }
| EXISTS PATH STRING
  {
    $$ = JSONTableColOpts{Path: string($3), Exists: true}
  }

val_on_empty:
  NULL ON EMPTY
  {
    $$ = &NullVal{}
  }
| DEFAULT value ON EMPTY
  {
    $$ = $2
  }

val_on_error:
  NULL ON ERROR
  {
    $$ = &NullVal{}
  }
| DEFAULT value ON ERROR
  {
    $$ = $2
  }

trigger_name:
  sql_id
  {
    $$ = TriggerName{Name: $1}
  }
| table_id '.' sql_id
  {
    $$ = TriggerName{Qualifier: $1, Name: $3}
  }

load_into_table_name:
  INTO TABLE table_name
  {
    $$ = $3
  }

into_table_name:
  INTO table_name
  {
    $$ = $2
  }
| table_name
  {
    $$ = $1
  }

table_name:
  table_id
  {
    $$ = TableName{Name: $1}
  }
| table_id '.' reserved_table_id
  {
    $$ = TableName{DbQualifier: $1, Name: $3}
  }
| column_name_safe_keyword
  {
    $$ = TableName{Name: NewTableIdent(string($1))}
  }
| non_reserved_keyword2
  {
    $$ = TableName{Name: NewTableIdent(string($1))}
  }
| function_call_keywords
  {
    $$ = TableName{Name: NewTableIdent(string($1))}
  }
| ACCOUNT
  {
    $$ = TableName{Name: NewTableIdent(string($1))}
  }

procedure_name:
  sql_id
  {
    $$ = ProcedureName{Name: $1}
  }
| table_id '.' sql_id
  {
    $$ = ProcedureName{Qualifier: $1, Name: $3}
  }

delete_table_name:
table_id '.' '*'
  {
    $$ = TableName{Name: $1}
  }

index_hint_list:
  {
    $$ = nil
  }
| USE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: UseStr, Indexes: $4}
  }
| IGNORE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: IgnoreStr, Indexes: $4}
  }
| FORCE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: ForceStr, Indexes: $4}
  }

where_expression_opt:
  {
    $$ = nil
  }
| WHERE expression
  {
    $$ = $2
  }

expression:
  condition
  {
    $$ = $1
  }
| expression AND expression
  {
    $$ = &AndExpr{Left: $1, Right: $3}
  }
| expression OR expression
  {
    $$ = &OrExpr{Left: $1, Right: $3}
  }
| expression XOR expression
  {
    $$ = &XorExpr{Left: $1, Right: $3}
  }
| NOT expression
  {
    $$ = &NotExpr{Expr: $2}
  }
| expression IS is_suffix
  {
    $$ = &IsExpr{Operator: $3, Expr: $1}
  }
| value_expression
  {
    $$ = $1
  }
| DEFAULT default_opt
  {
    $$ = &Default{ColName: $2}
  }

default_opt:
  /* empty */
  {
    $$ = ""
  }
| openb ID closeb
  {
    $$ = string($2)
  }

boolean_value:
  TRUE
  {
    $$ = BoolVal(true)
  }
| FALSE
  {
    $$ = BoolVal(false)
  }

condition:
  value_expression compare value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }
| value_expression IN col_tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: InStr, Right: $3}
  }
| value_expression NOT IN col_tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotInStr, Right: $4}
  }
| value_expression LIKE value_expression like_escape_opt
  {
    $$ = &ComparisonExpr{Left: $1, Operator: LikeStr, Right: $3, Escape: $4}
  }
| value_expression NOT LIKE value_expression like_escape_opt
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotLikeStr, Right: $4, Escape: $5}
  }
| value_expression REGEXP value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: RegexpStr, Right: $3}
  }
| value_expression NOT REGEXP value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotRegexpStr, Right: $4}
  }
| value_expression BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: BetweenStr, From: $3, To: $5}
  }
| value_expression NOT BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: NotBetweenStr, From: $4, To: $6}
  }
| EXISTS subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }

is_suffix:
  NULL
  {
    $$ = IsNullStr
  }
| NOT NULL
  {
    $$ = IsNotNullStr
  }
| TRUE
  {
    $$ = IsTrueStr
  }
| NOT TRUE
  {
    $$ = IsNotTrueStr
  }
| FALSE
  {
    $$ = IsFalseStr
  }
| NOT FALSE
  {
    $$ = IsNotFalseStr
  }

compare:
  '='
  {
    $$ = EqualStr
  }
| '<'
  {
    $$ = LessThanStr
  }
| '>'
  {
    $$ = GreaterThanStr
  }
| LE
  {
    $$ = LessEqualStr
  }
| GE
  {
    $$ = GreaterEqualStr
  }
| NE
  {
    $$ = NotEqualStr
  }
| NULL_SAFE_EQUAL
  {
    $$ = NullSafeEqualStr
  }

like_escape_opt:
  {
    $$ = nil
  }
| ESCAPE value_expression
  {
    $$ = $2
  }

col_tuple:
  row_tuple
  {
    $$ = $1
  }
| subquery
  {
    $$ = $1
  }
| LIST_ARG
  {
    $$ = ListArg($1)
  }

subquery:
  openb select_statement_with_no_trailing_into closeb
  {
    $$ = &Subquery{Select: $2}
  }

subquery_or_values:
  subquery
  {
    $$ = $1
  }
| openb values_statement closeb
  {
    $$ = $2
  }

argument_expression_list_opt:
  {
    $$ = nil
  }
| argument_expression_list

argument_expression_list:
  argument_expression
  {
    $$ = SelectExprs{$1}
  }
| argument_expression_list ',' argument_expression
  {
    $$ = append($1, $3)
  }

expression_list:
  expression
  {
    $$ = Exprs{$1}
  }
| expression_list ',' expression
  {
    $$ = append($1, $3)
  }

value_expression:
  value
  {
    $$ = $1
  }
| ACCOUNT
  {
    $$ = &ColName{Name: NewColIdent(string($1))}
  }
| FORMAT
  {
    $$ = &ColName{Name: NewColIdent(string($1))}
  }
| boolean_value
  {
    $$ = $1
  }
| column_name
  {
    $$ = $1
  }
| column_name_safe_keyword
  {
    $$ = &ColName{Name: NewColIdent(string($1))}
  }
| tuple_expression
  {
    $$ = $1
  }
| subquery
  {
    $$ = $1
  }
| value_expression '&' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: BitAndStr, Right: $3}
  }
| value_expression '|' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: BitOrStr, Right: $3}
  }
| value_expression '^' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: BitXorStr, Right: $3}
  }
| value_expression '+' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: PlusStr, Right: $3}
  }
| value_expression '-' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: MinusStr, Right: $3}
  }
| value_expression '*' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: MultStr, Right: $3}
  }
| value_expression '/' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: DivStr, Right: $3}
  }
| value_expression DIV value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: IntDivStr, Right: $3}
  }
| value_expression '%' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ModStr, Right: $3}
  }
| value_expression MOD value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ModStr, Right: $3}
  }
| value_expression SHIFT_LEFT value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ShiftLeftStr, Right: $3}
  }
| value_expression SHIFT_RIGHT value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ShiftRightStr, Right: $3}
  }
| column_name JSON_EXTRACT_OP value
  {
    $$ = &BinaryExpr{Left: $1, Operator: JSONExtractOp, Right: $3}
  }
| column_name JSON_UNQUOTE_EXTRACT_OP value
  {
    $$ = &BinaryExpr{Left: $1, Operator: JSONUnquoteExtractOp, Right: $3}
  }
| value_expression COLLATE charset
  {
    $$ = &CollateExpr{Expr: $1, Collation: $3}
  }
| BINARY value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BinaryStr, Expr: $2}
  }
| underscore_charsets value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: $1, Expr: $2}
  }
| '+'  value_expression %prec UNARY
  {
    if num, ok := $2.(*SQLVal); ok && num.Type == IntVal {
      $$ = num
    } else {
      $$ = &UnaryExpr{Operator: UPlusStr, Expr: $2}
    }
  }
| '-'  value_expression %prec UNARY
  {
    if num, ok := $2.(*SQLVal); ok && num.Type == IntVal {
      // Handle double negative
      if num.Val[0] == '-' {
        num.Val = num.Val[1:]
        $$ = num
      } else {
        $$ = NewIntVal(append([]byte("-"), num.Val...))
      }
    } else {
      $$ = &UnaryExpr{Operator: UMinusStr, Expr: $2}
    }
  }
| '~'  value_expression
  {
    $$ = &UnaryExpr{Operator: TildaStr, Expr: $2}
  }
| '!' value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BangStr, Expr: $2}
  }
| INTERVAL value_expression sql_id
  {
    // This rule prevents the usage of INTERVAL
    // as a function. If support is needed for that,
    // we'll need to revisit this. The solution
    // will be non-trivial because of grammar conflicts.
    $$ = &IntervalExpr{Expr: $2, Unit: $3.String()}
  }
| function_call_generic
| function_call_keyword
| function_call_nonkeyword
| function_call_conflict
| function_call_window
| function_call_aggregate_with_window

/*
  Regular function calls without special token or syntax, guaranteed to not
  introduce side effects due to being a simple identifier
*/
function_call_generic:
  sql_id openb distinct_opt argument_expression_list_opt closeb
  {
    $$ = &FuncExpr{Name: $1, Distinct: $3 == DistinctStr, Exprs: $4}
  }
| table_id '.' reserved_sql_id openb argument_expression_list_opt closeb
  {
    $$ = &FuncExpr{Qualifier: $1, Name: $3, Exprs: $5}
  }

/*
   Special aggregate function calls that can't be treated like a normal function call, because they have an optional
   OVER clause (not legal on any other non-window, non-aggregate function)
 */
function_call_aggregate_with_window:
 MAX openb distinct_opt argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $4, Distinct: $3 == DistinctStr, Over: $6}
  }
| AVG openb distinct_opt argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $4, Distinct: $3 == DistinctStr, Over: $6}
  }
| BIT_AND openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| BIT_OR openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| BIT_XOR openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| COUNT openb distinct_opt argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $4, Distinct: $3 == DistinctStr, Over: $6}
  }
| JSON_ARRAYAGG openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| JSON_OBJECTAGG openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| MIN openb distinct_opt argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $4, Distinct: $3 == DistinctStr, Over: $6}
  }
| STDDEV_POP openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| STDDEV openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| STD openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| STDDEV_SAMP openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| SUM openb distinct_opt argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $4, Distinct: $3 == DistinctStr, Over: $6}
  }
| VAR_POP openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| VARIANCE openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| VAR_SAMP openb argument_expression_list closeb over_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }

/*
  Function calls with an OVER expression, only valid for certain aggregate and window functions
*/
function_call_window:
  CUME_DIST openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }
| DENSE_RANK openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }
| FIRST_VALUE openb argument_expression closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{$3}, Over: $5}
  }
| LAG openb argument_expression_list closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| LAST_VALUE openb argument_expression closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{$3}, Over: $5}
  }
| LEAD openb argument_expression_list closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| NTH_VALUE openb argument_expression_list closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3, Over: $5}
  }
| NTILE openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }
| PERCENT_RANK openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }
| RANK openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }
| ROW_NUMBER openb closeb over
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Over: $4}
  }

/*
  Function calls using reserved keywords, with dedicated grammar rules
  as a result
  TODO: some of these change the case or even the name of the function expression. Should be preserved.
*/
function_call_keyword:
  LEFT openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| RIGHT openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| FORMAT openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| GROUPING openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| SCHEMA openb closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1))}
  }
| CONVERT openb expression ',' convert_type closeb
  {
    $$ = &ConvertExpr{Name: string($1), Expr: $3, Type: $5}
  }
| CAST openb expression AS convert_type closeb
  {
    $$ = &ConvertExpr{Name: string($1), Expr: $3, Type: $5}
  }
| CHAR openb argument_expression_list closeb
  {
    $$ = &CharExpr{Exprs: $3}
  }
| CHAR openb argument_expression_list USING charset closeb
  {
    $$ = &CharExpr{Exprs: $3, Type: $5}
  }
| CONVERT openb expression USING charset closeb
  {
    $$ = &ConvertUsingExpr{Expr: $3, Type: $5}
  }
| POSITION openb value_expression IN value_expression closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("LOCATE"), Exprs: []SelectExpr{&AliasedExpr{Expr: $3}, &AliasedExpr{Expr: $5}}}
  }
| INSERT openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| SUBSTR openb column_name FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{Name: $3, From: $5, To: $7}
  }
| SUBSTRING openb column_name FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{Name: $3, From: $5, To: $7}
  }
| SUBSTR openb STRING FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{StrVal: NewStrVal($3), From: $5, To: $7}
  }
| SUBSTRING openb STRING FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{StrVal: NewStrVal($3), From: $5, To: $7}
  }
| TRIM openb value_expression closeb
  {
    $$ = &TrimExpr{Pattern: NewStrVal([]byte(" ")), Str: $3, Dir: Both}
  }
| TRIM openb value_expression FROM value_expression closeb
  {
    $$ = &TrimExpr{Pattern: $3, Str: $5, Dir: Both}
  }
| TRIM openb LEADING value_expression FROM value_expression closeb
  {
    $$ = &TrimExpr{Pattern: $4, Str: $6, Dir: Leading}
  }
| TRIM openb TRAILING value_expression FROM value_expression closeb
  {
    $$ = &TrimExpr{Pattern: $4, Str: $6, Dir: Trailing}
  }
| TRIM openb BOTH value_expression FROM value_expression closeb
  {
    $$ = &TrimExpr{Pattern: $4, Str: $6, Dir: Both}
  }
| MATCH openb argument_expression_list closeb AGAINST openb value_expression match_option closeb
  {
  $$ = &MatchExpr{Columns: $3, Expr: $7, Option: $8}
  }
| FIRST openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| GROUP_CONCAT openb distinct_opt argument_expression_list order_by_opt separator_opt closeb
  {
    $$ = &GroupConcatExpr{Distinct: $3, Exprs: $4, OrderBy: $5, Separator: $6}
  }
| CASE expression_opt when_expression_list else_expression_opt END
  {
    $$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
  }
| VALUES openb column_name closeb
  {
    $$ = &ValuesFuncExpr{Name: $3}
  }
| VALUES openb column_name_safe_keyword closeb
  {
    $$ = &ValuesFuncExpr{Name: NewColName(string($3))}
  }
// TODO: non_reserved_keyword and non_reserved_keyword2 cause grammar conflicts
| VALUES openb non_reserved_keyword3 closeb
  {
    $$ = &ValuesFuncExpr{Name: NewColName(string($3))}
  }
| REPEAT openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }

/*
  Function calls using non reserved keywords but with special syntax forms.
  Dedicated grammar rules are needed because of the special syntax
*/
function_call_nonkeyword:
// functions that do not support fractional second precision (fsp)
  CURRENT_DATE func_parens_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1))}
  }
| CURRENT_USER func_parens_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1))}
  }
| UTC_DATE func_parens_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1))}
  }
// functions that can be called with optional second argument
| function_call_on_update
  {
    $$ = $1
  }
| CURRENT_TIME func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }
| UTC_TIME func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }
| UTC_TIMESTAMP func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }
| TIMESTAMPADD openb time_unit ',' value_expression ',' value_expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampadd"), Unit:string($3), Expr1:$5, Expr2:$7}
  }
| TIMESTAMPDIFF openb time_unit ',' value_expression ',' value_expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampdiff"), Unit:string($3), Expr1:$5, Expr2:$7}
  }
| EXTRACT openb time_unit FROM value_expression closeb
  {
    $$ = &ExtractFuncExpr{Name: string($1), Unit: string($3), Expr: $5}
  }

// functions that can be used with the ON UPDATE clause
function_call_on_update:
  // NOW is special; it can't be called without parentheses
  NOW openb closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1))}
  }
| NOW openb INTEGRAL closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: NewIntVal($3)}}}
  }
| CURRENT_TIMESTAMP func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }
| LOCALTIME func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }
| LOCALTIMESTAMP func_datetime_prec_opt
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: SelectExprs{&AliasedExpr{Expr: $2}}}
  }

// Optional parens for certain keyword functions that don't require them.
func_parens_opt:
  /* empty */
| openb closeb

// Optional datetime precision for certain functions.
func_datetime_prec_opt:
  // no arg is the same as 0
  func_parens_opt
  {
    $$ = NewIntVal([]byte("0"))
  }
| openb INTEGRAL closeb
  {
    $$ = NewIntVal($2)
  }

/*
  Function calls using non reserved keywords with *normal* syntax forms. Because
  the names are non-reserved, they need a dedicated rule so as not to conflict
*/
function_call_conflict:
  IF openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| DATABASE openb argument_expression_list_opt closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| MOD openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| REPLACE openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| SUBSTR openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }
| SUBSTRING openb argument_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent(string($1)), Exprs: $3}
  }

match_option:
/*empty*/
  {
    $$ = ""
  }
| IN BOOLEAN MODE
  {
    $$ = BooleanModeStr
  }
| IN NATURAL LANGUAGE MODE
 {
    $$ = NaturalLanguageModeStr
 }
| IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
 {
    $$ = NaturalLanguageModeWithQueryExpansionStr
 }
| WITH QUERY EXPANSION
 {
    $$ = QueryExpansionStr
 }

charset:
  ID
{
    $$ = string($1)
}
| STRING
{
    $$ = string($1)
}
| BINARY
{
    $$ = string($1)
}

underscore_charsets:
  UNDERSCORE_ARMSCII8
  {
    $$ = Armscii8Str
  }
| UNDERSCORE_ASCII
  {
    $$ = AsciiStr
  }
| UNDERSCORE_BIG5
  {
    $$ = Big5Str
  }
| UNDERSCORE_BINARY
  {
    $$ = UBinaryStr
  }
| UNDERSCORE_CP1250
  {
    $$ = Cp1250Str
  }
| UNDERSCORE_CP1251
  {
    $$ = Cp1251Str
  }
| UNDERSCORE_CP1256
  {
    $$ = Cp1256Str
  }
| UNDERSCORE_CP1257
  {
    $$ = Cp1257Str
  }
| UNDERSCORE_CP850
  {
    $$ = Cp850Str
  }
| UNDERSCORE_CP852
  {
    $$ = Cp852Str
  }
| UNDERSCORE_CP866
  {
    $$ = Cp866Str
  }
| UNDERSCORE_CP932
  {
    $$ = Cp932Str
  }
| UNDERSCORE_DEC8
  {
    $$ = Dec8Str
  }
| UNDERSCORE_EUCJPMS
  {
    $$ = EucjpmsStr
  }
| UNDERSCORE_EUCKR
  {
    $$ = EuckrStr
  }
| UNDERSCORE_GB18030
  {
    $$ = Gb18030Str
  }
| UNDERSCORE_GB2312
  {
    $$ = Gb2312Str
  }
| UNDERSCORE_GBK
  {
    $$ = GbkStr
  }
| UNDERSCORE_GEOSTD8
  {
    $$ = Geostd8Str
  }
| UNDERSCORE_GREEK
  {
    $$ = GreekStr
  }
| UNDERSCORE_HEBREW
  {
    $$ = HebrewStr
  }
| UNDERSCORE_HP8
  {
    $$ = Hp8Str
  }
| UNDERSCORE_KEYBCS2
  {
    $$ = Keybcs2Str
  }
| UNDERSCORE_KOI8R
  {
    $$ = Koi8rStr
  }
| UNDERSCORE_KOI8U
  {
    $$ = Koi8uStr
  }
| UNDERSCORE_LATIN1
  {
    $$ = Latin1Str
  }
| UNDERSCORE_LATIN2
  {
    $$ = Latin2Str
  }
| UNDERSCORE_LATIN5
  {
    $$ = Latin5Str
  }
| UNDERSCORE_LATIN7
  {
    $$ = Latin7Str
  }
| UNDERSCORE_MACCE
  {
    $$ = MacceStr
  }
| UNDERSCORE_MACROMAN
  {
    $$ = MacromanStr
  }
| UNDERSCORE_SJIS
  {
    $$ = SjisStr
  }
| UNDERSCORE_SWE7
  {
    $$ = Swe7Str
  }
| UNDERSCORE_TIS620
  {
    $$ = Tis620Str
  }
| UNDERSCORE_UCS2
  {
    $$ = Ucs2Str
  }
| UNDERSCORE_UJIS
  {
    $$ = UjisStr
  }
| UNDERSCORE_UTF16
  {
    $$ = Utf16Str
  }
| UNDERSCORE_UTF16LE
  {
    $$ = Utf16leStr
  }
| UNDERSCORE_UTF32
  {
    $$ = Utf32Str
  }
| UNDERSCORE_UTF8
  {
    $$ = Utf8mb3Str
  }
| UNDERSCORE_UTF8MB3
  {
    $$ = Utf8mb3Str
  }
| UNDERSCORE_UTF8MB4
  {
    $$ = Utf8mb4Str
  }

convert_type:
  BINARY length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| CHAR length_opt charset_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: $3, Operator: CharacterSetStr}
  }
| CHAR length_opt ID
  {
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: string($3)}
  }
| CHARACTER length_opt charset_opt
  {
    $$ = &ConvertType{Type: "CHAR", Length: $2, Charset: $3, Operator: CharacterSetStr}
  }
| CHARACTER length_opt ID
  {
    $$ = &ConvertType{Type: "CHAR", Length: $2, Charset: string($3)}
  }
| DATE
  {
    $$ = &ConvertType{Type: string($1)}
  }
| DATETIME length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| DECIMAL decimal_length_opt
  {
    $$ = &ConvertType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DOUBLE
  {
    $$ = &ConvertType{Type: string($1)}
  }
| DOUBLE PRECISION
  {
    $$ = &ConvertType{Type: string($1)}
  }
| REAL
  {
    $$ = &ConvertType{Type: string($1)}
  }
| FLOAT_TYPE
  {
    $$ = &ConvertType{Type: string($1)}
  }
| JSON
  {
    $$ = &ConvertType{Type: string($1)}
  }
| NCHAR length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| SIGNED
  {
    $$ = &ConvertType{Type: string($1)}
  }
| SIGNED INTEGER
  {
    $$ = &ConvertType{Type: string($1)}
  }
| TIME length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| UNSIGNED
  {
    $$ = &ConvertType{Type: string($1)}
  }
| UNSIGNED INTEGER
  {
    $$ = &ConvertType{Type: string($1)}
  }
| YEAR
  {
    $$ = &ConvertType{Type: string($1)}
  }

char_or_character:
  CHAR
  {
    $$ = $1
  }
| CHARACTER
  {
    $$ = $1
  }

expression_opt:
  {
    $$ = nil
  }
| expression
  {
    $$ = $1
  }

separator_opt:
  {
    $$ = Separator{SeparatorString: "", DefaultSeparator: true}
  }
| SEPARATOR STRING
  {
    $$ = Separator{SeparatorString: string($2), DefaultSeparator: false}
  }

when_expression_list:
  when_expression
  {
    $$ = []*When{$1}
  }
| when_expression_list when_expression
  {
    $$ = append($1, $2)
  }

when_expression:
  WHEN expression THEN expression
  {
    $$ = &When{Cond: $2, Val: $4}
  }

else_expression_opt:
  {
    $$ = nil
  }
| ELSE expression
  {
    $$ = $2
  }

column_name:
  sql_id
  {
    $$ = &ColName{Name: $1}
  }
| non_reserved_keyword2
  {
    $$ = &ColName{Name: NewColIdent(string($1))}
  }
| table_id '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: $3}
  }
| table_id '.' non_reserved_keyword2
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: NewColIdent(string($3))}
  }
| table_id '.' column_name_safe_keyword
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: NewColIdent(string($3))}
  }
| table_id '.' ACCOUNT
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: NewColIdent(string($3))}
  }
| table_id '.' FORMAT
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: NewColIdent(string($3))}
  }
| column_name_safe_keyword '.' sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: $3}
  }
| qualified_column_name_safe_reserved_keyword '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: $3}
  }
| non_reserved_keyword2 '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: $3}
  }
| non_reserved_keyword2 '.' FULL
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: NewColIdent(string($3))}
  }
| ACCOUNT '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: $3}
  }
| FORMAT '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: NewTableIdent(string($1))}, Name: $3}
  }
| function_call_keywords
  {
    $$ = &ColName{Name: NewColIdent(string($1))}
  }
| table_id '.' reserved_table_id '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{DbQualifier: $1, Name: $3}, Name: $5}
  }

value:
  STRING
  {
    $$ = NewStrVal($1)
  }
| DATE STRING
  {
    $$ = NewStrVal($2)
  }
| TIME STRING
  {
    $$ = NewStrVal($2)
  }
| TIMESTAMP STRING
  {
    $$ = NewStrVal($2)
  }
| HEX
  {
    $$ = NewHexVal($1)
  }
| BIT_LITERAL
  {
    $$ = NewBitVal($1)
  }
| INTEGRAL
  {
    $$ = NewIntVal($1)
  }
| FLOAT
  {
    $$ = NewFloatVal($1)
  }
| HEXNUM
  {
    $$ = NewHexNum($1)
  }
| VALUE_ARG
  {
    $$ = NewValArg($1)
  }
| NULL
  {
    $$ = &NullVal{}
  }

num_val:
  VALUE
  {
    $$ = NewIntVal([]byte("1"))
  }
| INTEGRAL VALUES
  {
    $$ = NewIntVal($1)
  }
| VALUE_ARG VALUES
  {
    $$ = NewValArg($1)
  }

group_by_opt:
  {
    $$ = nil
  }
| GROUP BY group_by_list
  {
    $$ = $3
  }

group_by_list:
  group_by
  {
    $$ = Exprs{$1}
  }
| group_by_list ',' group_by
  {
    $$ = append($1, $3)
  }

group_by:
  expression
  {
    $$ = $1
  }

having_opt:
  {
    $$ = nil
  }
| HAVING having
  {
    $$ = $2
  }

having:
  expression
  {
    $$ = $1
  }

order_by_opt:
  {
    $$ = nil
  }
| ORDER BY order_list
  {
    $$ = $3
  }

order_list:
  order
  {
    $$ = OrderBy{$1}
  }
| order_list ',' order
  {
    $$ = append($1, $3)
  }

order:
  expression asc_desc_opt
  {
    $$ = &Order{Expr: $1, Direction: $2}
  }

asc_desc_opt:
  {
    $$ = AscScr
  }
| ASC
  {
    $$ = AscScr
  }
| DESC
  {
    $$ = DescScr
  }

limit_opt:
  {
    $$ = nil
  }
| LIMIT limit_val
  {
    $$ = &Limit{Rowcount: $2}
  }
| LIMIT limit_val ',' limit_val
  {
    $$ = &Limit{Offset: $2, Rowcount: $4}
  }
| LIMIT limit_val OFFSET limit_val
  {
    $$ = &Limit{Offset: $4, Rowcount: $2}
  }

limit_val:
INTEGRAL
  {
    $$ = NewIntVal($1)
  }
| VALUE_ARG
  {
    $$ = NewValArg($1)
  }
| column_name
  {
    $$ = $1
  }

lock_opt:
  {
    $$ = ""
  }
| FOR UPDATE
  {
    $$ = ForUpdateStr
  }
| FOR UPDATE SKIP LOCKED
  {
    $$ = ForUpdateSkipLockedStr
  }
| LOCK IN SHARE MODE
  {
    $$ = ShareModeStr
  }

insert_data_alias:
  insert_data
  {
    $$ = $1
  }
| insert_data as_opt table_alias column_list_opt
  {
    $$ = $1
    // Rows is guarenteed to be an *AliasedValues here.
    rows := $$.Rows.(*AliasedValues)
    rows.As = $3
    if $4 != nil {
        rows.Columns = $4
    }
    $$.Rows = rows
  }

// insert_data expands all combinations into a single rule.
// This avoids a shift/reduce conflict while encountering the
// following two possible constructs:
// insert into t1(a, b) (select * from t2)
// insert into t1(select * from t2)
// Because the rules are together, the parser can keep shifting
// the tokens until it disambiguates a as sql_id and select as keyword.
insert_data:
  insert_data_values
  {
    $$ = $1
  }
| openb closeb insert_data_values
  {
    $3.Columns = []ColIdent{}
    $$ = $3
  }
| openb ins_column_list closeb insert_data_values
  {
    $4.Columns = $2
    $$ = $4
  }

insert_data_select:
  select_statement_with_no_trailing_into
  {
    $$ = &Insert{Rows: $1}
  }
| openb ins_column_list closeb select_statement_with_no_trailing_into
  {
    $$ = &Insert{Columns: $2, Rows: $4}
  }
| openb select_statement_with_no_trailing_into closeb
  {
    // Drop the redundant parenthesis.
    $$ = &Insert{Rows: $2}
  }
| openb ins_column_list closeb openb select_statement_with_no_trailing_into closeb
  {
    // Drop the redundant parenthesis.
    $$ = &Insert{Columns: $2, Rows: $5}
  }

insert_data_values:
  value_or_values tuple_list
  {
    $$ = &Insert{Rows: &AliasedValues{Values: $2}}
  }
| openb insert_data_values closeb
  {
    $$ = $2
  }

value_or_values:
  VALUES
| VALUE

ins_column_list_opt:
  {
    $$ = nil
  }
| openb ins_column_list closeb
  {
    $$ = $2
  }

ins_column_list:
  ins_column
  {
    $$ = Columns{$1}
  }
| ins_column_list ',' ins_column
  {
    $$ = append($$, $3)
  }

ins_column:
 reserved_sql_id '.' reserved_sql_id // TODO: This throws away the qualifier, not a huge deal for insert into, but is incorrect
  {
    $$ = $3
  }
| reserved_sql_id
  {
    $$ = $1
  }
| column_name_safe_keyword
  {
    $$ = NewColIdent(string($1))
  }
| non_reserved_keyword2
  {
    $$ = NewColIdent(string($1))
  }
| non_reserved_keyword3
  {
    $$ = NewColIdent(string($1))
  }
| ESCAPE
  {
    $$ = NewColIdent(string($1))
  }

on_dup_opt:
  {
    $$ = nil
  }
| ON DUPLICATE KEY UPDATE assignment_list
  {
    $$ = $5
  }

tuple_list:
  tuple_or_empty
  {
    $$ = Values{$1}
  }
| tuple_list ',' tuple_or_empty
  {
    $$ = append($1, $3)
  }

tuple_or_empty:
  row_opt row_tuple
  {
    $$ = $2
  }
| row_opt openb closeb
  {
    $$ = ValTuple{}
  }

row_tuple:
  openb expression_list closeb
  {
    $$ = ValTuple($2)
  }

tuple_expression:
  row_tuple
  {
    if len($1) == 1 {
      $$ = &ParenExpr{$1[0]}
    } else {
      $$ = $1
    }
  }

assignment_list:
  assignment_expression
  {
    $$ = AssignmentExprs{$1}
  }
| assignment_list ',' assignment_expression
  {
    $$ = append($1, $3)
  }

assignment_expression:
  column_name assignment_op expression
  {
    $$ = &AssignmentExpr{Name: $1, Expr: $3}
  }
| column_name_safe_keyword assignment_op expression {
    $$ = &AssignmentExpr{Name: &ColName{Name: NewColIdent(string($1))}, Expr: $3}
  }
| non_reserved_keyword3 assignment_op expression
  {
    $$ = &AssignmentExpr{Name: &ColName{Name: NewColIdent(string($1))}, Expr: $3}
  }
| ESCAPE assignment_op expression
  {
    $$ = &AssignmentExpr{Name: &ColName{Name: NewColIdent(string($1))}, Expr: $3}
  }

set_list:
  set_expression
  {
    $$ = SetVarExprs{$1}
  }
| set_list ',' set_expression
  {
    $$ = append($1, $3)
  }

set_expression:
  set_expression_assignment
  {
    colName, scope, _, err := VarScopeForColName($1.Name)
    if err != nil {
      yylex.Error(err.Error())
      return 1
    }
    $1.Name = colName
    $1.Scope = scope
    $$ = $1
  }
| set_scope_primary set_expression_assignment
  {
    _, scope, _, err := VarScopeForColName($2.Name)
    if err != nil {
      yylex.Error(err.Error())
      return 1
    } else if scope != SetScope_None {
      yylex.Error(fmt.Sprintf("invalid system variable name `%s`", $2.Name.Name.val))
      return 1
    }
    $2.Scope = $1
    $$ = $2
  }
| set_scope_secondary set_expression_assignment
  {
    _, scope, _, err := VarScopeForColName($2.Name)
    if err != nil {
      yylex.Error(err.Error())
      return 1
    } else if scope != SetScope_None {
      yylex.Error(fmt.Sprintf("invalid system variable name `%s`", $2.Name.Name.val))
      return 1
    }
    $2.Scope = $1
    $$ = $2
  }
| charset_or_character_set charset_value collate_opt
  {
    $$ = &SetVarExpr{Name: NewColName(string($1)), Expr: $2, Scope: SetScope_Session}
  }

set_scope_primary:
  GLOBAL
  {
    $$ = SetScope_Global
  }
| SESSION
  {
    $$ = SetScope_Session
  }

set_scope_secondary:
  LOCAL
  {
    $$ = SetScope_Session
  }
| PERSIST
  {
    $$ = SetScope_Persist
  }
| PERSIST_ONLY
  {
    $$ = SetScope_PersistOnly
  }

set_expression_assignment:
  column_name assignment_op ON
  {
    $$ = &SetVarExpr{Name: $1, Expr: NewStrVal($3), Scope: SetScope_None}
  }
| column_name assignment_op OFF
  {
    $$ = &SetVarExpr{Name: $1, Expr: NewStrVal($3), Scope: SetScope_None}
  }
| column_name assignment_op STRING STRING
  {
    // NOTE: This is a fix to allow MySQL dumps to load cleanly when they contain the following:
    //       SET @@GLOBAL.GTID_PURGED= /*!80000 '+'*/ 'beabe64c-9dc6-11ed-8021-a0f9021e8e70:1-126';
    //       The full fix is for any adjacent single-quoted or double-quoted strings to be concatenated but
    //       this fixes the most pressing case. For more details, see: https://github.com/dolthub/dolt/issues/5232
    // In other places we can correctly concatenate adjacent string literals, but the special comments break it
    $$ = &SetVarExpr{Name: $1, Expr: NewStrVal([]byte(string($3)+string($4))), Scope: SetScope_None}
  }
| column_name assignment_op expression
  {
    $$ = &SetVarExpr{Name: $1, Expr: $3, Scope: SetScope_None}
  }

charset_or_character_set:
  CHARSET
| CHARACTER SET
  {
    $$ = []byte("charset")
  }
| NAMES

charset_value:
  sql_id
  {
    $$ = NewStrVal([]byte($1.String()))
  }
| STRING
  {
    $$ = NewStrVal($1)
  }
| DEFAULT
  {
    $$ = &Default{}
  }

for_from:
  FOR
| FROM

temp_opt:
  { $$ = 0 }
| TEMPORARY
  { $$ = 1 }

exists_opt:
  { $$ = 0 }
| IF EXISTS
  { $$ = 1 }

not_exists_opt:
  { $$ = 0 }
| IF NOT EXISTS
  { $$ = 1 }

ignore_opt:
  { $$ = "" }
| IGNORE
  { $$ = IgnoreStr }

ignore_number_opt:
  { $$ = nil }
| IGNORE INTEGRAL LINES
  { $$ = NewIntVal($2) }
| IGNORE INTEGRAL ROWS
  { $$ = NewIntVal($2) }

to_or_as_opt:
  { $$ = struct{}{} }
| to_or_as
  { $$ = struct{}{} }

to_or_as:
  TO
  { $$ = struct{}{} }
| AS
  { $$ = struct{}{} }

as_opt:
  { $$ = struct{}{} }
| AS
  { $$ = struct{}{} }

key_type:
  UNIQUE
  { $$ = UniqueStr }
| FULLTEXT
  { $$ = FulltextStr }
| SPATIAL
  { $$ = SpatialStr }

key_type_opt:
  { $$ = "" }
| key_type
  { $$ = $1 }

using_opt:
  { $$ = ColIdent{} }
| USING sql_id
  { $$ = $2 }

sql_id:
  ID
  {
    $$ = NewColIdent(string($1))
  }
| non_reserved_keyword
  {
    $$ = NewColIdent(string($1))
  }

reserved_sql_id_list:
  reserved_sql_id
  {
    $$ = []ColIdent{$1}
  }
| reserved_sql_id_list ',' reserved_sql_id
  {
    $$ = append($$, $3)
  }

reserved_sql_id:
  sql_id
| reserved_keyword
  {
    $$ = NewColIdent(string($1))
  }

table_id:
  ID
  {
    $$ = NewTableIdent(string($1))
  }
| non_reserved_keyword
  {
    $$ = NewTableIdent(string($1))
  }

reserved_table_id:
  table_id
| reserved_keyword
  {
    $$ = NewTableIdent(string($1))
  }
| non_reserved_keyword2
  {
    $$ = NewTableIdent(string($1))
  }
| non_reserved_keyword3
  {
    $$ = NewTableIdent(string($1))
  }

infile_opt:
  { $$ = string("") }
| INFILE STRING
  { $$ = string($2)}

ignore_or_replace_opt:
  { $$ = string("") }
| IGNORE
  { $$ = IgnoreStr }
| REPLACE
  { $$ = ReplaceStr }

local_opt:
  { $$ = BoolVal(false) }
| LOCAL
  { $$ = BoolVal(true) }

enclosed_by_opt:
  {
    $$ = nil
  }
| optionally_opt ENCLOSED BY STRING
  {
    $$ = &EnclosedBy{Optionally: $1, Delim: NewStrVal($4)}
  }

optionally_opt:
  {
    $$ = BoolVal(false)
  }
| OPTIONALLY
  {
    $$ = BoolVal(true)
  }

terminated_by_opt:
  {
    $$ = nil
  }
| TERMINATED BY STRING
  {
    $$ = NewStrVal($3)
  }

escaped_by_opt:
  {
    $$ = nil
  }
| ESCAPED BY STRING
  {
    $$ = NewStrVal($3)
  }

// TODO: support any number of enclosed_by_opt
// TODO: support any number of escaped_by_opt
// TODO: escaped_by_opt and enclosed_by_opt can appear in any order
fields_opt:
  {
    $$ = nil
  }
| columns_or_fields terminated_by_opt enclosed_by_opt escaped_by_opt
  {
    $$ = &Fields{TerminatedBy: $2, EnclosedBy: $3, EscapedBy: $4}
  }

lines_opt:
  {
    $$ = nil
  }
| LINES starting_by_opt terminated_by_opt
  {
    $$ = &Lines{StartingBy: $2, TerminatedBy: $3}
  }

starting_by_opt:
  {
    $$ = nil
  }
| STARTING BY STRING
  {
    $$ = NewStrVal($3)
  }

lock_statement:
  LOCK TABLES lock_table_list
  {
    $$ = &LockTables{Tables: $3}
  }

lock_table_list:
  lock_table
  {
    $$ = TableAndLockTypes{$1}
  }
| lock_table_list ',' lock_table
  {
    $$ = append($1, $3)
  }

lock_table:
  table_name lock_type
  {
    $$ = &TableAndLockType{Table:&AliasedTableExpr{Expr: $1}, Lock:$2}
  }
|  table_name AS table_alias lock_type
  {
    $$ = &TableAndLockType{Table:&AliasedTableExpr{Expr: $1, As: $3}, Lock:$4}
  }

lock_type:
  READ
  {
    $$ = LockRead
  }
| READ LOCAL
  {
    $$ = LockReadLocal
  }
| WRITE
  {
    $$ = LockWrite
  }
| LOW_PRIORITY WRITE
  {
    $$ = LockLowPriorityWrite
  }

unlock_statement:
  UNLOCK TABLES
  {
    $$ = &UnlockTables{}
  }

kill_statement:
  KILL INTEGRAL
  {
    $$ = &Kill{Connection: true, ConnID: NewIntVal($2)}
  }
|  KILL QUERY INTEGRAL
  {
    $$ = &Kill{ConnID: NewIntVal($3)}
  }
| KILL CONNECTION INTEGRAL
  {
    $$ = &Kill{Connection: true, ConnID: NewIntVal($3)}
  }

/*
  Reserved keywords are keywords that MUST be backtick quoted to be used as an identifier. They cannot
  parse correctly as an identifier otherwise. These are not all necessarily reserved keywords in MySQL, but some are.
  These are reserved because they may cause parse conflicts in our grammar.

  TODO: Would be helpful to annotate which are NOT reserved in MySQL. Each of those non-reserved keywords that
        we require to be backtick quoted is a potential customer issue since there's a compatibility issue with MySQL.

  If you want to move one that is not reserved in MySQL (i.e. ESCAPE) to the
  non_reserved_keywords, you'll need to deal with any conflicts.

  Sorted alphabetically
*/
reserved_keyword:
  ACCESSIBLE
| ADD
| ALL
| ALTER
| ANALYZE
| AND
| AS
| ASC
| ASENSITIVE
| BEFORE
| BETWEEN
| BIGINT
| BINARY
| BIT_AND
| BIT_OR
| BIT_XOR
| BLOB
| BOTH
| BY
| CALL
| CASCADE
| CASE
| CHANGE
| CHAR
| CHARACTER
| CHECK
| COLLATE
| COLUMN
| CONSTRAINT
| CONDITION
| CONTINUE
| CONVERT
| CUME_DIST
| CREATE
| CROSS
| CUBE
| CURRENT_DATE
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| CURSOR
| DATABASE
| DATABASES
| DAY_HOUR
| DAY_MICROSECOND
| DAY_MINUTE
| DAY_SECOND
| DEC
| DECIMAL
| DECLARE
| DEFAULT
| DELAYED
| DELETE
| DENSE_RANK
| DESC
| DESCRIBE
| DETERMINISTIC
| DISTINCT
| DISTINCTROW
| DIV
| DOUBLE
| DROP
| DUAL
| EACH
| ELSE
| ELSEIF
| EMPTY
| ENCLOSED
| ESCAPED
| EXCEPT
| EXISTS
| EXIT
| EXPLAIN
| FALSE
| FETCH
| FIRST_VALUE
| FLOAT_TYPE
| FLOAT4
| FLOAT8
| FOR
| FORCE
| FOREIGN
| FROM
| FULLTEXT
| FUNCTION
| GENERATED
| GET
| GRANT
| GROUP
| GROUPING
| GROUPS
| HAVING
| HIGH_PRIORITY
| HOUR_MICROSECOND
| HOUR_MINUTE
| HOUR_SECOND
| IF
| IGNORE
| IN
| INDEX
| INFILE
| INNER
| INOUT
| INSERT
| INSENSITIVE
| INT
| INT1
| INT2
| INT3
| INT4
| INT8
| INTEGER
| INTERVAL
| INTO
| IO_AFTER_GTIDS
| IO_BEFORE_GTIDS
| IS
| ITERATE
| JOIN
| JSON_ARRAYAGG
| JSON_OBJECTAGG
| JSON_TABLE
| KEY
| KEYS
| KILL
| LAG
| LAST_VALUE
| LATERAL
| LEAD
| LEADING
| LEAVE
| LINEAR
| LINES
| LOAD
| LONG
| LONGBLOB
| LONGTEXT
| LOOP
| LOW_PRIORITY
| LEFT
| LIKE
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| LOCK
| MASTER_BIND
| MASTER_SSL_VERIFY_SERVER_CERT
| MATCH
| MAXVALUE
| MEDIUMBLOB
| MEDIUMINT
| MEDIUMTEXT
| MEMBER
| MIDDLEINT
| MINUTE_MICROSECOND
| MOD
| MODIFIES
| NATURAL
| NOT
| NO_WRITE_TO_BINLOG
| NTH_VALUE
| NTILE
| NULL
| NUMERIC
| NVAR
| OF
| ON
| OPTIMIZE
| OPTIMIZER_COSTS
| OPTION
| OPTIONALLY
| OR
| ORDER
| OUT
| OUTER
| OUTFILE
| OVER
| PARTITION
| PASSWORD_LOCK
| PERCENT_RANK
| PRECISION
| PRIMARY
| PROCEDURE
| PURGE
| READ
| READS
| READ_WRITE
| RANGE
| RANK
| REAL
| RECURSIVE
| REFERENCES
| REGEXP
| RELEASE
| RENAME
| REPEAT
| REPLACE
| REQUIRE
| RESIGNAL
| RESTRICT
| RETURN
| REVOKE
| RIGHT
| RLIKE
| ROW
| ROWS
| ROW_NUMBER
| SCHEMA
| SCHEMAS
| SECOND_MICROSECOND
| SELECT
| SENSITIVE
| SEPARATOR
| SET
| SHOW
| SIGNAL
| SMALLINT
| SPATIAL
| SPECIFIC
| SQL
| SQLEXCEPTION
| SQLSTATE
| SQLWARNING
| SQL_BIG_RESULT
| SQL_CALC_FOUND_ROWS
| SQL_SMALL_RESULT
| SSL
| STARTING
| STD
| STDDEV
| STDDEV_POP
| STDDEV_SAMP
| STORED
| STRAIGHT_JOIN
| SUBSTR
| SUBSTRING
| SYSTEM
| TABLE
| TERMINATED
| THEN
| TINYBLOB
| TINYINT
| TINYTEXT
| TO
| TRAILING
| TRIGGER
| TRUE
| UNDO
| UNION
| UNIQUE
| UNLOCK
| UNSIGNED
| UPDATE
| USAGE
| USE
| USING
| UTC_DATE
| UTC_TIME
| UTC_TIMESTAMP
| VALUES
| VARBINARY
| VARCHAR
| VARCHARACTER
| VARIANCE
| VARYING
| VAR_POP
| VAR_SAMP
| VIRTUAL
| WHEN
| WHERE
| WHILE
| WINDOW
| WITH
| WRITE
| XOR
| YEAR_MONTH
| ZEROFILL

// These reserved keywords are safe to use, unquoted, as the qualifier on a column reference (e.g. 'accessible.myColumn')
qualified_column_name_safe_reserved_keyword:
  ACCESSIBLE
| ADD
| ALTER
| ANALYZE
| AND
| AS
| ASC
| ASENSITIVE
| BEFORE
| BETWEEN
| BIGINT
| BINARY
| BIT_AND
| BIT_OR
| BIT_XOR
| BLOB
| BOTH
| BY
| CALL
| CASCADE
| CASE
| CHANGE
| CHAR
| CHARACTER
| CHECK
| COLLATE
| COLUMN
| CONSTRAINT
| CONDITION
| CONTINUE
| CONVERT
| CUME_DIST
| CREATE
| CROSS
| CUBE
| CURRENT_DATE
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| CURSOR
| DATABASE
| DATABASES
| DAY_HOUR
| DAY_MICROSECOND
| DAY_MINUTE
| DAY_SECOND
| DEC
| DECIMAL
| DECLARE
| DEFAULT
| DELAYED
| DELETE
| DENSE_RANK
| DESC
| DESCRIBE
| DETERMINISTIC
| DISTINCTROW
| DOUBLE
| DROP
| EACH
| ELSE
| ELSEIF
| EMPTY
| ENCLOSED
| ESCAPED
| EXCEPT
| EXISTS
| EXIT
| EXPLAIN
| FALSE
| FETCH
| FIRST_VALUE
| FLOAT_TYPE
| FLOAT4
| FLOAT8
| FOR
| FORCE
| FOREIGN
| FROM
| FULLTEXT
| FUNCTION
| GENERATED
| GET
| GRANT
| GROUP
| GROUPING
| GROUPS
| HAVING
| HIGH_PRIORITY
| HOUR_MICROSECOND
| HOUR_MINUTE
| HOUR_SECOND
| IF
| IGNORE
| IN
| INDEX
| INFILE
| INNER
| INOUT
| INSERT
| INSENSITIVE
| INT
| INT1
| INT2
| INT3
| INT4
| INT8
| INTEGER
| INTERVAL
| INTO
| IO_AFTER_GTIDS
| IO_BEFORE_GTIDS
| IS
| ITERATE
| JOIN
| JSON_ARRAYAGG
| JSON_OBJECTAGG
| JSON_TABLE
| KEYS
| KILL
| LAG
| LAST_VALUE
| LATERAL
| LEAD
| LEADING
| LEAVE
| LINEAR
| LINES
| LOAD
| LONG
| LONGBLOB
| LONGTEXT
| LOOP
| LOW_PRIORITY
| LEFT
| LIKE
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| LOCK
| MASTER_BIND
| MASTER_SSL_VERIFY_SERVER_CERT
| MATCH
| MAXVALUE
| MEDIUMBLOB
| MEDIUMINT
| MEDIUMTEXT
| MEMBER
| MIDDLEINT
| MINUTE_MICROSECOND
| MOD
| MODIFIES
| NATURAL
| NOT
| NO_WRITE_TO_BINLOG
| NTH_VALUE
| NTILE
| NULL
| NUMERIC
| NVAR
| OF
| ON
| OPTIMIZE
| OPTIMIZER_COSTS
| OPTION
| OPTIONALLY
| OR
| ORDER
| OUT
| OUTER
| OUTFILE
| OVER
| PARTITION
| PASSWORD_LOCK
| PERCENT_RANK
| PRECISION
| PRIMARY
| PROCEDURE
| PURGE
| READ
| READS
| READ_WRITE
| RANGE
| RANK
| REAL
| RECURSIVE
| REFERENCES
| REGEXP
| RELEASE
| RENAME
| REPEAT
| REPLACE
| REQUIRE
| RESIGNAL
| RESTRICT
| RETURN
| REVOKE
| RIGHT
| RLIKE
| ROW
| ROWS
| ROW_NUMBER
| SCHEMA
| SCHEMAS
| SECOND_MICROSECOND
| SENSITIVE
| SEPARATOR
| SET
| SHOW
| SIGNAL
| SMALLINT
| SPATIAL
| SPECIFIC
| SQL
| SQLEXCEPTION
| SQLSTATE
| SQLWARNING
| SQL_BIG_RESULT
| SQL_SMALL_RESULT
| SSL
| STARTING
| STD
| STDDEV
| STDDEV_POP
| STDDEV_SAMP
| STORED
| SUBSTR
| SUBSTRING
| SYSTEM
| TABLE
| TERMINATED
| THEN
| TINYBLOB
| TINYINT
| TINYTEXT
| TO
| TRAILING
| TRIGGER
| TRUE
| UNDO
| UNION
| UNIQUE
| UNLOCK
| UNSIGNED
| UPDATE
| USAGE
| USE
| USING
| UTC_DATE
| UTC_TIME
| UTC_TIMESTAMP
| VALUES
| VARBINARY
| VARCHAR
| VARCHARACTER
| VARIANCE
| VARYING
| VAR_POP
| VAR_SAMP
| VIRTUAL
| WHERE
| WHILE
| WINDOW
| WITH
| WRITE
| XOR
| YEAR_MONTH
| ZEROFILL

/*
  These are non-reserved keywords for Vitess – they don't cause conflicts in the grammar when used as identifiers.
  They can be safely used as unquoted identifiers in queries. Some may be reserved in MySQL, so we backtick quote
  them when we rewrite the query, to prevent any issues.

  Sorted alphabetically
*/
non_reserved_keyword:
  ACTION
| ACTIVE
| ADMIN
| AFTER
| AGAINST
| ALGORITHM
| ALWAYS
| ARRAY
| AT
| AUTHENTICATION
| AUTOEXTEND_SIZE
| AUTO_INCREMENT
| AVG_ROW_LENGTH
| BEGIN
| SERIAL
| BIT
| BOOL
| BOOLEAN
| BUCKETS
| CASCADED
| CATALOG_NAME
| CHAIN
| CHANNEL
| CHARSET
| CHECKSUM
| CIPHER
| CLASS_ORIGIN
| CLIENT
| CLONE
| CLOSE
| COALESCE
| COLLATION
| COLUMNS
| COLUMN_NAME
| COMMIT
| COMPACT
| COMPRESSED
| COMPRESSION
| COMMITTED
| CONNECTION
| COMPLETION
| COMPONENT
| CONSTRAINT_CATALOG
| CONSTRAINT_NAME
| CONSTRAINT_SCHEMA
| CONTAINS
| CURRENT
| CURSOR_NAME
| DATA
| DATE %prec STRING_TYPE_PREFIX_NON_KEYWORD
| DATETIME
| DAY
| DEALLOCATE
| DEFINER
| DEFINITION
| DELAY_KEY_WRITE
| DESCRIPTION
| DIRECTORY
| DISABLE
| DISCARD
| DISK
| DO
| DUMPFILE
| DUPLICATE
| DYNAMIC
| ENABLE
| ENCRYPTION
| END
| ENDS
| ENFORCED
| ENGINE
| ENGINES
| ENGINE_ATTRIBUTE
| ENUM
| ERROR
| ERRORS
| EVENTS
| EVERY
| EXCHANGE
| EXCLUDE
| EXPANSION
| EXPIRE
| EXTENDED
| FIELDS
| FILTER
| FIXED
| FLUSH
| FOLLOWING
| FOLLOWS
| FOUND
| GENERAL
| GEOMCOLLECTION
| GEOMETRY
| GEOMETRYCOLLECTION
| GET_MASTER_PUBLIC_KEY
| GLOBAL
| GRANTS
| HANDLER
| HASH
| HISTOGRAM
| HISTORY
| HOSTS
| HOUR
| IMPORT
| INACTIVE
| INDEXES
| INITIAL
| INSERT_METHOD
| INVISIBLE
| INVOKER
| ISOLATION
| ISSUER
| JSON
| KEY_BLOCK_SIZE
| LANGUAGE
| LAST
| LAST_INSERT_ID
| LESS
| LEVEL
| LINESTRING
| LIST
| LOCAL
| LOCKED
| LOG
| LOGS
| MASTER
| MASTER_COMPRESSION_ALGORITHMS
| MASTER_PUBLIC_KEY_PATH
| MASTER_TLS_CIPHERSUITES
| MASTER_ZSTD_COMPRESSION_LEVEL
| MAX_CONNECTIONS_PER_HOUR
| MAX_QUERIES_PER_HOUR
| MAX_ROWS
| MAX_UPDATES_PER_HOUR
| MAX_USER_CONNECTIONS
| MEMORY
| MERGE
| MESSAGE_TEXT
| MICROSECOND
| MIN_ROWS
| MINUTE
| MODE
| MODIFY
| MONTH
| MULTILINESTRING
| MULTIPOINT
| MULTIPOLYGON
| MYSQL_ERRNO
| NAME
| NAMES
| NATIONAL
| NCHAR
| NESTED
| NETWORK_NAMESPACE
| NEVER
| NO
| NOWAIT
| NULLS
| NVARCHAR
| OFFSET
| OJ
| OLD
| ONLY
| OPEN
| OPTIONAL
| ORDINALITY
| ORGANIZATION
| OTHERS
| PACK_KEYS
| PARTITIONING
| PARTITIONS
| PATH
| PERSIST
| PERSIST_ONLY
| PLUGINS
| POINT
| POLYGON
| PRECEDES
| PRECEDING
| PREPARE
| PRESERVE
| PRIVILEGE_CHECKS_USER
| PRIVILEGES
| PROCESSLIST
| PROXY
| QUARTER
| QUERY
| RANDOM
| REBUILD
| REDUNDANT
| REFERENCE
| RELAY
| REMOVE
| REORGANIZE
| REPAIR
| REPEATABLE
| REPLICA
| REPLICAS
| REPLICATE_DO_TABLE
| REPLICATE_IGNORE_TABLE
| REPLICATION
| REQUIRE_ROW_FORMAT
| RESET
| RESOURCE
| RESPECT
| RESTART
| RETAIN
| REUSE
| ROLE
| ROLLBACK
| ROUTINE
| ROW_FORMAT
| SAVEPOINT
| SCHEDULE
| SCHEMA_NAME
| SECOND
| SECONDARY
| SECONDARY_ENGINE
| SECONDARY_ENGINE_ATTRIBUTE
| SECONDARY_LOAD
| SECONDARY_UNLOAD
| SECURITY
| SEQUENCE
| SERIALIZABLE
| SESSION
| SHARE
| SIGNED
| SKIP
| SLAVE
| SLOW
| SOURCE
| SOURCE_CONNECT_RETRY
| SOURCE_HOST
| SOURCE_PASSWORD
| SOURCE_PORT
| SOURCE_AUTO_POSITION
| SOURCE_RETRY_COUNT
| SOURCE_USER
| SRID
| START
| STARTS
| STATUS
| STATS_AUTO_RECALC
| STATS_PERSISTENT
| STATS_SAMPLE_PAGES
| STOP
| STORAGE
| STREAM
| SUBCLASS_ORIGIN
| SUBJECT
| SUBPARTITION
| SUBPARTITIONS
| TABLE_CHECKSUM
| TABLES
| TABLESPACE
| TABLE_NAME
| TEMPORARY
| TEMPTABLE
| TEXT
| THAN
| THREAD_PRIORITY
| TIES
| TIME %prec STRING_TYPE_PREFIX_NON_KEYWORD
| TIMESTAMP %prec STRING_TYPE_PREFIX_NON_KEYWORD
| TRANSACTION
| TRIGGERS
| TRUNCATE
| UNBOUNDED
| UNCOMMITTED
| UNTIL
| UNUSED
| USER
| USER_RESOURCES
| VALIDATION
| VALUE
| VARIABLES
| VERSION
| VCPU
| VISIBLE
| UNDEFINED
| WARNINGS
| WEEK
| WITHOUT
| WORK
| X509
| YEAR

// non_reserved_keyword that can't go in non_reserved_keyword for some reason
non_reserved_keyword2:
  ATTRIBUTE
| COMMENT_KEYWORD
| CONTAINED
| EXECUTE
| EXTRACT
| FAILED_LOGIN_ATTEMPTS
| FILE
| FIRST
| FULL
| IDENTIFIED
| NONE
| PASSWORD
| PASSWORD_LOCK_TIME
| PROCESS
| RELOAD
| SHUTDOWN
| SUPER
| TIMESTAMPADD
| TIMESTAMPDIFF
| VERSIONS
| VIEW

// non_reserved_keyword that can't go in non_reserved_keyword or non_reserved_keyword2 for some reason
non_reserved_keyword3:
  ACCOUNT
| FORMAT
| NEXT
| OFF
| SQL_CACHE
| SQL_NO_CACHE

// Keywords that cause grammar conflicts in some places, but are safe to use as column name / alias identifiers.
column_name_safe_keyword:
  AVG
| COUNT
| EVENT
| MAX
| MIN
| SUM

// Names of functions that require special grammar support. These aren't reserved or non-reserved in MySQL's docs, but are labeled as keywords in our grammar because of their custom syntax.
function_call_keywords:
  CAST
| POSITION
| TRIM

openb:
  '('
  {
    if incNesting(yylex) {
      yylex.Error("max nesting level reached")
      return 1
    }
  }

closeb:
  ')'
  {
    decNesting(yylex)
  }
