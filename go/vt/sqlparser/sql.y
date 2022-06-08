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

func setParseTree(yylex yyLexer, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex yyLexer, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func setDDL(yylex yyLexer, node Statement) {
  yylex.(*Tokenizer).partialDDL = node
}

func incNesting(yylex yyLexer) bool {
  yylex.(*Tokenizer).nesting++
  if yylex.(*Tokenizer).nesting == 200 {
    return true
  }
  return false
}

func decNesting(yylex yyLexer) {
  yylex.(*Tokenizer).nesting--
}

// skipToEnd forces the lexer to end prematurely. Not all SQL statements
// are supported by the Parser, thus calling skipToEnd will make the lexer
// return EOF early.
func skipToEnd(yylex yyLexer) {
  yylex.(*Tokenizer).SkipToEnd = true
}

func bindVariable(yylex yyLexer, bvar string) {
  yylex.(*Tokenizer).BindVars[bvar] = struct{}{}
}

%}

%struct {
  empty         struct{}
  LengthScaleOption LengthScaleOption
  tableName     TableName
  tableIdent    TableIdent
  str           string
  strs          []string
  vindexParam   VindexParam
  jsonObjectParam *JSONObjectParam
  colIdent      ColIdent
  joinCondition *JoinCondition
  databaseOption DatabaseOption
  columnType    ColumnType
  columnCharset ColumnCharset
  jsonPathParam JSONPathParam
}

%union {
  statement     Statement
  selStmt       SelectStatement
  tableExpr     TableExpr
  expr          Expr
  colTuple      ColTuple
  optVal        Expr
  constraintInfo ConstraintInfo
  alterOption      AlterOption
  characteristic Characteristic

  ins           *Insert
  colName       *ColName
  indexHint    *IndexHint
  indexHints    IndexHints
  indexHintForType IndexHintForType
  literal        *Literal
  subquery      *Subquery
  derivedTable  *DerivedTable
  when          *When
  with          *With
  cte           *CommonTableExpr
  ctes          []*CommonTableExpr
  order         *Order
  limit         *Limit

  updateExpr    *UpdateExpr
  setExpr       *SetExpr
  convertType   *ConvertType
  aliasedTableName *AliasedTableExpr
  tableSpec  *TableSpec
  columnDefinition *ColumnDefinition
  indexDefinition *IndexDefinition
  indexInfo     *IndexInfo
  indexOption   *IndexOption
  indexColumn   *IndexColumn
  partDef       *PartitionDefinition
  partSpec      *PartitionSpec
  showFilter    *ShowFilter
  optLike       *OptLike
  selectInto	  *SelectInto
  createDatabase  *CreateDatabase
  alterDatabase  *AlterDatabase
  createTable      *CreateTable
  tableAndLockType *TableAndLockType
  alterTable       *AlterTable
  tableOption      *TableOption
  columnTypeOptions *ColumnTypeOptions
  partitionDefinitionOptions *PartitionDefinitionOptions
  subPartitionDefinition *SubPartitionDefinition
  subPartitionDefinitions SubPartitionDefinitions
  subPartitionDefinitionOptions *SubPartitionDefinitionOptions
  constraintDefinition *ConstraintDefinition
  revertMigration *RevertMigration
  alterMigration  *AlterMigration
  trimType        TrimType
  frameClause     *FrameClause
  framePoint 	  *FramePoint
  frameUnitType   FrameUnitType
  framePointType  FramePointType
  argumentLessWindowExprType ArgumentLessWindowExprType
  windowSpecification *WindowSpecification
  overClause *OverClause
  nullTreatmentClause *NullTreatmentClause
  nullTreatmentType NullTreatmentType
  firstOrLastValueExprType FirstOrLastValueExprType
  fromFirstLastType FromFirstLastType
  fromFirstLastClause *FromFirstLastClause
  lagLeadExprType LagLeadExprType
  windowDefinition *WindowDefinition
  windowDefinitions WindowDefinitions
  namedWindow *NamedWindow
  namedWindows NamedWindows

  whens         []*When
  columnDefinitions []*ColumnDefinition
  indexOptions  []*IndexOption
  indexColumns  []*IndexColumn
  databaseOptions []DatabaseOption
  tableAndLockTypes TableAndLockTypes
  renameTablePairs []*RenameTablePair
  alterOptions	   []AlterOption
  vindexParams  []VindexParam
  jsonPathParams []JSONPathParam
  jsonObjectParams []*JSONObjectParam
  partDefs      []*PartitionDefinition
  partitionValueRange	*PartitionValueRange
  partitionEngine *PartitionEngine
  partSpecs     []*PartitionSpec
  characteristics []Characteristic
  selectExpr    SelectExpr
  columns       Columns
  partitions    Partitions
  tableExprs    TableExprs
  tableNames    TableNames
  exprs         Exprs
  values        Values
  valTuple      ValTuple
  orderBy       OrderBy
  updateExprs   UpdateExprs
  setExprs      SetExprs
  selectExprs   SelectExprs
  tableOptions     TableOptions

  colKeyOpt     ColumnKeyOption
  referenceAction ReferenceAction
  matchAction MatchAction
  isolationLevel IsolationLevel
  insertAction InsertAction
  scope 	Scope
  lock 		Lock
  joinType  	JoinType
  comparisonExprOperator ComparisonExprOperator
  isExprOperator IsExprOperator
  matchExprOption MatchExprOption
  orderDirection  OrderDirection
  explainType 	  ExplainType
  intervalType	  IntervalTypes
  lockType LockType
  referenceDefinition *ReferenceDefinition

  columnStorage ColumnStorage
  columnFormat ColumnFormat

  boolean bool
  boolVal BoolVal
  ignore Ignore
  partitionOption *PartitionOption
  subPartition  *SubPartition
  partitionByType PartitionByType
  definer 	*Definer
  integer 	int

  JSONTableExpr	*JSONTableExpr
  jtColumnDefinition *JtColumnDefinition
  jtColumnList	[]*JtColumnDefinition
  jtOnResponse	*JtOnResponse
}

// These precedence rules are there to handle shift-reduce conflicts.
%nonassoc <str> MEMBER
// FUNCTION_CALL_NON_KEYWORD is used to resolve shift-reduce conflicts occuring due to function_call_generic symbol and
// having special parsing for functions whose names are non-reserved keywords. The shift-reduce conflict occurrs because
// after seeing a non-reserved keyword, if we see '(', then we can either shift to use the special parsing grammar rule or
// reduce the non-reserved keyword into sql_id and eventually use a rule from function_call_generic.
// The way to fix this conflict is to give shifting higher precedence than reducing.
// Adding no precedence also works, since shifting is the default, but it reports a large number of conflicts
// Shifting on '(' already has an assigned precedence.
// All we need to add is a lower precedence to reducing the grammar symbol to non-reserved keywords.
// In order to ensure lower precedence of reduction, this rule has to come before the precedence declaration of '('.
// This precedence should not be used anywhere else other than with function names that are non-reserved-keywords.
%nonassoc <str> FUNCTION_CALL_NON_KEYWORD

%token LEX_ERROR
%left <str> UNION
%token <str> SELECT STREAM VSTREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR
%token <str> ALL DISTINCT AS EXISTS ASC DESC INTO DUPLICATE DEFAULT SET LOCK UNLOCK KEYS DO CALL
%token <str> DISTINCTROW PARSER GENERATED ALWAYS
%token <str> OUTFILE S3 DATA LOAD LINES TERMINATED ESCAPED ENCLOSED
%token <str> DUMPFILE CSV HEADER MANIFEST OVERWRITE STARTING OPTIONALLY
%token <str> VALUES LAST_INSERT_ID
%token <str> NEXT VALUE SHARE MODE
%token <str> SQL_NO_CACHE SQL_CACHE SQL_CALC_FOUND_ROWS
%left <str> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <str> ON USING INPLACE COPY INSTANT ALGORITHM NONE SHARED EXCLUSIVE
%left <str> SUBQUERY_AS_EXPR
%left <str> '(' ',' ')'
%token <str> ID AT_ID AT_AT_ID HEX STRING NCHAR_STRING INTEGRAL FLOAT DECIMAL HEXNUM VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD BIT_LITERAL COMPRESSION
%token <str> JSON_PRETTY JSON_STORAGE_SIZE JSON_STORAGE_FREE JSON_CONTAINS JSON_CONTAINS_PATH JSON_EXTRACT JSON_KEYS JSON_OVERLAPS JSON_SEARCH JSON_VALUE
%token <str> EXTRACT
%token <str> NULL TRUE FALSE OFF
%token <str> DISCARD IMPORT ENABLE DISABLE TABLESPACE
%token <str> VIRTUAL STORED
%token <str> BOTH LEADING TRAILING

%left EMPTY_FROM_CLAUSE
%right INTO

// Precedence dictated by mysql. But the vitess grammar is simplified.
// Some of these operators don't conflict in our situation. Nevertheless,
// it's better to have these listed in the correct order. Also, we don't
// support all operators yet.
// * NOTE: If you change anything here, update precedence.go as well *
%nonassoc <str> LOWER_THAN_CHARSET
%nonassoc <str> CHARSET
// Resolve column attribute ambiguity.
%right <str> UNIQUE KEY
%left <str> EXPRESSION_PREC_SETTER
%left <str> OR '|'
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP RLIKE IN
%left <str> '&'
%left <str> SHIFT_LEFT SHIFT_RIGHT
%left <str> '+' '-'
%left <str> '*' '/' DIV '%' MOD
%left <str> '^'
%right <str> '~' UNARY
%left <str> COLLATE
%right <str> BINARY UNDERSCORE_ARMSCII8 UNDERSCORE_ASCII UNDERSCORE_BIG5 UNDERSCORE_BINARY UNDERSCORE_CP1250 UNDERSCORE_CP1251
%right <str> UNDERSCORE_CP1256 UNDERSCORE_CP1257 UNDERSCORE_CP850 UNDERSCORE_CP852 UNDERSCORE_CP866 UNDERSCORE_CP932
%right <str> UNDERSCORE_DEC8 UNDERSCORE_EUCJPMS UNDERSCORE_EUCKR UNDERSCORE_GB18030 UNDERSCORE_GB2312 UNDERSCORE_GBK UNDERSCORE_GEOSTD8
%right <str> UNDERSCORE_GREEK UNDERSCORE_HEBREW UNDERSCORE_HP8 UNDERSCORE_KEYBCS2 UNDERSCORE_KOI8R UNDERSCORE_KOI8U UNDERSCORE_LATIN1 UNDERSCORE_LATIN2 UNDERSCORE_LATIN5
%right <str> UNDERSCORE_LATIN7 UNDERSCORE_MACCE UNDERSCORE_MACROMAN UNDERSCORE_SJIS UNDERSCORE_SWE7 UNDERSCORE_TIS620 UNDERSCORE_UCS2 UNDERSCORE_UJIS UNDERSCORE_UTF16
%right <str> UNDERSCORE_UTF16LE UNDERSCORE_UTF32 UNDERSCORE_UTF8 UNDERSCORE_UTF8MB4 UNDERSCORE_UTF8MB3
%right <str> INTERVAL
%nonassoc <str> '.'
%left <str> WINDOW_EXPR

// There is no need to define precedence for the JSON
// operators because the syntax is restricted enough that
// they don't cause conflicts.
%token <empty> JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP

// DDL Tokens
%token <str> CREATE ALTER DROP RENAME ANALYZE ADD FLUSH CHANGE MODIFY DEALLOCATE
%token <str> REVERT
%token <str> SCHEMA TABLE INDEX VIEW TO IGNORE IF PRIMARY COLUMN SPATIAL FULLTEXT KEY_BLOCK_SIZE CHECK INDEXES
%token <str> ACTION CASCADE CONSTRAINT FOREIGN NO REFERENCES RESTRICT
%token <str> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE COALESCE EXCHANGE REBUILD PARTITIONING REMOVE PREPARE EXECUTE
%token <str> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER
%token <str> VINDEX VINDEXES DIRECTORY NAME UPGRADE
%token <str> STATUS VARIABLES WARNINGS CASCADED DEFINER OPTION SQL UNDEFINED
%token <str> SEQUENCE MERGE TEMPORARY TEMPTABLE INVOKER SECURITY FIRST AFTER LAST

// Migration tokens
%token <str> VITESS_MIGRATION CANCEL RETRY COMPLETE CLEANUP THROTTLE UNTHROTTLE EXPIRE RATIO

// Transaction Tokens
%token <str> BEGIN START TRANSACTION COMMIT ROLLBACK SAVEPOINT RELEASE WORK

// Type Tokens
%token <str> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM
%token <str> REAL DOUBLE FLOAT_TYPE DECIMAL_TYPE NUMERIC
%token <str> TIME TIMESTAMP DATETIME YEAR
%token <str> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR
%token <str> TEXT TINYTEXT MEDIUMTEXT LONGTEXT
%token <str> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON JSON_SCHEMA_VALID JSON_SCHEMA_VALIDATION_REPORT ENUM
%token <str> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON
%token <str> ASCII UNICODE // used in CONVERT/CAST types

// Type Modifiers
%token <str> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL

// SHOW tokens
%token <str> CODE COLLATION COLUMNS DATABASES ENGINES EVENT EXTENDED FIELDS FULL FUNCTION GTID_EXECUTED
%token <str> KEYSPACES OPEN PLUGINS PRIVILEGES PROCESSLIST SCHEMAS TABLES TRIGGERS USER
%token <str> VGTID_EXECUTED VITESS_KEYSPACES VITESS_METADATA VITESS_MIGRATIONS VITESS_REPLICATION_STATUS VITESS_SHARDS VITESS_TABLETS VITESS_TARGET VSCHEMA VITESS_THROTTLED_APPS

// SET tokens
%token <str> NAMES GLOBAL SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE

// Functions
%token <str> CURRENT_TIMESTAMP DATABASE CURRENT_DATE NOW
%token <str> CURRENT_TIME LOCALTIME LOCALTIMESTAMP CURRENT_USER
%token <str> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <str> DAY DAY_HOUR DAY_MICROSECOND DAY_MINUTE DAY_SECOND HOUR HOUR_MICROSECOND HOUR_MINUTE HOUR_SECOND MICROSECOND MINUTE MINUTE_MICROSECOND MINUTE_SECOND MONTH QUARTER SECOND SECOND_MICROSECOND YEAR_MONTH WEEK
%token <str> REPLACE
%token <str> CONVERT CAST
%token <str> SUBSTR SUBSTRING
%token <str> GROUP_CONCAT SEPARATOR
%token <str> TIMESTAMPADD TIMESTAMPDIFF
%token <str> WEIGHT_STRING
%token <str> LTRIM RTRIM TRIM
%token <str> JSON_ARRAY JSON_OBJECT JSON_QUOTE
%token <str> JSON_DEPTH JSON_TYPE JSON_LENGTH JSON_VALID
%token <str> JSON_ARRAY_APPEND JSON_ARRAY_INSERT JSON_INSERT JSON_MERGE JSON_MERGE_PATCH JSON_MERGE_PRESERVE JSON_REMOVE JSON_REPLACE JSON_SET JSON_UNQUOTE
%token <str> REGEXP_INSTR REGEXP_LIKE REGEXP_REPLACE REGEXP_SUBSTR
%token <str> ExtractValue UpdateXML
%token <str> GET_LOCK RELEASE_LOCK RELEASE_ALL_LOCKS IS_FREE_LOCK IS_USED_LOCK

// Match
%token <str> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION WITHOUT VALIDATION

// MySQL reserved words that are unused by this grammar will map to this token.
%token <str> UNUSED ARRAY BYTE CUME_DIST DESCRIPTION DENSE_RANK EMPTY EXCEPT FIRST_VALUE GROUPING GROUPS JSON_TABLE LAG LAST_VALUE LATERAL LEAD
%token <str> NTH_VALUE NTILE OF OVER PERCENT_RANK RANK RECURSIVE ROW_NUMBER SYSTEM WINDOW
%token <str> ACTIVE ADMIN AUTOEXTEND_SIZE BUCKETS CLONE COLUMN_FORMAT COMPONENT DEFINITION ENFORCED ENGINE_ATTRIBUTE EXCLUDE FOLLOWING GEOMCOLLECTION GET_MASTER_PUBLIC_KEY HISTOGRAM HISTORY
%token <str> INACTIVE INVISIBLE LOCKED MASTER_COMPRESSION_ALGORITHMS MASTER_PUBLIC_KEY_PATH MASTER_TLS_CIPHERSUITES MASTER_ZSTD_COMPRESSION_LEVEL
%token <str> NESTED NETWORK_NAMESPACE NOWAIT NULLS OJ OLD OPTIONAL ORDINALITY ORGANIZATION OTHERS PARTIAL PATH PERSIST PERSIST_ONLY PRECEDING PRIVILEGE_CHECKS_USER PROCESS
%token <str> RANDOM REFERENCE REQUIRE_ROW_FORMAT RESOURCE RESPECT RESTART RETAIN REUSE ROLE SECONDARY SECONDARY_ENGINE SECONDARY_ENGINE_ATTRIBUTE SECONDARY_LOAD SECONDARY_UNLOAD SIMPLE SKIP SRID
%token <str> THREAD_PRIORITY TIES UNBOUNDED VCPU VISIBLE RETURNING

// Explain tokens
%token <str> FORMAT TREE VITESS TRADITIONAL

// Lock type tokens
%token <str> LOCAL LOW_PRIORITY

// Flush tokens
%token <str> NO_WRITE_TO_BINLOG LOGS ERROR GENERAL HOSTS OPTIMIZER_COSTS USER_RESOURCES SLOW CHANNEL RELAY EXPORT

// Window Functions Token
%token <str> CURRENT ROW ROWS

// TableOptions tokens
%token <str> AVG_ROW_LENGTH CONNECTION CHECKSUM DELAY_KEY_WRITE ENCRYPTION ENGINE INSERT_METHOD MAX_ROWS MIN_ROWS PACK_KEYS PASSWORD
%token <str> FIXED DYNAMIC COMPRESSED REDUNDANT COMPACT ROW_FORMAT STATS_AUTO_RECALC STATS_PERSISTENT STATS_SAMPLE_PAGES STORAGE MEMORY DISK

// Partitions tokens
%token <str> PARTITIONS LINEAR RANGE LIST SUBPARTITION SUBPARTITIONS HASH

%type <partitionByType> range_or_list
%type <integer> partitions_opt algorithm_opt subpartitions_opt partition_max_rows partition_min_rows
%type <statement> command
%type <selStmt> query_expression_parens query_expression query_expression_body select_statement query_primary select_stmt_with_into
%type <statement> explain_statement explainable_statement
%type <statement> prepare_statement
%type <statement> execute_statement deallocate_statement
%type <statement> stream_statement vstream_statement insert_statement update_statement delete_statement set_statement set_transaction_statement
%type <statement> create_statement alter_statement rename_statement drop_statement truncate_statement flush_statement do_statement
%type <with> with_clause_opt with_clause
%type <cte> common_table_expr
%type <ctes> with_list
%type <renameTablePairs> rename_list
%type <createTable> create_table_prefix
%type <alterTable> alter_table_prefix
%type <alterOption> alter_option alter_commands_modifier lock_index algorithm_index
%type <alterOptions> alter_options alter_commands_list alter_commands_modifier_list algorithm_lock_opt
%type <alterTable> create_index_prefix
%type <createDatabase> create_database_prefix
%type <alterDatabase> alter_database_prefix
%type <databaseOption> collate character_set encryption
%type <databaseOptions> create_options create_options_opt
%type <boolean> default_optional first_opt linear_opt jt_exists_opt jt_path_opt partition_storage_opt
%type <statement> analyze_statement show_statement use_statement other_statement
%type <statement> begin_statement commit_statement rollback_statement savepoint_statement release_statement load_statement
%type <statement> lock_statement unlock_statement call_statement
%type <statement> revert_statement
%type <strs> comment_opt comment_list
%type <str> wild_opt check_option_opt cascade_or_local_opt restrict_or_cascade_opt
%type <explainType> explain_format_opt
%type <trimType> trim_type
%type <frameUnitType> frame_units
%type <argumentLessWindowExprType> argument_less_window_expr_type
%type <framePoint> frame_point
%type <frameClause> frame_clause frame_clause_opt
%type <windowSpecification> window_spec
%type <overClause> over_clause
%type <nullTreatmentType> null_treatment_type
%type <nullTreatmentClause> null_treatment_clause null_treatment_clause_opt
%type <fromFirstLastType> from_first_last_type
%type <fromFirstLastClause> from_first_last_clause from_first_last_clause_opt
%type <firstOrLastValueExprType> first_or_last_value_expr_type
%type <lagLeadExprType> lag_lead_expr_type
%type <windowDefinition> window_definition
%type <windowDefinitions> window_definition_list
%type <namedWindow> named_window
%type <namedWindows> named_windows_list named_windows_list_opt
%type <insertAction> insert_or_replace
%type <str> explain_synonyms
%type <partitionOption> partitions_options_opt partitions_options_beginning
%type <partitionDefinitionOptions> partition_definition_attribute_list_opt
%type <subPartition> subpartition_opt
%type <subPartitionDefinition> subpartition_definition
%type <subPartitionDefinitions> subpartition_definition_list subpartition_definition_list_with_brackets
%type <subPartitionDefinitionOptions> subpartition_definition_attribute_list_opt
%type <intervalType> interval_time_stamp interval
%type <str> cache_opt separator_opt flush_option for_channel_opt maxvalue
%type <matchExprOption> match_option
%type <boolean> distinct_opt union_op replace_opt local_opt
%type <selectExprs> select_expression_list select_expression_list_opt
%type <selectExpr> select_expression
%type <strs> select_options flush_option_list
%type <str> select_option algorithm_view security_view security_view_opt
%type <str> generated_always_opt user_username address_opt
%type <definer> definer_opt user
%type <expr> expression frame_expression signed_literal signed_literal_or_null null_as_literal now_or_signed_literal signed_literal bit_expr regular_expressions xml_expressions
%type <expr> interval_value simple_expr literal NUM_literal text_literal text_literal_or_arg bool_pri literal_or_null now predicate tuple_expression null_int_variable_arg
%type <tableExprs> from_opt table_references from_clause
%type <tableExpr> table_reference table_factor join_table json_table_function
%type <jtColumnDefinition> jt_column
%type <jtColumnList> jt_columns_clause columns_list
%type <jtOnResponse> on_error on_empty json_on_response
%type <joinCondition> join_condition join_condition_opt on_expression_opt
%type <tableNames> table_name_list delete_table_list view_name_list
%type <joinType> inner_join outer_join straight_join natural_join
%type <tableName> table_name into_table_name delete_table_name
%type <aliasedTableName> aliased_table_name
%type <indexHint> index_hint
%type <indexHintForType> index_hint_for_opt
%type <indexHints> index_hint_list index_hint_list_opt
%type <expr> where_expression_opt
%type <boolVal> boolean_value
%type <comparisonExprOperator> compare
%type <ins> insert_data
%type <expr> num_val
%type <expr> function_call_keyword function_call_nonkeyword function_call_generic function_call_conflict
%type <isExprOperator> is_suffix
%type <colTuple> col_tuple
%type <exprs> expression_list expression_list_opt window_partition_clause_opt
%type <values> tuple_list
%type <valTuple> row_tuple tuple_or_empty
%type <subquery> subquery
%type <derivedTable> derived_table
%type <colName> column_name after_opt
%type <whens> when_expression_list
%type <when> when_expression
%type <expr> expression_opt else_expression_opt default_with_comma_opt
%type <exprs> group_by_opt
%type <expr> having_opt
%type <orderBy> order_by_opt order_list order_by_clause
%type <order> order
%type <orderDirection> asc_desc_opt
%type <limit> limit_opt limit_clause
%type <selectInto> into_clause
%type <columnTypeOptions> column_attribute_list_opt generated_column_attribute_list_opt
%type <str> header_opt export_options manifest_opt overwrite_opt format_opt optionally_opt regexp_symbol
%type <str> fields_opts fields_opt_list fields_opt lines_opts lines_opt lines_opt_list
%type <lock> locking_clause
%type <columns> ins_column_list column_list at_id_list column_list_opt index_list execute_statement_list_opt
%type <partitions> opt_partition_clause partition_list
%type <updateExprs> on_dup_opt
%type <updateExprs> update_list
%type <setExprs> set_list
%type <str> charset_or_character_set charset_or_character_set_or_names
%type <updateExpr> update_expression
%type <setExpr> set_expression
%type <characteristic> transaction_char
%type <characteristics> transaction_chars
%type <isolationLevel> isolation_level
%type <str> for_from from_or_on
%type <str> default_opt
%type <ignore> ignore_opt
%type <str> columns_or_fields extended_opt storage_opt
%type <showFilter> like_or_where_opt like_opt
%type <boolean> exists_opt not_exists_opt enforced enforced_opt temp_opt full_opt
%type <empty> to_opt
%type <str> reserved_keyword non_reserved_keyword
%type <colIdent> sql_id sql_id_opt reserved_sql_id col_alias as_ci_opt
%type <expr> charset_value
%type <tableIdent> table_id reserved_table_id table_alias as_opt_id table_id_opt from_database_opt
%type <empty> as_opt work_opt savepoint_opt
%type <empty> skip_to_end ddl_skip_to_end
%type <str> charset
%type <scope> set_session_or_global
%type <convertType> convert_type returning_type_opt convert_type_weight_string
%type <boolean> array_opt
%type <columnType> column_type
%type <columnType> int_type decimal_type numeric_type time_type char_type spatial_type
%type <literal> length_opt partition_comment partition_data_directory partition_index_directory
%type <expr> func_datetime_precision
%type <columnCharset> charset_opt
%type <str> collate_opt
%type <boolean> binary_opt
%type <LengthScaleOption> float_length_opt decimal_length_opt
%type <boolean> unsigned_opt zero_fill_opt without_valid_opt
%type <strs> enum_values
%type <columnDefinition> column_definition
%type <columnDefinitions> column_definition_list
%type <indexDefinition> index_definition
%type <constraintDefinition> constraint_definition check_constraint_definition
%type <str> index_or_key index_symbols from_or_in index_or_key_opt
%type <str> name_opt constraint_name_opt
%type <str> equal_opt partition_tablespace_name
%type <tableSpec> table_spec table_column_list
%type <optLike> create_like
%type <str> table_opt_value
%type <tableOption> table_option
%type <tableOptions> table_option_list table_option_list_opt space_separated_table_option_list
%type <indexInfo> index_info
%type <indexColumn> index_column
%type <indexColumns> index_column_list
%type <indexOption> index_option using_index_type
%type <indexOptions> index_option_list index_option_list_opt using_opt
%type <constraintInfo> constraint_info check_constraint_info
%type <partDefs> partition_definitions partition_definitions_opt
%type <partDef> partition_definition partition_name
%type <partitionValueRange> partition_value_range
%type <partitionEngine> partition_engine
%type <partSpec> partition_operation
%type <vindexParam> vindex_param
%type <vindexParams> vindex_param_list vindex_params_opt
%type <jsonPathParam> json_path_param
%type <jsonPathParams> json_path_param_list json_path_param_list_opt
%type <jsonObjectParam> json_object_param
%type <jsonObjectParams> json_object_param_list json_object_param_opt
%type <colIdent> id_or_var vindex_type vindex_type_opt id_or_var_opt
%type <str> database_or_schema column_opt insert_method_options row_format_options
%type <referenceAction> fk_reference_action fk_on_delete fk_on_update
%type <matchAction> fk_match fk_match_opt fk_match_action
%type <tableAndLockTypes> lock_table_list
%type <tableAndLockType> lock_table
%type <lockType> lock_type
%type <empty> session_or_local_opt
%type <columnStorage> column_storage
%type <columnFormat> column_format
%type <colKeyOpt> keys
%type <referenceDefinition> reference_definition reference_definition_opt
%type <str> underscore_charsets
%type <str> expire_opt
%type <literal> ratio_opt
%start any_command

%%

any_command:
  command semicolon_opt
  {
    setParseTree(yylex, $1)
  }

semicolon_opt:
/*empty*/ {}
| ';' {}

command:
  select_statement
  {
    $$ = $1
  }
| stream_statement
| vstream_statement
| insert_statement
| update_statement
| delete_statement
| set_statement
| set_transaction_statement
| create_statement
| alter_statement
| rename_statement
| drop_statement
| truncate_statement
| analyze_statement
| show_statement
| use_statement
| begin_statement
| commit_statement
| rollback_statement
| savepoint_statement
| release_statement
| explain_statement
| other_statement
| flush_statement
| do_statement
| load_statement
| lock_statement
| unlock_statement
| call_statement
| revert_statement
| prepare_statement
| execute_statement
| deallocate_statement
| /*empty*/
{
  setParseTree(yylex, nil)
}

id_or_var:
  ID
  {
    $$ = NewColIdentWithAt(string($1), NoAt)
  }
| AT_ID
  {
    $$ = NewColIdentWithAt(string($1), SingleAt)
  }
| AT_AT_ID
  {
    $$ = NewColIdentWithAt(string($1), DoubleAt)
  }

id_or_var_opt:
  {
    $$ = NewColIdentWithAt("", NoAt)
  }
| id_or_var
  {
    $$ = $1
  }

do_statement:
  DO expression_list
  {
    $$ = &OtherAdmin{}
  }

load_statement:
  LOAD DATA skip_to_end
  {
    $$ = &Load{}
  }

with_clause:
  WITH with_list
  {
	$$ = &With{ctes: $2, Recursive: false}
  }
| WITH RECURSIVE with_list
  {
	$$ = &With{ctes: $3, Recursive: true}
  }

with_clause_opt:
  {
    $$ = nil
  }
 | with_clause
 {
 	$$ = $1
 }

with_list:
  with_list ',' common_table_expr
  {
	$$ = append($1, $3)
  }
| common_table_expr
  {
	$$ = []*CommonTableExpr{$1}
  }

common_table_expr:
  table_id column_list_opt AS subquery
  {
	$$ = &CommonTableExpr{TableID: $1, Columns: $2, Subquery: $4}
  }

query_expression_parens:
  openb query_expression_parens closeb
  {
  	$$ = $2
  }
| openb query_expression closeb
  {
     $$ = $2
  }
| openb query_expression locking_clause closeb
  {
    setLockInSelect($2, $3)
    $$ = $2
  }

// TODO; (Manan, Ritwiz) : Use this in create, insert statements
//query_expression_or_parens:
//	query_expression
//	{
//		$$ = $1
//	}
//	| query_expression locking_clause
//	{
//		setLockInSelect($1, $2)
//		$$ = $1
//	}
//	| query_expression_parens
//	{
//		$$ = $1
//	}

query_expression:
 query_expression_body order_by_opt limit_opt
  {
	$1.SetOrderBy($2)
	$1.SetLimit($3)
	$$ = $1
  }
| query_expression_parens limit_clause
  {
	$1.SetLimit($2)
	$$ = $1
  }
| query_expression_parens order_by_clause limit_opt
  {
	$1.SetOrderBy($2)
	$1.SetLimit($3)
	$$ = $1
  }
| with_clause query_expression_body order_by_opt limit_opt
  {
  		$2.SetWith($1)
		$2.SetOrderBy($3)
		$2.SetLimit($4)
		$$ = $2
  }
| with_clause query_expression_parens limit_clause
  {
  		$2.SetWith($1)
		$2.SetLimit($3)
		$$ = $2
  }
| with_clause query_expression_parens order_by_clause limit_opt
  {
  		$2.SetWith($1)
		$2.SetOrderBy($3)
		$2.SetLimit($4)
		$$ = $2
  }
| with_clause query_expression_parens
  {
	$2.SetWith($1)
  }
| SELECT comment_opt cache_opt NEXT num_val for_from table_name
  {
	$$ = NewSelect(Comments($2), SelectExprs{&Nextval{Expr: $5}}, []string{$3}/*options*/, nil, TableExprs{&AliasedTableExpr{Expr: $7}}, nil/*where*/, nil/*groupBy*/, nil/*having*/, nil)
  }

query_expression_body:
 query_primary
  {
	$$ = $1
  }
| query_expression_body union_op query_primary
  {
 	$$ = &Union{Left: $1, Distinct: $2, Right: $3}
  }
| query_expression_parens union_op query_primary
  {
	$$ = &Union{Left: $1, Distinct: $2, Right: $3}
  }
| query_expression_body union_op query_expression_parens
  {
  	$$ = &Union{Left: $1, Distinct: $2, Right: $3}
  }
| query_expression_parens union_op query_expression_parens
  {
	$$ = &Union{Left: $1, Distinct: $2, Right: $3}
  }

select_statement:
query_expression
  {
	$$ = $1
  }
| query_expression locking_clause
  {
	setLockInSelect($1, $2)
	$$ = $1
  }
| query_expression_parens
  {
	$$ = $1
  }
| select_stmt_with_into
  {
	$$ = $1
  }

select_stmt_with_into:
  openb select_stmt_with_into closeb
  {
	$$ = $2;
  }
| query_expression into_clause
  {
	$1.SetInto($2)
	$$ = $1
  }
| query_expression into_clause locking_clause
  {
	$1.SetInto($2)
	$1.SetLock($3)
	$$ = $1
  }
| query_expression locking_clause into_clause
  {
	$1.SetInto($3)
	$1.SetLock($2)
	$$ = $1
  }
| query_expression_parens into_clause
  {
 	$1.SetInto($2)
	$$ = $1
  }

stream_statement:
  STREAM comment_opt select_expression FROM table_name
  {
    $$ = &Stream{Comments: Comments($2).Parsed(), SelectExpr: $3, Table: $5}
  }

vstream_statement:
  VSTREAM comment_opt select_expression FROM table_name where_expression_opt limit_opt
  {
    $$ = &VStream{Comments: Comments($2).Parsed(), SelectExpr: $3, Table: $5, Where: NewWhere(WhereClause, $6), Limit: $7}
  }

// query_primary is an unparenthesized SELECT with no order by clause or beyond.
query_primary:
//  1         2            3              4                    5             6                7           8            9           10
  SELECT comment_opt select_options select_expression_list into_clause from_opt where_expression_opt group_by_opt having_opt named_windows_list_opt
  {
    $$ = NewSelect(Comments($2), $4/*SelectExprs*/, $3/*options*/, $5/*into*/, $6/*from*/, NewWhere(WhereClause, $7), GroupBy($8), NewWhere(HavingClause, $9), $10)
  }
| SELECT comment_opt select_options select_expression_list from_opt where_expression_opt group_by_opt having_opt named_windows_list_opt
  {
    $$ = NewSelect(Comments($2), $4/*SelectExprs*/, $3/*options*/, nil, $5/*from*/, NewWhere(WhereClause, $6), GroupBy($7), NewWhere(HavingClause, $8), $9)
  }



insert_statement:
  insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause insert_data on_dup_opt
  {
    // insert_data returns a *Insert pre-filled with Columns & Values
    ins := $6
    ins.Action = $1
    ins.Comments = Comments($2).Parsed()
    ins.Ignore = $3
    ins.Table = $4
    ins.Partitions = $5
    ins.OnDup = OnDup($7)
    $$ = ins
  }
| insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause SET update_list on_dup_opt
  {
    cols := make(Columns, 0, len($7))
    vals := make(ValTuple, 0, len($8))
    for _, updateList := range $7 {
      cols = append(cols, updateList.Name.Name)
      vals = append(vals, updateList.Expr)
    }
    $$ = &Insert{Action: $1, Comments: Comments($2).Parsed(), Ignore: $3, Table: $4, Partitions: $5, Columns: cols, Rows: Values{vals}, OnDup: OnDup($8)}
  }

insert_or_replace:
  INSERT
  {
    $$ = InsertAct
  }
| REPLACE
  {
    $$ = ReplaceAct
  }

update_statement:
  with_clause_opt UPDATE comment_opt ignore_opt table_references SET update_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{With: $1, Comments: Comments($3).Parsed(), Ignore: $4, TableExprs: $5, Exprs: $7, Where: NewWhere(WhereClause, $8), OrderBy: $9, Limit: $10}
  }

delete_statement:
  with_clause_opt DELETE comment_opt ignore_opt FROM table_name as_opt_id opt_partition_clause where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{With: $1, Comments: Comments($3).Parsed(), Ignore: $4, TableExprs: TableExprs{&AliasedTableExpr{Expr:$6, As: $7}}, Partitions: $8, Where: NewWhere(WhereClause, $9), OrderBy: $10, Limit: $11}
  }
| with_clause_opt DELETE comment_opt ignore_opt FROM table_name_list USING table_references where_expression_opt
  {
    $$ = &Delete{With: $1, Comments: Comments($3).Parsed(), Ignore: $4, Targets: $6, TableExprs: $8, Where: NewWhere(WhereClause, $9)}
  }
| with_clause_opt DELETE comment_opt ignore_opt table_name_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{With: $1, Comments: Comments($3).Parsed(), Ignore: $4, Targets: $5, TableExprs: $7, Where: NewWhere(WhereClause, $8)}
  }
| with_clause_opt DELETE comment_opt ignore_opt delete_table_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{With: $1, Comments: Comments($3).Parsed(), Ignore: $4, Targets: $5, TableExprs: $7, Where: NewWhere(WhereClause, $8)}
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
    $$ = &Set{Comments: Comments($2).Parsed(), Exprs: $3}
  }

set_transaction_statement:
  SET comment_opt set_session_or_global TRANSACTION transaction_chars
  {
    $$ = &SetTransaction{Comments: Comments($2).Parsed(), Scope: $3, Characteristics: $5}
  }
| SET comment_opt TRANSACTION transaction_chars
  {
    $$ = &SetTransaction{Comments: Comments($2).Parsed(), Characteristics: $4, Scope: ImplicitScope}
  }

transaction_chars:
  transaction_char
  {
    $$ = []Characteristic{$1}
  }
| transaction_chars ',' transaction_char
  {
    $$ = append($$, $3)
  }

transaction_char:
  ISOLATION LEVEL isolation_level
  {
    $$ = $3
  }
| READ WRITE
  {
    $$ = ReadWrite
  }
| READ ONLY
  {
    $$ = ReadOnly
  }

isolation_level:
  REPEATABLE READ
  {
    $$ = RepeatableRead
  }
| READ COMMITTED
  {
    $$ = ReadCommitted
  }
| READ UNCOMMITTED
  {
    $$ = ReadUncommitted
  }
| SERIALIZABLE
  {
    $$ = Serializable
  }

set_session_or_global:
  SESSION
  {
    $$ = SessionScope
  }
| GLOBAL
  {
    $$ = GlobalScope
  }

create_statement:
  create_table_prefix table_spec
  {
    $1.TableSpec = $2
    $1.FullyParsed = true
    $$ = $1
  }
| create_table_prefix create_like
  {
    // Create table [name] like [name]
    $1.OptLike = $2
    $1.FullyParsed = true
    $$ = $1
  }
| create_index_prefix '(' index_column_list ')' index_option_list_opt algorithm_lock_opt
  {
    indexDef := $1.AlterOptions[0].(*AddIndexDefinition).IndexDefinition
    indexDef.Columns = $3
    indexDef.Options = append(indexDef.Options,$5...)
    $1.AlterOptions = append($1.AlterOptions,$6...)
    $1.FullyParsed = true
    $$ = $1
  }
| CREATE comment_opt replace_opt algorithm_view definer_opt security_view_opt VIEW table_name column_list_opt AS select_statement check_option_opt
  {
    $$ = &CreateView{ViewName: $8.ToViewName(), Comments: Comments($2).Parsed(), IsReplace:$3, Algorithm:$4, Definer: $5 ,Security:$6, Columns:$9, Select: $11, CheckOption: $12 }
  }
| create_database_prefix create_options_opt
  {
    $1.FullyParsed = true
    $1.CreateOptions = $2
    $$ = $1
  }

replace_opt:
  {
    $$ = false
  }
| OR REPLACE
  {
    $$ = true
  }

vindex_type_opt:
  {
    $$ = NewColIdent("")
  }
| USING vindex_type
  {
    $$ = $2
  }

vindex_type:
  sql_id
  {
    $$ = $1
  }

vindex_params_opt:
  {
    var v []VindexParam
    $$ = v
  }
| WITH vindex_param_list
  {
    $$ = $2
  }

vindex_param_list:
  vindex_param
  {
    $$ = make([]VindexParam, 0, 4)
    $$ = append($$, $1)
  }
| vindex_param_list ',' vindex_param
  {
    $$ = append($$, $3)
  }

vindex_param:
  reserved_sql_id '=' table_opt_value
  {
    $$ = VindexParam{Key: $1, Val: $3}
  }

json_object_param_opt:
  {
    $$ = nil
  }
| json_object_param_list
  {
    $$ = $1
  }

json_object_param_list:
  json_object_param
  {
    $$ = []*JSONObjectParam{$1}
  }
| json_object_param_list ',' json_object_param
  {
    $$ = append($$, $3)
  }

json_object_param:
  expression ',' expression
  {
    $$ = &JSONObjectParam{Key:$1, Value:$3}
  }

create_table_prefix:
  CREATE comment_opt temp_opt TABLE not_exists_opt table_name
  {
    $$ = &CreateTable{Comments: Comments($2).Parsed(), Table: $6, IfNotExists: $5, Temp: $3}
    setDDL(yylex, $$)
  }

alter_table_prefix:
  ALTER comment_opt TABLE table_name
  {
    $$ = &AlterTable{Comments: Comments($2).Parsed(), Table: $4}
    setDDL(yylex, $$)
  }

create_index_prefix:
  CREATE comment_opt INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $7, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$4, Type:string($3)}, Options:$5}}}}
    setDDL(yylex, $$)
  }
| CREATE comment_opt FULLTEXT INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $8, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$5, Type:string($3)+" "+string($4), Fulltext:true}, Options:$6}}}}
    setDDL(yylex, $$)
  }
| CREATE comment_opt SPATIAL INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $8, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$5, Type:string($3)+" "+string($4), Spatial:true}, Options:$6}}}}
    setDDL(yylex, $$)
  }
| CREATE comment_opt UNIQUE INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $8, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$5, Type:string($3)+" "+string($4), Unique:true}, Options:$6}}}}
    setDDL(yylex, $$)
  }

create_database_prefix:
  CREATE comment_opt database_or_schema comment_opt not_exists_opt table_id
  {
    $$ = &CreateDatabase{Comments: Comments($4).Parsed(), DBName: $6, IfNotExists: $5}
    setDDL(yylex,$$)
  }

alter_database_prefix:
  ALTER comment_opt database_or_schema
  {
    $$ = &AlterDatabase{}
    setDDL(yylex,$$)
  }

database_or_schema:
  DATABASE
| SCHEMA

table_spec:
  '(' table_column_list ')' table_option_list_opt partitions_options_opt
  {
    $$ = $2
    $$.Options = $4
    $$.PartitionOption = $5
  }

create_options_opt:
  {
    $$ = nil
  }
| create_options
  {
    $$ = $1
  }

create_options:
  character_set
  {
    $$ = []DatabaseOption{$1}
  }
| collate
  {
    $$ = []DatabaseOption{$1}
  }
| encryption
  {
    $$ = []DatabaseOption{$1}
  }
| create_options collate
  {
    $$ = append($1,$2)
  }
| create_options character_set
  {
    $$ = append($1,$2)
  }
| create_options encryption
  {
    $$ = append($1,$2)
  }

default_optional:
  /* empty */ %prec LOWER_THAN_CHARSET
  {
    $$ = false
  }
| DEFAULT
  {
    $$ = true
  }

character_set:
  default_optional charset_or_character_set equal_opt id_or_var
  {
    $$ = DatabaseOption{Type:CharacterSetType, Value:($4.String()), IsDefault:$1}
  }
| default_optional charset_or_character_set equal_opt STRING
  {
    $$ = DatabaseOption{Type:CharacterSetType, Value:(encodeSQLString($4)), IsDefault:$1}
  }

collate:
  default_optional COLLATE equal_opt id_or_var
  {
    $$ = DatabaseOption{Type:CollateType, Value:($4.String()), IsDefault:$1}
  }
| default_optional COLLATE equal_opt STRING
  {
    $$ = DatabaseOption{Type:CollateType, Value:(encodeSQLString($4)), IsDefault:$1}
  }

encryption:
  default_optional ENCRYPTION equal_opt id_or_var
  {
    $$ = DatabaseOption{Type:EncryptionType, Value:($4.String()), IsDefault:$1}
  }
| default_optional ENCRYPTION equal_opt STRING
  {
    $$ = DatabaseOption{Type:EncryptionType, Value:(encodeSQLString($4)), IsDefault:$1}
  }

create_like:
  LIKE table_name
  {
    $$ = &OptLike{LikeTable: $2}
  }
| '(' LIKE table_name ')'
  {
    $$ = &OptLike{LikeTable: $3}
  }

column_definition_list:
  column_definition
  {
    $$ = []*ColumnDefinition{$1}
  }
| column_definition_list ',' column_definition
  {
    $$ = append($1,$3)
  }

table_column_list:
  column_definition
  {
    $$ = &TableSpec{}
    $$.AddColumn($1)
  }
| check_constraint_definition
  {
    $$ = &TableSpec{}
    $$.AddConstraint($1)
  }
| table_column_list ',' column_definition
  {
    $$.AddColumn($3)
  }
| table_column_list ',' column_definition check_constraint_definition
  {
    $$.AddColumn($3)
    $$.AddConstraint($4)
  }
| table_column_list ',' index_definition
  {
    $$.AddIndex($3)
  }
| table_column_list ',' constraint_definition
  {
    $$.AddConstraint($3)
  }
| table_column_list ',' check_constraint_definition
  {
    $$.AddConstraint($3)
  }

// collate_opt has to be in the first rule so that we don't have a shift reduce conflict when seeing a COLLATE
// with column_attribute_list_opt. Always shifting there would have meant that we would have always ended up using the
// second rule in the grammar whenever COLLATE was specified.
// We now have a shift reduce conflict between COLLATE and collate_opt. Shifting there is fine. Essentially, we have
// postponed the decision of which rule to use until we have consumed the COLLATE id/string tokens.
column_definition:
  sql_id column_type collate_opt column_attribute_list_opt reference_definition_opt
  {
    $2.Options = $4
    if $2.Options.Collate == "" {
    	$2.Options.Collate = $3
    }
    $2.Options.Reference = $5
    $$ = &ColumnDefinition{Name: $1, Type: $2}
  }
| sql_id column_type collate_opt generated_always_opt AS '(' expression ')' generated_column_attribute_list_opt reference_definition_opt
  {
    $2.Options = $9
    $2.Options.As = $7
    $2.Options.Reference = $10
    $2.Options.Collate = $3
    $$ = &ColumnDefinition{Name: $1, Type: $2}
  }

generated_always_opt:
  {
    $$ = ""
  }
|  GENERATED ALWAYS
  {
    $$ = ""
  }

// There is a shift reduce conflict that arises here because UNIQUE and KEY are column_type_option and so is UNIQUE KEY.
// So in the state "column_type_options UNIQUE. KEY" there is a shift-reduce conflict(resovled by "%rigth <str> UNIQUE KEY").
// This has been added to emulate what MySQL does. The previous architecture was such that the order of the column options
// was specific (as stated in the MySQL guide) and did not accept arbitrary order options. For example NOT NULL DEFAULT 1 and not DEFAULT 1 NOT NULL
column_attribute_list_opt:
  {
    $$ = &ColumnTypeOptions{Null: nil, Default: nil, OnUpdate: nil, Autoincrement: false, KeyOpt: colKeyNone, Comment: nil, As: nil, Invisible: nil, Format: UnspecifiedFormat, EngineAttribute: nil, SecondaryEngineAttribute: nil }
  }
| column_attribute_list_opt NULL
  {
    val := true
    $1.Null = &val
    $$ = $1
  }
| column_attribute_list_opt NOT NULL
  {
    val := false
    $1.Null = &val
    $$ = $1
  }
| column_attribute_list_opt DEFAULT openb expression closeb
  {
	$1.Default = $4
	$$ = $1
  }
| column_attribute_list_opt DEFAULT now_or_signed_literal
  {
    $1.Default = $3
    $$ = $1
  }
| column_attribute_list_opt ON UPDATE function_call_nonkeyword
  {
    $1.OnUpdate = $4
    $$ = $1
  }
| column_attribute_list_opt AUTO_INCREMENT
  {
    $1.Autoincrement = true
    $$ = $1
  }
| column_attribute_list_opt COMMENT_KEYWORD STRING
  {
    $1.Comment = NewStrLiteral($3)
    $$ = $1
  }
| column_attribute_list_opt keys
  {
    $1.KeyOpt = $2
    $$ = $1
  }
| column_attribute_list_opt COLLATE STRING
  {
    $1.Collate = encodeSQLString($3)
  }
| column_attribute_list_opt COLLATE id_or_var
  {
    $1.Collate = string($3.String())
    $$ = $1
  }
| column_attribute_list_opt COLUMN_FORMAT column_format
  {
    $1.Format = $3
  }
| column_attribute_list_opt SRID INTEGRAL
  {
    $1.SRID = NewIntLiteral($3)
    $$ = $1
  }
| column_attribute_list_opt VISIBLE
  {
    val := false
    $1.Invisible = &val
    $$ = $1
  }
| column_attribute_list_opt INVISIBLE
  {
    val := true
    $1.Invisible = &val
    $$ = $1
  }
| column_attribute_list_opt ENGINE_ATTRIBUTE equal_opt STRING
  {
    $1.EngineAttribute = NewStrLiteral($4)
  }
| column_attribute_list_opt SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $1.SecondaryEngineAttribute = NewStrLiteral($4)
  }

column_format:
  FIXED
{
  $$ = FixedFormat
}
| DYNAMIC
{
  $$ = DynamicFormat
}
| DEFAULT
{
  $$ = DefaultFormat
}

column_storage:
  VIRTUAL
{
  $$ = VirtualStorage
}
| STORED
{
  $$ = StoredStorage
}

generated_column_attribute_list_opt:
  {
    $$ = &ColumnTypeOptions{}
  }
| generated_column_attribute_list_opt column_storage
  {
    $1.Storage = $2
    $$ = $1
  }
| generated_column_attribute_list_opt NULL
  {
    val := true
    $1.Null = &val
    $$ = $1
  }
| generated_column_attribute_list_opt NOT NULL
  {
    val := false
    $1.Null = &val
    $$ = $1
  }
| generated_column_attribute_list_opt COMMENT_KEYWORD STRING
  {
    $1.Comment = NewStrLiteral($3)
    $$ = $1
  }
| generated_column_attribute_list_opt keys
  {
    $1.KeyOpt = $2
    $$ = $1
  }
| generated_column_attribute_list_opt VISIBLE
  {
    val := false
    $1.Invisible = &val
    $$ = $1
  }
| generated_column_attribute_list_opt INVISIBLE
  {
    val := true
    $1.Invisible = &val
    $$ = $1
  }

now_or_signed_literal:
now
  {
  	$$ = $1
  }
| signed_literal_or_null

now:
CURRENT_TIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("current_timestamp"), Fsp: $2}
  }
| LOCALTIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("localtime"), Fsp: $2}
  }
| LOCALTIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("localtimestamp"), Fsp: $2}
  }
| UTC_TIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("utc_timestamp"), Fsp:$2}
  }
| NOW func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("now"), Fsp: $2}
  }


signed_literal_or_null:
signed_literal
| null_as_literal

 null_as_literal:
NULL
 {
    $$ = &NullVal{}
 }

 signed_literal:
 literal
| '+' NUM_literal
   {
 	$$= $2
   }
| '-' NUM_literal
   {
   	$$ = &UnaryExpr{Operator: UMinusOp, Expr: $2}
   }

literal:
text_literal
  {
   $$= $1
  }
| NUM_literal
  {
  	$$= $1
  }
| boolean_value
  {
  	$$ = $1
  }
| HEX
  {
	$$ = NewHexLiteral($1)
  }
| HEXNUM
  {
  	$$ = NewHexNumLiteral($1)
  }
| BIT_LITERAL
  {
	$$ = NewBitLiteral($1)
  }
| VALUE_ARG
  {
    $$ = NewArgument($1[1:])
    bindVariable(yylex, $1[1:])
  }
| underscore_charsets  BIT_LITERAL %prec UNARY
  {
  	$$ = &IntroducerExpr{CharacterSet: $1, Expr: NewBitLiteral($2)}
  }
| underscore_charsets HEXNUM %prec UNARY
  {
  	$$ = &IntroducerExpr{CharacterSet: $1, Expr: NewHexNumLiteral($2)}
  }
| underscore_charsets HEX %prec UNARY
  {
   	$$ = &IntroducerExpr{CharacterSet: $1, Expr: NewHexLiteral($2)}
  }
| underscore_charsets column_name %prec UNARY
  {
    $$ = &IntroducerExpr{CharacterSet: $1, Expr: $2}
  }
| underscore_charsets VALUE_ARG %prec UNARY
  {
    bindVariable(yylex, $2[1:])
    $$ = &IntroducerExpr{CharacterSet: $1, Expr: NewArgument($2[1:])}
  }

underscore_charsets:
  UNDERSCORE_ARMSCII8
  {
    $$ = Armscii8Str
  }
| UNDERSCORE_ASCII
  {
    $$ = ASCIIStr
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
    $$ = Utf8Str
  }
| UNDERSCORE_UTF8MB4
  {
    $$ = Utf8mb4Str
  }
| UNDERSCORE_UTF8MB3
  {
    $$ = Utf8Str
  }

literal_or_null:
literal
| null_as_literal

NUM_literal:
INTEGRAL
  {
    $$ = NewIntLiteral($1)
  }
| FLOAT
  {
    $$ = NewFloatLiteral($1)
  }
| DECIMAL
  {
    $$ = NewDecimalLiteral($1)
  }

text_literal:
STRING
  {
	$$ = NewStrLiteral($1)
  }
| NCHAR_STRING
  {
	$$ = &UnaryExpr{Operator: NStringOp, Expr: NewStrLiteral($1)}
  }
 | underscore_charsets STRING %prec UNARY
   {
   	$$ = &IntroducerExpr{CharacterSet: $1, Expr: NewStrLiteral($2)}
   }

text_literal_or_arg:
  text_literal
  {
    $$ = $1
  }
| VALUE_ARG
  {
    $$ = NewArgument($1[1:])
    bindVariable(yylex, $1[1:])
  }

keys:
  PRIMARY KEY
  {
    $$ = colKeyPrimary
  }
| UNIQUE
  {
    $$ = colKeyUnique
  }
| UNIQUE KEY
  {
    $$ = colKeyUniqueKey
  }
| KEY
  {
    $$ = colKey
  }

column_type:
  numeric_type unsigned_opt zero_fill_opt
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
| FLOAT_TYPE float_length_opt
  {
    $$ = ColumnType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
  }
| DECIMAL_TYPE decimal_length_opt
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
| YEAR length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }

char_type:
  CHAR length_opt charset_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2, Charset: $3}
  }
| CHAR length_opt BYTE
  {
    // CHAR BYTE is an alias for binary. See also:
    // https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html
    $$ = ColumnType{Type: "binary", Length: $2}
  }
| VARCHAR length_opt charset_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2, Charset: $3}
  }
| BINARY length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| VARBINARY length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| TEXT charset_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2}
  }
| TINYTEXT charset_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2}
  }
| MEDIUMTEXT charset_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2}
  }
| LONGTEXT charset_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2}
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
| ENUM '(' enum_values ')' charset_opt
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3, Charset: $5}
  }
// need set_values / SetValues ?
| SET '(' enum_values ')' charset_opt
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3, Charset: $5}
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
    $$ = append($$, encodeSQLString($1))
  }
| enum_values ',' STRING
  {
    $$ = append($1, encodeSQLString($3))
  }

length_opt:
  {
    $$ = nil
  }
| '(' INTEGRAL ')'
  {
    $$ = NewIntLiteral($2)
  }

float_length_opt:
  {
    $$ = LengthScaleOption{}
  }
| '(' INTEGRAL ',' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntLiteral($2),
        Scale: NewIntLiteral($4),
    }
  }

decimal_length_opt:
  {
    $$ = LengthScaleOption{}
  }
| '(' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntLiteral($2),
    }
  }
| '(' INTEGRAL ',' INTEGRAL ')'
  {
    $$ = LengthScaleOption{
        Length: NewIntLiteral($2),
        Scale: NewIntLiteral($4),
    }
  }

unsigned_opt:
  {
    $$ = false
  }
| UNSIGNED
  {
    $$ = true
  }
| SIGNED
  {
    $$ = false
  }

zero_fill_opt:
  {
    $$ = false
  }
| ZEROFILL
  {
    $$ = true
  }

charset_opt:
  {
    $$ = ColumnCharset{}
  }
| charset_or_character_set sql_id binary_opt
  {
    $$ = ColumnCharset{Name: string($2.String()), Binary: $3}
  }
| charset_or_character_set STRING binary_opt
  {
    $$ = ColumnCharset{Name: encodeSQLString($2), Binary: $3}
  }
| charset_or_character_set BINARY
  {
    $$ = ColumnCharset{Name: string($2)}
  }
| ASCII binary_opt
  {
    // ASCII: Shorthand for CHARACTER SET latin1.
    $$ = ColumnCharset{Name: "latin1", Binary: $2}
  }
| UNICODE binary_opt
  {
    // UNICODE: Shorthand for CHARACTER SET ucs2.
    $$ = ColumnCharset{Name: "ucs2", Binary: $2}
  }
| BINARY
  {
    // BINARY: Shorthand for default CHARACTER SET but with binary collation
    $$ = ColumnCharset{Name: "", Binary: true}
  }
| BINARY ASCII
  {
    // BINARY ASCII: Shorthand for CHARACTER SET latin1 with binary collation
    $$ = ColumnCharset{Name: "latin1", Binary: true}
  }
| BINARY UNICODE
  {
    // BINARY UNICODE: Shorthand for CHARACTER SET ucs2 with binary collation
    $$ = ColumnCharset{Name: "ucs2", Binary: true}
  }

binary_opt:
  {
    $$ = false
  }
| BINARY
  {
    $$ = true
  }

collate_opt:
  {
    $$ = ""
  }
| COLLATE id_or_var
  {
    $$ = string($2.String())
  }
| COLLATE STRING
  {
    $$ = encodeSQLString($2)
  }


index_definition:
  index_info '(' index_column_list ')' index_option_list_opt
  {
    $$ = &IndexDefinition{Info: $1, Columns: $3, Options: $5}
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
  using_index_type
  {
    $$ = $1
  }
| KEY_BLOCK_SIZE equal_opt INTEGRAL
  {
    // should not be string
    $$ = &IndexOption{Name: string($1), Value: NewIntLiteral($3)}
  }
| COMMENT_KEYWORD STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrLiteral($2)}
  }
| VISIBLE
  {
    $$ = &IndexOption{Name: string($1) }
  }
| INVISIBLE
  {
    $$ = &IndexOption{Name: string($1) }
  }
| WITH PARSER id_or_var
  {
    $$ = &IndexOption{Name: string($1) + " " + string($2), String: $3.String()}
  }
| ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrLiteral($3)}
  }
| SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &IndexOption{Name: string($1), Value: NewStrLiteral($3)}
  }

equal_opt:
  /* empty */
  {
    $$ = ""
  }
| '='
  {
    $$ = string($1)
  }

index_info:
  constraint_name_opt PRIMARY KEY name_opt
  {
    $$ = &IndexInfo{Type: string($2) + " " + string($3), ConstraintName: NewColIdent($1), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
  }
| SPATIAL index_or_key_opt name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Spatial: true, Unique: false}
  }
| FULLTEXT index_or_key_opt name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Fulltext: true, Unique: false}
  }
| constraint_name_opt UNIQUE index_or_key_opt name_opt
  {
    $$ = &IndexInfo{Type: string($2) + " " + string($3), ConstraintName: NewColIdent($1), Name: NewColIdent($4), Unique: true}
  }
| index_or_key name_opt
  {
    $$ = &IndexInfo{Type: string($1), Name: NewColIdent($2), Unique: false}
  }

constraint_name_opt:
  {
    $$ = ""
  }
| CONSTRAINT name_opt
  {
    $$ = $2
  }

index_symbols:
  INDEX
  {
    $$ = string($1)
  }
| KEYS
  {
    $$ = string($1)
  }
| INDEXES
  {
    $$ = string($1)
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

index_or_key_opt:
  {
    $$ = "key"
  }
| index_or_key
  {
    $$ = $1
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

name_opt:
  {
    $$ = ""
  }
| id_or_var
  {
    $$ = string($1.String())
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
  sql_id length_opt asc_desc_opt
  {
    $$ = &IndexColumn{Column: $1, Length: $2, Direction: $3}
  }
| openb expression closeb asc_desc_opt
  {
    $$ = &IndexColumn{Expression: $2, Direction: $4}
  }

constraint_definition:
  CONSTRAINT id_or_var_opt constraint_info
  {
    $$ = &ConstraintDefinition{Name: $2, Details: $3}
  }
|  constraint_info
  {
    $$ = &ConstraintDefinition{Details: $1}
  }

check_constraint_definition:
  CONSTRAINT id_or_var_opt check_constraint_info
  {
    $$ = &ConstraintDefinition{Name: $2, Details: $3}
  }
|  check_constraint_info
  {
    $$ = &ConstraintDefinition{Details: $1}
  }

constraint_info:
  FOREIGN KEY name_opt '(' column_list ')' reference_definition
  {
    $$ = &ForeignKeyDefinition{IndexName: NewColIdent($3), Source: $5, ReferenceDefinition: $7}
  }

reference_definition:
  REFERENCES table_name '(' column_list ')' fk_match_opt
  {
    $$ = &ReferenceDefinition{ReferencedTable: $2, ReferencedColumns: $4, Match: $6}
  }
| REFERENCES table_name '(' column_list ')' fk_match_opt fk_on_delete
  {
    $$ = &ReferenceDefinition{ReferencedTable: $2, ReferencedColumns: $4, Match: $6, OnDelete: $7}
  }
| REFERENCES table_name '(' column_list ')' fk_match_opt fk_on_update
  {
    $$ = &ReferenceDefinition{ReferencedTable: $2, ReferencedColumns: $4, Match: $6, OnUpdate: $7}
  }
| REFERENCES table_name '(' column_list ')' fk_match_opt fk_on_delete fk_on_update
  {
    $$ = &ReferenceDefinition{ReferencedTable: $2, ReferencedColumns: $4, Match: $6, OnDelete: $7, OnUpdate: $8}
  }
| REFERENCES table_name '(' column_list ')' fk_match_opt fk_on_update fk_on_delete
  {
    $$ = &ReferenceDefinition{ReferencedTable: $2, ReferencedColumns: $4, Match: $6, OnUpdate: $7, OnDelete: $8}
  }

reference_definition_opt:
  {
    $$ = nil
  }
| reference_definition
  {
    $$ = $1
  }

check_constraint_info:
  CHECK '(' expression ')' enforced_opt
  {
    $$ = &CheckConstraintDefinition{Expr: $3, Enforced: $5}
  }

fk_match:
  MATCH fk_match_action
  {
    $$ = $2
  }

fk_match_action:
  FULL
  {
    $$ = Full
  }
| PARTIAL
  {
    $$ = Partial
  }
| SIMPLE
  {
    $$ = Simple
  }

fk_match_opt:
  {
    $$ = DefaultMatch
  }
| fk_match
  {
    $$ = $1
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

restrict_or_cascade_opt:
  {
    $$ = ""
  }
| RESTRICT
  {
    $$ = string($1)
  }
| CASCADE
  {
    $$ = string($1)
  }

enforced:
  ENFORCED
  {
    $$ = true
  }
| NOT ENFORCED
  {
    $$ = false
  }

enforced_opt:
  {
    $$ = true
  }
| enforced
  {
    $$ = $1
  }

table_option_list_opt:
  {
    $$ = nil
  }
| table_option_list
  {
    $$ = $1
  }

table_option_list:
  table_option
  {
    $$ = TableOptions{$1}
  }
| table_option_list ',' table_option
  {
    $$ = append($1,$3)
  }
| table_option_list table_option
  {
    $$ = append($1,$2)
  }

space_separated_table_option_list:
  table_option
  {
    $$ = TableOptions{$1}
  }
| space_separated_table_option_list table_option
  {
    $$ = append($1,$2)
  }

table_option:
  AUTO_INCREMENT equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| AUTOEXTEND_SIZE equal_opt INTEGRAL
  {
    $$ = &TableOption{Name: string($1), Value: NewIntLiteral($3)}
  }
| AVG_ROW_LENGTH equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| default_optional charset_or_character_set equal_opt charset
  {
    $$ = &TableOption{Name:(string($2)), String:$4, CaseSensitive: true}
  }
| default_optional COLLATE equal_opt charset
  {
    $$ = &TableOption{Name:string($2), String:$4, CaseSensitive: true}
  }
| CHECKSUM equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| COMMENT_KEYWORD equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
  }
| COMPRESSION equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
  }
| CONNECTION equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
  }
| DATA DIRECTORY equal_opt STRING
  {
    $$ = &TableOption{Name:(string($1)+" "+string($2)), Value:NewStrLiteral($4)}
  }
| INDEX DIRECTORY equal_opt STRING
  {
    $$ = &TableOption{Name:(string($1)+" "+string($2)), Value:NewStrLiteral($4)}
  }
| DELAY_KEY_WRITE equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| ENCRYPTION equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
  }
| ENGINE equal_opt table_alias
  {
    $$ = &TableOption{Name:string($1), String:$3.String(), CaseSensitive: true}
  }
| ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: NewStrLiteral($3)}
  }
| INSERT_METHOD equal_opt insert_method_options
  {
    $$ = &TableOption{Name:string($1), String:string($3)}
  }
| KEY_BLOCK_SIZE equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| MAX_ROWS equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| MIN_ROWS equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| PACK_KEYS equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| PACK_KEYS equal_opt DEFAULT
  {
    $$ = &TableOption{Name:string($1), String:string($3)}
  }
| PASSWORD equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
  }
| ROW_FORMAT equal_opt row_format_options
  {
    $$ = &TableOption{Name:string($1), String:string($3)}
  }
| SECONDARY_ENGINE_ATTRIBUTE equal_opt STRING
  {
    $$ = &TableOption{Name: string($1), Value: NewStrLiteral($3)}
  }
| STATS_AUTO_RECALC equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| STATS_AUTO_RECALC equal_opt DEFAULT
  {
    $$ = &TableOption{Name:string($1), String:string($3)}
  }
| STATS_PERSISTENT equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| STATS_PERSISTENT equal_opt DEFAULT
  {
    $$ = &TableOption{Name:string($1), String:string($3)}
  }
| STATS_SAMPLE_PAGES equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| TABLESPACE equal_opt sql_id storage_opt
  {
    $$ = &TableOption{Name:string($1), String: ($3.String() + $4)}
  }
| UNION equal_opt '(' table_name_list ')'
  {
    $$ = &TableOption{Name:string($1), Tables: $4}
  }

storage_opt:
  {
    $$ = ""
  }
| STORAGE DISK
  {
    $$ = " " + string($1) + " " + string($2)
  }
| STORAGE MEMORY
  {
    $$ = " " + string($1) + " " + string($2)
  }

row_format_options:
  DEFAULT
| DYNAMIC
| FIXED
| COMPRESSED
| REDUNDANT
| COMPACT

insert_method_options:
  NO
| FIRST
| LAST

table_opt_value:
  reserved_sql_id
  {
    $$ = $1.String()
  }
| STRING
  {
    $$ = encodeSQLString($1)
  }
| INTEGRAL
  {
    $$ = string($1)
  }

column_opt:
  {
    $$ = ""
  }
| COLUMN

first_opt:
  {
    $$ = false
  }
| FIRST
  {
    $$ = true
  }

after_opt:
  {
    $$ = nil
  }
| AFTER column_name
  {
    $$ = $2
  }

expire_opt:
  {
    $$ = ""
  }
| EXPIRE STRING
  {
    $$ = string($2)
  }

ratio_opt:
  {
    $$ = nil
  }
| RATIO INTEGRAL
  {
    $$ = NewIntLiteral($2)
  }
| RATIO DECIMAL
  {
    $$ = NewDecimalLiteral($2)
  }

alter_commands_list:
  {
    $$ = nil
  }
| alter_options
  {
    $$ = $1
  }
| alter_options ',' ORDER BY column_list
  {
    $$ = append($1,&OrderByOption{Cols:$5})
  }
| alter_commands_modifier_list
  {
    $$ = $1
  }
| alter_commands_modifier_list ',' alter_options
  {
    $$ = append($1,$3...)
  }
| alter_commands_modifier_list ',' alter_options ',' ORDER BY column_list
  {
    $$ = append(append($1,$3...),&OrderByOption{Cols:$7})
  }

alter_options:
  alter_option
  {
    $$ = []AlterOption{$1}
  }
| alter_options ',' alter_option
  {
    $$ = append($1,$3)
  }
| alter_options ',' alter_commands_modifier
  {
    $$ = append($1,$3)
  }

alter_option:
  space_separated_table_option_list
  {
    $$ = $1
  }
| ADD check_constraint_definition
  {
    $$ = &AddConstraintDefinition{ConstraintDefinition: $2}
  }
| ADD constraint_definition
  {
    $$ = &AddConstraintDefinition{ConstraintDefinition: $2}
  }
| ADD index_definition
  {
    $$ = &AddIndexDefinition{IndexDefinition: $2}
  }
| ADD column_opt '(' column_definition_list ')'
  {
    $$ = &AddColumns{Columns: $4}
  }
| ADD column_opt column_definition first_opt after_opt
  {
    $$ = &AddColumns{Columns: []*ColumnDefinition{$3}, First:$4, After:$5}
  }
| ALTER column_opt column_name DROP DEFAULT
  {
    $$ = &AlterColumn{Column: $3, DropDefault:true}
  }
| ALTER column_opt column_name SET DEFAULT signed_literal_or_null
  {
    $$ = &AlterColumn{Column: $3, DropDefault:false, DefaultVal:$6}
  }
| ALTER column_opt column_name SET DEFAULT openb expression closeb
  {
	$$ = &AlterColumn{Column: $3, DropDefault:false, DefaultVal:$7}
  }
| ALTER column_opt column_name SET VISIBLE
  {
    val := false
    $$ = &AlterColumn{Column: $3, Invisible:&val}
  }
| ALTER column_opt column_name SET INVISIBLE
  {
    val := true
    $$ = &AlterColumn{Column: $3, Invisible:&val}
  }
| ALTER CHECK id_or_var enforced
  {
    $$ = &AlterCheck{Name: $3, Enforced: $4}
  }
| ALTER INDEX id_or_var VISIBLE
  {
    $$ = &AlterIndex{Name: $3, Invisible: false}
  }
| ALTER INDEX id_or_var INVISIBLE
  {
    $$ = &AlterIndex{Name: $3, Invisible: true}
  }
| CHANGE column_opt column_name column_definition first_opt after_opt
  {
    $$ = &ChangeColumn{OldColumn:$3, NewColDefinition:$4, First:$5, After:$6}
  }
| MODIFY column_opt column_definition first_opt after_opt
  {
    $$ = &ModifyColumn{NewColDefinition:$3, First:$4, After:$5}
  }
| CONVERT TO charset_or_character_set charset collate_opt
  {
    $$ = &AlterCharset{CharacterSet:$4, Collate:$5}
  }
| DISABLE KEYS
  {
    $$ = &KeyState{Enable:false}
  }
| ENABLE KEYS
  {
    $$ = &KeyState{Enable:true}
  }
| DISCARD TABLESPACE
  {
    $$ = &TablespaceOperation{Import:false}
  }
| IMPORT TABLESPACE
  {
    $$ = &TablespaceOperation{Import:true}
  }
| DROP column_opt column_name
  {
    $$ = &DropColumn{Name:$3}
  }
| DROP index_or_key id_or_var
  {
    $$ = &DropKey{Type:NormalKeyType, Name:$3}
  }
| DROP PRIMARY KEY
  {
    $$ = &DropKey{Type:PrimaryKeyType}
  }
| DROP FOREIGN KEY id_or_var
  {
    $$ = &DropKey{Type:ForeignKeyType, Name:$4}
  }
| DROP CHECK id_or_var
  {
    $$ = &DropKey{Type:CheckKeyType, Name:$3}
  }
| DROP CONSTRAINT id_or_var
  {
    $$ = &DropKey{Type:CheckKeyType, Name:$3}
  }
| FORCE
  {
    $$ = &Force{}
  }
| RENAME to_opt table_name
  {
    $$ = &RenameTableName{Table:$3}
  }
| RENAME index_or_key id_or_var TO id_or_var
  {
    $$ = &RenameIndex{OldName:$3, NewName:$5}
  }

alter_commands_modifier_list:
  alter_commands_modifier
  {
    $$ = []AlterOption{$1}
  }
| alter_commands_modifier_list ',' alter_commands_modifier
  {
    $$ = append($1,$3)
  }

alter_commands_modifier:
  ALGORITHM equal_opt DEFAULT
    {
      $$ = AlgorithmValue(string($3))
    }
  | ALGORITHM equal_opt INPLACE
    {
      $$ = AlgorithmValue(string($3))
    }
  | ALGORITHM equal_opt COPY
    {
      $$ = AlgorithmValue(string($3))
    }
  | ALGORITHM equal_opt INSTANT
    {
      $$ = AlgorithmValue(string($3))
    }
  | LOCK equal_opt DEFAULT
    {
      $$ = &LockOption{Type:DefaultType}
    }
  | LOCK equal_opt NONE
    {
      $$ = &LockOption{Type:NoneType}
    }
  | LOCK equal_opt SHARED
    {
      $$ = &LockOption{Type:SharedType}
    }
  | LOCK equal_opt EXCLUSIVE
    {
      $$ = &LockOption{Type:ExclusiveType}
    }
  | WITH VALIDATION
    {
      $$ = &Validation{With:true}
    }
  | WITHOUT VALIDATION
    {
      $$ = &Validation{With:false}
    }

alter_statement:
  alter_table_prefix alter_commands_list partitions_options_opt
  {
    $1.FullyParsed = true
    $1.AlterOptions = $2
    $1.PartitionOption = $3
    $$ = $1
  }
| alter_table_prefix alter_commands_list REMOVE PARTITIONING
  {
    $1.FullyParsed = true
    $1.AlterOptions = $2
    $1.PartitionSpec = &PartitionSpec{Action:RemoveAction}
    $$ = $1
  }
| alter_table_prefix alter_commands_modifier_list ',' partition_operation
  {
    $1.FullyParsed = true
    $1.AlterOptions = $2
    $1.PartitionSpec = $4
    $$ = $1
  }
| alter_table_prefix partition_operation
  {
    $1.FullyParsed = true
    $1.PartitionSpec = $2
    $$ = $1
  }
| ALTER comment_opt algorithm_view definer_opt security_view_opt VIEW table_name column_list_opt AS select_statement check_option_opt
  {
    $$ = &AlterView{ViewName: $7.ToViewName(), Comments: Comments($2).Parsed(), Algorithm:$3, Definer: $4 ,Security:$5, Columns:$8, Select: $10, CheckOption: $11 }
  }
// The syntax here causes a shift / reduce issue, because ENCRYPTION is a non reserved keyword
// and the database identifier is optional. When no identifier is given, the current database
// is used. This means though that `alter database encryption` is ambiguous whether it means
// the encryption keyword, or the encryption database name, resulting in the conflict.
// The preference here is to shift, so it is treated as a database name. This matches the MySQL
// behavior as well.
| alter_database_prefix table_id_opt create_options
  {
    $1.FullyParsed = true
    $1.DBName = $2
    $1.AlterOptions = $3
    $$ = $1
  }
| alter_database_prefix table_id UPGRADE DATA DIRECTORY NAME
  {
    $1.FullyParsed = true
    $1.DBName = $2
    $1.UpdateDataDirectory = true
    $$ = $1
  }
| ALTER comment_opt VSCHEMA CREATE VINDEX table_name vindex_type_opt vindex_params_opt
  {
    $$ = &AlterVschema{
        Action: CreateVindexDDLAction,
        Table: $6,
        VindexSpec: &VindexSpec{
          Name: NewColIdent($6.Name.String()),
          Type: $7,
          Params: $8,
        },
      }
  }
| ALTER comment_opt VSCHEMA DROP VINDEX table_name
  {
    $$ = &AlterVschema{
        Action: DropVindexDDLAction,
        Table: $6,
        VindexSpec: &VindexSpec{
          Name: NewColIdent($6.Name.String()),
        },
      }
  }
| ALTER comment_opt VSCHEMA ADD TABLE table_name
  {
    $$ = &AlterVschema{Action: AddVschemaTableDDLAction, Table: $6}
  }
| ALTER comment_opt VSCHEMA DROP TABLE table_name
  {
    $$ = &AlterVschema{Action: DropVschemaTableDDLAction, Table: $6}
  }
| ALTER comment_opt VSCHEMA ON table_name ADD VINDEX sql_id '(' column_list ')' vindex_type_opt vindex_params_opt
  {
    $$ = &AlterVschema{
        Action: AddColVindexDDLAction,
        Table: $5,
        VindexSpec: &VindexSpec{
            Name: $8,
            Type: $12,
            Params: $13,
        },
        VindexCols: $10,
      }
  }
| ALTER comment_opt VSCHEMA ON table_name DROP VINDEX sql_id
  {
    $$ = &AlterVschema{
        Action: DropColVindexDDLAction,
        Table: $5,
        VindexSpec: &VindexSpec{
            Name: $8,
        },
      }
  }
| ALTER comment_opt VSCHEMA ADD SEQUENCE table_name
  {
    $$ = &AlterVschema{Action: AddSequenceDDLAction, Table: $6}
  }
| ALTER comment_opt VSCHEMA ON table_name ADD AUTO_INCREMENT sql_id USING table_name
  {
    $$ = &AlterVschema{
        Action: AddAutoIncDDLAction,
        Table: $5,
        AutoIncSpec: &AutoIncSpec{
            Column: $8,
            Sequence: $10,
        },
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING RETRY
  {
    $$ = &AlterMigration{
      Type: RetryMigrationType,
      UUID: string($4),
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING CLEANUP
  {
    $$ = &AlterMigration{
      Type: CleanupMigrationType,
      UUID: string($4),
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING COMPLETE
  {
    $$ = &AlterMigration{
      Type: CompleteMigrationType,
      UUID: string($4),
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING CANCEL
  {
    $$ = &AlterMigration{
      Type: CancelMigrationType,
      UUID: string($4),
    }
  }
| ALTER comment_opt VITESS_MIGRATION CANCEL ALL
  {
    $$ = &AlterMigration{
      Type: CancelAllMigrationType,
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING THROTTLE expire_opt ratio_opt
  {
    $$ = &AlterMigration{
      Type: ThrottleMigrationType,
      UUID: string($4),
      Expire: $6,
      Ratio: $7,
    }
  }
| ALTER comment_opt VITESS_MIGRATION THROTTLE ALL expire_opt ratio_opt
  {
    $$ = &AlterMigration{
      Type: ThrottleAllMigrationType,
      Expire: $6,
      Ratio: $7,
    }
  }
| ALTER comment_opt VITESS_MIGRATION STRING UNTHROTTLE
  {
    $$ = &AlterMigration{
      Type: UnthrottleMigrationType,
      UUID: string($4),
    }
  }
| ALTER comment_opt VITESS_MIGRATION UNTHROTTLE ALL
  {
    $$ = &AlterMigration{
      Type: UnthrottleAllMigrationType,
    }
  }

partitions_options_opt:
  {
    $$ = nil
  }
| PARTITION BY partitions_options_beginning partitions_opt subpartition_opt partition_definitions_opt
    {
      $3.Partitions = $4
      $3.SubPartition = $5
      $3.Definitions = $6
      $$ = $3
    }

partitions_options_beginning:
  linear_opt HASH '(' expression ')'
    {
      $$ = &PartitionOption {
        IsLinear: $1,
        Type: HashType,
        Expr: $4,
      }
    }
| linear_opt KEY algorithm_opt '(' column_list ')'
    {
      $$ = &PartitionOption {
        IsLinear: $1,
        Type: KeyType,
        KeyAlgorithm: $3,
        ColList: $5,
      }
    }
| range_or_list '(' expression ')'
    {
      $$ = &PartitionOption {
        Type: $1,
        Expr: $3,
      }
    }
| range_or_list COLUMNS '(' column_list ')'
  {
    $$ = &PartitionOption {
        Type: $1,
        ColList: $4,
    }
  }

subpartition_opt:
  {
    $$ = nil
  }
| SUBPARTITION BY linear_opt HASH '(' expression ')' subpartitions_opt
  {
    $$ = &SubPartition {
      IsLinear: $3,
      Type: HashType,
      Expr: $6,
      SubPartitions: $8,
    }
  }
| SUBPARTITION BY linear_opt KEY algorithm_opt '(' column_list ')' subpartitions_opt
  {
    $$ = &SubPartition {
      IsLinear: $3,
      Type: KeyType,
      KeyAlgorithm: $5,
      ColList: $7,
      SubPartitions: $9,
    }
  }

partition_definitions_opt:
  {
    $$ = nil
  }
| '(' partition_definitions ')'
  {
    $$ = $2
  }

linear_opt:
  {
    $$ = false
  }
| LINEAR
  {
    $$ = true
  }

algorithm_opt:
  {
    $$ = 0
  }
| ALGORITHM '=' INTEGRAL
  {
    $$ = convertStringToInt($3)
  }

json_table_function:
  JSON_TABLE openb expression ',' text_literal_or_arg jt_columns_clause closeb as_opt_id
  {
    $$ = &JSONTableExpr{Expr: $3, Filter: $5, Columns: $6, Alias: $8}
  }

jt_columns_clause:
  COLUMNS openb columns_list closeb
  {
    $$= $3
  }

columns_list:
  jt_column
  {
    $$= []*JtColumnDefinition{$1}
  }
| columns_list ',' jt_column
  {
    $$ = append($1, $3)
  }

jt_column:
 sql_id FOR ORDINALITY
  {
    $$ = &JtColumnDefinition{JtOrdinal: &JtOrdinalColDef{Name: $1}}
  }
| sql_id column_type collate_opt jt_exists_opt PATH text_literal_or_arg
  {
    $2.Options= &ColumnTypeOptions{Collate:$3}
    jtPath := &JtPathColDef{Name: $1, Type: $2, JtColExists: $4, Path: $6}
    $$ = &JtColumnDefinition{JtPath: jtPath}
  }
| sql_id column_type collate_opt jt_exists_opt PATH text_literal_or_arg on_empty
  {
    $2.Options= &ColumnTypeOptions{Collate:$3}
    jtPath := &JtPathColDef{Name: $1, Type: $2, JtColExists: $4, Path: $6, EmptyOnResponse: $7}
    $$ = &JtColumnDefinition{JtPath: jtPath}
  }
| sql_id column_type collate_opt jt_exists_opt PATH text_literal_or_arg on_error
  {
    $2.Options= &ColumnTypeOptions{Collate:$3}
    jtPath := &JtPathColDef{Name: $1, Type: $2, JtColExists: $4, Path: $6, ErrorOnResponse: $7}
    $$ = &JtColumnDefinition{JtPath: jtPath}
  }
| sql_id column_type collate_opt jt_exists_opt PATH text_literal_or_arg on_empty on_error
  {
    $2.Options= &ColumnTypeOptions{Collate:$3}
    jtPath := &JtPathColDef{Name: $1, Type: $2, JtColExists: $4, Path: $6, EmptyOnResponse: $7, ErrorOnResponse: $8}
    $$ = &JtColumnDefinition{JtPath: jtPath}
  }
| NESTED jt_path_opt text_literal_or_arg jt_columns_clause
  {
    jtNestedPath := &JtNestedPathColDef{Path: $3, Columns: $4}
    $$ = &JtColumnDefinition{JtNestedPath: jtNestedPath}
  }

jt_path_opt:
  {
    $$ = false
  }
| PATH
  {
    $$ = true
  }
jt_exists_opt:
  {
    $$=false
  }
| EXISTS
  {
    $$=true
  }

on_empty:
  json_on_response ON EMPTY
  {
    $$= $1
  }

on_error:
  json_on_response ON ERROR
  {
    $$= $1
  }

json_on_response:
  ERROR
  {
    $$ = &JtOnResponse{ResponseType: ErrorJSONType}
  }
| NULL
  {
    $$ = &JtOnResponse{ResponseType: NullJSONType}
  }
| DEFAULT text_literal_or_arg
  {
    $$ = &JtOnResponse{ResponseType: DefaultJSONType, Expr: $2}
  }

range_or_list:
  RANGE
  {
    $$ = RangeType
  }
| LIST
  {
    $$ = ListType
  }

partitions_opt:
  {
    $$ = -1
  }
| PARTITIONS INTEGRAL
  {
    $$ = convertStringToInt($2)
  }

subpartitions_opt:
  {
    $$ = -1
  }
| SUBPARTITIONS INTEGRAL
  {
    $$ = convertStringToInt($2)
  }

partition_operation:
  ADD PARTITION '(' partition_definition ')'
  {
    $$ = &PartitionSpec{Action: AddAction, Definitions: []*PartitionDefinition{$4}}
  }
| DROP PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:DropAction, Names:$3}
  }
| REORGANIZE PARTITION partition_list INTO openb partition_definitions closeb
  {
    $$ = &PartitionSpec{Action: ReorganizeAction, Names: $3, Definitions: $6}
  }
| DISCARD PARTITION partition_list TABLESPACE
  {
    $$ = &PartitionSpec{Action:DiscardAction, Names:$3}
  }
| DISCARD PARTITION ALL TABLESPACE
  {
    $$ = &PartitionSpec{Action:DiscardAction, IsAll:true}
  }
| IMPORT PARTITION partition_list TABLESPACE
  {
    $$ = &PartitionSpec{Action:ImportAction, Names:$3}
  }
| IMPORT PARTITION ALL TABLESPACE
  {
    $$ = &PartitionSpec{Action:ImportAction, IsAll:true}
  }
| TRUNCATE PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:TruncateAction, Names:$3}
  }
| TRUNCATE PARTITION ALL
  {
    $$ = &PartitionSpec{Action:TruncateAction, IsAll:true}
  }
| COALESCE PARTITION INTEGRAL
  {
    $$ = &PartitionSpec{Action:CoalesceAction, Number:NewIntLiteral($3) }
  }
| EXCHANGE PARTITION sql_id WITH TABLE table_name without_valid_opt
  {
    $$ = &PartitionSpec{Action:ExchangeAction, Names: Partitions{$3}, TableName: $6, WithoutValidation: $7}
  }
| ANALYZE PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:AnalyzeAction, Names:$3}
  }
| ANALYZE PARTITION ALL
  {
    $$ = &PartitionSpec{Action:AnalyzeAction, IsAll:true}
  }
| CHECK PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:CheckAction, Names:$3}
  }
| CHECK PARTITION ALL
  {
    $$ = &PartitionSpec{Action:CheckAction, IsAll:true}
  }
| OPTIMIZE PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:OptimizeAction, Names:$3}
  }
| OPTIMIZE PARTITION ALL
  {
    $$ = &PartitionSpec{Action:OptimizeAction, IsAll:true}
  }
| REBUILD PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:RebuildAction, Names:$3}
  }
| REBUILD PARTITION ALL
  {
    $$ = &PartitionSpec{Action:RebuildAction, IsAll:true}
  }
| REPAIR PARTITION partition_list
  {
    $$ = &PartitionSpec{Action:RepairAction, Names:$3}
  }
| REPAIR PARTITION ALL
  {
    $$ = &PartitionSpec{Action:RepairAction, IsAll:true}
  }
| UPGRADE PARTITIONING
  {
    $$ = &PartitionSpec{Action:UpgradeAction}
  }

without_valid_opt:
  {
    $$ = false
  }
| WITH VALIDATION
  {
    $$ = false
  }
| WITHOUT VALIDATION
  {
    $$ = true
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
  partition_name partition_definition_attribute_list_opt
  {
    $$.Options = $2
  }

partition_definition_attribute_list_opt:
  {
    $$ = &PartitionDefinitionOptions{}
  }
| partition_definition_attribute_list_opt partition_value_range
  {
    $1.ValueRange = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_comment
  {
    $1.Comment = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_engine
  {
    $1.Engine = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_data_directory
  {
    $1.DataDirectory = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_index_directory
  {
    $1.IndexDirectory = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_max_rows
  {
    val := $2
    $1.MaxRows = &val
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_min_rows
  {
    val := $2
    $1.MinRows = &val
    $$ = $1
  }
| partition_definition_attribute_list_opt partition_tablespace_name
  {
    $1.TableSpace = $2
    $$ = $1
  }
| partition_definition_attribute_list_opt subpartition_definition_list_with_brackets
  {
    $1.SubPartitionDefinitions = $2
    $$ = $1
  }

subpartition_definition_list_with_brackets:
  openb subpartition_definition_list closeb{
    $$ = $2
  }

subpartition_definition_list:
  subpartition_definition
  {
    $$ = SubPartitionDefinitions{$1}
  }
| subpartition_definition_list ',' subpartition_definition
  {
    $$ = append($1, $3)
  }

subpartition_definition:
  SUBPARTITION sql_id subpartition_definition_attribute_list_opt
  {
    $$ = &SubPartitionDefinition{Name:$2, Options: $3}
  }

subpartition_definition_attribute_list_opt:
  {
    $$ = &SubPartitionDefinitionOptions{}
  }
| subpartition_definition_attribute_list_opt partition_comment
  {
    $1.Comment = $2
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_engine
  {
    $1.Engine = $2
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_data_directory
  {
    $1.DataDirectory = $2
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_index_directory
  {
    $1.IndexDirectory = $2
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_max_rows
  {
    val := $2
    $1.MaxRows = &val
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_min_rows
  {
    val := $2
    $1.MinRows = &val
    $$ = $1
  }
| subpartition_definition_attribute_list_opt partition_tablespace_name
  {
    $1.TableSpace = $2
    $$ = $1
  }

partition_value_range:
  VALUES LESS THAN row_tuple
  {
    $$ = &PartitionValueRange{
    	Type: LessThanType,
    	Range: $4,
    }
  }
| VALUES LESS THAN maxvalue
  {
    $$ = &PartitionValueRange{
    	Type: LessThanType,
    	Maxvalue: true,
    }
  }
| VALUES IN row_tuple
  {
    $$ = &PartitionValueRange{
    	Type: InType,
    	Range: $3,
    }
  }

partition_storage_opt:
  {
    $$ = false
  }
| STORAGE
  {
    $$ = true
  }

partition_engine:
  partition_storage_opt ENGINE equal_opt table_alias
  {
    $$ = &PartitionEngine{Storage:$1, Name: $4.String()}
  }

partition_comment:
  COMMENT_KEYWORD equal_opt STRING
  {
    $$ = NewStrLiteral($3)
  }

partition_data_directory:
  DATA DIRECTORY equal_opt STRING
  {
    $$ = NewStrLiteral($4)
  }

partition_index_directory:
  INDEX DIRECTORY equal_opt STRING
  {
    $$ = NewStrLiteral($4)
  }

partition_max_rows:
  MAX_ROWS equal_opt INTEGRAL
  {
    $$ = convertStringToInt($3)
  }

partition_min_rows:
  MIN_ROWS equal_opt INTEGRAL
  {
    $$ = convertStringToInt($3)
  }

partition_tablespace_name:
  TABLESPACE equal_opt table_alias
  {
    $$ = $3.String()
  }

partition_name:
  PARTITION sql_id
  {
    $$ = &PartitionDefinition{Name: $2}
  }

maxvalue:
  MAXVALUE
  {
    $$ = ""
  }
| openb MAXVALUE closeb
  {
    $$ = ""
  }

rename_statement:
  RENAME TABLE rename_list
  {
    $$ = &RenameTable{TablePairs: $3}
  }

rename_list:
  table_name TO table_name
  {
    $$ = []*RenameTablePair{{FromTable: $1, ToTable: $3}}
  }
| rename_list ',' table_name TO table_name
  {
    $$ = append($1, &RenameTablePair{FromTable: $3, ToTable: $5})
  }

drop_statement:
  DROP comment_opt temp_opt TABLE exists_opt table_name_list restrict_or_cascade_opt
  {
    $$ = &DropTable{FromTables: $6, IfExists: $5, Comments: Comments($2).Parsed(), Temp: $3}
  }
| DROP comment_opt INDEX id_or_var ON table_name algorithm_lock_opt
  {
    // Change this to an alter statement
    if $4.Lowered() == "primary" {
      $$ = &AlterTable{FullyParsed:true, Table: $6,AlterOptions: append([]AlterOption{&DropKey{Type:PrimaryKeyType}},$7...)}
    } else {
      $$ = &AlterTable{FullyParsed: true, Table: $6,AlterOptions: append([]AlterOption{&DropKey{Type:NormalKeyType, Name:$4}},$7...)}
    }
  }
| DROP comment_opt VIEW exists_opt view_name_list restrict_or_cascade_opt
  {
    $$ = &DropView{FromTables: $5, Comments: Comments($2).Parsed(), IfExists: $4}
  }
| DROP comment_opt database_or_schema exists_opt table_id
  {
    $$ = &DropDatabase{Comments: Comments($2).Parsed(), DBName: $5, IfExists: $4}
  }

truncate_statement:
  TRUNCATE TABLE table_name
  {
    $$ = &TruncateTable{Table: $3}
  }
| TRUNCATE table_name
  {
    $$ = &TruncateTable{Table: $2}
  }
analyze_statement:
  ANALYZE TABLE table_name
  {
    $$ = &OtherRead{}
  }

show_statement:
  SHOW charset_or_character_set like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Charset, Filter: $3}}
  }
| SHOW COLLATION like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Collation, Filter: $3}}
  }
| SHOW full_opt columns_or_fields from_or_in table_name from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Full: $2, Command: Column, Tbl: $5, DbName: $6, Filter: $7}}
  }
| SHOW DATABASES like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Database, Filter: $3}}
  }
| SHOW SCHEMAS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Database, Filter: $3}}
  }
| SHOW KEYSPACES like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Keyspace, Filter: $3}}
  }
| SHOW VITESS_KEYSPACES like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Keyspace, Filter: $3}}
  }
| SHOW FUNCTION STATUS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Function, Filter: $4}}
  }
| SHOW extended_opt index_symbols from_or_in table_name from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Index, Tbl: $5, DbName: $6, Filter: $7}}
  }
| SHOW OPEN TABLES from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: OpenTable, DbName:$4, Filter: $5}}
  }
| SHOW PRIVILEGES
  {
    $$ = &Show{&ShowBasic{Command: Privilege}}
  }
| SHOW PROCEDURE STATUS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Procedure, Filter: $4}}
  }
| SHOW session_or_local_opt STATUS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: StatusSession, Filter: $4}}
  }
| SHOW GLOBAL STATUS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: StatusGlobal, Filter: $4}}
  }
| SHOW session_or_local_opt VARIABLES like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VariableSession, Filter: $4}}
  }
| SHOW GLOBAL VARIABLES like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VariableGlobal, Filter: $4}}
  }
| SHOW TABLE STATUS from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: TableStatus, DbName:$4, Filter: $5}}
  }
| SHOW full_opt TABLES from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Table, Full: $2, DbName:$4, Filter: $5}}
  }
| SHOW TRIGGERS from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: Trigger, DbName:$3, Filter: $4}}
  }
| SHOW CREATE DATABASE table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateDb, Op: $4}}
  }
| SHOW CREATE EVENT table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateE, Op: $4}}
  }
| SHOW CREATE FUNCTION table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateF, Op: $4}}
  }
| SHOW CREATE PROCEDURE table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateProc, Op: $4}}
  }
| SHOW CREATE TABLE table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateTbl, Op: $4}}
  }
| SHOW CREATE TRIGGER table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateTr, Op: $4}}
  }
| SHOW CREATE VIEW table_name
  {
    $$ = &Show{&ShowCreate{Command: CreateV, Op: $4}}
  }
| SHOW ENGINES
  {
    $$ = &Show{&ShowBasic{Command: Engines}}
  }
| SHOW PLUGINS
  {
    $$ = &Show{&ShowBasic{Command: Plugins}}
  }
| SHOW GLOBAL GTID_EXECUTED from_database_opt
  {
    $$ = &Show{&ShowBasic{Command: GtidExecGlobal, DbName: $4}}
  }
| SHOW GLOBAL VGTID_EXECUTED from_database_opt
  {
    $$ = &Show{&ShowBasic{Command: VGtidExecGlobal, DbName: $4}}
  }
| SHOW VITESS_METADATA VARIABLES like_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessVariables, Filter: $4}}
  }
| SHOW VITESS_MIGRATIONS from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessMigrations, Filter: $4, DbName: $3}}
  }
| SHOW VITESS_MIGRATION STRING LOGS
  {
    $$ = &ShowMigrationLogs{UUID: string($3)}
  }
| SHOW VITESS_THROTTLED_APPS
  {
    $$ = &ShowThrottledApps{}
  }
| SHOW VITESS_REPLICATION_STATUS like_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessReplicationStatus, Filter: $3}}
  }
| SHOW VSCHEMA TABLES
  {
    $$ = &Show{&ShowBasic{Command: VschemaTables}}
  }
| SHOW VSCHEMA VINDEXES
  {
    $$ = &Show{&ShowBasic{Command: VschemaVindexes}}
  }
| SHOW VSCHEMA VINDEXES from_or_on table_name
  {
    $$ = &Show{&ShowBasic{Command: VschemaVindexes, Tbl: $5}}
  }
| SHOW WARNINGS
  {
    $$ = &Show{&ShowBasic{Command: Warnings}}
  }
| SHOW VITESS_SHARDS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessShards, Filter: $3}}
  }
| SHOW VITESS_TABLETS like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessTablets, Filter: $3}}
  }
| SHOW VITESS_TARGET
  {
    $$ = &Show{&ShowBasic{Command: VitessTarget}}
  }
/*
 * Catch-all for show statements without vitess keywords:
 */
| SHOW id_or_var ddl_skip_to_end
  {
    $$ = &Show{&ShowOther{Command: string($2.String())}}
  }
| SHOW CREATE USER ddl_skip_to_end
  {
    $$ = &Show{&ShowOther{Command: string($2) + " " + string($3)}}
   }
| SHOW BINARY id_or_var ddl_skip_to_end /* SHOW BINARY ... */
  {
    $$ = &Show{&ShowOther{Command: string($2) + " " + $3.String()}}
  }
| SHOW BINARY LOGS ddl_skip_to_end /* SHOW BINARY LOGS */
  {
    $$ = &Show{&ShowOther{Command: string($2) + " " + string($3)}}
  }
| SHOW ENGINE ddl_skip_to_end
  {
    $$ = &Show{&ShowOther{Command: string($2)}}
  }
| SHOW FUNCTION CODE table_name
  {
    $$ = &Show{&ShowOther{Command: string($2) + " " + string($3) + " " + String($4)}}
  }
| SHOW PROCEDURE CODE table_name
  {
    $$ = &Show{&ShowOther{Command: string($2) + " " + string($3) + " " + String($4)}}
  }
| SHOW full_opt PROCESSLIST from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowOther{Command: string($3)}}
  }
| SHOW STORAGE ddl_skip_to_end
  {
    $$ = &Show{&ShowOther{Command: string($2)}}
  }

extended_opt:
  /* empty */
  {
    $$ = ""
  }
  | EXTENDED
  {
    $$ = "extended "
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
    $$ = NewTableIdent("")
  }
| FROM table_id
  {
    $$ = $2
  }
| IN table_id
  {
    $$ = $2
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
| WHERE expression
  {
    $$ = &ShowFilter{Filter:$2}
  }

like_opt:
  /* empty */
    {
      $$ = nil
    }
  | LIKE STRING
    {
      $$ = &ShowFilter{Like:string($2)}
    }

session_or_local_opt:
  /* empty */
  {
    $$ = struct{}{}
  }
| SESSION
  {
    $$ = struct{}{}
  }
| LOCAL
  {
    $$ = struct{}{}
  }

from_or_on:
  FROM
  {
    $$ = string($1)
  }
| ON
  {
    $$ = string($1)
  }

use_statement:
  USE table_id
  {
    $$ = &Use{DBName: $2}
  }
| USE
  {
    $$ = &Use{DBName:TableIdent{v:""}}
  }
| USE table_id AT_ID
  {
    $$ = &Use{DBName:NewTableIdent($2.String()+"@"+string($3))}
  }

begin_statement:
  BEGIN
  {
    $$ = &Begin{}
  }
| START TRANSACTION
  {
    $$ = &Begin{}
  }

commit_statement:
  COMMIT
  {
    $$ = &Commit{}
  }

rollback_statement:
  ROLLBACK
  {
    $$ = &Rollback{}
  }
| ROLLBACK work_opt TO savepoint_opt sql_id
  {
    $$ = &SRollback{Name: $5}
  }

work_opt:
  { $$ = struct{}{} }
| WORK
  { $$ = struct{}{} }

savepoint_opt:
  { $$ = struct{}{} }
| SAVEPOINT
  { $$ = struct{}{} }


savepoint_statement:
  SAVEPOINT sql_id
  {
    $$ = &Savepoint{Name: $2}
  }

release_statement:
  RELEASE SAVEPOINT sql_id
  {
    $$ = &Release{Name: $3}
  }

explain_format_opt:
  {
    $$ = EmptyType
  }
| FORMAT '=' JSON
  {
    $$ = JSONType
  }
| FORMAT '=' TREE
  {
    $$ = TreeType
  }
| FORMAT '=' VITESS
  {
    $$ = VitessType
  }
| FORMAT '=' TRADITIONAL
  {
    $$ = TraditionalType
  }
| ANALYZE
  {
    $$ = AnalyzeType
  }

explain_synonyms:
  EXPLAIN
  {
    $$ = $1
  }
| DESCRIBE
  {
    $$ = $1
  }
| DESC
  {
    $$ = $1
  }

explainable_statement:
  select_statement
  {
    $$ = $1
  }
| update_statement
  {
    $$ = $1
  }
| insert_statement
  {
    $$ = $1
  }
| delete_statement
  {
    $$ = $1
  }

wild_opt:
  {
    $$ = ""
  }
| sql_id
  {
    $$ = $1.val
  }
| STRING
  {
    $$ = encodeSQLString($1)
  }

explain_statement:
  explain_synonyms table_name wild_opt
  {
    $$ = &ExplainTab{Table: $2, Wild: $3}
  }
| explain_synonyms explain_format_opt explainable_statement
  {
    $$ = &ExplainStmt{Type: $2, Statement: $3}
  }

other_statement:
  REPAIR skip_to_end
  {
    $$ = &OtherAdmin{}
  }
| OPTIMIZE skip_to_end
  {
    $$ = &OtherAdmin{}
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
  aliased_table_name lock_type
  {
    $$ = &TableAndLockType{Table:$1, Lock:$2}
  }

lock_type:
  READ
  {
    $$ = Read
  }
| READ LOCAL
  {
    $$ = ReadLocal
  }
| WRITE
  {
    $$ = Write
  }
| LOW_PRIORITY WRITE
  {
    $$ = LowPriorityWrite
  }

unlock_statement:
  UNLOCK TABLES
  {
    $$ = &UnlockTables{}
  }

revert_statement:
  REVERT comment_opt VITESS_MIGRATION STRING
  {
    $$ = &RevertMigration{Comments: Comments($2).Parsed(), UUID: string($4)}
  }

flush_statement:
  FLUSH local_opt flush_option_list
  {
    $$ = &Flush{IsLocal: $2, FlushOptions:$3}
  }
| FLUSH local_opt TABLES
  {
    $$ = &Flush{IsLocal: $2}
  }
| FLUSH local_opt TABLES WITH READ LOCK
  {
    $$ = &Flush{IsLocal: $2, WithLock:true}
  }
| FLUSH local_opt TABLES table_name_list
  {
    $$ = &Flush{IsLocal: $2, TableNames:$4}
  }
| FLUSH local_opt TABLES table_name_list WITH READ LOCK
  {
    $$ = &Flush{IsLocal: $2, TableNames:$4, WithLock:true}
  }
| FLUSH local_opt TABLES table_name_list FOR EXPORT
  {
    $$ = &Flush{IsLocal: $2, TableNames:$4, ForExport:true}
  }

flush_option_list:
  flush_option
  {
    $$ = []string{$1}
  }
| flush_option_list ',' flush_option
  {
    $$ = append($1,$3)
  }

flush_option:
  BINARY LOGS
  {
    $$ = string($1) + " " + string($2)
  }
| ENGINE LOGS
  {
    $$ = string($1) + " " + string($2)
  }
| ERROR LOGS
  {
    $$ = string($1) + " " + string($2)
  }
| GENERAL LOGS
  {
    $$ = string($1) + " " + string($2)
  }
| HOSTS
  {
    $$ = string($1)
  }
| LOGS
  {
    $$ = string($1)
  }
| PRIVILEGES
  {
    $$ = string($1)
  }
| RELAY LOGS for_channel_opt
  {
    $$ = string($1) + " " + string($2) + $3
  }
| SLOW LOGS
  {
    $$ = string($1) + " " + string($2)
  }
| OPTIMIZER_COSTS
  {
    $$ = string($1)
  }
| STATUS
  {
    $$ = string($1)
  }
| USER_RESOURCES
  {
    $$ = string($1)
  }

local_opt:
  {
    $$ = false
  }
| LOCAL
  {
    $$ = true
  }
| NO_WRITE_TO_BINLOG
  {
    $$ = true
  }

for_channel_opt:
  {
    $$ = ""
  }
| FOR CHANNEL id_or_var
  {
    $$ = " " + string($1) + " " + string($2) + " " + $3.String()
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
    $$ = true
  }
| UNION ALL
  {
    $$ = false
  }
| UNION DISTINCT
  {
    $$ = true
  }

cache_opt:
{
  $$ = ""
}
| SQL_NO_CACHE
{
  $$ = SQLNoCacheStr
}
| SQL_CACHE
{
  $$ = SQLCacheStr
}

distinct_opt:
  {
    $$ = false
  }
| DISTINCT
  {
    $$ = true
  }
| DISTINCTROW
  {
    $$ = true
  }

prepare_statement:
  PREPARE comment_opt sql_id FROM text_literal_or_arg
  {
    $$ = &PrepareStmt{Name:$3, Comments: Comments($2).Parsed(), Statement:$5}
  }
| PREPARE comment_opt sql_id FROM AT_ID
  {
    $$ = &PrepareStmt{
    	Name:$3,
    	Comments: Comments($2).Parsed(),
    	Statement: &ColName{
    		Name: NewColIdentWithAt(string($5), SingleAt),
    	},
    }
  }

execute_statement:
  EXECUTE comment_opt sql_id execute_statement_list_opt
  {
    $$ = &ExecuteStmt{Name:$3, Comments: Comments($2).Parsed(), Arguments: $4}
  }

execute_statement_list_opt:
  {
    $$ = nil
  }
| USING at_id_list
  {
    $$ = $2
  }

deallocate_statement:
  DEALLOCATE comment_opt PREPARE sql_id
  {
    $$ = &DeallocateStmt{Type:DeallocateType, Comments: Comments($2).Parsed(), Name:$4}
  }
| DROP comment_opt PREPARE sql_id
  {
    $$ = &DeallocateStmt{Type: DropType, Comments: Comments($2).Parsed(), Name: $4}
  }

select_expression_list_opt:
  {
    $$ = nil
  }
| select_expression_list
  {
    $$ = $1
  }

select_options:
  {
    $$ = nil
  }
| select_option
  {
    $$ = []string{$1}
  }
| select_option select_option // TODO: figure out a way to do this recursively instead.
  {                           // TODO: This is a hack since I couldn't get it to work in a nicer way. I got 'conflicts: 8 shift/reduce'
    $$ = []string{$1, $2}
  }
| select_option select_option select_option
  {
    $$ = []string{$1, $2, $3}
  }
| select_option select_option select_option select_option
  {
    $$ = []string{$1, $2, $3, $4}
  }

select_option:
  SQL_NO_CACHE
  {
    $$ = SQLNoCacheStr
  }
| SQL_CACHE
  {
    $$ = SQLCacheStr
  }
| DISTINCT
  {
    $$ = DistinctStr
  }
| DISTINCTROW
  {
    $$ = DistinctStr
  }
| STRAIGHT_JOIN
  {
    $$ = StraightJoinHint
  }
| SQL_CALC_FOUND_ROWS
  {
    $$ = SQLCalcFoundRowsStr
  }
| ALL
  {
    $$ = AllStr // These are not picked up by NewSelect, and so ALL will be dropped. But this is OK, since it's redundant anyway
  }

select_expression_list:
  select_expression
  {
    $$ = SelectExprs{$1}
  }
| select_expression_list ',' select_expression
  {
    $$ = append($$, $3)
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
    $$ = &StarExpr{TableName: TableName{Qualifier: $1, Name: $3}}
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

col_alias:
  sql_id
| STRING
  {
    $$ = NewColIdent(string($1))
  }

from_opt:
  %prec EMPTY_FROM_CLAUSE {
    $$ = TableExprs{&AliasedTableExpr{Expr:TableName{Name: NewTableIdent("dual")}}}
  }
  | from_clause
  {
  	$$ = $1
  }

from_clause:
FROM table_references
  {
    $$ = $2
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
| derived_table as_opt table_id column_list_opt
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $3, Columns: $4}
  }
| openb table_references closeb
  {
    $$ = &ParenTableExpr{Exprs: $2}
  }
| json_table_function
  {
    $$ = $1
  }

derived_table:
  openb query_expression closeb
  {
    $$ = &DerivedTable{Lateral: false, Select: $2}
  }
| LATERAL openb query_expression closeb
  {
    $$ = &DerivedTable{Lateral: true, Select: $3}
  }

aliased_table_name:
table_name as_opt_id index_hint_list_opt
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $2, Hints: $3}
  }
| table_name PARTITION openb partition_list closeb as_opt_id index_hint_list_opt
  {
    $$ = &AliasedTableExpr{Expr:$1, Partitions: $4, As: $6, Hints: $7}
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

at_id_list:
  AT_ID
  {
    $$ = Columns{NewColIdentWithAt(string($1), SingleAt)}
  }
| column_list ',' AT_ID
  {
    $$ = append($$, NewColIdentWithAt(string($3), SingleAt))
  }

index_list:
  sql_id
  {
    $$ = Columns{$1}
  }
| PRIMARY
  {
    $$ = Columns{NewColIdent(string($1))}
  }
| index_list ',' sql_id
  {
    $$ = append($$, $3)
  }
| index_list ',' PRIMARY
  {
    $$ = append($$, NewColIdent(string($3)))
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
  { $$ = &JoinCondition{On: $2} }
| USING '(' column_list ')'
  { $$ = &JoinCondition{Using: $3} }

join_condition_opt:
%prec JOIN
  { $$ = &JoinCondition{} }
| join_condition
  { $$ = $1 }

on_expression_opt:
%prec JOIN
  { $$ = &JoinCondition{} }
| ON expression
  { $$ = &JoinCondition{On: $2} }

as_opt:
  { $$ = struct{}{} }
| AS
  { $$ = struct{}{} }

as_opt_id:
  {
    $$ = NewTableIdent("")
  }
| table_alias
  {
    $$ = $1
  }
| AS table_alias
  {
    $$ = $2
  }

table_alias:
  table_id
| STRING
  {
    $$ = NewTableIdent(string($1))
  }

inner_join:
  JOIN
  {
    $$ = NormalJoinType
  }
| INNER JOIN
  {
    $$ = NormalJoinType
  }
| CROSS JOIN
  {
    $$ = NormalJoinType
  }

straight_join:
  STRAIGHT_JOIN
  {
    $$ = StraightJoinType
  }

outer_join:
  LEFT JOIN
  {
    $$ = LeftJoinType
  }
| LEFT OUTER JOIN
  {
    $$ = LeftJoinType
  }
| RIGHT JOIN
  {
    $$ = RightJoinType
  }
| RIGHT OUTER JOIN
  {
    $$ = RightJoinType
  }

natural_join:
 NATURAL JOIN
  {
    $$ = NaturalJoinType
  }
| NATURAL outer_join
  {
    if $2 == LeftJoinType {
      $$ = NaturalLeftJoinType
    } else {
      $$ = NaturalRightJoinType
    }
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
    $$ = TableName{Qualifier: $1, Name: $3}
  }

delete_table_name:
table_id '.' '*'
  {
    $$ = TableName{Name: $1}
  }

index_hint_list_opt:
  {
    $$ = nil
  }
| index_hint_list
  {
    $$ = $1
  }

index_hint_list:
index_hint
  {
    $$ = IndexHints{$1}
  }
| index_hint_list index_hint
  {
    $$ = append($1,$2)
  }

index_hint:
  USE index_or_key index_hint_for_opt openb index_list closeb
  {
    $$ = &IndexHint{Type: UseOp, ForType:$3, Indexes: $5}
  }
| USE index_or_key index_hint_for_opt openb closeb
  {
    $$ = &IndexHint{Type: UseOp, ForType: $3}
  }
| IGNORE index_or_key index_hint_for_opt openb index_list closeb
  {
    $$ = &IndexHint{Type: IgnoreOp, ForType: $3, Indexes: $5}
  }
| FORCE index_or_key index_hint_for_opt openb index_list closeb
  {
    $$ = &IndexHint{Type: ForceOp, ForType: $3, Indexes: $5}
  }

index_hint_for_opt:
  {
    $$ = NoForType
  }
| FOR JOIN
  {
    $$ = JoinForType
  }
| FOR ORDER BY
  {
    $$ = OrderByForType
  }
| FOR GROUP BY
  {
    $$ = GroupByForType
  }


where_expression_opt:
  {
    $$ = nil
  }
| WHERE expression
  {
    $$ = $2
  }

/* all possible expressions */
expression:
  expression OR expression %prec OR
  {
	$$ = &OrExpr{Left: $1, Right: $3}
  }
| expression XOR expression %prec XOR
  {
	$$ = &XorExpr{Left: $1, Right: $3}
  }
| expression AND expression %prec AND
  {
	$$ = &AndExpr{Left: $1, Right: $3}
  }
| NOT expression %prec NOT
  {
	  $$ = &NotExpr{Expr: $2}
  }
| bool_pri IS is_suffix %prec IS
  {
	 $$ = &IsExpr{Left: $1, Right: $3}
  }
| bool_pri %prec EXPRESSION_PREC_SETTER
  {
	$$ = $1
  }
| expression MEMBER OF openb expression closeb
  {
    $$ = &MemberOfExpr{Value: $1, JSONArr:$5 }
  }

bool_pri:
bool_pri IS NULL %prec IS
  {
	 $$ = &IsExpr{Left: $1, Right: IsNullOp}
  }
| bool_pri IS NOT NULL %prec IS
  {
  	$$ = &IsExpr{Left: $1, Right: IsNotNullOp}
  }
| bool_pri compare predicate
  {
	$$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }
| predicate %prec EXPRESSION_PREC_SETTER
  {
	$$ = $1
  }

predicate:
bit_expr IN col_tuple
  {
	$$ = &ComparisonExpr{Left: $1, Operator: InOp, Right: $3}
  }
| bit_expr NOT IN col_tuple
  {
	$$ = &ComparisonExpr{Left: $1, Operator: NotInOp, Right: $4}
  }
| bit_expr BETWEEN bit_expr AND predicate
  {
	 $$ = &BetweenExpr{Left: $1, IsBetween: true, From: $3, To: $5}
  }
| bit_expr NOT BETWEEN bit_expr AND predicate
  {
	$$ = &BetweenExpr{Left: $1, IsBetween: false, From: $4, To: $6}
  }
| bit_expr LIKE simple_expr
  {
	  $$ = &ComparisonExpr{Left: $1, Operator: LikeOp, Right: $3}
  }
| bit_expr NOT LIKE simple_expr
  {
	$$ = &ComparisonExpr{Left: $1, Operator: NotLikeOp, Right: $4}
  }
| bit_expr LIKE simple_expr ESCAPE simple_expr %prec LIKE
  {
	  $$ = &ComparisonExpr{Left: $1, Operator: LikeOp, Right: $3, Escape: $5}
  }
| bit_expr NOT LIKE simple_expr ESCAPE simple_expr %prec LIKE
  {
	$$ = &ComparisonExpr{Left: $1, Operator: NotLikeOp, Right: $4, Escape: $6}
  }
| bit_expr regexp_symbol bit_expr
  {
	$$ = &ComparisonExpr{Left: $1, Operator: RegexpOp, Right: $3}
  }
| bit_expr NOT regexp_symbol bit_expr
  {
	 $$ = &ComparisonExpr{Left: $1, Operator: NotRegexpOp, Right: $4}
  }
| bit_expr %prec EXPRESSION_PREC_SETTER
 {
	$$ = $1
 }

regexp_symbol:
  REGEXP
  {
  }
| RLIKE
  {
  }


bit_expr:
bit_expr '|' bit_expr %prec '|'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: BitOrOp, Right: $3}
  }
| bit_expr '&' bit_expr %prec '&'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: BitAndOp, Right: $3}
  }
| bit_expr SHIFT_LEFT bit_expr %prec SHIFT_LEFT
  {
	  $$ = &BinaryExpr{Left: $1, Operator: ShiftLeftOp, Right: $3}
  }
| bit_expr SHIFT_RIGHT bit_expr %prec SHIFT_RIGHT
  {
	  $$ = &BinaryExpr{Left: $1, Operator: ShiftRightOp, Right: $3}
  }
| bit_expr '+' bit_expr %prec '+'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: PlusOp, Right: $3}
  }
| bit_expr '-' bit_expr %prec '-'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: MinusOp, Right: $3}
  }
| bit_expr '*' bit_expr %prec '*'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: MultOp, Right: $3}
  }
| bit_expr '/' bit_expr %prec '/'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: DivOp, Right: $3}
  }
| bit_expr '%' bit_expr %prec '%'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: ModOp, Right: $3}
  }
| bit_expr DIV bit_expr %prec DIV
  {
	  $$ = &BinaryExpr{Left: $1, Operator: IntDivOp, Right: $3}
  }
| bit_expr MOD bit_expr %prec MOD
  {
	  $$ = &BinaryExpr{Left: $1, Operator: ModOp, Right: $3}
  }
| bit_expr '^' bit_expr %prec '^'
  {
	  $$ = &BinaryExpr{Left: $1, Operator: BitXorOp, Right: $3}
  }
| simple_expr %prec EXPRESSION_PREC_SETTER
  {
	$$ = $1
  }

simple_expr:
function_call_keyword
  {
  	$$ = $1
  }
| function_call_nonkeyword
  {
  	$$ = $1
  }
| function_call_generic
  {
  	$$ = $1
  }
| function_call_conflict
  {
  	$$ = $1
  }
| simple_expr COLLATE charset %prec UNARY
  {
	$$ = &CollateExpr{Expr: $1, Collation: $3}
  }
| literal_or_null
  {
  	$$ = $1
  }
| column_name
  {
  	$$ = $1
  }
| '+' simple_expr %prec UNARY
  {
	$$= $2; // TODO: do we really want to ignore unary '+' before any kind of literals?
  }
| '-' simple_expr %prec UNARY
  {
	$$ = &UnaryExpr{Operator: UMinusOp, Expr: $2}
  }
| '~' simple_expr %prec UNARY
  {
	$$ = &UnaryExpr{Operator: TildaOp, Expr: $2}
  }
| '!' simple_expr %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BangOp, Expr: $2}
  }
| subquery
  {
	$$= $1
  }
| tuple_expression
  {
	$$ = $1
  }
| EXISTS subquery
  {
	$$ = &ExistsExpr{Subquery: $2}
  }
| MATCH openb select_expression_list closeb AGAINST openb bit_expr match_option closeb
  {
  $$ = &MatchExpr{Columns: $3, Expr: $7, Option: $8}
  }
| CAST openb expression AS convert_type array_opt closeb
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5, Array: $6}
  }
| CONVERT openb expression ',' convert_type array_opt closeb
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5, Array: $6}
  }
| CONVERT openb expression USING charset closeb
  {
    $$ = &ConvertUsingExpr{Expr: $3, Type: $5}
  }
| BINARY simple_expr %prec UNARY
  {
    // From: https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#operator_binary
    // To convert a string expression to a binary string, these constructs are equivalent:
    //    CAST(expr AS BINARY)
    //    BINARY expr
    $$ = &ConvertExpr{Expr: $2, Type: &ConvertType{Type: $1}}
  }
| DEFAULT default_opt
  {
	 $$ = &Default{ColName: $2}
  }
| interval_value
  {
	// This rule prevents the usage of INTERVAL
	// as a function. If support is needed for that,
	// we'll need to revisit this. The solution
	// will be non-trivial because of grammar conflicts.
	$$ = $1
  }
| column_name JSON_EXTRACT_OP text_literal_or_arg
  {
	$$ = &BinaryExpr{Left: $1, Operator: JSONExtractOp, Right: $3}
  }
| column_name JSON_UNQUOTE_EXTRACT_OP text_literal_or_arg
  {
	$$ = &BinaryExpr{Left: $1, Operator: JSONUnquoteExtractOp, Right: $3}
  }

interval_value:
  INTERVAL simple_expr sql_id
  {
     $$ = &IntervalExpr{Expr: $2, Unit: $3.String()}
  }

trim_type:
  BOTH
  {
    $$ = BothTrimType
  }
| LEADING
  {
    $$ = LeadingTrimType
  }
| TRAILING
  {
    $$ = TrailingTrimType
  }

frame_units:
  ROWS
  {
    $$ = FrameRowsType
  }
| RANGE
  {
    $$ = FrameRangeType
  }


argument_less_window_expr_type:
  CUME_DIST
  {
    $$ = CumeDistExprType
  }
| DENSE_RANK
  {
    $$ = DenseRankExprType
  }
| PERCENT_RANK
  {
    $$ = PercentRankExprType
  }
| RANK
  {
    $$ = RankExprType
  }
| ROW_NUMBER
  {
    $$ = RowNumberExprType
  }

frame_point:
  CURRENT ROW
  {
    $$ = &FramePoint{Type:CurrentRowType}
  }
| UNBOUNDED PRECEDING
  {
    $$ = &FramePoint{Type:UnboundedPrecedingType}
  }
| UNBOUNDED FOLLOWING
  {
    $$ = &FramePoint{Type:UnboundedFollowingType}
  }
| frame_expression PRECEDING
  {
    $$ = &FramePoint{Type:ExprPrecedingType, Expr:$1}
  }
| frame_expression FOLLOWING
  {
    $$ = &FramePoint{Type:ExprFollowingType, Expr:$1}
  }

frame_expression:
  NUM_literal
  {
    $$ = $1
  }
| interval_value
  {
    $$ = $1
  }

frame_clause_opt:
  {
    $$ = nil
  }
| frame_clause
  {
    $$ = $1
  }

frame_clause:
  frame_units frame_point
  {
    $$ = &FrameClause{ Unit: $1, Start: $2 }
  }
| frame_units BETWEEN frame_point AND frame_point
  {
    $$ = &FrameClause{ Unit: $1, Start: $3, End: $5 }
  }

window_partition_clause_opt:
  {
    $$= nil
  }
| PARTITION BY expression_list
  {
    $$ = $3
  }

sql_id_opt:
  {
  }
| sql_id
  {
    $$ = $1
  }

window_spec:
sql_id_opt window_partition_clause_opt order_by_opt frame_clause_opt
  {
    $$ = &WindowSpecification{ Name: $1, PartitionClause: $2, OrderClause: $3, FrameClause: $4}
  }

over_clause:
  OVER openb window_spec closeb
  {
    $$ = &OverClause{ WindowSpec: $3 }
  }
| OVER sql_id
  {
    $$ = &OverClause{WindowName: $2}
  }

null_treatment_clause_opt:
  {
    $$ = nil
  }
| null_treatment_clause

null_treatment_clause:
  null_treatment_type
  {
    $$ = &NullTreatmentClause{$1}
  }

null_treatment_type:
  RESPECT NULLS
  {
    $$ = RespectNullsType
  }
| IGNORE NULLS
  {
    $$ = IgnoreNullsType
  }

first_or_last_value_expr_type:
  FIRST_VALUE
  {
    $$ = FirstValueExprType
  }
| LAST_VALUE
  {
    $$ = LastValueExprType
  }

from_first_last_type:
  FROM FIRST
  {
    $$ = FromFirstType
  }
| FROM LAST
  {
    $$ = FromLastType
  }

from_first_last_clause_opt:
  {
    $$ = nil
  }
| from_first_last_clause

from_first_last_clause:
  from_first_last_type
  {
    $$ = &FromFirstLastClause{$1}
  }

lag_lead_expr_type:
  LAG
  {
    $$ = LagExprType
  }
| LEAD
  {
    $$ = LeadExprType
  }

window_definition:
  sql_id AS openb window_spec closeb
  {
    $$ = &WindowDefinition{Name:$1, WindowSpec:$4}
  }

window_definition_list:
  window_definition
  {
    $$ = WindowDefinitions{$1}
  }
| window_definition_list ',' window_definition
  {
    $$ = append($1,$3)
  }

default_opt:
  /* empty */
  {
    $$ = ""
  }
| openb id_or_var closeb
  {
    $$ = string($2.String())
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


is_suffix:
 TRUE
  {
    $$ = IsTrueOp
  }
| NOT TRUE
  {
    $$ = IsNotTrueOp
  }
| FALSE
  {
    $$ = IsFalseOp
  }
| NOT FALSE
  {
    $$ = IsNotFalseOp
  }

compare:
  '='
  {
    $$ = EqualOp
  }
| '<'
  {
    $$ = LessThanOp
  }
| '>'
  {
    $$ = GreaterThanOp
  }
| LE
  {
    $$ = LessEqualOp
  }
| GE
  {
    $$ = GreaterEqualOp
  }
| NE
  {
    $$ = NotEqualOp
  }
| NULL_SAFE_EQUAL
  {
    $$ = NullSafeEqualOp
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
    $$ = ListArg($1[2:])
    bindVariable(yylex, $1[2:])
  }

subquery:
  query_expression_parens %prec SUBQUERY_AS_EXPR
  {
  	$$ = &Subquery{$1}
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

/*
  Regular function calls without special token or syntax, guaranteed to not
  introduce side effects due to being a simple identifier
*/
function_call_generic:
  sql_id openb select_expression_list_opt closeb
  {
    $$ = &FuncExpr{Name: $1, Exprs: $3}
  }
| sql_id openb DISTINCT select_expression_list closeb
  {
    $$ = &FuncExpr{Name: $1, Distinct: true, Exprs: $4}
  }
| sql_id openb DISTINCTROW select_expression_list closeb
  {
    $$ = &FuncExpr{Name: $1, Distinct: true, Exprs: $4}
  }
| table_id '.' reserved_sql_id openb select_expression_list_opt closeb
  {
    $$ = &FuncExpr{Qualifier: $1, Name: $3, Exprs: $5}
  }

/*
  Function calls using reserved keywords, with dedicated grammar rules
  as a result
*/
function_call_keyword:
  LEFT openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("left"), Exprs: $3}
  }
| RIGHT openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("right"), Exprs: $3}
  }
| SUBSTRING openb expression ',' expression ',' expression closeb
  {
    $$ = &SubstrExpr{Name: $3, From: $5, To: $7}
  }
| SUBSTRING openb expression ',' expression closeb
  {
    $$ = &SubstrExpr{Name: $3, From: $5}
  }
| SUBSTRING openb expression FROM expression FOR expression closeb
  {
  	$$ = &SubstrExpr{Name: $3, From: $5, To: $7}
  }
| SUBSTRING openb expression FROM expression closeb
  {
  	$$ = &SubstrExpr{Name: $3, From: $5}
  }
| GROUP_CONCAT openb distinct_opt select_expression_list order_by_opt separator_opt limit_opt closeb
  {
    $$ = &GroupConcatExpr{Distinct: $3, Exprs: $4, OrderBy: $5, Separator: $6, Limit: $7}
  }
| CASE expression_opt when_expression_list else_expression_opt END
  {
    $$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
  }
| VALUES openb column_name closeb
  {
    $$ = &ValuesFuncExpr{Name: $3}
  }
| CURRENT_USER func_paren_opt
  {
    $$ =  &FuncExpr{Name: NewColIdent($1)}
  }

/*
  Function calls using non reserved keywords but with special syntax forms.
  Dedicated grammar rules are needed because of the special syntax
*/
function_call_nonkeyword:
/* doesn't support fsp */
UTC_DATE func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_date")}
  }
| now
  {
  	$$ = $1
  }
  // curdate
/* doesn't support fsp */
| CURRENT_DATE func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_date")}
  }
| UTC_TIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("utc_time"), Fsp: $2}
  }
  // curtime
| CURRENT_TIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("current_time"), Fsp: $2}
  }
| TIMESTAMPADD openb sql_id ',' expression ',' expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampadd"), Unit:$3.String(), Expr1:$5, Expr2:$7}
  }
| TIMESTAMPDIFF openb sql_id ',' expression ',' expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampdiff"), Unit:$3.String(), Expr1:$5, Expr2:$7}
  }
| EXTRACT openb interval FROM expression closeb
  {
	$$ = &ExtractFuncExpr{IntervalTypes: $3, Expr: $5}
  }
| WEIGHT_STRING openb expression convert_type_weight_string closeb
  {
    $$ = &WeightStringFuncExpr{Expr: $3, As: $4}
  }
| JSON_PRETTY openb expression closeb
  {
    $$ = &JSONPrettyExpr{JSONVal: $3}
  }
| JSON_STORAGE_FREE openb expression closeb
  {
    $$ = &JSONStorageFreeExpr{ JSONVal: $3}
  }
| JSON_STORAGE_SIZE openb expression closeb
  {
    $$ = &JSONStorageSizeExpr{ JSONVal: $3}
  }
| LTRIM openb expression closeb
  {
    $$ = &TrimFuncExpr{TrimFuncType:LTrimType, StringArg: $3}
  }
| RTRIM openb expression closeb
  {
    $$ = &TrimFuncExpr{TrimFuncType:RTrimType, StringArg: $3}
  }
| TRIM openb trim_type expression_opt FROM expression closeb
  {
    $$ = &TrimFuncExpr{Type:$3, TrimArg:$4, StringArg: $6}
  }
| TRIM openb expression closeb
  {
    $$ = &TrimFuncExpr{StringArg: $3}
  }
| TRIM openb expression FROM expression closeb
  {
    $$ = &TrimFuncExpr{TrimArg:$3, StringArg: $5}
  }
| GET_LOCK openb expression ',' expression closeb
  {
    $$ = &LockingFunc{Type: GetLock, Name:$3, Timeout:$5}
  }
| IS_FREE_LOCK openb expression closeb
  {
    $$ = &LockingFunc{Type: IsFreeLock, Name:$3}
  }
| IS_USED_LOCK openb expression closeb
  {
    $$ = &LockingFunc{Type: IsUsedLock, Name:$3}
  }
| RELEASE_ALL_LOCKS openb closeb
  {
    $$ = &LockingFunc{Type: ReleaseAllLocks}
  }
| RELEASE_LOCK openb expression closeb
  {
    $$ = &LockingFunc{Type: ReleaseLock, Name:$3}
  }
| JSON_SCHEMA_VALID openb expression ',' expression closeb
  {
    $$ = &JSONSchemaValidFuncExpr{ Schema: $3, Document: $5}
  }
| JSON_SCHEMA_VALIDATION_REPORT openb expression ',' expression closeb
  {
    $$ = &JSONSchemaValidationReportFuncExpr{ Schema: $3, Document: $5}
  }
| JSON_ARRAY openb expression_list_opt closeb
  {
    $$ = &JSONArrayExpr{ Params:$3 }
  }
| JSON_OBJECT openb json_object_param_opt closeb
  {
    $$ = &JSONObjectExpr{ Params:$3 }
  }
| JSON_QUOTE openb expression closeb
  {
    $$ = &JSONQuoteExpr{ StringArg:$3 }
  }
| JSON_CONTAINS openb expression ',' expression json_path_param_list_opt closeb
  {
    $$ = &JSONContainsExpr{Target: $3, Candidate: $5, PathList: $6}
  }
| JSON_CONTAINS_PATH openb expression ',' expression ',' json_path_param_list closeb
  {
    $$ = &JSONContainsPathExpr{JSONDoc: $3, OneOrAll: $5, PathList: $7}
  }
| JSON_EXTRACT openb expression ',' json_path_param_list closeb
  {
    $$ = &JSONExtractExpr{JSONDoc: $3, PathList: $5}
  }
| JSON_KEYS openb expression json_path_param_list_opt closeb
  {
    $$ = &JSONKeysExpr{JSONDoc: $3, PathList: $4}
  }
| JSON_OVERLAPS openb expression ',' expression closeb
  {
    $$ = &JSONOverlapsExpr{JSONDoc1:$3, JSONDoc2:$5}
  }
| JSON_SEARCH openb expression ',' expression ',' expression closeb
  {
    $$ = &JSONSearchExpr{JSONDoc: $3, OneOrAll: $5, SearchStr: $7 }
  }
| JSON_SEARCH openb expression ',' expression ',' expression ',' expression json_path_param_list_opt closeb
  {
    $$ = &JSONSearchExpr{JSONDoc: $3, OneOrAll: $5, SearchStr: $7, EscapeChar: $9, PathList:$10 }
  }
| JSON_VALUE openb expression ',' json_path_param returning_type_opt closeb
  {
    $$ = &JSONValueExpr{JSONDoc: $3, Path: $5, ReturningType: $6}
  }
| JSON_VALUE openb expression ',' json_path_param returning_type_opt on_empty closeb
  {
    $$ = &JSONValueExpr{JSONDoc: $3, Path: $5, ReturningType: $6, EmptyOnResponse: $7}
  }
| JSON_VALUE openb expression ',' json_path_param returning_type_opt on_error closeb
  {
    $$ = &JSONValueExpr{JSONDoc: $3, Path: $5, ReturningType: $6, ErrorOnResponse: $7}
  }
| JSON_VALUE openb expression ',' json_path_param returning_type_opt on_empty on_error closeb
  {
    $$ = &JSONValueExpr{JSONDoc: $3, Path: $5, ReturningType: $6, EmptyOnResponse: $7, ErrorOnResponse: $8}
  }
| JSON_DEPTH openb expression closeb
  {
    $$ = &JSONAttributesExpr{Type:DepthAttributeType, JSONDoc:$3}
  }
| JSON_VALID openb expression closeb
  {
    $$ = &JSONAttributesExpr{Type:ValidAttributeType, JSONDoc:$3}
  }
| JSON_TYPE openb expression closeb
  {
    $$ = &JSONAttributesExpr{Type:TypeAttributeType, JSONDoc:$3}
  }
| JSON_LENGTH openb expression closeb
  {
    $$ = &JSONAttributesExpr{Type:LengthAttributeType, JSONDoc:$3 }
  }
| JSON_LENGTH openb expression ',' json_path_param closeb
  {
    $$ = &JSONAttributesExpr{Type:LengthAttributeType, JSONDoc:$3, Path: $5 }
  }
| JSON_ARRAY_APPEND openb expression ',' json_object_param_list closeb
  {
    $$ = &JSONValueModifierExpr{Type:JSONArrayAppendType ,JSONDoc:$3, Params:$5}
  }
| JSON_ARRAY_INSERT openb expression ',' json_object_param_list closeb
  {
    $$ = &JSONValueModifierExpr{Type:JSONArrayInsertType ,JSONDoc:$3, Params:$5}
  }
| JSON_INSERT openb expression ',' json_object_param_list closeb
  {
    $$ = &JSONValueModifierExpr{Type:JSONInsertType ,JSONDoc:$3, Params:$5}
  }
| JSON_REPLACE openb expression ',' json_object_param_list closeb
  {
    $$ = &JSONValueModifierExpr{Type:JSONReplaceType ,JSONDoc:$3, Params:$5}
  }
| JSON_SET openb expression ',' json_object_param_list closeb
  {
    $$ = &JSONValueModifierExpr{Type:JSONSetType ,JSONDoc:$3, Params:$5}
  }
| JSON_MERGE openb expression ',' expression_list closeb
  {
    $$ = &JSONValueMergeExpr{Type: JSONMergeType, JSONDoc: $3, JSONDocList: $5}
  }
| JSON_MERGE_PATCH openb expression ',' expression_list closeb
  {
    $$ = &JSONValueMergeExpr{Type: JSONMergePatchType, JSONDoc: $3, JSONDocList: $5}
  }
| JSON_MERGE_PRESERVE openb expression ',' expression_list closeb
  {
    $$ = &JSONValueMergeExpr{Type: JSONMergePreserveType, JSONDoc: $3, JSONDocList: $5}
  }
| JSON_REMOVE openb expression ',' expression_list closeb
  {
    $$ = &JSONRemoveExpr{JSONDoc:$3, PathList: $5}
  }
| JSON_UNQUOTE openb expression closeb
  {
    $$ = &JSONUnquoteExpr{JSONValue:$3}
  }
| argument_less_window_expr_type openb closeb over_clause
  {
    $$ = &ArgumentLessWindowExpr{ Type: $1, OverClause : $4 }
  }
| first_or_last_value_expr_type openb expression closeb null_treatment_clause_opt over_clause
  {
    $$ = &FirstOrLastValueExpr{ Type: $1, Expr : $3 , NullTreatmentClause:$5 ,OverClause:$6 }
  }
| NTILE openb null_int_variable_arg closeb over_clause
  {
    $$ =  &NtileExpr{N: $3, OverClause: $5}
  }
| NTH_VALUE openb expression ',' null_int_variable_arg closeb from_first_last_clause_opt null_treatment_clause_opt over_clause
  {
    $$ =  &NTHValueExpr{ Expr: $3, N: $5, FromFirstLastClause:$7, NullTreatmentClause:$8, OverClause: $9}
  }
| lag_lead_expr_type openb expression closeb null_treatment_clause_opt over_clause
  {
    $$ = &LagLeadExpr{ Type:$1 , Expr: $3, NullTreatmentClause:$5, OverClause: $6 }
  }
| lag_lead_expr_type openb expression ',' null_int_variable_arg default_with_comma_opt closeb null_treatment_clause_opt over_clause
  {
    $$ =  &LagLeadExpr{ Type:$1 , Expr: $3, N: $5, Default: $6, NullTreatmentClause:$8, OverClause: $9}
  }
| regular_expressions
| xml_expressions

null_int_variable_arg:
  null_as_literal
  {
    $$ = $1
  }
| INTEGRAL
  {
    $$ = NewIntLiteral($1)
  }
//  we are currently using id_or_var for variables. This will require a reiteration later.
| id_or_var
  {
    $$ = &ColName{Name: $1}
  }
| VALUE_ARG
  {
    $$ = NewArgument($1[1:])
    bindVariable(yylex, $1[1:])
  }

default_with_comma_opt:
  {
    $$ = nil
  }
| ',' expression
  {
    $$ = $2
  }


regular_expressions:
  REGEXP_INSTR openb expression ',' expression closeb
  {
    $$ = &RegexpInstrExpr{Expr:$3, Pattern:$5}
  }
| REGEXP_INSTR openb expression ',' expression ',' expression closeb
  {
    $$ = &RegexpInstrExpr{Expr:$3, Pattern:$5, Position: $7}
  }
| REGEXP_INSTR openb expression ',' expression ',' expression ',' expression closeb
  {
    $$ = &RegexpInstrExpr{Expr:$3, Pattern:$5, Position: $7, Occurrence: $9}
  }
| REGEXP_INSTR openb expression ',' expression ',' expression ',' expression ',' expression closeb
  {
    $$ = &RegexpInstrExpr{Expr:$3, Pattern:$5, Position: $7, Occurrence: $9, ReturnOption: $11}
  }
| REGEXP_INSTR openb expression ',' expression ',' expression ',' expression ',' expression ',' expression closeb
  {
    // Match type is kept expression as TRIM( ' m  ') is accepted
    $$ = &RegexpInstrExpr{Expr:$3, Pattern:$5, Position: $7, Occurrence: $9, ReturnOption: $11, MatchType: $13}
  }
| REGEXP_LIKE openb expression ',' expression closeb
  {
    $$ = &RegexpLikeExpr{Expr:$3, Pattern:$5}
  }
| REGEXP_LIKE openb expression ',' expression ',' expression closeb
  {
    $$ = &RegexpLikeExpr{Expr:$3, Pattern:$5, MatchType: $7}
  }
| REGEXP_REPLACE openb expression ',' expression ',' expression closeb
  {
    $$ = &RegexpReplaceExpr{Expr:$3, Pattern:$5, Repl: $7}
  }
| REGEXP_REPLACE openb expression ',' expression ',' expression ',' expression closeb
  {
    $$ = &RegexpReplaceExpr{Expr:$3, Pattern:$5, Repl: $7, Position: $9}
  }
| REGEXP_REPLACE openb expression ',' expression ',' expression ',' expression ',' expression closeb
  {
    $$ = &RegexpReplaceExpr{Expr:$3, Pattern:$5, Repl: $7, Position: $9, Occurrence: $11}
  }
| REGEXP_REPLACE openb expression ',' expression ',' expression ',' expression ',' expression ',' expression closeb
  {
    // Match type is kept expression as TRIM( ' m  ') is accepted
    $$ = &RegexpReplaceExpr{Expr:$3, Pattern:$5, Repl: $7, Position: $9, Occurrence: $11, MatchType: $13}
  }
| REGEXP_SUBSTR openb expression ',' expression closeb
  {
    $$ = &RegexpSubstrExpr{Expr:$3, Pattern:$5 }
  }
| REGEXP_SUBSTR openb expression ',' expression ',' expression closeb
  {
    $$ = &RegexpSubstrExpr{Expr:$3, Pattern:$5, Position: $7}
  }
| REGEXP_SUBSTR openb expression ',' expression ',' expression ',' expression closeb
  {
    $$ = &RegexpSubstrExpr{Expr:$3, Pattern:$5, Position: $7, Occurrence: $9}
  }
| REGEXP_SUBSTR openb expression ',' expression ',' expression ',' expression ',' expression closeb
  {
    // Match type is kept expression as TRIM( ' m  ') is accepted
    $$ = &RegexpSubstrExpr{Expr:$3, Pattern:$5, Position: $7, Occurrence: $9, MatchType: $11}
  }

xml_expressions:
  ExtractValue openb expression ',' expression closeb
  {
    $$ = &ExtractValueExpr{Fragment:$3, XPathExpr:$5}
  }
| UpdateXML openb expression ',' expression ',' expression closeb
  {
    $$ = &UpdateXMLExpr{ Target:$3, XPathExpr:$5, NewXML:$7 }
  }

returning_type_opt:
  {
    $$ = nil
  }
| RETURNING convert_type
  {
    $$ = $2
  }

json_path_param_list_opt:
  {
    $$ = nil
  }
| ',' json_path_param_list
  {
    $$ = $2
  }

json_path_param_list:
  json_path_param
  {
    $$ = []JSONPathParam{$1}
  }
| json_path_param_list ',' json_path_param
  {
    $$ = append($$, $3)
  }

json_path_param:
  text_literal_or_arg
  {
    $$ = JSONPathParam($1)
  }
| column_name
  {
    $$ = JSONPathParam($1)
  }

interval:
 interval_time_stamp
 {}
| DAY_HOUR
  {
	$$=IntervalDayHour
  }
| DAY_MICROSECOND
  {
	$$=IntervalDayMicrosecond
  }
| DAY_MINUTE
  {
	$$=IntervalDayMinute
  }
| DAY_SECOND
  {
	$$=IntervalDaySecond
  }
| HOUR_MICROSECOND
  {
	$$=IntervalHourMicrosecond
  }
| HOUR_MINUTE
  {
	$$=IntervalHourMinute
  }
| HOUR_SECOND
  {
	$$=IntervalHourSecond
  }
| MINUTE_MICROSECOND
  {
	$$=IntervalMinuteMicrosecond
  }
| MINUTE_SECOND
  {
	$$=IntervalMinuteSecond
  }
| SECOND_MICROSECOND
  {
	$$=IntervalSecondMicrosecond
  }
| YEAR_MONTH
  {
	$$=IntervalYearMonth
  }

interval_time_stamp:
 DAY
  {
 	$$=IntervalDay
  }
| WEEK
  {
  	$$=IntervalWeek
  }
| HOUR
  {
 	$$=IntervalHour
  }
| MINUTE
  {
 	$$=IntervalMinute
  }
| MONTH
  {
	$$=IntervalMonth
  }
| QUARTER
  {
	$$=IntervalQuarter
  }
| SECOND
  {
	$$=IntervalSecond
  }
| MICROSECOND
  {
	$$=IntervalMicrosecond
  }
| YEAR
  {
	$$=IntervalYear
  }

func_paren_opt:
  /* empty */
| openb closeb

func_datetime_precision:
  /* empty */
  {
  	$$ = nil
  }
| openb closeb
  {
    $$ = nil
  }
| openb INTEGRAL closeb
  {
  	$$ = NewIntLiteral($2)
  }
| openb VALUE_ARG closeb
  {
    $$ = NewArgument($2[1:])
    bindVariable(yylex, $2[1:])
  }

/*
  Function calls using non reserved keywords with *normal* syntax forms. Because
  the names are non-reserved, they need a dedicated rule so as not to conflict
*/
function_call_conflict:
  IF openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("if"), Exprs: $3}
  }
| DATABASE openb select_expression_list_opt closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("database"), Exprs: $3}
  }
| SCHEMA openb select_expression_list_opt closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("schema"), Exprs: $3}
  }
| MOD openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("mod"), Exprs: $3}
  }
| REPLACE openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("replace"), Exprs: $3}
  }

match_option:
/*empty*/
  {
    $$ = NoOption
  }
| IN BOOLEAN MODE
  {
    $$ = BooleanModeOpt
  }
| IN NATURAL LANGUAGE MODE
 {
    $$ = NaturalLanguageModeOpt
 }
| IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
 {
    $$ = NaturalLanguageModeWithQueryExpansionOpt
 }
| WITH QUERY EXPANSION
 {
    $$ = QueryExpansionOpt
 }

charset:
  sql_id
  {
    $$ = string($1.String())
  }
| STRING
  {
    $$ = string($1)
  }
| BINARY
  {
    $$ = string($1)
  }

convert_type_weight_string:
  /* empty */
  {
    $$ = nil
  }
| AS BINARY '(' INTEGRAL ')'
  {
    $$ = &ConvertType{Type: string($2), Length: NewIntLiteral($4)}
  }
| AS CHAR '(' INTEGRAL ')'
  {
    $$ = &ConvertType{Type: string($2), Length: NewIntLiteral($4)}
  }

convert_type:
  BINARY length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| CHAR length_opt charset_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: $3}
  }
| DATE
  {
    $$ = &ConvertType{Type: string($1)}
  }
| DATETIME length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| DECIMAL_TYPE decimal_length_opt
  {
    $$ = &ConvertType{Type: string($1)}
    $$.Length = $2.Length
    $$.Scale = $2.Scale
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
| FLOAT_TYPE length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| DOUBLE
  {
    $$ = &ConvertType{Type: string($1)}
  }
| REAL
  {
    $$ = &ConvertType{Type: string($1)}
  }

array_opt:
  /* empty */
  {
    $$ = false
  }
| ARRAY
  {
    $$ = true
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
    $$ = string("")
  }
| SEPARATOR STRING
  {
    $$ = " separator "+encodeSQLString($2)
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
| table_id '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: $3}
  }
| table_id '.' reserved_table_id '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Qualifier: $1, Name: $3}, Name: $5}
  }

num_val:
  sql_id
  {
    // TODO(sougou): Deprecate this construct.
    if $1.Lowered() != "value" {
      yylex.Error("expecting value after next")
      return 1
    }
    $$ = NewIntLiteral("1")
  }
| INTEGRAL VALUES
  {
    $$ = NewIntLiteral($1)
  }
| VALUE_ARG VALUES
  {
    $$ = NewArgument($1[1:])
    bindVariable(yylex, $1[1:])
  }

group_by_opt:
  {
    $$ = nil
  }
| GROUP BY expression_list
  {
    $$ = $3
  }

having_opt:
  {
    $$ = nil
  }
| HAVING expression
  {
    $$ = $2
  }

named_window:
  WINDOW window_definition_list %prec WINDOW_EXPR
  {
    $$ = &NamedWindow{$2}
  }

named_windows_list:
  named_window
  {
    $$ = NamedWindows{$1}
  }
| named_windows_list ',' named_window
  {
    $$ = append($1,$3)
  }

named_windows_list_opt:
  {
    $$ = nil
  }
| named_windows_list
  {
    $$ = $1
  }

order_by_opt:
  {
    $$ = nil
  }
 | order_by_clause
 {
 	$$ = $1
 }

order_by_clause:
ORDER BY order_list
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
    $$ = AscOrder
  }
| ASC
  {
    $$ = AscOrder
  }
| DESC
  {
    $$ = DescOrder
  }

limit_opt:
  {
    $$ = nil
  }
 | limit_clause
 {
 	$$ = $1
 }

limit_clause:
LIMIT expression
  {
    $$ = &Limit{Rowcount: $2}
  }
| LIMIT expression ',' expression
  {
    $$ = &Limit{Offset: $2, Rowcount: $4}
  }
| LIMIT expression OFFSET expression
  {
    $$ = &Limit{Offset: $4, Rowcount: $2}
  }

algorithm_lock_opt:
  {
    $$ = nil
  }
| lock_index algorithm_index
  {
     $$ = []AlterOption{$1,$2}
  }
| algorithm_index lock_index
  {
     $$ = []AlterOption{$1,$2}
  }
| algorithm_index
  {
     $$ = []AlterOption{$1}
  }
| lock_index
  {
     $$ = []AlterOption{$1}
  }


lock_index:
  LOCK equal_opt DEFAULT
  {
    $$ = &LockOption{Type:DefaultType}
  }
| LOCK equal_opt NONE
  {
    $$ = &LockOption{Type:NoneType}
  }
| LOCK equal_opt SHARED
  {
    $$ = &LockOption{Type:SharedType}
  }
| LOCK equal_opt EXCLUSIVE
  {
    $$ = &LockOption{Type:ExclusiveType}
  }

algorithm_index:
  ALGORITHM equal_opt DEFAULT
  {
    $$ = AlgorithmValue($3)
  }
| ALGORITHM equal_opt INPLACE
  {
    $$ = AlgorithmValue($3)
  }
| ALGORITHM equal_opt COPY
  {
    $$ = AlgorithmValue($3)
  }
| ALGORITHM equal_opt INSTANT
  {
    $$ = AlgorithmValue($3)
  }

algorithm_view:
  {
    $$ = ""
  }
| ALGORITHM '=' UNDEFINED
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

security_view_opt:
  {
    $$ = ""
  }
| SQL SECURITY security_view
  {
    $$ = $3
  }

security_view:
  DEFINER
  {
    $$ = string($1)
  }
| INVOKER
  {
    $$ = string($1)
  }

check_option_opt:
  {
    $$ = ""
  }
| WITH cascade_or_local_opt CHECK OPTION
  {
    $$ = $2
  }

cascade_or_local_opt:
  {
    $$ = "cascaded"
  }
| CASCADED
  {
    $$ = string($1)
  }
| LOCAL
  {
    $$ = string($1)
  }

definer_opt:
  {
    $$ = nil
  }
| DEFINER '=' user
  {
    $$ = $3
  }

user:
CURRENT_USER
  {
    $$ = &Definer{
    	Name: string($1),
    }
  }
| CURRENT_USER '(' ')'
  {
    $$ = &Definer{
        Name: string($1),
    }
  }
| user_username address_opt
  {
    $$ = &Definer{
        Name: $1,
        Address: $2,
    }
  }

user_username:
  STRING
  {
    $$ = encodeSQLString($1)
  }
| ID
  {
    $$ = formatIdentifier($1)
  }

address_opt:
  {
    $$ = ""
  }
| AT_ID
  {
    $$ = formatAddress($1)
  }

locking_clause:
FOR UPDATE
  {
    $$ = ForUpdateLock
  }
| LOCK IN SHARE MODE
  {
    $$ = ShareModeLock
  }

into_clause:
INTO OUTFILE S3 STRING charset_opt format_opt export_options manifest_opt overwrite_opt
{
$$ = &SelectInto{Type:IntoOutfileS3, FileName:encodeSQLString($4), Charset:$5, FormatOption:$6, ExportOption:$7, Manifest:$8, Overwrite:$9}
}
| INTO DUMPFILE STRING
{
$$ = &SelectInto{Type:IntoDumpfile, FileName:encodeSQLString($3), Charset:ColumnCharset{}, FormatOption:"", ExportOption:"", Manifest:"", Overwrite:""}
}
| INTO OUTFILE STRING charset_opt export_options
{
$$ = &SelectInto{Type:IntoOutfile, FileName:encodeSQLString($3), Charset:$4, FormatOption:"", ExportOption:$5, Manifest:"", Overwrite:""}
}

format_opt:
  {
    $$ = ""
  }
| FORMAT CSV header_opt
  {
    $$ = " format csv" + $3
  }
| FORMAT TEXT header_opt
  {
    $$ = " format text" + $3
  }

header_opt:
  {
    $$ = ""
  }
| HEADER
  {
    $$ = " header"
  }

manifest_opt:
  {
    $$ = ""
  }
| MANIFEST ON
  {
    $$ = " manifest on"
  }
| MANIFEST OFF
  {
    $$ = " manifest off"
  }

overwrite_opt:
  {
    $$ = ""
  }
| OVERWRITE ON
  {
    $$ = " overwrite on"
  }
| OVERWRITE OFF
  {
    $$ = " overwrite off"
  }

export_options:
  fields_opts lines_opts
  {
    $$ = $1 + $2
  }

lines_opts:
  {
    $$ = ""
  }
| LINES lines_opt_list
  {
    $$ = " lines" + $2
  }

lines_opt_list:
  lines_opt
  {
    $$ = $1
  }
| lines_opt_list lines_opt
  {
    $$ = $1 + $2
  }

lines_opt:
  STARTING BY STRING
  {
    $$ = " starting by " + encodeSQLString($3)
  }
| TERMINATED BY STRING
  {
    $$ = " terminated by " + encodeSQLString($3)
  }

fields_opts:
  {
    $$ = ""
  }
| columns_or_fields fields_opt_list
  {
    $$ = " " + $1 + $2
  }

fields_opt_list:
  fields_opt
  {
    $$ = $1
  }
| fields_opt_list fields_opt
  {
    $$ = $1 + $2
  }

fields_opt:
  TERMINATED BY STRING
  {
    $$ = " terminated by " + encodeSQLString($3)
  }
| optionally_opt ENCLOSED BY STRING
  {
    $$ = $1 + " enclosed by " + encodeSQLString($4)
  }
| ESCAPED BY STRING
  {
    $$ = " escaped by " + encodeSQLString($3)
  }

optionally_opt:
  {
    $$ = ""
  }
| OPTIONALLY
  {
    $$ = " optionally"
  }

// insert_data expands all combinations into a single rule.
// This avoids a shift/reduce conflict while encountering the
// following two possible constructs:
// insert into t1(a, b) (select * from t2)
// insert into t1(select * from t2)
// Because the rules are together, the parser can keep shifting
// the tokens until it disambiguates a as sql_id and select as keyword.
insert_data:
  VALUES tuple_list
  {
    $$ = &Insert{Rows: $2}
  }
| select_statement
  {
    $$ = &Insert{Rows: $1}
  }
| openb ins_column_list closeb VALUES tuple_list
  {
    $$ = &Insert{Columns: $2, Rows: $5}
  }
| openb closeb VALUES tuple_list
  {
    $$ = &Insert{Rows: $4}
  }
| openb ins_column_list closeb select_statement
  {
    $$ = &Insert{Columns: $2, Rows: $4}
  }

ins_column_list:
  sql_id
  {
    $$ = Columns{$1}
  }
| sql_id '.' sql_id
  {
    $$ = Columns{$3}
  }
| ins_column_list ',' sql_id
  {
    $$ = append($$, $3)
  }
| ins_column_list ',' sql_id '.' sql_id
  {
    $$ = append($$, $5)
  }

on_dup_opt:
  {
    $$ = nil
  }
| ON DUPLICATE KEY UPDATE update_list
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
  row_tuple
  {
    $$ = $1
  }
| openb closeb
  {
    $$ = ValTuple{}
  }

row_tuple:
  openb expression_list closeb
  {
    $$ = ValTuple($2)
  }
| ROW openb expression_list closeb
  {
    $$ = ValTuple($3)
  }
tuple_expression:
 row_tuple
  {
    if len($1) == 1 {
      $$ = $1[0]
    } else {
      $$ = $1
    }
  }

update_list:
  update_expression
  {
    $$ = UpdateExprs{$1}
  }
| update_list ',' update_expression
  {
    $$ = append($1, $3)
  }

update_expression:
  column_name '=' expression
  {
    $$ = &UpdateExpr{Name: $1, Expr: $3}
  }

set_list:
  set_expression
  {
    $$ = SetExprs{$1}
  }
| set_list ',' set_expression
  {
    $$ = append($1, $3)
  }

set_expression:
  reserved_sql_id '=' ON
  {
    $$ = &SetExpr{Name: $1, Scope: ImplicitScope, Expr: NewStrLiteral("on")}
  }
| reserved_sql_id '=' OFF
  {
    $$ = &SetExpr{Name: $1, Scope: ImplicitScope, Expr: NewStrLiteral("off")}
  }
| reserved_sql_id '=' expression
  {
    $$ = &SetExpr{Name: $1, Scope: ImplicitScope, Expr: $3}
  }
| charset_or_character_set_or_names charset_value collate_opt
  {
    $$ = &SetExpr{Name: NewColIdent(string($1)), Scope: ImplicitScope, Expr: $2}
  }
|  set_session_or_global set_expression
  {
    $2.Scope = $1
    $$ = $2
  }

charset_or_character_set:
  CHARSET
| CHARACTER SET
  {
    $$ = "charset"
  }

charset_or_character_set_or_names:
  charset_or_character_set
| NAMES

charset_value:
  sql_id
  {
    $$ = NewStrLiteral($1.String())
  }
| STRING
  {
    $$ = NewStrLiteral($1)
  }
| DEFAULT
  {
    $$ = &Default{}
  }

for_from:
  FOR
| FROM

temp_opt:
  { $$ = false }
| TEMPORARY
  { $$ = true }

exists_opt:
  { $$ = false }
| IF EXISTS
  { $$ = true }

not_exists_opt:
  { $$ = false }
| IF NOT EXISTS
  { $$ = true }

ignore_opt:
  { $$ = false }
| IGNORE
  { $$ = true }

to_opt:
  { $$ = struct{}{} }
| TO
  { $$ = struct{}{} }
| AS
  { $$ = struct{}{} }

call_statement:
  CALL table_name openb expression_list_opt closeb
  {
    $$ = &CallProc{Name: $2, Params: $4}
  }

expression_list_opt:
  {
    $$ = nil
  }
| expression_list
  {
    $$ = $1
  }

using_opt:
  { $$ = nil }
| using_index_type
  { $$ = []*IndexOption{$1} }

using_index_type:
  USING sql_id
  {
    $$ = &IndexOption{Name: string($1), String: string($2.String())}
  }

sql_id:
  id_or_var
  {
    $$ = $1
  }
| non_reserved_keyword
  {
    $$ = NewColIdent(string($1))
  }

reserved_sql_id:
  sql_id
| reserved_keyword
  {
    $$ = NewColIdent(string($1))
  }

table_id:
  id_or_var
  {
    $$ = NewTableIdent(string($1.String()))
  }
| non_reserved_keyword
  {
    $$ = NewTableIdent(string($1))
  }

table_id_opt:
  /* empty */ %prec LOWER_THAN_CHARSET
  {
    $$ = NewTableIdent("")
  }
| table_id
  {
    $$ = $1
  }

reserved_table_id:
  table_id
| reserved_keyword
  {
    $$ = NewTableIdent(string($1))
  }
/*
  These are not all necessarily reserved in MySQL, but some are.

  These are more importantly reserved because they may conflict with our grammar.
  If you want to move one that is not reserved in MySQL (i.e. ESCAPE) to the
  non_reserved_keywords, you'll need to deal with any conflicts.

  Sorted alphabetically
*/
reserved_keyword:
  ADD
| ALL
| AND
| AS
| ASC
| BETWEEN
| BINARY
| BOTH
| BY
| CASE
| CALL
| CHANGE
| CHARACTER
| CHECK
| COLLATE
| COLUMN
| CONVERT
| CREATE
| CROSS
| CUME_DIST
| CURRENT_DATE
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| SUBSTR
| SUBSTRING
| DATABASE
| DATABASES
| DEFAULT
| DELETE
| DENSE_RANK
| DESC
| DESCRIBE
| DISTINCT
| DISTINCTROW
| DIV
| DROP
| ELSE
| EMPTY
| ESCAPE
| EXISTS
| EXPLAIN
| EXTRACT
| FALSE
| FIRST_VALUE
| FOR
| FORCE
| FOREIGN
| FROM
| FULLTEXT
| GENERATED
| GROUP
| GROUPING
| GROUPS
| HAVING
| IF
| IGNORE
| IN
| INDEX
| INNER
| INSERT
| INTERVAL
| INTO
| IS
| JOIN
| JSON_TABLE
| KEY
| LAG
| LAST_VALUE
| LATERAL
| LEAD
| LEADING
| LEFT
| LIKE
| LIMIT
| LINEAR
| LOCALTIME
| LOCALTIMESTAMP
| LOCK
| LOW_PRIORITY
| MATCH
| MAXVALUE
| MOD
| NATURAL
| NEXT // next should be doable as non-reserved, but is not due to the special `select next num_val` query that vitess supports
| NO_WRITE_TO_BINLOG
| NOT
| NOW
| NTH_VALUE
| NTILE
| NULL
| OF
| OFF
| ON
| OPTIMIZER_COSTS
| OR
| ORDER
| OUTER
| OUTFILE
| OVER
| PARTITION
| PERCENT_RANK
| PRIMARY
| RANGE
| RANK
| READ
| RECURSIVE
| REGEXP
| RENAME
| REPLACE
| RIGHT
| RLIKE
| ROW
| ROW_NUMBER
| ROWS
| SCHEMA
| SCHEMAS
| SELECT
| SEPARATOR
| SET
| SHOW
| SPATIAL
| STORED
| STRAIGHT_JOIN
| SYSTEM
| TABLE
| THEN
| TIMESTAMPADD
| TIMESTAMPDIFF
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| UNLOCK
| UPDATE
| USE
| USING
| UTC_DATE
| UTC_TIME
| UTC_TIMESTAMP
| VALUES
| VIRTUAL
| WITH
| WHEN
| WHERE
| WINDOW
| WRITE
| XOR

/*
  These are non-reserved Vitess, because they don't cause conflicts in the grammar.
  Some of them may be reserved in MySQL. The good news is we backtick quote them
  when we rewrite the query, so no issue should arise.

  Sorted alphabetically
*/
non_reserved_keyword:
  AGAINST
| ACTION
| ACTIVE
| ADMIN
| AFTER
| ALGORITHM
| ALWAYS
| ARRAY
| ASCII
| AUTO_INCREMENT
| AUTOEXTEND_SIZE
| AVG_ROW_LENGTH
| BEGIN
| BIGINT
| BIT
| BLOB
| BOOL
| BOOLEAN
| BUCKETS
| BYTE
| CANCEL
| CASCADE
| CASCADED
| CHANNEL
| CHAR
| CHARSET
| CHECKSUM
| CLEANUP
| CLONE
| COALESCE
| CODE
| COLLATION
| COLUMN_FORMAT
| COLUMNS
| COMMENT_KEYWORD
| COMMIT
| COMMITTED
| COMPACT
| COMPLETE
| COMPONENT
| COMPRESSED
| COMPRESSION
| CONNECTION
| COPY
| CSV
| CURRENT
| DATA
| DATE
| DATETIME
| DEALLOCATE
| DECIMAL_TYPE
| DELAY_KEY_WRITE
| DEFINER
| DEFINITION
| DESCRIPTION
| DIRECTORY
| DISABLE
| DISCARD
| DISK
| DO
| DOUBLE
| DUMPFILE
| DUPLICATE
| DYNAMIC
| ENABLE
| ENCLOSED
| ENCRYPTION
| END
| ENFORCED
| ENGINE
| ENGINE_ATTRIBUTE
| ENGINES
| ENUM
| ERROR
| ESCAPED
| EVENT
| EXCHANGE
| EXCLUDE
| EXCLUSIVE
| EXECUTE
| EXPANSION
| EXPIRE
| EXPORT
| EXTENDED
| ExtractValue %prec FUNCTION_CALL_NON_KEYWORD
| FLOAT_TYPE
| FIELDS
| FIRST
| FIXED
| FLUSH
| FOLLOWING
| FORMAT
| FULL
| FUNCTION
| GENERAL
| GEOMCOLLECTION
| GEOMETRY
| GEOMETRYCOLLECTION
| GET_LOCK %prec FUNCTION_CALL_NON_KEYWORD
| GET_MASTER_PUBLIC_KEY
| GLOBAL
| GTID_EXECUTED
| HASH
| HEADER
| HISTOGRAM
| HISTORY
| HOSTS
| IMPORT
| INACTIVE
| INPLACE
| INSERT_METHOD
| INSTANT
| INT
| INTEGER
| INVISIBLE
| INVOKER
| INDEXES
| IS_FREE_LOCK %prec FUNCTION_CALL_NON_KEYWORD
| IS_USED_LOCK %prec FUNCTION_CALL_NON_KEYWORD
| ISOLATION
| JSON
| JSON_ARRAY %prec FUNCTION_CALL_NON_KEYWORD
| JSON_ARRAY_APPEND %prec FUNCTION_CALL_NON_KEYWORD
| JSON_ARRAY_INSERT %prec FUNCTION_CALL_NON_KEYWORD
| JSON_CONTAINS %prec FUNCTION_CALL_NON_KEYWORD
| JSON_CONTAINS_PATH %prec FUNCTION_CALL_NON_KEYWORD
| JSON_DEPTH %prec FUNCTION_CALL_NON_KEYWORD
| JSON_EXTRACT %prec FUNCTION_CALL_NON_KEYWORD
| JSON_INSERT %prec FUNCTION_CALL_NON_KEYWORD
| JSON_KEYS %prec FUNCTION_CALL_NON_KEYWORD
| JSON_MERGE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_MERGE_PATCH %prec FUNCTION_CALL_NON_KEYWORD
| JSON_MERGE_PRESERVE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_OBJECT %prec FUNCTION_CALL_NON_KEYWORD
| JSON_OVERLAPS %prec FUNCTION_CALL_NON_KEYWORD
| JSON_PRETTY %prec FUNCTION_CALL_NON_KEYWORD
| JSON_QUOTE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_REMOVE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_REPLACE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_SCHEMA_VALID %prec FUNCTION_CALL_NON_KEYWORD
| JSON_SCHEMA_VALIDATION_REPORT %prec FUNCTION_CALL_NON_KEYWORD
| JSON_SEARCH %prec FUNCTION_CALL_NON_KEYWORD
| JSON_SET %prec FUNCTION_CALL_NON_KEYWORD
| JSON_STORAGE_FREE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_STORAGE_SIZE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_TYPE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_VALID %prec FUNCTION_CALL_NON_KEYWORD
| JSON_VALUE %prec FUNCTION_CALL_NON_KEYWORD
| JSON_UNQUOTE %prec FUNCTION_CALL_NON_KEYWORD
| KEY_BLOCK_SIZE
| KEYS
| KEYSPACES
| LANGUAGE
| LAST
| LAST_INSERT_ID
| LESS
| LEVEL
| LINES
| LINESTRING
| LIST
| LOAD
| LOCAL
| LOCKED
| LOGS
| LONGBLOB
| LONGTEXT
| LTRIM %prec FUNCTION_CALL_NON_KEYWORD
| MANIFEST
| MASTER_COMPRESSION_ALGORITHMS
| MASTER_PUBLIC_KEY_PATH
| MASTER_TLS_CIPHERSUITES
| MASTER_ZSTD_COMPRESSION_LEVEL
| MAX_ROWS
| MEDIUMBLOB
| MEDIUMINT
| MEDIUMTEXT
| MEMORY
| MEMBER
| MERGE
| MIN_ROWS
| MODE
| MODIFY
| MULTILINESTRING
| MULTIPOINT
| MULTIPOLYGON
| NAME
| NAMES
| NCHAR
| NESTED
| NETWORK_NAMESPACE
| NOWAIT
| NO
| NONE
| NULLS
| NUMERIC
| OFFSET
| OJ
| OLD
| OPEN
| OPTION
| OPTIONAL
| OPTIONALLY
| ORDINALITY
| ORGANIZATION
| ONLY
| OPTIMIZE
| OTHERS
| OVERWRITE
| PACK_KEYS
| PARSER
| PARTIAL
| PARTITIONING
| PARTITIONS
| PASSWORD
| PATH
| PERSIST
| PERSIST_ONLY
| PRECEDING
| PREPARE
| PRIVILEGE_CHECKS_USER
| PRIVILEGES
| PROCESS
| PLUGINS
| POINT
| POLYGON
| PROCEDURE
| PROCESSLIST
| QUERY
| RANDOM
| RATIO
| REAL
| REBUILD
| REDUNDANT
| REFERENCE
| REFERENCES
| REGEXP_INSTR %prec FUNCTION_CALL_NON_KEYWORD
| REGEXP_LIKE %prec FUNCTION_CALL_NON_KEYWORD
| REGEXP_REPLACE %prec FUNCTION_CALL_NON_KEYWORD
| REGEXP_SUBSTR %prec FUNCTION_CALL_NON_KEYWORD
| RELAY
| RELEASE_ALL_LOCKS %prec FUNCTION_CALL_NON_KEYWORD
| RELEASE_LOCK %prec FUNCTION_CALL_NON_KEYWORD
| REMOVE
| REORGANIZE
| REPAIR
| REPEATABLE
| RESTRICT
| REQUIRE_ROW_FORMAT
| RESOURCE
| RESPECT
| RESTART
| RETAIN
| RETRY
| RETURNING
| REUSE
| ROLE
| ROLLBACK
| ROW_FORMAT
| RTRIM %prec FUNCTION_CALL_NON_KEYWORD
| S3
| SECONDARY
| SECONDARY_ENGINE
| SECONDARY_ENGINE_ATTRIBUTE
| SECONDARY_LOAD
| SECONDARY_UNLOAD
| SECURITY
| SEQUENCE
| SESSION
| SERIALIZABLE
| SHARE
| SHARED
| SIGNED
| SIMPLE
| SKIP
| SLOW
| SMALLINT
| SQL
| SRID
| START
| STARTING
| STATS_AUTO_RECALC
| STATS_PERSISTENT
| STATS_SAMPLE_PAGES
| STATUS
| STORAGE
| STREAM
| SUBPARTITION
| SUBPARTITIONS
| TABLES
| TABLESPACE
| TEMPORARY
| TEMPTABLE
| TERMINATED
| TEXT
| THAN
| THREAD_PRIORITY
| THROTTLE
| TIES
| TIME
| TIMESTAMP
| TINYBLOB
| TINYINT
| TINYTEXT
| TRANSACTION
| TREE
| TRIGGER
| TRIGGERS
| TRIM %prec FUNCTION_CALL_NON_KEYWORD
| TRUNCATE
| UNBOUNDED
| UNCOMMITTED
| UNDEFINED
| UNICODE
| UNSIGNED
| UNTHROTTLE
| UNUSED
| UpdateXML %prec FUNCTION_CALL_NON_KEYWORD
| UPGRADE
| USER
| USER_RESOURCES
| VALIDATION
| VARBINARY
| VARCHAR
| VARIABLES
| VCPU
| VGTID_EXECUTED
| VIEW
| VINDEX
| VINDEXES
| VISIBLE
| VITESS
| VITESS_KEYSPACES
| VITESS_METADATA
| VITESS_MIGRATION
| VITESS_MIGRATIONS
| VITESS_REPLICATION_STATUS
| VITESS_SHARDS
| VITESS_TABLETS
| VITESS_TARGET
| VITESS_THROTTLED_APPS
| VSCHEMA
| WARNINGS
| WITHOUT
| WORK
| YEAR
| ZEROFILL
| DAY
| DAY_HOUR
| DAY_MICROSECOND
| DAY_MINUTE
| DAY_SECOND
| HOUR
| HOUR_MICROSECOND
| HOUR_MINUTE
| HOUR_SECOND
| MICROSECOND
| MINUTE
| MINUTE_MICROSECOND
| MINUTE_SECOND
| MONTH
| QUARTER
| SECOND
| SECOND_MICROSECOND
| YEAR_MONTH
| WEIGHT_STRING %prec FUNCTION_CALL_NON_KEYWORD

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

skip_to_end:
{
  skipToEnd(yylex)
}

ddl_skip_to_end:
  {
    skipToEnd(yylex)
  }
| openb
  {
    skipToEnd(yylex)
  }
| reserved_sql_id
  {
    skipToEnd(yylex)
  }
