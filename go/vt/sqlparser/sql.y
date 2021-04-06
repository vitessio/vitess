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
  colIdent      ColIdent
  joinCondition JoinCondition
  collateAndCharset CollateAndCharset
  columnType    ColumnType
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
  indexHints    *IndexHints
  literal        *Literal
  subquery      *Subquery
  derivedTable  *DerivedTable
  when          *When
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
  constraintDefinition *ConstraintDefinition
  revertMigration *RevertMigration
  alterMigration  *AlterMigration

  whens         []*When
  columnDefinitions []*ColumnDefinition
  indexOptions  []*IndexOption
  indexColumns  []*IndexColumn
  collateAndCharsets []CollateAndCharset
  tableAndLockTypes TableAndLockTypes
  renameTablePairs []*RenameTablePair
  alterOptions	   []AlterOption
  vindexParams  []VindexParam
  partDefs      []*PartitionDefinition
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
  ReferenceAction ReferenceAction
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
  lockType LockType

  boolean bool
  boolVal BoolVal
  ignore Ignore
}

%token LEX_ERROR
%left <str> UNION
%token <str> SELECT STREAM VSTREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR
%token <str> ALL DISTINCT AS EXISTS ASC DESC INTO DUPLICATE KEY DEFAULT SET LOCK UNLOCK KEYS DO CALL
%token <str> DISTINCTROW PARSER
%token <str> OUTFILE S3 DATA LOAD LINES TERMINATED ESCAPED ENCLOSED
%token <str> DUMPFILE CSV HEADER MANIFEST OVERWRITE STARTING OPTIONALLY
%token <str> VALUES LAST_INSERT_ID
%token <str> NEXT VALUE SHARE MODE
%token <str> SQL_NO_CACHE SQL_CACHE SQL_CALC_FOUND_ROWS
%left <str> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <str> ON USING INPLACE COPY ALGORITHM NONE SHARED EXCLUSIVE
%token <empty> '(' ',' ')'
%token <str> ID AT_ID AT_AT_ID HEX STRING INTEGRAL FLOAT HEXNUM VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD BIT_LITERAL COMPRESSION
%token <str> NULL TRUE FALSE OFF
%token <str> DISCARD IMPORT ENABLE DISABLE TABLESPACE

// Precedence dictated by mysql. But the vitess grammar is simplified.
// Some of these operators don't conflict in our situation. Nevertheless,
// it's better to have these listed in the correct order. Also, we don't
// support all operators yet.
// * NOTE: If you change anything here, update precedence.go as well *
%left <str> OR
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN
%left <str> '|'
%left <str> '&'
%left <str> SHIFT_LEFT SHIFT_RIGHT
%left <str> '+' '-'
%left <str> '*' '/' DIV '%' MOD
%left <str> '^'
%right <str> '~' UNARY
%left <str> COLLATE
%right <str> BINARY UNDERSCORE_BINARY UNDERSCORE_UTF8MB4 UNDERSCORE_UTF8 UNDERSCORE_LATIN1
%right <str> INTERVAL
%nonassoc <str> '.'

// There is no need to define precedence for the JSON
// operators because the syntax is restricted enough that
// they don't cause conflicts.
%token <empty> JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP

// DDL Tokens
%token <str> CREATE ALTER DROP RENAME ANALYZE ADD FLUSH CHANGE MODIFY
%token <str> REVERT
%token <str> SCHEMA TABLE INDEX VIEW TO IGNORE IF UNIQUE PRIMARY COLUMN SPATIAL FULLTEXT KEY_BLOCK_SIZE CHECK INDEXES
%token <str> ACTION CASCADE CONSTRAINT FOREIGN NO REFERENCES RESTRICT
%token <str> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE COALESCE EXCHANGE REBUILD PARTITIONING REMOVE
%token <str> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER
%token <str> VINDEX VINDEXES DIRECTORY NAME UPGRADE
%token <str> STATUS VARIABLES WARNINGS CASCADED DEFINER OPTION SQL UNDEFINED
%token <str> SEQUENCE MERGE TEMPORARY TEMPTABLE INVOKER SECURITY FIRST AFTER LAST

// Migration tokens
%token <str> VITESS_MIGRATION CANCEL RETRY COMPLETE

// Transaction Tokens
%token <str> BEGIN START TRANSACTION COMMIT ROLLBACK SAVEPOINT RELEASE WORK

// Type Tokens
%token <str> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM
%token <str> REAL DOUBLE FLOAT_TYPE DECIMAL NUMERIC
%token <str> TIME TIMESTAMP DATETIME YEAR
%token <str> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR
%token <str> TEXT TINYTEXT MEDIUMTEXT LONGTEXT
%token <str> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON ENUM
%token <str> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON

// Type Modifiers
%token <str> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL

// SHOW tokens
%token <str> COLLATION DATABASES SCHEMAS TABLES VITESS_METADATA VSCHEMA FULL PROCESSLIST COLUMNS FIELDS ENGINES PLUGINS EXTENDED
%token <str> KEYSPACES VITESS_KEYSPACES VITESS_SHARDS VITESS_TABLETS VITESS_MIGRATIONS CODE PRIVILEGES FUNCTION OPEN TRIGGERS EVENT USER

// SET tokens
%token <str> NAMES CHARSET GLOBAL SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE

// Functions
%token <str> CURRENT_TIMESTAMP DATABASE CURRENT_DATE
%token <str> CURRENT_TIME LOCALTIME LOCALTIMESTAMP CURRENT_USER
%token <str> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <str> REPLACE
%token <str> CONVERT CAST
%token <str> SUBSTR SUBSTRING
%token <str> GROUP_CONCAT SEPARATOR
%token <str> TIMESTAMPADD TIMESTAMPDIFF

// Match
%token <str> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION WITHOUT VALIDATION

// MySQL reserved words that are unused by this grammar will map to this token.
%token <str> UNUSED ARRAY CUME_DIST DESCRIPTION DENSE_RANK EMPTY EXCEPT FIRST_VALUE GROUPING GROUPS JSON_TABLE LAG LAST_VALUE LATERAL LEAD MEMBER
%token <str> NTH_VALUE NTILE OF OVER PERCENT_RANK RANK RECURSIVE ROW_NUMBER SYSTEM WINDOW
%token <str> ACTIVE ADMIN BUCKETS CLONE COMPONENT DEFINITION ENFORCED EXCLUDE FOLLOWING GEOMCOLLECTION GET_MASTER_PUBLIC_KEY HISTOGRAM HISTORY
%token <str> INACTIVE INVISIBLE LOCKED MASTER_COMPRESSION_ALGORITHMS MASTER_PUBLIC_KEY_PATH MASTER_TLS_CIPHERSUITES MASTER_ZSTD_COMPRESSION_LEVEL
%token <str> NESTED NETWORK_NAMESPACE NOWAIT NULLS OJ OLD OPTIONAL ORDINALITY ORGANIZATION OTHERS PATH PERSIST PERSIST_ONLY PRECEDING PRIVILEGE_CHECKS_USER PROCESS
%token <str> RANDOM REFERENCE REQUIRE_ROW_FORMAT RESOURCE RESPECT RESTART RETAIN REUSE ROLE SECONDARY SECONDARY_ENGINE SECONDARY_LOAD SECONDARY_UNLOAD SKIP SRID
%token <str> THREAD_PRIORITY TIES UNBOUNDED VCPU VISIBLE

// Explain tokens
%token <str> FORMAT TREE VITESS TRADITIONAL

// Lock type tokens
%token <str> LOCAL LOW_PRIORITY

// Flush tokens
%token <str> NO_WRITE_TO_BINLOG LOGS ERROR GENERAL HOSTS OPTIMIZER_COSTS USER_RESOURCES SLOW CHANNEL RELAY EXPORT

// TableOptions tokens
%token <str> AVG_ROW_LENGTH CONNECTION CHECKSUM DELAY_KEY_WRITE ENCRYPTION ENGINE INSERT_METHOD MAX_ROWS MIN_ROWS PACK_KEYS PASSWORD
%token <str> FIXED DYNAMIC COMPRESSED REDUNDANT COMPACT ROW_FORMAT STATS_AUTO_RECALC STATS_PERSISTENT STATS_SAMPLE_PAGES STORAGE MEMORY DISK

%type <statement> command
%type <selStmt> simple_select select_statement base_select union_rhs
%type <statement> explain_statement explainable_statement
%type <statement> stream_statement vstream_statement insert_statement update_statement delete_statement set_statement set_transaction_statement
%type <statement> create_statement alter_statement rename_statement drop_statement truncate_statement flush_statement do_statement
%type <renameTablePairs> rename_list
%type <createTable> create_table_prefix
%type <alterTable> alter_table_prefix
%type <alterOption> alter_option alter_commands_modifier lock_index algorithm_index
%type <alterOptions> alter_options alter_commands_list alter_commands_modifier_list algorithm_lock_opt
%type <alterTable> create_index_prefix
%type <createDatabase> create_database_prefix
%type <alterDatabase> alter_database_prefix
%type <collateAndCharset> collate character_set
%type <collateAndCharsets> create_options create_options_opt
%type <boolean> default_optional
%type <statement> analyze_statement show_statement use_statement other_statement
%type <statement> begin_statement commit_statement rollback_statement savepoint_statement release_statement load_statement
%type <statement> lock_statement unlock_statement call_statement
%type <statement> revert_statement
%type <strs> comment_opt comment_list
%type <str> wild_opt check_option_opt cascade_or_local_opt restrict_or_cascade_opt
%type <explainType> explain_format_opt
%type <insertAction> insert_or_replace
%type <str> explain_synonyms
%type <str> cache_opt separator_opt flush_option for_channel_opt
%type <matchExprOption> match_option
%type <boolean> distinct_opt union_op replace_opt local_opt
%type <expr> like_escape_opt
%type <selectExprs> select_expression_list select_expression_list_opt
%type <selectExpr> select_expression
%type <strs> select_options flush_option_list
%type <str> select_option algorithm_view security_view security_view_opt
%type <str> definer_opt user
%type <expr> expression
%type <tableExprs> from_opt table_references
%type <tableExpr> table_reference table_factor join_table
%type <joinCondition> join_condition join_condition_opt on_expression_opt
%type <tableNames> table_name_list delete_table_list view_name_list
%type <joinType> inner_join outer_join straight_join natural_join
%type <tableName> table_name into_table_name delete_table_name
%type <aliasedTableName> aliased_table_name
%type <indexHints> index_hint_list
%type <expr> where_expression_opt
%type <expr> condition
%type <boolVal> boolean_value
%type <comparisonExprOperator> compare
%type <ins> insert_data
%type <expr> value value_expression num_val
%type <expr> function_call_keyword function_call_nonkeyword function_call_generic function_call_conflict func_datetime_precision
%type <isExprOperator> is_suffix
%type <colTuple> col_tuple
%type <exprs> expression_list expression_list_opt
%type <values> tuple_list
%type <valTuple> row_tuple tuple_or_empty
%type <expr> tuple_expression
%type <subquery> subquery
%type <derivedTable> derived_table
%type <colName> column_name first_opt after_opt
%type <whens> when_expression_list
%type <when> when_expression
%type <expr> expression_opt else_expression_opt
%type <exprs> group_by_opt
%type <expr> having_opt
%type <orderBy> order_by_opt order_list
%type <order> order
%type <orderDirection> asc_desc_opt
%type <limit> limit_opt
%type <selectInto> into_option
%type <columnTypeOptions> column_type_options
%type <str> header_opt export_options manifest_opt overwrite_opt format_opt optionally_opt
%type <str> fields_opts fields_opt_list fields_opt lines_opts lines_opt lines_opt_list
%type <lock> lock_opt
%type <columns> ins_column_list column_list column_list_opt
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
%type <str> for_from
%type <str> default_opt
%type <ignore> ignore_opt
%type <str> columns_or_fields extended_opt storage_opt
%type <showFilter> like_or_where_opt like_opt
%type <boolean> exists_opt not_exists_opt enforced_opt temp_opt full_opt
%type <empty> to_opt
%type <str> reserved_keyword non_reserved_keyword
%type <colIdent> sql_id reserved_sql_id col_alias as_ci_opt
%type <expr> charset_value
%type <tableIdent> table_id reserved_table_id table_alias as_opt_id table_id_opt from_database_opt
%type <empty> as_opt work_opt savepoint_opt
%type <empty> skip_to_end ddl_skip_to_end
%type <str> charset
%type <scope> set_session_or_global
%type <convertType> convert_type
%type <columnType> column_type
%type <columnType> int_type decimal_type numeric_type time_type char_type spatial_type
%type <literal> length_opt
%type <str> charset_opt collate_opt
%type <LengthScaleOption> float_length_opt decimal_length_opt
%type <boolean> unsigned_opt zero_fill_opt without_valid_opt
%type <strs> enum_values
%type <columnDefinition> column_definition
%type <columnDefinitions> column_definition_list
%type <indexDefinition> index_definition
%type <constraintDefinition> constraint_definition check_constraint_definition
%type <str> index_or_key index_symbols from_or_in index_or_key_opt
%type <str> name_opt constraint_name_opt
%type <str> equal_opt
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
%type <partDefs> partition_definitions
%type <partDef> partition_definition
%type <partSpec> partition_operation
%type <vindexParam> vindex_param
%type <vindexParams> vindex_param_list vindex_params_opt
%type <colIdent> id_or_var vindex_type vindex_type_opt id_or_var_opt
%type <str> database_or_schema column_opt insert_method_options row_format_options
%type <ReferenceAction> fk_reference_action fk_on_delete fk_on_update
%type <str> vitess_topo
%type <tableAndLockTypes> lock_table_list
%type <tableAndLockType> lock_table
%type <lockType> lock_type
%type <empty> session_or_local_opt


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

select_statement:
  base_select order_by_opt limit_opt lock_opt into_option
  {
    sel := $1.(*Select)
    sel.OrderBy = $2
    sel.Limit = $3
    sel.Lock = $4
    sel.Into = $5
    $$ = sel
  }
| openb select_statement closeb order_by_opt limit_opt lock_opt
  {
    $$ = &Union{FirstStatement: &ParenSelect{Select: $2}, OrderBy: $4, Limit:$5, Lock:$6}
  }
| select_statement union_op union_rhs order_by_opt limit_opt lock_opt
  {
    $$ = Unionize($1, $3, $2, $4, $5, $6)
  }
| SELECT comment_opt cache_opt NEXT num_val for_from table_name
  {
    $$ = NewSelect(Comments($2), SelectExprs{&Nextval{Expr: $5}}, []string{$3}/*options*/, TableExprs{&AliasedTableExpr{Expr: $7}}, nil/*where*/, nil/*groupBy*/, nil/*having*/)
  }

// simple_select is an unparenthesized select used for subquery.
// Allowing parenthesis for subqueries leads to grammar ambiguity.
// MySQL also seems to have run into this and resolved it the same way.
// The specific ambiguity comes from the fact that parenthesis means
// many things:
// 1. Grouping: (select id from t) order by id
// 2. Tuple: id in (1, 2, 3)
// 3. Subquery: id in (select id from t)
// Example:
// ((select id from t))
// Interpretation 1: inner () is for subquery (rule 3), and outer ()
// is Tuple (rule 2), which degenerates to a simple expression
// for single value expressions.
// Interpretation 2: inner () is for grouping (rule 1), and outer
// is for subquery.
// Not allowing parenthesis for subselects will force the above
// construct to use the first interpretation.
simple_select:
  base_select order_by_opt limit_opt lock_opt
  {
    sel := $1.(*Select)
    sel.OrderBy = $2
    sel.Limit = $3
    sel.Lock = $4
    $$ = sel
  }
| simple_select union_op union_rhs order_by_opt limit_opt lock_opt
  {
    $$ = Unionize($1, $3, $2, $4, $5, $6)
  }

stream_statement:
  STREAM comment_opt select_expression FROM table_name
  {
    $$ = &Stream{Comments: Comments($2), SelectExpr: $3, Table: $5}
  }

vstream_statement:
  VSTREAM comment_opt select_expression FROM table_name where_expression_opt limit_opt
  {
    $$ = &VStream{Comments: Comments($2), SelectExpr: $3, Table: $5, Where: NewWhere(WhereClause, $6), Limit: $7}
  }

// base_select is an unparenthesized SELECT with no order by clause or beyond.
base_select:
//  1         2            3              4                    5             6                7           8
  SELECT comment_opt select_options select_expression_list from_opt where_expression_opt group_by_opt having_opt
  {
    $$ = NewSelect(Comments($2), $4/*SelectExprs*/, $3/*options*/, $5/*from*/, NewWhere(WhereClause, $6), GroupBy($7), NewWhere(HavingClause, $8))
  }

union_rhs:
  base_select
  {
    $$ = $1
  }
| openb select_statement closeb
  {
    $$ = &ParenSelect{Select: $2}
  }


insert_statement:
  insert_or_replace comment_opt ignore_opt into_table_name opt_partition_clause insert_data on_dup_opt
  {
    // insert_data returns a *Insert pre-filled with Columns & Values
    ins := $6
    ins.Action = $1
    ins.Comments = $2
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
    $$ = &Insert{Action: $1, Comments: Comments($2), Ignore: $3, Table: $4, Partitions: $5, Columns: cols, Rows: Values{vals}, OnDup: OnDup($8)}
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
  UPDATE comment_opt ignore_opt table_references SET update_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{Comments: Comments($2), Ignore: $3, TableExprs: $4, Exprs: $6, Where: NewWhere(WhereClause, $7), OrderBy: $8, Limit: $9}
  }

delete_statement:
  DELETE comment_opt ignore_opt FROM table_name opt_partition_clause where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{Comments: Comments($2), Ignore: $3, TableExprs:  TableExprs{&AliasedTableExpr{Expr:$5}}, Partitions: $6, Where: NewWhere(WhereClause, $7), OrderBy: $8, Limit: $9}
  }
| DELETE comment_opt ignore_opt FROM table_name_list USING table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Ignore: $3, Targets: $5, TableExprs: $7, Where: NewWhere(WhereClause, $8)}
  }
| DELETE comment_opt ignore_opt table_name_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Ignore: $3, Targets: $4, TableExprs: $6, Where: NewWhere(WhereClause, $7)}
  }
| DELETE comment_opt ignore_opt delete_table_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Ignore: $3, Targets: $4, TableExprs: $6, Where: NewWhere(WhereClause, $7)}
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

set_transaction_statement:
  SET comment_opt set_session_or_global TRANSACTION transaction_chars
  {
    $$ = &SetTransaction{Comments: Comments($2), Scope: $3, Characteristics: $5}
  }
| SET comment_opt TRANSACTION transaction_chars
  {
    $$ = &SetTransaction{Comments: Comments($2), Characteristics: $4, Scope: ImplicitScope}
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
| CREATE replace_opt algorithm_view definer_opt security_view_opt VIEW table_name column_list_opt AS select_statement check_option_opt
  {
    $$ = &CreateView{ViewName: $7.ToViewName(), IsReplace:$2, Algorithm:$3, Definer: $4 ,Security:$5, Columns:$8, Select: $10, CheckOption: $11 }
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
  id_or_var
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

create_table_prefix:
  CREATE temp_opt TABLE not_exists_opt table_name
  {
    $$ = &CreateTable{Table: $5, IfNotExists: $4, Temp: $2}
    setDDL(yylex, $$)
  }

alter_table_prefix:
  ALTER TABLE table_name
  {
    $$ = &AlterTable{Table: $3}
    setDDL(yylex, $$)
  }

create_index_prefix:
  CREATE INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $6, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$3, Type:string($2)}, Options:$4}}}}
    setDDL(yylex, $$)
  }
| CREATE FULLTEXT INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $7, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$4, Type:string($2)+" "+string($3), Fulltext:true}, Options:$5}}}}
    setDDL(yylex, $$)
  }
| CREATE SPATIAL INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $7, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$4, Type:string($2)+" "+string($3), Spatial:true}, Options:$5}}}}
    setDDL(yylex, $$)
  }
| CREATE UNIQUE INDEX id_or_var using_opt ON table_name
  {
    $$ = &AlterTable{Table: $7, AlterOptions: []AlterOption{&AddIndexDefinition{IndexDefinition:&IndexDefinition{Info: &IndexInfo{Name:$4, Type:string($2)+" "+string($3), Unique:true}, Options:$5}}}}
    setDDL(yylex, $$)
  }

create_database_prefix:
  CREATE database_or_schema comment_opt not_exists_opt table_id
  {
    $$ = &CreateDatabase{Comments: Comments($3), DBName: $5, IfNotExists: $4}
    setDDL(yylex,$$)
  }

alter_database_prefix:
  ALTER database_or_schema
  {
    $$ = &AlterDatabase{}
    setDDL(yylex,$$)
  }

database_or_schema:
  DATABASE
| SCHEMA

table_spec:
  '(' table_column_list ')' table_option_list_opt
  {
    $$ = $2
    $$.Options = $4
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
    $$ = []CollateAndCharset{$1}
  }
| collate
  {
    $$ = []CollateAndCharset{$1}
  }
| create_options collate
  {
    $$ = append($1,$2)
  }
| create_options character_set
  {
    $$ = append($1,$2)
  }

default_optional:
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
    $$ = CollateAndCharset{Type:CharacterSetType, Value:($4.String()), IsDefault:$1}
  }
| default_optional charset_or_character_set equal_opt STRING
  {
    $$ = CollateAndCharset{Type:CharacterSetType, Value:(encodeSQLString($4)), IsDefault:$1}
  }

collate:
  default_optional COLLATE equal_opt id_or_var
  {
    $$ = CollateAndCharset{Type:CollateType, Value:($4.String()), IsDefault:$1}
  }
| default_optional COLLATE equal_opt STRING
  {
    $$ = CollateAndCharset{Type:CollateType, Value:(encodeSQLString($4)), IsDefault:$1}
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

column_definition:
  sql_id column_type column_type_options
  {
    $2.Options = $3
    $$ = &ColumnDefinition{Name: $1, Type: $2}
  }

// There is a shift reduce conflict that arises here because UNIQUE and KEY are column_type_option and so is UNIQUE KEY.
// So in the state "column_type_options UNIQUE. KEY" there is a shift-reduce conflict.
// This has been added to emulate what MySQL does. The previous architecture was such that the order of the column options
// was specific (as stated in the MySQL guide) and did not accept arbitrary order options. For example NOT NULL DEFAULT 1 and not DEFAULT 1 NOT NULL
column_type_options:
  {
    $$ = &ColumnTypeOptions{Null: nil, Default: nil, OnUpdate: nil, Autoincrement: false, KeyOpt: colKeyNone, Comment: nil}
  }
| column_type_options NULL
  {
    val := true
    $1.Null = &val
    $$ = $1
  }
| column_type_options NOT NULL
  {
    val := false
    $1.Null = &val
    $$ = $1
  }
| column_type_options DEFAULT value_expression
  {
    $1.Default = $3
    $$ = $1
  }
| column_type_options ON UPDATE function_call_nonkeyword
  {
    $1.OnUpdate = $4
    $$ = $1
  }
| column_type_options AUTO_INCREMENT
  {
    $1.Autoincrement = true
    $$ = $1
  }
| column_type_options COMMENT_KEYWORD STRING
  {
    $1.Comment = NewStrLiteral($3)
    $$ = $1
  }
| column_type_options PRIMARY KEY
  {
    $1.KeyOpt = colKeyPrimary
    $$ = $1
  }
| column_type_options KEY
  {
    $1.KeyOpt = colKey
    $$ = $1
  }
| column_type_options UNIQUE KEY
  {
    $1.KeyOpt = colKeyUniqueKey
    $$ = $1
  }
| column_type_options UNIQUE
  {
    $1.KeyOpt = colKeyUnique
    $$ = $1
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
  CHAR length_opt charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2, Charset: $3, Collate: $4}
  }
| VARCHAR length_opt charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2, Charset: $3, Collate: $4}
  }
| BINARY length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| VARBINARY length_opt
  {
    $$ = ColumnType{Type: string($1), Length: $2}
  }
| TEXT charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
  }
| TINYTEXT charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
  }
| MEDIUMTEXT charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
  }
| LONGTEXT charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
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
| ENUM '(' enum_values ')' charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3, Charset: $5, Collate: $6}
  }
// need set_values / SetValues ?
| SET '(' enum_values ')' charset_opt collate_opt
  {
    $$ = ColumnType{Type: string($1), EnumValues: $3, Charset: $5, Collate: $6}
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
    $$ = ""
  }
| charset_or_character_set id_or_var
  {
    $$ = string($2.String())
  }
| charset_or_character_set STRING
  {
    $$ = encodeSQLString($2)
  }
| charset_or_character_set BINARY
  {
    $$ = string($2)
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
| WITH PARSER id_or_var
  {
    $$ = &IndexOption{Name: string($1) + " " + string($2), String: $3.String()}
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
  constraint_name_opt PRIMARY KEY
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
  FOREIGN KEY '(' column_list ')' REFERENCES table_name '(' column_list ')'
  {
    $$ = &ForeignKeyDefinition{Source: $4, ReferencedTable: $7, ReferencedColumns: $9}
  }
| FOREIGN KEY '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_delete
  {
    $$ = &ForeignKeyDefinition{Source: $4, ReferencedTable: $7, ReferencedColumns: $9, OnDelete: $11}
  }
| FOREIGN KEY '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_update
  {
    $$ = &ForeignKeyDefinition{Source: $4, ReferencedTable: $7, ReferencedColumns: $9, OnUpdate: $11}
  }
| FOREIGN KEY '(' column_list ')' REFERENCES table_name '(' column_list ')' fk_on_delete fk_on_update
  {
    $$ = &ForeignKeyDefinition{Source: $4, ReferencedTable: $7, ReferencedColumns: $9, OnDelete: $11, OnUpdate: $12}
  }

check_constraint_info:
  CHECK '(' expression ')' enforced_opt
  {
    $$ = &CheckConstraintDefinition{Expr: $3, Enforced: $5}
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

enforced_opt:
  {
    $$ = true
  }
| ENFORCED
  {
    $$ = true
  }
| NOT ENFORCED
  {
    $$ = false
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
| AVG_ROW_LENGTH equal_opt INTEGRAL
  {
    $$ = &TableOption{Name:string($1), Value:NewIntLiteral($3)}
  }
| default_optional charset_or_character_set equal_opt charset
  {
    $$ = &TableOption{Name:(string($2)), String:$4}
  }
| default_optional COLLATE equal_opt charset
  {
    $$ = &TableOption{Name:string($2), String:$4}
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
| ENGINE equal_opt id_or_var
  {
    $$ = &TableOption{Name:string($1), String:$3.String()}
  }
| ENGINE equal_opt STRING
  {
    $$ = &TableOption{Name:string($1), Value:NewStrLiteral($3)}
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
    $$ = nil
  }
| FIRST column_name
  {
    $$ = $2
  }

after_opt:
  {
    $$ = nil
  }
| AFTER column_name
  {
    $$ = $2
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
| ALTER column_opt column_name SET DEFAULT value
  {
    $$ = &AlterColumn{Column: $3, DropDefault:false, DefaultVal:$6}
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
  alter_table_prefix alter_commands_list
  {
    $1.FullyParsed = true
    $1.AlterOptions = $2
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
| ALTER algorithm_view definer_opt security_view_opt VIEW table_name column_list_opt AS select_statement check_option_opt
  {
    $$ = &AlterView{ViewName: $6.ToViewName(), Algorithm:$2, Definer: $3 ,Security:$4, Columns:$7, Select: $9, CheckOption: $10 }
  }
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
| ALTER VSCHEMA CREATE VINDEX table_name vindex_type_opt vindex_params_opt
  {
    $$ = &AlterVschema{
        Action: CreateVindexDDLAction,
        Table: $5,
        VindexSpec: &VindexSpec{
          Name: NewColIdent($5.Name.String()),
          Type: $6,
          Params: $7,
        },
      }
  }
| ALTER VSCHEMA DROP VINDEX table_name
  {
    $$ = &AlterVschema{
        Action: DropVindexDDLAction,
        Table: $5,
        VindexSpec: &VindexSpec{
          Name: NewColIdent($5.Name.String()),
        },
      }
  }
| ALTER VSCHEMA ADD TABLE table_name
  {
    $$ = &AlterVschema{Action: AddVschemaTableDDLAction, Table: $5}
  }
| ALTER VSCHEMA DROP TABLE table_name
  {
    $$ = &AlterVschema{Action: DropVschemaTableDDLAction, Table: $5}
  }
| ALTER VSCHEMA ON table_name ADD VINDEX sql_id '(' column_list ')' vindex_type_opt vindex_params_opt
  {
    $$ = &AlterVschema{
        Action: AddColVindexDDLAction,
        Table: $4,
        VindexSpec: &VindexSpec{
            Name: $7,
            Type: $11,
            Params: $12,
        },
        VindexCols: $9,
      }
  }
| ALTER VSCHEMA ON table_name DROP VINDEX sql_id
  {
    $$ = &AlterVschema{
        Action: DropColVindexDDLAction,
        Table: $4,
        VindexSpec: &VindexSpec{
            Name: $7,
        },
      }
  }
| ALTER VSCHEMA ADD SEQUENCE table_name
  {
    $$ = &AlterVschema{Action: AddSequenceDDLAction, Table: $5}
  }
| ALTER VSCHEMA ON table_name ADD AUTO_INCREMENT sql_id USING table_name
  {
    $$ = &AlterVschema{
        Action: AddAutoIncDDLAction,
        Table: $4,
        AutoIncSpec: &AutoIncSpec{
            Column: $7,
            Sequence: $9,
        },
    }
  }
| ALTER VITESS_MIGRATION STRING RETRY
  {
    $$ = &AlterMigration{
      Type: RetryMigrationType,
      UUID: string($3),
    }
  }
| ALTER VITESS_MIGRATION STRING COMPLETE
  {
    $$ = &AlterMigration{
      Type: CompleteMigrationType,
      UUID: string($3),
    }
  }
| ALTER VITESS_MIGRATION STRING CANCEL
  {
    $$ = &AlterMigration{
      Type: CancelMigrationType,
      UUID: string($3),
    }
  }
| ALTER VITESS_MIGRATION CANCEL ALL
  {
    $$ = &AlterMigration{
      Type: CancelAllMigrationType,
    }
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
  PARTITION sql_id VALUES LESS THAN openb value_expression closeb
  {
    $$ = &PartitionDefinition{Name: $2, Limit: $7}
  }
| PARTITION sql_id VALUES LESS THAN openb MAXVALUE closeb
  {
    $$ = &PartitionDefinition{Name: $2, Maxvalue: true}
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
  DROP temp_opt TABLE exists_opt table_name_list restrict_or_cascade_opt
  {
    $$ = &DropTable{FromTables: $5, IfExists: $4, Temp: $2}
  }
| DROP INDEX id_or_var ON table_name algorithm_lock_opt
  {
    // Change this to an alter statement
    if $3.Lowered() == "primary" {
      $$ = &AlterTable{Table: $5,AlterOptions: append([]AlterOption{&DropKey{Type:PrimaryKeyType}},$6...)}
    } else {
      $$ = &AlterTable{Table: $5,AlterOptions: append([]AlterOption{&DropKey{Type:NormalKeyType, Name:$3}},$6...)}
    }
  }
| DROP VIEW exists_opt view_name_list restrict_or_cascade_opt
  {
    $$ = &DropView{FromTables: $4, IfExists: $3}
  }
| DROP database_or_schema comment_opt exists_opt table_id
  {
    $$ = &DropDatabase{Comments: Comments($3), DBName: $5, IfExists: $4}
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
| SHOW CREATE USER ddl_skip_to_end
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Scope: ImplicitScope}}
   }
|  SHOW BINARY id_or_var ddl_skip_to_end /* SHOW BINARY ... */
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3.String()), Scope: ImplicitScope}}
  }
|  SHOW BINARY LOGS ddl_skip_to_end /* SHOW BINARY LOGS */
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Scope: ImplicitScope}}
  }
| SHOW ENGINES
  {
    $$ = &Show{&ShowLegacy{Type: string($2), Scope: ImplicitScope}}
  }
| SHOW FUNCTION CODE table_name
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Table: $4, Scope: ImplicitScope}}
  }
| SHOW PLUGINS
  {
    $$ = &Show{&ShowLegacy{Type: string($2), Scope: ImplicitScope}}
  }
| SHOW PROCEDURE CODE table_name
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Table: $4, Scope: ImplicitScope}}
  }
| SHOW full_opt PROCESSLIST from_database_opt like_or_where_opt
  {
      $$ = &Show{&ShowLegacy{Type: string($3), Scope: ImplicitScope}}
  }
| SHOW VITESS_METADATA VARIABLES like_opt
  {
    showTablesOpt := &ShowTablesOpt{Filter: $4}
    $$ = &Show{&ShowLegacy{Scope: VitessMetadataScope, Type: string($3), ShowTablesOpt: showTablesOpt}}
  }
| SHOW VITESS_MIGRATIONS from_database_opt like_or_where_opt
  {
    $$ = &Show{&ShowBasic{Command: VitessMigrations, Filter: $4, DbName: $3}}
  }
| SHOW VSCHEMA TABLES
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Scope: ImplicitScope}}
  }
| SHOW VSCHEMA VINDEXES
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), Scope: ImplicitScope}}
  }
| SHOW VSCHEMA VINDEXES ON table_name
  {
    $$ = &Show{&ShowLegacy{Type: string($2) + " " + string($3), OnTable: $5, Scope: ImplicitScope}}
  }
| SHOW WARNINGS
  {
    $$ = &Show{&ShowLegacy{Type: string($2), Scope: ImplicitScope}}
  }
/* vitess_topo supports SHOW VITESS_SHARDS / SHOW VITESS_TABLETS */
| SHOW vitess_topo like_or_where_opt
  {
    // This should probably be a different type (ShowVitessTopoOpt), but
    // just getting the thing working for now
    showTablesOpt := &ShowTablesOpt{Filter: $3}
    $$ = &Show{&ShowLegacy{Type: $2, ShowTablesOpt: showTablesOpt}}
  }
/*
 * Catch-all for show statements without vitess keywords:
 *
 *  SHOW BINARY LOGS
 *  SHOW INVALID
 *  SHOW VITESS_TARGET
 */
| SHOW id_or_var ddl_skip_to_end
  {
    $$ = &Show{&ShowLegacy{Type: string($2.String()), Scope: ImplicitScope}}
  }
| SHOW ENGINE ddl_skip_to_end
  {
    $$ = &Show{&ShowLegacy{Type: string($2), Scope: ImplicitScope}}
  }
| SHOW STORAGE ddl_skip_to_end
  {
    $$ = &Show{&ShowLegacy{Type: string($2), Scope: ImplicitScope}}
  }

vitess_topo:
  VITESS_TABLETS
  {
    $$ = string($1)
  }
| VITESS_SHARDS
  {
    $$ = string($1)
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

use_statement:
  USE table_id
  {
    $$ = &Use{DBName: $2}
  }
| USE
  {
    $$ = &Use{DBName:TableIdent{v:""}}
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
  REVERT VITESS_MIGRATION STRING
  {
    $$ = &RevertMigration{UUID: string($3)}
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
  {
    $$ = TableExprs{&AliasedTableExpr{Expr:TableName{Name: NewTableIdent("dual")}}}
  }
| FROM table_references
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
| derived_table as_opt table_id
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $3}
  }
| openb table_references closeb
  {
    $$ = &ParenTableExpr{Exprs: $2}
  }

derived_table:
  openb select_statement closeb
  {
    $$ = &DerivedTable{$2}
  }

aliased_table_name:
table_name as_opt_id index_hint_list
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $2, Hints: $3}
  }
| table_name PARTITION openb partition_list closeb as_opt_id index_hint_list
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

index_hint_list:
  {
    $$ = nil
  }
| USE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: UseOp, Indexes: $4}
  }
| USE INDEX openb closeb
  {
    $$ = &IndexHints{Type: UseOp}
  }
| IGNORE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: IgnoreOp, Indexes: $4}
  }
| FORCE INDEX openb column_list closeb
  {
    $$ = &IndexHints{Type: ForceOp, Indexes: $4}
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

condition:
  value_expression compare value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }
| value_expression IN col_tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: InOp, Right: $3}
  }
| value_expression NOT IN col_tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotInOp, Right: $4}
  }
| value_expression LIKE value_expression like_escape_opt
  {
    $$ = &ComparisonExpr{Left: $1, Operator: LikeOp, Right: $3, Escape: $4}
  }
| value_expression NOT LIKE value_expression like_escape_opt
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotLikeOp, Right: $4, Escape: $5}
  }
| value_expression REGEXP value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: RegexpOp, Right: $3}
  }
| value_expression NOT REGEXP value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: NotRegexpOp, Right: $4}
  }
| value_expression BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: BetweenOp, From: $3, To: $5}
  }
| value_expression NOT BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: NotBetweenOp, From: $4, To: $6}
  }
| EXISTS subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }

is_suffix:
  NULL
  {
    $$ = IsNullOp
  }
| NOT NULL
  {
    $$ = IsNotNullOp
  }
| TRUE
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
    bindVariable(yylex, $1[2:])
  }

subquery:
  openb simple_select closeb
  {
    $$ = &Subquery{$2}
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
| boolean_value
  {
    $$ = $1
  }
| column_name
  {
    $$ = $1
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
    $$ = &BinaryExpr{Left: $1, Operator: BitAndOp, Right: $3}
  }
| value_expression '|' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: BitOrOp, Right: $3}
  }
| value_expression '^' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: BitXorOp, Right: $3}
  }
| value_expression '+' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: PlusOp, Right: $3}
  }
| value_expression '-' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: MinusOp, Right: $3}
  }
| value_expression '*' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: MultOp, Right: $3}
  }
| value_expression '/' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: DivOp, Right: $3}
  }
| value_expression DIV value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: IntDivOp, Right: $3}
  }
| value_expression '%' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ModOp, Right: $3}
  }
| value_expression MOD value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ModOp, Right: $3}
  }
| value_expression SHIFT_LEFT value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ShiftLeftOp, Right: $3}
  }
| value_expression SHIFT_RIGHT value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: ShiftRightOp, Right: $3}
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
    $$ = &CollateExpr{Expr: $1, Charset: $3}
  }
| BINARY value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BinaryOp, Expr: $2}
  }
| UNDERSCORE_BINARY value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: UBinaryOp, Expr: $2}
  }
| UNDERSCORE_UTF8 value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: Utf8Op, Expr: $2}
  }
| UNDERSCORE_UTF8MB4 value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: Utf8mb4Op, Expr: $2}
  }
| UNDERSCORE_LATIN1 value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: Latin1Op, Expr: $2}
  }
| '+'  value_expression %prec UNARY
  {
    $$ = $2
  }
| '-'  value_expression %prec UNARY
  {
    $$ = handleUnaryMinus($2)
  }
| '~'  value_expression
  {
    $$ = &UnaryExpr{Operator: TildaOp, Expr: $2}
  }
| '!' value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BangOp, Expr: $2}
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
| CONVERT openb expression ',' convert_type closeb
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5}
  }
| CAST openb expression AS convert_type closeb
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5}
  }
| CONVERT openb expression USING charset closeb
  {
    $$ = &ConvertUsingExpr{Expr: $3, Type: $5}
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
    $$ = &SubstrExpr{StrVal: NewStrLiteral($3), From: $5, To: $7}
  }
| SUBSTRING openb STRING FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{StrVal: NewStrLiteral($3), From: $5, To: $7}
  }
| MATCH openb select_expression_list closeb AGAINST openb value_expression match_option closeb
  {
  $$ = &MatchExpr{Columns: $3, Expr: $7, Option: $8}
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
  CURRENT_TIMESTAMP func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_timestamp")}
  }
| UTC_TIMESTAMP func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_timestamp")}
  }
| UTC_TIME func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_time")}
  }
/* doesn't support fsp */
| UTC_DATE func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_date")}
  }
  // now
| LOCALTIME func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("localtime")}
  }
  // now
| LOCALTIMESTAMP func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("localtimestamp")}
  }
  // curdate
/* doesn't support fsp */
| CURRENT_DATE func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_date")}
  }
  // curtime
| CURRENT_TIME func_paren_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_time")}
  }
// these functions can also be called with an optional argument
|  CURRENT_TIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("current_timestamp"), Fsp:$2}
  }
| UTC_TIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("utc_timestamp"), Fsp:$2}
  }
| UTC_TIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("utc_time"), Fsp:$2}
  }
  // now
| LOCALTIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("localtime"), Fsp:$2}
  }
  // now
| LOCALTIMESTAMP func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("localtimestamp"), Fsp:$2}
  }
  // curtime
| CURRENT_TIME func_datetime_precision
  {
    $$ = &CurTimeFuncExpr{Name:NewColIdent("current_time"), Fsp:$2}
  }
| TIMESTAMPADD openb sql_id ',' value_expression ',' value_expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampadd"), Unit:$3.String(), Expr1:$5, Expr2:$7}
  }
| TIMESTAMPDIFF openb sql_id ',' value_expression ',' value_expression closeb
  {
    $$ = &TimestampFuncExpr{Name:string("timestampdiff"), Unit:$3.String(), Expr1:$5, Expr2:$7}
  }

func_paren_opt:
  /* empty */
| openb closeb

func_datetime_precision:
  openb value_expression closeb
  {
    $$ = $2
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
| SUBSTR openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("substr"), Exprs: $3}
  }
| SUBSTRING openb select_expression_list closeb
  {
    $$ = &FuncExpr{Name: NewColIdent("substr"), Exprs: $3}
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
  id_or_var
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

convert_type:
  BINARY length_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2}
  }
| CHAR length_opt charset_opt
  {
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: $3, Operator: CharacterSetOp}
  }
| CHAR length_opt id_or_var
  {
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: string($3.String())}
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

value:
  STRING
  {
    $$ = NewStrLiteral($1)
  }
| HEX
  {
    $$ = NewHexLiteral($1)
  }
| BIT_LITERAL
  {
    $$ = NewBitLiteral($1)
  }
| INTEGRAL
  {
    $$ = NewIntLiteral($1)
  }
| FLOAT
  {
    $$ = NewFloatLiteral($1)
  }
| HEXNUM
  {
    $$ = NewHexNumLiteral($1)
  }
| VALUE_ARG
  {
    $$ = NewArgument($1)
    bindVariable(yylex, $1[1:])
  }
| NULL
  {
    $$ = &NullVal{}
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
    $$ = NewArgument($1)
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
| LIMIT expression
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
    $$ = ""
  }
| DEFINER '=' user
  {
    $$ = $3
  }

user:
CURRENT_USER
  {
    $$ = string($1)
  }
| CURRENT_USER '(' ')'
  {
    $$ = string($1)
  }
| STRING AT_ID
  {
    $$ = encodeSQLString($1) + "@" + string($2)
  }
| ID
  {
    $$ = string($1)
  }

lock_opt:
  {
    $$ = NoLock
  }
| FOR UPDATE
  {
    $$ = ForUpdateLock
  }
| LOCK IN SHARE MODE
  {
    $$ = ShareModeLock
  }

into_option:
  {
    $$ = nil
  }
| INTO OUTFILE S3 STRING charset_opt format_opt export_options manifest_opt overwrite_opt
  {
    $$ = &SelectInto{Type:IntoOutfileS3, FileName:encodeSQLString($4), Charset:$5, FormatOption:$6, ExportOption:$7, Manifest:$8, Overwrite:$9}
  }
| INTO DUMPFILE STRING
  {
    $$ = &SelectInto{Type:IntoDumpfile, FileName:encodeSQLString($3), Charset:"", FormatOption:"", ExportOption:"", Manifest:"", Overwrite:""}
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
| ARRAY
| AND
| AS
| ASC
| BETWEEN
| BINARY
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
| END
| ESCAPE
| EXISTS
| EXPLAIN
| FALSE
| FIRST_VALUE
| FOR
| FORCE
| FOREIGN
| FROM
| FULLTEXT
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
| LEFT
| LIKE
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| LOCK
| LOW_PRIORITY
| MEMBER
| MATCH
| MAXVALUE
| MOD
| NATURAL
| NEXT // next should be doable as non-reserved, but is not due to the special `select next num_val` query that vitess supports
| NO_WRITE_TO_BINLOG
| NOT
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
| RANK
| READ
| RECURSIVE
| REGEXP
| RENAME
| REPLACE
| RIGHT
| ROW_NUMBER
| SCHEMA
| SCHEMAS
| SELECT
| SEPARATOR
| SET
| SHOW
| SPATIAL
| STRAIGHT_JOIN
| SYSTEM
| TABLE
| THEN
| TIMESTAMPADD
| TIMESTAMPDIFF
| TO
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
| AUTO_INCREMENT
| AVG_ROW_LENGTH
| BEGIN
| BIGINT
| BIT
| BLOB
| BOOL
| BOOLEAN
| BUCKETS
| CANCEL
| CASCADE
| CASCADED
| CHANNEL
| CHAR
| CHARSET
| CHECKSUM
| CLONE
| COALESCE
| CODE
| COLLATION
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
| DATA
| DATE
| DATETIME
| DECIMAL
| DELAY_KEY_WRITE
| DEFINER
| DEFINITION
| DESCRIPTION
| DIRECTORY
| DISABLE
| DISCARD
| DISK
| DOUBLE
| DUMPFILE
| DUPLICATE
| DYNAMIC
| ENABLE
| ENCLOSED
| ENCRYPTION
| ENFORCED
| ENGINE
| ENGINES
| ENUM
| ERROR
| ESCAPED
| EVENT
| EXCHANGE
| EXCLUDE
| EXCLUSIVE
| EXPANSION
| EXPORT
| EXTENDED
| FLOAT_TYPE
| FIELDS
| FIRST
| FIXED
| FLUSH
| FOLLOWING
| FORMAT
| FUNCTION
| GENERAL
| GEOMCOLLECTION
| GEOMETRY
| GEOMETRYCOLLECTION
| GET_MASTER_PUBLIC_KEY
| GLOBAL
| HEADER
| HISTOGRAM
| HISTORY
| HOSTS
| IMPORT
| INACTIVE
| INPLACE
| INSERT_METHOD
| INT
| INTEGER
| INVISIBLE
| INVOKER
| INDEXES
| ISOLATION
| JSON
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
| LOAD
| LOCAL
| LOCKED
| LOGS
| LONGBLOB
| LONGTEXT
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
| PARTITIONING
| PASSWORD
| PATH
| PERSIST
| PERSIST_ONLY
| PRECEDING
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
| REAL
| REBUILD
| REDUNDANT
| REFERENCE
| REFERENCES
| RELAY
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
| REUSE
| ROLE
| ROLLBACK
| ROW_FORMAT
| S3
| SECONDARY
| SECONDARY_ENGINE
| SECONDARY_LOAD
| SECONDARY_UNLOAD
| SECURITY
| SEQUENCE
| SESSION
| SERIALIZABLE
| SHARE
| SHARED
| SIGNED
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
| TABLES
| TABLESPACE
| TEMPORARY
| TEMPTABLE
| TERMINATED
| TEXT
| THAN
| THREAD_PRIORITY
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
| TRUNCATE
| UNBOUNDED
| UNCOMMITTED
| UNDEFINED
| UNSIGNED
| UNUSED
| UPGRADE
| USER
| USER_RESOURCES
| VALIDATION
| VARBINARY
| VARCHAR
| VARIABLES
| VCPU
| VIEW
| VINDEX
| VINDEXES
| VISIBLE
| VITESS
| VITESS_KEYSPACES
| VITESS_METADATA
| VITESS_SHARDS
| VITESS_TABLETS
| VITESS_MIGRATION
| VITESS_MIGRATIONS
| VSCHEMA
| WARNINGS
| WITHOUT
| YEAR
| ZEROFILL

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
