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

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func setDDL(yylex interface{}, ddl *DDL) {
  yylex.(*Tokenizer).partialDDL = ddl
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

// skipToEnd forces the lexer to end prematurely. Not all SQL statements
// are supported by the Parser, thus calling skipToEnd will make the lexer
// return EOF early.
func skipToEnd(yylex interface{}) {
  yylex.(*Tokenizer).SkipToEnd = true
}

%}

%union {
  empty         struct{}
  statement     Statement
  selStmt       SelectStatement
  ddl           *DDL
  ins           *Insert
  byt           byte
  bytes         []byte
  bytes2        [][]byte
  str           string
  strs          []string
  selectExprs   SelectExprs
  selectExpr    SelectExpr
  columns       Columns
  partitions    Partitions
  colName       *ColName
  tableExprs    TableExprs
  tableExpr     TableExpr
  joinCondition JoinCondition
  tableName     TableName
  tableNames    TableNames
  indexHints    *IndexHints
  expr          Expr
  exprs         Exprs
  boolVal       BoolVal
  sqlVal        *SQLVal
  colTuple      ColTuple
  values        Values
  valTuple      ValTuple
  subquery      *Subquery
  whens         []*When
  when          *When
  orderBy       OrderBy
  order         *Order
  limit         *Limit
  updateExprs   UpdateExprs
  setExprs      SetExprs
  updateExpr    *UpdateExpr
  setExpr       *SetExpr
  colIdent      ColIdent
  tableIdent    TableIdent
  convertType   *ConvertType
  aliasedTableName *AliasedTableExpr
  TableSpec  *TableSpec
  columnType    ColumnType
  colKeyOpt     ColumnKeyOption
  optVal        Expr
  LengthScaleOption LengthScaleOption
  columnDefinition *ColumnDefinition
  indexDefinition *IndexDefinition
  indexInfo     *IndexInfo
  indexOption   *IndexOption
  indexOptions  []*IndexOption
  indexColumn   *IndexColumn
  indexColumns  []*IndexColumn
  constraintDefinition *ConstraintDefinition
  constraintInfo ConstraintInfo
  ReferenceAction ReferenceAction
  partDefs      []*PartitionDefinition
  partDef       *PartitionDefinition
  partSpec      *PartitionSpec
  vindexParam   VindexParam
  vindexParams  []VindexParam
  showFilter    *ShowFilter
  optLike       *OptLike
}

%token LEX_ERROR
%left <bytes> UNION
%token <bytes> SELECT STREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR
%token <bytes> ALL DISTINCT AS EXISTS ASC DESC INTO DUPLICATE KEY DEFAULT SET LOCK UNLOCK KEYS
%token <bytes> DISTINCTROW
%token <bytes> VALUES LAST_INSERT_ID
%token <bytes> NEXT VALUE SHARE MODE
%token <bytes> SQL_NO_CACHE SQL_CACHE
%left <bytes> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <bytes> ON USING
%token <empty> '(' ',' ')'
%token <bytes> ID AT_ID AT_AT_ID HEX STRING INTEGRAL FLOAT HEXNUM VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD BIT_LITERAL
%token <bytes> NULL TRUE FALSE OFF

// Precedence dictated by mysql. But the vitess grammar is simplified.
// Some of these operators don't conflict in our situation. Nevertheless,
// it's better to have these listed in the correct order. Also, we don't
// support all operators yet.
%left <bytes> OR
%left <bytes> AND
%right <bytes> NOT '!'
%left <bytes> BETWEEN CASE WHEN THEN ELSE END
%left <bytes> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN
%left <bytes> '|'
%left <bytes> '&'
%left <bytes> SHIFT_LEFT SHIFT_RIGHT
%left <bytes> '+' '-'
%left <bytes> '*' '/' DIV '%' MOD
%left <bytes> '^'
%right <bytes> '~' UNARY
%left <bytes> COLLATE
%right <bytes> BINARY UNDERSCORE_BINARY UNDERSCORE_UTF8MB4
%right <bytes> INTERVAL
%nonassoc <bytes> '.'

// There is no need to define precedence for the JSON
// operators because the syntax is restricted enough that
// they don't cause conflicts.
%token <empty> JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP

// DDL Tokens
%token <bytes> CREATE ALTER DROP RENAME ANALYZE ADD FLUSH
%token <bytes> SCHEMA TABLE INDEX VIEW TO IGNORE IF UNIQUE PRIMARY COLUMN SPATIAL FULLTEXT KEY_BLOCK_SIZE CHECK
%token <bytes> ACTION CASCADE CONSTRAINT FOREIGN NO REFERENCES RESTRICT
%token <bytes> SHOW DESCRIBE EXPLAIN DATE ESCAPE REPAIR OPTIMIZE TRUNCATE
%token <bytes> MAXVALUE PARTITION REORGANIZE LESS THAN PROCEDURE TRIGGER
%token <bytes> VINDEX VINDEXES
%token <bytes> STATUS VARIABLES WARNINGS
%token <bytes> SEQUENCE

// Transaction Tokens
%token <bytes> BEGIN START TRANSACTION COMMIT ROLLBACK

// Type Tokens
%token <bytes> BIT TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INTNUM
%token <bytes> REAL DOUBLE FLOAT_TYPE DECIMAL NUMERIC
%token <bytes> TIME TIMESTAMP DATETIME YEAR
%token <bytes> CHAR VARCHAR BOOL CHARACTER VARBINARY NCHAR 
%token <bytes> TEXT TINYTEXT MEDIUMTEXT LONGTEXT
%token <bytes> BLOB TINYBLOB MEDIUMBLOB LONGBLOB JSON ENUM
%token <bytes> GEOMETRY POINT LINESTRING POLYGON GEOMETRYCOLLECTION MULTIPOINT MULTILINESTRING MULTIPOLYGON

// Type Modifiers
%token <bytes> NULLX AUTO_INCREMENT APPROXNUM SIGNED UNSIGNED ZEROFILL

// Supported SHOW tokens
%token <bytes> COLLATION DATABASES TABLES VITESS_METADATA VSCHEMA FULL PROCESSLIST COLUMNS FIELDS ENGINES PLUGINS

// SET tokens
%token <bytes> NAMES CHARSET GLOBAL SESSION ISOLATION LEVEL READ WRITE ONLY REPEATABLE COMMITTED UNCOMMITTED SERIALIZABLE

// Functions
%token <bytes> CURRENT_TIMESTAMP DATABASE CURRENT_DATE
%token <bytes> CURRENT_TIME LOCALTIME LOCALTIMESTAMP
%token <bytes> UTC_DATE UTC_TIME UTC_TIMESTAMP
%token <bytes> REPLACE
%token <bytes> CONVERT CAST
%token <bytes> SUBSTR SUBSTRING
%token <bytes> GROUP_CONCAT SEPARATOR
%token <bytes> TIMESTAMPADD TIMESTAMPDIFF

// Match
%token <bytes> MATCH AGAINST BOOLEAN LANGUAGE WITH QUERY EXPANSION

// MySQL reserved words that are unused by this grammar will map to this token.
%token <bytes> UNUSED ARRAY CUME_DIST DESCRIPTION DENSE_RANK EMPTY EXCEPT FIRST_VALUE GROUPING GROUPS JSON_TABLE LAG LAST_VALUE LATERAL LEAD MEMBER
%token <bytes> NTH_VALUE NTILE OF OVER PERCENT_RANK RANK RECURSIVE ROW_NUMBER SYSTEM WINDOW
%token <bytes> ACTIVE ADMIN BUCKETS CLONE COMPONENT DEFINITION ENFORCED EXCLUDE FOLLOWING GEOMCOLLECTION GET_MASTER_PUBLIC_KEY HISTOGRAM HISTORY
%token <bytes> INACTIVE INVISIBLE LOCKED MASTER_COMPRESSION_ALGORITHMS MASTER_PUBLIC_KEY_PATH MASTER_TLS_CIPHERSUITES MASTER_ZSTD_COMPRESSION_LEVEL
%token <bytes> NESTED NETWORK_NAMESPACE NOWAIT NULLS OJ OLD OPTIONAL ORDINALITY ORGANIZATION OTHERS PATH PERSIST PERSIST_ONLY PRECEDING PRIVILEGE_CHECKS_USER PROCESS
%token <bytes> RANDOM REFERENCE REQUIRE_ROW_FORMAT RESOURCE RESPECT RESTART RETAIN REUSE ROLE SECONDARY SECONDARY_ENGINE SECONDARY_LOAD SECONDARY_UNLOAD SKIP SRID
%token <bytes> THREAD_PRIORITY TIES UNBOUNDED VCPU VISIBLE

%type <statement> command
%type <selStmt> select_statement base_select union_lhs union_rhs
%type <statement> stream_statement insert_statement update_statement delete_statement set_statement
%type <statement> create_statement alter_statement rename_statement drop_statement truncate_statement flush_statement
%type <ddl> create_table_prefix rename_list
%type <statement> analyze_statement show_statement use_statement other_statement
%type <statement> begin_statement commit_statement rollback_statement
%type <bytes2> comment_opt comment_list
%type <str> union_op insert_or_replace
%type <str> distinct_opt straight_join_opt cache_opt match_option separator_opt
%type <expr> like_escape_opt
%type <selectExprs> select_expression_list select_expression_list_opt
%type <selectExpr> select_expression
%type <expr> expression
%type <tableExprs> from_opt table_references
%type <tableExpr> table_reference table_factor join_table
%type <joinCondition> join_condition join_condition_opt on_expression_opt
%type <tableNames> table_name_list delete_table_list
%type <str> inner_join outer_join straight_join natural_join
%type <tableName> table_name into_table_name delete_table_name
%type <aliasedTableName> aliased_table_name
%type <indexHints> index_hint_list
%type <expr> where_expression_opt
%type <expr> condition
%type <boolVal> boolean_value
%type <str> compare
%type <ins> insert_data
%type <expr> value value_expression num_val
%type <expr> function_call_keyword function_call_nonkeyword function_call_generic function_call_conflict func_datetime_precision
%type <str> is_suffix
%type <colTuple> col_tuple
%type <exprs> expression_list
%type <values> tuple_list
%type <valTuple> row_tuple tuple_or_empty
%type <expr> tuple_expression
%type <subquery> subquery
%type <colName> column_name
%type <whens> when_expression_list
%type <when> when_expression
%type <expr> expression_opt else_expression_opt
%type <exprs> group_by_opt
%type <expr> having_opt
%type <orderBy> order_by_opt order_list
%type <order> order
%type <str> asc_desc_opt
%type <limit> limit_opt
%type <str> lock_opt
%type <columns> ins_column_list column_list
%type <partitions> opt_partition_clause partition_list
%type <updateExprs> on_dup_opt
%type <updateExprs> update_list
%type <setExprs> set_list transaction_chars
%type <bytes> charset_or_character_set
%type <updateExpr> update_expression
%type <setExpr> set_expression transaction_char
%type <str> isolation_level
%type <bytes> for_from
%type <str> ignore_opt default_opt
%type <str> full_opt from_database_opt tables_or_processlist columns_or_fields
%type <showFilter> like_or_where_opt like_opt
%type <byt> exists_opt
%type <empty> not_exists_opt non_add_drop_or_rename_operation to_opt index_opt constraint_opt
%type <bytes> reserved_keyword non_reserved_keyword
%type <colIdent> sql_id reserved_sql_id col_alias as_ci_opt using_opt
%type <expr> charset_value
%type <tableIdent> table_id reserved_table_id table_alias as_opt_id
%type <empty> as_opt
%type <empty> skip_to_end ddl_skip_to_end
%type <str> charset
%type <str> set_session_or_global show_session_or_global
%type <convertType> convert_type
%type <columnType> column_type
%type <columnType> int_type decimal_type numeric_type time_type char_type spatial_type
%type <sqlVal> length_opt column_comment_opt
%type <optVal> column_default_opt on_update_opt
%type <str> charset_opt collate_opt
%type <boolVal> unsigned_opt zero_fill_opt
%type <LengthScaleOption> float_length_opt decimal_length_opt
%type <boolVal> null_opt auto_increment_opt
%type <colKeyOpt> column_key_opt
%type <strs> enum_values
%type <columnDefinition> column_definition
%type <indexDefinition> index_definition
%type <constraintDefinition> constraint_definition
%type <str> index_or_key
%type <str> name_opt
%type <str> equal_opt
%type <TableSpec> table_spec table_column_list
%type <optLike> create_like
%type <str> table_option_list table_option table_opt_value
%type <indexInfo> index_info
%type <indexColumn> index_column
%type <indexColumns> index_column_list
%type <indexOption> index_option
%type <indexOptions> index_option_list
%type <constraintInfo> constraint_info
%type <partDefs> partition_definitions
%type <partDef> partition_definition
%type <partSpec> partition_operation
%type <vindexParam> vindex_param
%type <vindexParams> vindex_param_list vindex_params_opt
%type <colIdent> id_or_var vindex_type vindex_type_opt
%type <bytes> alter_object_type
%type <ReferenceAction> fk_reference_action fk_on_delete fk_on_update

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
| show_statement
| use_statement
| begin_statement
| commit_statement
| rollback_statement
| other_statement
| flush_statement
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

select_statement:
  base_select order_by_opt limit_opt lock_opt
  {
    sel := $1.(*Select)
    sel.OrderBy = $2
    sel.Limit = $3
    sel.Lock = $4
    $$ = sel
  }
| union_lhs union_op union_rhs order_by_opt limit_opt lock_opt
  {
    $$ = &Union{Type: $2, Left: $1, Right: $3, OrderBy: $4, Limit: $5, Lock: $6}
  }
| SELECT comment_opt cache_opt NEXT num_val for_from table_name
  {
    $$ = &Select{Comments: Comments($2), Cache: $3, SelectExprs: SelectExprs{Nextval{Expr: $5}}, From: TableExprs{&AliasedTableExpr{Expr: $7}}}
  }

stream_statement:
  STREAM comment_opt select_expression FROM table_name
  {
    $$ = &Stream{Comments: Comments($2), SelectExpr: $3, Table: $5}
  }

// base_select is an unparenthesized SELECT with no order by clause or beyond.
base_select:
  SELECT comment_opt cache_opt distinct_opt straight_join_opt select_expression_list from_opt where_expression_opt group_by_opt having_opt
  {
    $$ = &Select{Comments: Comments($2), Cache: $3, Distinct: $4, Hints: $5, SelectExprs: $6, From: $7, Where: NewWhere(WhereStr, $8), GroupBy: GroupBy($9), Having: NewWhere(HavingStr, $10)}
  }

union_lhs:
  select_statement
  {
    $$ = $1
  }
| openb select_statement closeb
  {
    $$ = &ParenSelect{Select: $2}
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
    $$ = InsertStr
  }
| REPLACE
  {
    $$ = ReplaceStr
  }

update_statement:
  UPDATE comment_opt ignore_opt table_references SET update_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{Comments: Comments($2), Ignore: $3, TableExprs: $4, Exprs: $6, Where: NewWhere(WhereStr, $7), OrderBy: $8, Limit: $9}
  }

delete_statement:
  DELETE comment_opt FROM table_name opt_partition_clause where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{Comments: Comments($2), TableExprs:  TableExprs{&AliasedTableExpr{Expr:$4}}, Partitions: $5, Where: NewWhere(WhereStr, $6), OrderBy: $7, Limit: $8}
  }
| DELETE comment_opt FROM table_name_list USING table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Targets: $4, TableExprs: $6, Where: NewWhere(WhereStr, $7)}
  }
| DELETE comment_opt table_name_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Targets: $3, TableExprs: $5, Where: NewWhere(WhereStr, $6)}
  }
|DELETE comment_opt delete_table_list from_or_using table_references where_expression_opt
  {
    $$ = &Delete{Comments: Comments($2), Targets: $3, TableExprs: $5, Where: NewWhere(WhereStr, $6)}
  }

from_or_using:
  FROM {}
| USING {}

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
| SET comment_opt set_session_or_global set_list
  {
    $$ = &Set{Comments: Comments($2), Scope: $3, Exprs: $4}
  }
| SET comment_opt set_session_or_global TRANSACTION transaction_chars
  {
    $$ = &Set{Comments: Comments($2), Scope: $3, Exprs: $5}
  }
| SET comment_opt TRANSACTION transaction_chars
  {
    $$ = &Set{Comments: Comments($2), Exprs: $4}
  }

transaction_chars:
  transaction_char
  {
    $$ = SetExprs{$1}
  }
| transaction_chars ',' transaction_char
  {
    $$ = append($$, $3)
  }

transaction_char:
  ISOLATION LEVEL isolation_level
  {
    $$ = &SetExpr{Name: NewColIdent(TransactionStr), Expr: NewStrVal([]byte($3))}
  }
| READ WRITE
  {
    $$ = &SetExpr{Name: NewColIdent(TransactionStr), Expr: NewStrVal([]byte(TxReadWrite))}
  }
| READ ONLY
  {
    $$ = &SetExpr{Name: NewColIdent(TransactionStr), Expr: NewStrVal([]byte(TxReadOnly))}
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

set_session_or_global:
  SESSION
  {
    $$ = SessionStr
  }
| GLOBAL
  {
    $$ = GlobalStr
  }

create_statement:
  create_table_prefix table_spec
  {
    $1.TableSpec = $2
    $$ = $1
  }
| create_table_prefix create_like
  {
    // Create table [name] like [name]
    $1.OptLike = $2
    $$ = $1
  }
| CREATE constraint_opt INDEX id_or_var using_opt ON table_name ddl_skip_to_end
  {
    // Change this to an alter statement
    $$ = &DDL{Action: AlterStr, Table: $7}
  }
| CREATE VIEW table_name ddl_skip_to_end
  {
    $$ = &DDL{Action: CreateStr, Table: $3.ToViewName()}
  }
| CREATE OR REPLACE VIEW table_name ddl_skip_to_end
  {
    $$ = &DDL{Action: CreateStr, Table: $5.ToViewName()}
  }
| CREATE DATABASE not_exists_opt id_or_var ddl_skip_to_end
  {
    $$ = &DBDDL{Action: CreateStr, DBName: string($4.String())}
  }
| CREATE SCHEMA not_exists_opt id_or_var ddl_skip_to_end
  {
    $$ = &DBDDL{Action: CreateStr, DBName: string($4.String())}
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
  CREATE TABLE not_exists_opt table_name
  {
    $$ = &DDL{Action: CreateStr, Table: $4}
    setDDL(yylex, $$)
  }

table_spec:
  '(' table_column_list ')' table_option_list
  {
    $$ = $2
    $$.Options = $4
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

table_column_list:
  column_definition
  {
    $$ = &TableSpec{}
    $$.AddColumn($1)
  }
| table_column_list ',' column_definition
  {
    $$.AddColumn($3)
  }
| table_column_list ',' index_definition
  {
    $$.AddIndex($3)
  }
| table_column_list ',' constraint_definition
  {
    $$.AddConstraint($3)
  }

column_definition:
  id_or_var column_type null_opt column_default_opt on_update_opt auto_increment_opt column_key_opt column_comment_opt
  {
    $2.NotNull = $3
    $2.Default = $4
    $2.OnUpdate = $5
    $2.Autoincrement = $6
    $2.KeyOpt = $7
    $2.Comment = $8
    $$ = &ColumnDefinition{Name: $1, Type: $2}
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
| YEAR
  {
    $$ = ColumnType{Type: string($1)}
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
    $$ = append($$, "'" + string($1) + "'")
  }
| enum_values ',' STRING
  {
    $$ = append($1, "'" + string($3) + "'")
  }

length_opt:
  {
    $$ = nil
  }
| '(' INTEGRAL ')'
  {
    $$ = NewIntVal($2)
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

unsigned_opt:
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

// Null opt returns false to mean NULL (i.e. the default) and true for NOT NULL
null_opt:
  {
    $$ = BoolVal(false)
  }
| NULL
  {
    $$ = BoolVal(false)
  }
| NOT NULL
  {
    $$ = BoolVal(true)
  }

column_default_opt:
  {
    $$ = nil
  }
| DEFAULT value_expression
  {
    $$ = $2
  }

on_update_opt:
  {
    $$ = nil
  }
| ON UPDATE function_call_nonkeyword
{
  $$ = $3
}

auto_increment_opt:
  {
    $$ = BoolVal(false)
  }
| AUTO_INCREMENT
  {
    $$ = BoolVal(true)
  }

charset_opt:
  {
    $$ = ""
  }
| CHARACTER SET id_or_var
  {
    $$ = string($3.String())
  }
| CHARACTER SET BINARY
  {
    $$ = string($3)
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
    $$ = string($2)
  }

column_key_opt:
  {
    $$ = colKeyNone
  }
| PRIMARY KEY
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

column_comment_opt:
  {
    $$ = nil
  }
| COMMENT_KEYWORD STRING
  {
    $$ = NewStrVal($2)
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
  USING id_or_var
  {
    $$ = &IndexOption{Name: string($1), Using: string($2.String())}
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
  PRIMARY KEY
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
  }
| SPATIAL index_or_key name_opt
  {
    $$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent($3), Spatial: true, Unique: false}
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
  sql_id length_opt
  {
      $$ = &IndexColumn{Column: $1, Length: $2}
  }

constraint_definition:
  CONSTRAINT id_or_var constraint_info
  {
    $$ = &ConstraintDefinition{Name: string($2.String()), Details: $3}
  }
|  constraint_info
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

table_option_list:
  {
    $$ = ""
  }
| table_option
  {
    $$ = " " + string($1)
  }
| table_option_list ',' table_option
  {
    $$ = string($1) + ", " + string($3)
  }

// rather than explicitly parsing the various keywords for table options,
// just accept any number of keywords, IDs, strings, numbers, and '='
table_option:
  table_opt_value
  {
    $$ = $1
  }
| table_option table_opt_value
  {
    $$ = $1 + " " + $2
  }
| table_option '=' table_opt_value
  {
    $$ = $1 + "=" + $3
  }

table_opt_value:
  reserved_sql_id
  {
    $$ = $1.String()
  }
| STRING
  {
    $$ = "'" + string($1) + "'"
  }
| INTEGRAL
  {
    $$ = string($1)
  }

alter_statement:
  ALTER ignore_opt TABLE table_name non_add_drop_or_rename_operation skip_to_end
  {
    $$ = &DDL{Action: AlterStr, Table: $4}
  }
| ALTER ignore_opt TABLE table_name ADD alter_object_type skip_to_end
  {
    $$ = &DDL{Action: AlterStr, Table: $4}
  }
| ALTER ignore_opt TABLE table_name DROP alter_object_type skip_to_end
  {
    $$ = &DDL{Action: AlterStr, Table: $4}
  }
| ALTER ignore_opt TABLE table_name RENAME to_opt table_name
  {
    // Change this to a rename statement
    $$ = &DDL{Action: RenameStr, FromTables: TableNames{$4}, ToTables: TableNames{$7}}
  }
| ALTER ignore_opt TABLE table_name RENAME index_opt skip_to_end
  {
    // Rename an index can just be an alter
    $$ = &DDL{Action: AlterStr, Table: $4}
  }
| ALTER VIEW table_name ddl_skip_to_end
  {
    $$ = &DDL{Action: AlterStr, Table: $3.ToViewName()}
  }
| ALTER ignore_opt TABLE table_name partition_operation
  {
    $$ = &DDL{Action: AlterStr, Table: $4, PartitionSpec: $5}
  }
| ALTER DATABASE id_or_var ddl_skip_to_end
  {
    $$ = &DBDDL{Action: AlterStr, DBName: string($3.String())}
  }
| ALTER SCHEMA id_or_var ddl_skip_to_end
  {
    $$ = &DBDDL{Action: AlterStr, DBName: string($3.String())}
  }
| ALTER VSCHEMA CREATE VINDEX table_name vindex_type_opt vindex_params_opt
  {
    $$ = &DDL{
        Action: CreateVindexStr,
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
    $$ = &DDL{
        Action: DropVindexStr,
        Table: $5,
        VindexSpec: &VindexSpec{
          Name: NewColIdent($5.Name.String()),
        },
      }
  }
| ALTER VSCHEMA ADD TABLE table_name
  {
    $$ = &DDL{Action: AddVschemaTableStr, Table: $5}
  }
| ALTER VSCHEMA DROP TABLE table_name
  {
    $$ = &DDL{Action: DropVschemaTableStr, Table: $5}
  }
| ALTER VSCHEMA ON table_name ADD VINDEX sql_id '(' column_list ')' vindex_type_opt vindex_params_opt
  {
    $$ = &DDL{
        Action: AddColVindexStr,
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
    $$ = &DDL{
        Action: DropColVindexStr,
        Table: $4,
        VindexSpec: &VindexSpec{
            Name: $7,
        },
      }
  }
| ALTER VSCHEMA ADD SEQUENCE table_name
  {
    $$ = &DDL{Action: AddSequenceStr, Table: $5}
  }
| ALTER VSCHEMA ON table_name ADD AUTO_INCREMENT sql_id USING table_name
  {
    $$ = &DDL{
        Action: AddAutoIncStr,
        Table: $4,
        AutoIncSpec: &AutoIncSpec{
            Column: $7,
            Sequence: $9,
        },
    }
  }

alter_object_type:
  CHECK
| COLUMN
| CONSTRAINT
| FOREIGN
| FULLTEXT
| ID
| AT_ID
| AT_AT_ID
| INDEX
| KEY
| PRIMARY
| SPATIAL
| PARTITION
| UNIQUE

partition_operation:
  REORGANIZE PARTITION sql_id INTO openb partition_definitions closeb
  {
    $$ = &PartitionSpec{Action: ReorganizeStr, Name: $3, Definitions: $6}
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
    $$ = $3
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

drop_statement:
  DROP TABLE exists_opt table_name_list
  {
    var exists bool
    if $3 != 0 {
      exists = true
    }
    $$ = &DDL{Action: DropStr, FromTables: $4, IfExists: exists}
  }
| DROP INDEX id_or_var ON table_name ddl_skip_to_end
  {
    // Change this to an alter statement
    $$ = &DDL{Action: AlterStr, Table: $5}
  }
| DROP VIEW exists_opt table_name ddl_skip_to_end
  {
    var exists bool
        if $3 != 0 {
          exists = true
        }
    $$ = &DDL{Action: DropStr, FromTables: TableNames{$4.ToViewName()}, IfExists: exists}
  }
| DROP DATABASE exists_opt id_or_var
  {
    $$ = &DBDDL{Action: DropStr, DBName: string($4.String())}
  }
| DROP SCHEMA exists_opt id_or_var
  {
    $$ = &DBDDL{Action: DropStr, DBName: string($4.String())}
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
  ANALYZE TABLE table_name
  {
    $$ = &OtherRead{}
  }

show_statement:
  SHOW BINARY id_or_var ddl_skip_to_end /* SHOW BINARY LOGS */
  {
    $$ = &Show{Type: string($2) + " " + string($3.String())}
  }
/* SHOW CHARACTER SET and SHOW CHARSET are equivalent */
| SHOW CHARACTER SET like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{Filter: $4}
    $$ = &Show{Type: CharsetStr, ShowTablesOpt: showTablesOpt}
  }
| SHOW CHARSET like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{Filter: $3}
    $$ = &Show{Type: string($2), ShowTablesOpt: showTablesOpt}
  }
| SHOW CREATE DATABASE ddl_skip_to_end
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
/* Rule to handle SHOW CREATE EVENT, SHOW CREATE FUNCTION, etc. */
| SHOW CREATE id_or_var ddl_skip_to_end
  {
    $$ = &Show{Type: string($2) + " " + string($3.String())}
  }
| SHOW CREATE PROCEDURE ddl_skip_to_end
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW CREATE TABLE table_name
  {
    $$ = &Show{Type: string($2) + " " + string($3), Table: $4}
  }
| SHOW CREATE TRIGGER ddl_skip_to_end
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW CREATE VIEW ddl_skip_to_end
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW DATABASES ddl_skip_to_end
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW ENGINES
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW INDEX FROM table_name from_database_opt like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{DbName:$5, Filter:$6}
    $$ = &Show{Type: string($2), ShowTablesOpt: showTablesOpt, OnTable: $4}
  }
| SHOW KEYS FROM table_name from_database_opt like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{DbName:$5, Filter:$6}
    $$ = &Show{Type: string($2), ShowTablesOpt: showTablesOpt, OnTable: $4}
  }
| SHOW PLUGINS
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW PROCEDURE ddl_skip_to_end
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW show_session_or_global STATUS ddl_skip_to_end
  {
    $$ = &Show{Scope: $2, Type: string($3)}
  }
| SHOW TABLE ddl_skip_to_end
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW full_opt columns_or_fields FROM table_name from_database_opt like_or_where_opt
  {
    showTablesOpt := &ShowTablesOpt{Full:$2, DbName:$6, Filter:$7}
    $$ = &Show{Type: string($3), ShowTablesOpt: showTablesOpt, OnTable: $5}
  }
| SHOW full_opt tables_or_processlist from_database_opt like_or_where_opt
  {
    // this is ugly, but I couldn't find a better way for now
    if $3 == "processlist" {
      $$ = &Show{Type: $3}
    } else {
    showTablesOpt := &ShowTablesOpt{Full:$2, DbName:$4, Filter:$5}
      $$ = &Show{Type: $3, ShowTablesOpt: showTablesOpt}
    }
  }
| SHOW show_session_or_global VARIABLES ddl_skip_to_end
  {
    $$ = &Show{Scope: $2, Type: string($3)}
  }
| SHOW COLLATION
  {
    $$ = &Show{Type: string($2)}
  }
| SHOW COLLATION WHERE expression
  {
    // Cannot dereference $4 directly, or else the parser stackcannot be pooled. See yyParsePooled
    showCollationFilterOpt := $4
    $$ = &Show{Type: string($2), ShowCollationFilterOpt: &showCollationFilterOpt}
  }
| SHOW VITESS_METADATA VARIABLES like_opt
  {
    showTablesOpt := &ShowTablesOpt{Filter: $4}
    $$ = &Show{Scope: string($2), Type: string($3), ShowTablesOpt: showTablesOpt}
  }
| SHOW VSCHEMA TABLES
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW VSCHEMA VINDEXES
  {
    $$ = &Show{Type: string($2) + " " + string($3)}
  }
| SHOW VSCHEMA VINDEXES ON table_name
  {
    $$ = &Show{Type: string($2) + " " + string($3), OnTable: $5}
  }
| SHOW WARNINGS
  {
    $$ = &Show{Type: string($2)}
  }
/*
 * Catch-all for show statements without vitess keywords:
 *
 *  SHOW BINARY LOGS
 *  SHOW INVALID
 *  SHOW VITESS_KEYSPACES
 *  SHOW VITESS_TABLETS
 *  SHOW VITESS_SHARDS
 *  SHOW VITESS_TARGET
 */
| SHOW id_or_var ddl_skip_to_end
  {
    $$ = &Show{Type: string($2.String())}
  }

tables_or_processlist:
  TABLES
  {
    $$ = string($1)
  }
| PROCESSLIST
  {
    $$ = string($1)
  }

full_opt:
  /* empty */
  {
    $$ = ""
  }
| FULL
  {
    $$ = "full "
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

other_statement:
  DESC skip_to_end
  {
    $$ = &OtherRead{}
  }
| DESCRIBE skip_to_end
  {
    $$ = &OtherRead{}
  }
| EXPLAIN skip_to_end
  {
    $$ = &OtherRead{}
  }
| REPAIR skip_to_end
  {
    $$ = &OtherAdmin{}
  }
| OPTIMIZE skip_to_end
  {
    $$ = &OtherAdmin{}
  }
| LOCK TABLES skip_to_end
  {
    $$ = &OtherAdmin{}
  }
| UNLOCK TABLES skip_to_end
  {
    $$ = &OtherAdmin{}
  }

flush_statement:
  FLUSH skip_to_end
  {
    $$ = &DDL{Action: FlushStr}
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
    $$ = ""
  }
| DISTINCT
  {
    $$ = DistinctStr
  }
| DISTINCTROW
  {
    $$ = DistinctStr
  }

straight_join_opt:
  {
    $$ = ""
  }
| STRAIGHT_JOIN
  {
    $$ = StraightJoinHint
  }

select_expression_list_opt:
  {
    $$ = nil
  }
| select_expression_list
  {
    $$ = $1
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
| subquery as_opt table_id
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $3}
  }
| subquery
  {
    // missed alias for subquery
    yylex.Error("Every derived table must have its own alias")
    return 1
  }
| openb table_references closeb
  {
    $$ = &ParenTableExpr{Exprs: $2}
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
    $$ = &IndexHints{Type: UseStr, Indexes: $4}
  }
| USE INDEX openb closeb
  {
    $$ = &IndexHints{Type: UseStr}
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
  openb select_statement closeb
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
    $$ = &CollateExpr{Expr: $1, Charset: $3}
  }
| BINARY value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: BinaryStr, Expr: $2}
  }
| UNDERSCORE_BINARY value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: UBinaryStr, Expr: $2}
  }
| UNDERSCORE_UTF8MB4 value_expression %prec UNARY
  {
    $$ = &UnaryExpr{Operator: Utf8mb4Str, Expr: $2}
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
    $$ = &SubstrExpr{StrVal: NewStrVal($3), From: $5, To: $7}
  }
| SUBSTRING openb STRING FROM value_expression FOR value_expression closeb
  {
    $$ = &SubstrExpr{StrVal: NewStrVal($3), From: $5, To: $7}
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

/*
  Function calls using non reserved keywords but with special syntax forms.
  Dedicated grammar rules are needed because of the special syntax
*/
function_call_nonkeyword:
  CURRENT_TIMESTAMP func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_timestamp")}
  }
| UTC_TIMESTAMP func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_timestamp")}
  }
| UTC_TIME func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_time")}
  }
/* doesn't support fsp */
| UTC_DATE func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("utc_date")}
  }
  // now
| LOCALTIME func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("localtime")}
  }
  // now
| LOCALTIMESTAMP func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("localtimestamp")}
  }
  // curdate
/* doesn't support fsp */
| CURRENT_DATE func_datetime_opt
  {
    $$ = &FuncExpr{Name:NewColIdent("current_date")}
  }
  // curtime
| CURRENT_TIME func_datetime_opt
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

func_datetime_opt:
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
  id_or_var
{
    $$ = string($1.String())
}
| STRING
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
    $$ = &ConvertType{Type: string($1), Length: $2, Charset: $3, Operator: CharacterSetStr}
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
    $$ = " separator '"+string($2)+"'"
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
    $$ = NewStrVal($1)
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
  sql_id
  {
    // TODO(sougou): Deprecate this construct.
    if $1.Lowered() != "value" {
      yylex.Error("expecting value after next")
      return 1
    }
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

lock_opt:
  {
    $$ = ""
  }
| FOR UPDATE
  {
    $$ = ForUpdateStr
  }
| LOCK IN SHARE MODE
  {
    $$ = ShareModeStr
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
| openb select_statement closeb
  {
    // Drop the redundant parenthesis.
    $$ = &Insert{Rows: $2}
  }
| openb ins_column_list closeb VALUES tuple_list
  {
    $$ = &Insert{Columns: $2, Rows: $5}
  }
| openb ins_column_list closeb select_statement
  {
    $$ = &Insert{Columns: $2, Rows: $4}
  }
| openb ins_column_list closeb openb select_statement closeb
  {
    // Drop the redundant parenthesis.
    $$ = &Insert{Columns: $2, Rows: $5}
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
      $$ = &ParenExpr{$1[0]}
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
    $$ = &SetExpr{Name: $1, Expr: NewStrVal([]byte("on"))}
  }
| reserved_sql_id '=' OFF
  {
    $$ = &SetExpr{Name: $1, Expr: NewStrVal([]byte("off"))}
  }
| reserved_sql_id '=' expression
  {
    $$ = &SetExpr{Name: $1, Expr: $3}
  }
| charset_or_character_set charset_value collate_opt
  {
    $$ = &SetExpr{Name: NewColIdent(string($1)), Expr: $2}
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

exists_opt:
  { $$ = 0 }
| IF EXISTS
  { $$ = 1 }

not_exists_opt:
  { $$ = struct{}{} }
| IF NOT EXISTS
  { $$ = struct{}{} }

ignore_opt:
  { $$ = "" }
| IGNORE
  { $$ = IgnoreStr }

non_add_drop_or_rename_operation:
  ALTER
  { $$ = struct{}{} }
| AUTO_INCREMENT
  { $$ = struct{}{} }
| CHARACTER
  { $$ = struct{}{} }
| COMMENT_KEYWORD
  { $$ = struct{}{} }
| DEFAULT
  { $$ = struct{}{} }
| ORDER
  { $$ = struct{}{} }
| CONVERT
  { $$ = struct{}{} }
| PARTITION
  { $$ = struct{}{} }
| UNUSED
  { $$ = struct{}{} }
| id_or_var
  { $$ = struct{}{} }

to_opt:
  { $$ = struct{}{} }
| TO
  { $$ = struct{}{} }
| AS
  { $$ = struct{}{} }

index_opt:
  INDEX
  { $$ = struct{}{} }
| KEY
  { $$ = struct{}{} }

constraint_opt:
  { $$ = struct{}{} }
| UNIQUE
  { $$ = struct{}{} }
| sql_id
  { $$ = struct{}{} }

using_opt:
  { $$ = ColIdent{} }
| USING sql_id
  { $$ = $2 }

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
| AUTO_INCREMENT
| BETWEEN
| BINARY
| BY
| CASE
| COLLATE
| CONVERT
| CREATE
| CROSS
| CUME_DIST
| CURRENT_DATE
| CURRENT_TIME
| CURRENT_TIMESTAMP
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
| FROM
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
| MEMBER
| MATCH
| MAXVALUE
| MOD
| NATURAL
| NEXT // next should be doable as non-reserved, but is not due to the special `select next num_val` query that vitess supports
| NOT
| NTH_VALUE
| NTILE
| NULL
| OF
| OFF
| ON
| OR
| ORDER
| OUTER
| OVER
| PERCENT_RANK
| RANK
| RECURSIVE
| REGEXP
| RENAME
| REPLACE
| RIGHT
| ROW_NUMBER
| SCHEMA
| SELECT
| SEPARATOR
| SET
| SHOW
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
| WHEN
| WHERE
| WINDOW

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
| BEGIN
| BIGINT
| BIT
| BLOB
| BOOL
| BOOLEAN
| BUCKETS
| CASCADE
| CHAR
| CHARACTER
| CHARSET
| CHECK
| CLONE
| COLLATION
| COLUMNS
| COMMENT_KEYWORD
| COMMIT
| COMMITTED
| COMPONENT
| DATE
| DATETIME
| DECIMAL
| DEFINITION
| DESCRIPTION
| DOUBLE
| DUPLICATE
| ENFORCED
| ENGINES
| ENUM
| EXCLUDE
| EXPANSION
| FLOAT_TYPE
| FIELDS
| FLUSH
| FOLLOWING
| FOREIGN
| FULLTEXT
| GEOMCOLLECTION
| GEOMETRY
| GEOMETRYCOLLECTION
| GET_MASTER_PUBLIC_KEY
| GLOBAL
| HISTOGRAM
| HISTORY
| INACTIVE
| INT
| INTEGER
| INVISIBLE
| ISOLATION
| JSON
| KEY_BLOCK_SIZE
| KEYS
| LANGUAGE
| LAST_INSERT_ID
| LESS
| LEVEL
| LINESTRING
| LOCKED
| LONGBLOB
| LONGTEXT
| MASTER_COMPRESSION_ALGORITHMS
| MASTER_PUBLIC_KEY_PATH
| MASTER_TLS_CIPHERSUITES
| MASTER_ZSTD_COMPRESSION_LEVEL
| MEDIUMBLOB
| MEDIUMINT
| MEDIUMTEXT
| MODE
| MULTILINESTRING
| MULTIPOINT
| MULTIPOLYGON
| NAMES
| NCHAR
| NESTED
| NETWORK_NAMESPACE
| NOWAIT
| NO
| NULLS
| NUMERIC
| OFFSET
| OJ
| OLD
| OPTIONAL
| ORDINALITY
| ORGANIZATION
| ONLY
| OPTIMIZE
| OTHERS
| PARTITION
| PATH
| PERSIST
| PERSIST_ONLY
| PRECEDING
| PRIVILEGE_CHECKS_USER
| PROCESS
| PLUGINS
| POINT
| POLYGON
| PRIMARY
| PROCEDURE
| QUERY
| RANDOM
| READ
| REAL
| REFERENCE
| REFERENCES
| REORGANIZE
| REPAIR
| REPEATABLE
| RESTRICT
| REQUIRE_ROW_FORMAT
| RESOURCE
| RESPECT
| RESTART
| RETAIN
| REUSE
| ROLE
| ROLLBACK
| SECONDARY
| SECONDARY_ENGINE
| SECONDARY_LOAD
| SECONDARY_UNLOAD
| SEQUENCE
| SESSION
| SERIALIZABLE
| SHARE
| SIGNED
| SKIP
| SMALLINT
| SPATIAL
| SRID
| START
| STATUS
| TABLES
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
| TRIGGER
| TRUNCATE
| UNBOUNDED
| UNCOMMITTED
| UNSIGNED
| UNUSED
| VARBINARY
| VARCHAR
| VARIABLES
| VCPU
| VIEW
| VINDEX
| VINDEXES
| VISIBLE
| VITESS_METADATA
| VSCHEMA
| WARNINGS
| WITH
| WRITE
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
