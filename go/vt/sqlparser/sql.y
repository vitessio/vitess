// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

%{
package sqlparser

import "bytes"

func SetParseTree(yylex interface{}, stmt Statement) {
  tn := yylex.(*Tokenizer)
  tn.ParseTree = stmt
}

func SetAllowComments(yylex interface{}, allow bool) {
  tn := yylex.(*Tokenizer)
  tn.AllowComments = allow
}

func ForceEOF(yylex interface{}) {
  tn := yylex.(*Tokenizer)
  tn.ForceEOF = true
}

var (
  SHARE = []byte("share")
  MODE =  []byte("mode")
)

%}

%union {
  node        *Node
  statement   Statement
  comments    Comments
  byt         byte
  bytes       []byte
  str         string
  distinct    Distinct
  selectExprs SelectExprs
  selectExpr  SelectExpr
  columns     Columns
  colName     *ColName
  tableExprs  TableExprs
  tableExpr   TableExpr
  tableName   *TableName
  indexHints  *IndexHints
  names       [][]byte
  where       *Where
  expr        Expr
  boolExpr    BoolExpr
  valExpr     ValExpr
  valExprs    ValExprs
  values      Values
  subquery    *Subquery
  groupBy     GroupBy
  sqlNode     SQLNode
}

%token <node> SELECT INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT COMMENT FOR
%token <node> ALL DISTINCT AS EXISTS IN IS LIKE BETWEEN NULL ASC DESC VALUES INTO DUPLICATE KEY DEFAULT SET LOCK
%token <node> ID STRING NUMBER VALUE_ARG
%token <node> LE GE NE NULL_SAFE_EQUAL
%token <node> LEX_ERROR
%token <node> '(' '=' '<' '>' '~'

%left <node> UNION MINUS EXCEPT INTERSECT
%left <node> ','
%left <node> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <node> ON
%left <node> AND OR
%right <node> NOT
%left <node> '&' '|' '^'
%left <node> '+' '-'
%left <node> '*' '/' '%'
%nonassoc <node> '.'
%left <node> UNARY
%right <node> CASE, WHEN, THEN, ELSE
%left <node> END

// DDL Tokens
%token <node> CREATE ALTER DROP RENAME
%token <node> TABLE INDEX VIEW TO IGNORE IF UNIQUE USING

%start any_command

// Fake Tokens
%token <node> NODE_LIST CASE_WHEN WHEN_LIST NO_LOCK FOR_UPDATE LOCK_IN_SHARE_MODE

%type <statement> command
%type <statement> select_statement insert_statement update_statement delete_statement set_statement
%type <statement> create_statement alter_statement rename_statement drop_statement
%type <comments> comment_opt comment_list
%type <str> union_op
%type <distinct> distinct_opt
%type <selectExprs> select_expression_list
%type <selectExpr> select_expression
%type <bytes> as_lower_opt as_opt
%type <expr> expression
%type <tableExprs> table_expression_list
%type <tableExpr> table_expression
%type <str> join_type
%type <sqlNode> simple_table_expression
%type <tableName> dml_table_expression
%type <indexHints> index_hint_list
%type <names> index_list
%type <where> where_expression_opt
%type <boolExpr> boolean_expression condition
%type <str> compare
%type <sqlNode> values
%type <valExpr> value tuple value_expression
%type <valExprs> value_expression_list
%type <values> tuple_list
%type <node> keyword_as_func
%type <subquery> subquery
%type <byt> unary_operator
%type <colName> column_name
%type <node> case_expression when_expression_list when_expression
%type <groupBy> group_by_opt
%type <node> having_opt order_by_opt order_list order asc_desc_opt limit_opt lock_opt on_dup_opt
%type <columns> column_list_opt column_list
%type <node> update_list update_expression
%type <node> exists_opt not_exists_opt ignore_opt non_rename_operation to_opt constraint_opt using_opt
%type <node> sql_id
%type <node> force_eof

%%

any_command:
  command
  {
    SetParseTree(yylex, $1)
  }

command:
  select_statement
| insert_statement
| update_statement
| delete_statement
| set_statement
| create_statement
| alter_statement
| rename_statement
| drop_statement

select_statement:
  SELECT comment_opt distinct_opt select_expression_list FROM table_expression_list where_expression_opt group_by_opt having_opt order_by_opt limit_opt lock_opt
  {
    $$ = &Select{Comments: $2, Distinct: $3, SelectExprs: $4, From: $6, Where: $7, GroupBy: $8, Having: $9, OrderBy: $10, Limit: $11, Lock: $12}
  }
| select_statement union_op select_statement %prec UNION
  {
    $$ = &Union{Type: $2, Select1: $1.(SelectStatement), Select2: $3.(SelectStatement)}
  }

insert_statement:
  INSERT comment_opt INTO dml_table_expression column_list_opt values on_dup_opt
  {
    $$ = &Insert{Comments: $2, Table: $4, Columns: $5, Rows: $6, OnDup: $7}
  }

update_statement:
  UPDATE comment_opt dml_table_expression SET update_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{Comments: $2, Table: $3, List: $5, Where: $6, OrderBy: $7, Limit: $8}
  }

delete_statement:
  DELETE comment_opt FROM dml_table_expression where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{Comments: $2, Table: $4, Where: $5, OrderBy: $6, Limit: $7}
  }

set_statement:
  SET comment_opt update_list
  {
    $$ = &Set{Comments: $2, Updates: $3}
  }

create_statement:
  CREATE TABLE not_exists_opt ID force_eof
  {
    $$ = &DDLSimple{Action: CREATE, Table: $4}
  }
| CREATE constraint_opt INDEX sql_id using_opt ON ID force_eof
  {
    // Change this to an alter statement
    $$ = &DDLSimple{Action: ALTER, Table: $7}
  }
| CREATE VIEW sql_id force_eof
  {
    $$ = &DDLSimple{Action: CREATE, Table: $3}
  }

alter_statement:
  ALTER ignore_opt TABLE ID non_rename_operation force_eof
  {
    $$ = &DDLSimple{Action: ALTER, Table: $4}
  }
| ALTER ignore_opt TABLE ID RENAME to_opt ID
  {
    // Change this to a rename statement
    $$ = &Rename{OldName: $4, NewName: $7}
  }
| ALTER VIEW sql_id force_eof
  {
    $$ = &DDLSimple{Action: ALTER, Table: $3}
  }

rename_statement:
  RENAME TABLE ID TO ID
  {
    $$ = &Rename{OldName: $3, NewName: $5}
  }

drop_statement:
  DROP TABLE exists_opt ID
  {
    $$ = &DDLSimple{Action: DROP, Table: $4}
  }
| DROP INDEX sql_id ON ID
  {
    // Change this to an alter statement
    $$ = &DDLSimple{Action: ALTER, Table: $5}
  }
| DROP VIEW exists_opt sql_id force_eof
  {
    $$ = &DDLSimple{Action: DROP, Table: $4}
  }

comment_opt:
  {
    SetAllowComments(yylex, true)
  }
  comment_list
  {
    $$ = $2
    SetAllowComments(yylex, false)
  }

comment_list:
  {
    $$ = nil
  }
| comment_list COMMENT
  {
    $$ = append($$, Comment($2.Value))
  }

union_op:
  UNION
  {
    $$ = "union"
  }
| UNION ALL
  {
    $$ = "union all"
  }
| MINUS
  {
    $$ = "minus"
  }
| EXCEPT
  {
    $$ = "except"
  }
| INTERSECT
  {
    $$ = "intersect"
  }

distinct_opt:
  {
    $$ = Distinct(false)
  }
| DISTINCT
  {
    $$ = Distinct(true)
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
| expression as_lower_opt
  {
    $$ = &NonStarExpr{Expr: $1, As: $2}
  }
| ID '.' '*'
  {
    $$ = &StarExpr{TableName: $1.Value}
  }

expression:
  boolean_expression
  {
    $$ = $1
  }
| value_expression
  {
    $$ = $1
  }

as_lower_opt:
  {
    $$ = nil
  }
| sql_id
  {
    $$ = $1.Value
  }
| AS sql_id
  {
    $$ = $2.Value
  }

table_expression_list:
  table_expression
  {
    $$ = TableExprs{$1}
  }
| table_expression_list ',' table_expression
  {
    $$ = append($$, $3)
  }

table_expression:
  simple_table_expression as_opt index_hint_list
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $2, Hints: $3}
  }
| '(' table_expression ')'
  {
    $$ = &ParenTableExpr{Expr: $2}
  }
| table_expression join_type table_expression %prec JOIN
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3}
  }
| table_expression join_type table_expression ON boolean_expression %prec JOIN
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, On: $5}
  }

as_opt:
  {
    $$ = nil
  }
| ID
  {
    $$ = $1.Value
  }
| AS ID
  {
    $$ = $2.Value
  }

join_type:
  JOIN
  {
    $$ = "join"
  }
| STRAIGHT_JOIN
  {
    $$ = "straight_join"
  }
| LEFT JOIN
  {
    $$ = "left join"
  }
| LEFT OUTER JOIN
  {
    $$ = "left join"
  }
| RIGHT JOIN
  {
    $$ = "right join"
  }
| RIGHT OUTER JOIN
  {
    $$ = "right join"
  }
| INNER JOIN
  {
    $$ = "join"
  }
| CROSS JOIN
  {
    $$ = "cross join"
  }
| NATURAL JOIN
  {
    $$ = "natural join"
  }

simple_table_expression:
ID
  {
    $$ = &TableName{Name: $1.Value}
  }
| ID '.' ID
  {
    $$ = &TableName{Qualifier: $1.Value, Name: $3.Value}
  }
| subquery
  {
    $$ = $1
  }

dml_table_expression:
ID
  {
    $$ = &TableName{Name: $1.Value}
  }
| ID '.' ID
  {
    $$ = &TableName{Qualifier: $1.Value, Name: $3.Value}
  }

index_hint_list:
  {
    $$ = nil
  }
| USE INDEX '(' index_list ')'
  {
    $$ = &IndexHints{Type: "use", Indexes: $4}
  }
| IGNORE INDEX '(' index_list ')'
  {
    $$ = &IndexHints{Type: "ignore", Indexes: $4}
  }
| FORCE INDEX '(' index_list ')'
  {
    $$ = &IndexHints{Type: "force", Indexes: $4}
  }

index_list:
  sql_id
  {
    $$ = [][]byte{$1.Value}
  }
| index_list ',' sql_id
  {
    $$ = append($1, $3.Value)
  }

where_expression_opt:
  {
    $$ = nil
  }
| WHERE boolean_expression
  {
    $$ = &Where{Expr: $2}
  }

boolean_expression:
  condition
| boolean_expression AND boolean_expression
  {
    $$ = &AndExpr{Left: $1, Right: $3}
  }
| boolean_expression OR boolean_expression
  {
    $$ = &OrExpr{Left: $1, Right: $3}
  }
| NOT boolean_expression
  {
    $$ = &NotExpr{Expr: $2}
  }
| '(' boolean_expression ')'
  {
    $$ = &ParenBoolExpr{Expr: $2}
  }

condition:
  value_expression compare value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }
| value_expression IN tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: "in", Right: $3}
  }
| value_expression NOT IN tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: "not in", Right: $4}
  }
| value_expression LIKE value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: "like", Right: $3}
  }
| value_expression NOT LIKE value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: "not like", Right: $4}
  }
| value_expression BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: "between", From: $3, To: $5}
  }
| value_expression NOT BETWEEN value_expression AND value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: "not between", From: $4, To: $6}
  }
| value_expression IS NULL
  {
    $$ = &NullCheck{Operator: "is null", Expr: $1}
  }
| value_expression IS NOT NULL
  {
    $$ = &NullCheck{Operator: "is not null", Expr: $1}
  }
| EXISTS subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }

compare:
  '='
  {
    $$ = "="
  }
| '<'
  {
    $$ = "<"
  }
| '>'
  {
    $$ = ">"
  }
| LE
  {
    $$ = "<="
  }
| GE
  {
    $$ = ">="
  }
| NE
  {
    $$ = string($1.Value)
  }
| NULL_SAFE_EQUAL
  {
    $$ = "<=>"
  }

values:
  VALUES tuple_list
  {
    $$ = $2
  }
| select_statement
  {
    $$ = $1
  }

tuple_list:
  tuple
  {
    $$ = Values{$1.(Tuple)}
  }
| tuple_list ',' tuple
  {
    $$ = append($1, $3.(Tuple))
  }

tuple:
  '(' value_expression_list ')'
  {
    $$ = ValueTuple($2)
  }
| subquery
  {
    $$ = $1
  }

subquery:
  '(' select_statement ')'
  {
    $$ = &Subquery{$2.(SelectStatement)}
  }

value_expression_list:
  value_expression
  {
    $$ = ValExprs{$1}
  }
| value_expression_list ',' value_expression
  {
    $$ = append($1, $3)
  }

value_expression:
  value
  {
    $$ = $1
  }
| column_name
  {
    $$ = $1
  }
| tuple
  {
    $$ = $1
  }
| value_expression '&' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '&', Right: $3}
  }
| value_expression '|' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '|', Right: $3}
  }
| value_expression '^' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '^', Right: $3}
  }
| value_expression '+' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '+', Right: $3}
  }
| value_expression '-' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '-', Right: $3}
  }
| value_expression '*' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '*', Right: $3}
  }
| value_expression '/' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '/', Right: $3}
  }
| value_expression '%' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: '%', Right: $3}
  }
| unary_operator value_expression %prec UNARY
  {
    if num, ok := $2.(NumValue); ok {
      switch $1 {
      case '-':
        $$ = append(NumValue("-"), num...)
      case '+':
        $$ = num
      default:
        $$ = &UnaryExpr{Operator: $1, Expr: $2}
      }
    } else {
      $$ = &UnaryExpr{Operator: $1, Expr: $2}
    }
  }
| sql_id '(' ')'
  {
    $$ = &FuncExpr{Name: $1.Value}
  }
| sql_id '(' select_expression_list ')'
  {
    $$ = &FuncExpr{Name: $1.Value, Exprs: $3}
  }
| sql_id '(' DISTINCT select_expression_list ')'
  {
    $$ = &FuncExpr{Name: $1.Value, Distinct: true, Exprs: $4}
  }
| keyword_as_func '(' select_expression_list ')'
  {
    $$ = &FuncExpr{Name: $1.Value, Exprs: $3}
  }
| case_expression
  {
    c := CaseExpr(*$1)
    $$ = &c
  }

keyword_as_func:
  IF
| VALUES

unary_operator:
  '+'
  {
    $$ = '+'
  }
| '-'
  {
    $$ = '-'
  }
| '~'
  {
    $$ = '~'
  }

case_expression:
  CASE when_expression_list END
  {
    $$ = NewSimpleParseNode(CASE_WHEN, "case")
    $$.Push($2)
  }
| CASE expression when_expression_list END
  {
    $$.PushTwo($2, $3)
  }

when_expression_list:
  when_expression
  {
    $$ = NewSimpleParseNode(WHEN_LIST, "when_list")
    $$.Push($1)
  }
| when_expression_list when_expression
  {
    $$.Push($2)
  }

when_expression:
  WHEN expression THEN expression
  {
    $$.PushTwo($2, $4)
  }
| ELSE expression
  {
    $$.Push($2)
  }

column_name:
  sql_id
  {
    $$ = &ColName{Name: $1.Value}
  }
| ID '.' sql_id
  {
    $$ = &ColName{Qualifier: $1.Value, Name: $3.Value}
  }

value:
  STRING
  {
    $$ = StringValue($1.Value)
  }
| NUMBER
  {
    $$ = NumValue($1.Value)
  }
| VALUE_ARG
  {
    $$ = ValueArg($1.Value)
  }
| NULL
  {
    $$ = &NullValue{}
  }

group_by_opt:
  {
    $$ = nil
  }
| GROUP BY value_expression_list
  {
    $$ = GroupBy($3)
  }

having_opt:
  {
    $$ = NewSimpleParseNode(HAVING, "having")
  }
| HAVING boolean_expression
  {
    $$ = $1.Push($2)
  }

order_by_opt:
  {
    $$ = NewSimpleParseNode(ORDER, "order")
  }
| ORDER BY order_list
  {
    $$ = $1.Push($3)
  }

order_list:
  order
  {
    $$ = NewSimpleParseNode(NODE_LIST, "node_list")
    $$.Push($1)
  }
| order_list ',' order
  {
    $$ = $1.Push($3)
  }

order:
  value_expression asc_desc_opt
  {
    $$ = $2.Push($1)
  }

asc_desc_opt:
  {
    $$ = NewSimpleParseNode(ASC, "asc")
  }
| ASC
| DESC

limit_opt:
  {
    $$ = NewSimpleParseNode(LIMIT, "limit")
  }
| LIMIT value_expression
  {
    $$ = $1.Push($2)
  }
| LIMIT value_expression ',' value_expression
  {
    $$ = $1.PushTwo($2, $4)
  }

lock_opt:
  {
    $$ = NewSimpleParseNode(NO_LOCK, "")
  }
| FOR UPDATE
  {
    $$ = NewSimpleParseNode(FOR_UPDATE, " for update")
  }
| LOCK IN sql_id sql_id
  {
    if !bytes.Equal($3.Value, SHARE) {
      yylex.Error("expecting share")
      return 1
    }
    if !bytes.Equal($4.Value, MODE) {
      yylex.Error("expecting mode")
      return 1
    }
    $$ = NewSimpleParseNode(LOCK_IN_SHARE_MODE, " lock in share mode")
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
  column_name
  {
    $$ = Columns{&NonStarExpr{Expr: $1}}
  }
| column_list ',' column_name
  {
    $$ = append($$, &NonStarExpr{Expr: $3})
  }

on_dup_opt:
  {
    $$ = NewSimpleParseNode(DUPLICATE, "duplicate")
  }
| ON DUPLICATE KEY UPDATE update_list
  {
    $$ = $2.Push($5)
  }

update_list:
  update_expression
  {
    $$ = NewSimpleParseNode(NODE_LIST, "node_list")
    $$.Push($1)
  }
| update_list ',' update_expression
  {
    $$ = $1.Push($3)
  }

update_expression:
  column_name '=' value_expression
  {
    $$ = $2.PushTwo($1, $3)
  }

exists_opt:
  { $$ = nil }
| IF EXISTS

not_exists_opt:
  { $$ = nil }
| IF NOT EXISTS

ignore_opt:
  { $$ = nil }
| IGNORE

non_rename_operation:
  ALTER
| DEFAULT
| DROP
| ORDER
| ID

to_opt:
  { $$ = nil }
| TO

constraint_opt:
  { $$ = nil }
| UNIQUE

using_opt:
  { $$ = nil }
| USING sql_id

sql_id:
  ID
  {
    $$.LowerCase()
  }

force_eof:
{
  ForceEOF(yylex)
}
