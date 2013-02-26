/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

%{
package sqlparser

func SetParseTree(yylex interface{}, root *Node) {
	tn := yylex.(*Tokenizer)
	tn.ParseTree = root
}

func SetAllowComments(yylex interface{}, allow bool) {
	tn := yylex.(*Tokenizer)
	tn.AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	tn := yylex.(*Tokenizer)
	tn.ForceEOF = true
}

// Offsets for select parse tree. These need to match the Push order in the select_statement rule.
const (
	SELECT_COMMENT_OFFSET = iota
	SELECT_DISTINCT_OFFSET
	SELECT_EXPR_OFFSET
	SELECT_FROM_OFFSET
	SELECT_WHERE_OFFSET
	SELECT_GROUP_OFFSET
	SELECT_HAVING_OFFSET
	SELECT_ORDER_OFFSET
	SELECT_LIMIT_OFFSET
	SELECT_FOR_UPDATE_OFFSET
)

const (
	INSERT_COMMENT_OFFSET = iota
	INSERT_TABLE_OFFSET
	INSERT_COLUMN_LIST_OFFSET
	INSERT_VALUES_OFFSET
	INSERT_ON_DUP_OFFSET
)

const (
	UPDATE_COMMENT_OFFSET = iota
	UPDATE_TABLE_OFFSET
	UPDATE_LIST_OFFSET
	UPDATE_WHERE_OFFSET
	UPDATE_ORDER_OFFSET
	UPDATE_LIMIT_OFFSET
)

const (
	DELETE_COMMENT_OFFSET = iota
	DELETE_TABLE_OFFSET
	DELETE_WHERE_OFFSET
	DELETE_ORDER_OFFSET
	DELETE_LIMIT_OFFSET
)

%}

%union {
	node *Node
}

%token <node> SELECT INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT COMMENT FOR
%token <node> ALL DISTINCT AS EXISTS IN IS LIKE BETWEEN NULL ASC DESC VALUES INTO DUPLICATE KEY DEFAULT SET
%token <node> ID STRING NUMBER VALUE_ARG
%token <node> LE GE NE NULL_SAFE_EQUAL
%token <node> LEX_ERROR
%token <node> '(' '=' '<' '>' '~'

%left <node> UNION MINUS EXCEPT INTERSECT
%left <node> ','
%left <node> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE
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
%token <node> TABLE INDEX TO IGNORE IF UNIQUE USING

%start any_command

// Fake Tokens
%token <node> NODE_LIST UPLUS UMINUS CASE_WHEN WHEN_LIST SELECT_STAR NO_DISTINCT FUNCTION FOR_UPDATE NOT_FOR_UPDATE
%token <node> NOT_IN NOT_LIKE NOT_BETWEEN IS_NULL IS_NOT_NULL UNION_ALL COMMENT_LIST COLUMN_LIST TABLE_EXPR

%type <node> command
%type <node> select_statement insert_statement update_statement delete_statement set_statement
%type <node> create_statement alter_statement rename_statement drop_statement
%type <node> comment_opt comment_list
%type <node> union_op distinct_opt
%type <node> select_expression_list select_expression expression as_opt
%type <node> table_expression_list table_expression join_type simple_table_expression index_hint_list
%type <node> where_expression_opt boolean_expression condition compare
%type <node> values parenthesised_lists parenthesised_list value_expression_list value_expression keyword_as_func
%type <node> unary_operator case_expression when_expression_list when_expression column_name value
%type <node> group_by_opt having_opt order_by_opt order_list order asc_desc_opt limit_opt for_update_opt on_dup_opt
%type <node> column_list_opt column_list update_list update_expression
%type <node> exists_opt not_exists_opt ignore_opt non_rename_operation to_opt constraint_opt using_opt
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
	SELECT comment_opt distinct_opt select_expression_list FROM table_expression_list where_expression_opt group_by_opt having_opt order_by_opt limit_opt for_update_opt
	{
		$$ = $1
		$$.Push($2) // 0: comment_opt
		$$.Push($3) // 1: distinct_opt
		$$.Push($4) // 2: select_expression_list
		$$.Push($6) // 3: table_expression_list
		$$.Push($7) // 4: where_expression_opt
		$$.Push($8) // 5: group_by_opt
		$$.Push($9) // 6: having_opt
		$$.Push($10) // 7: order_by_opt
		$$.Push($11) // 8: limit_opt
		$$.Push($12) // 9: for_update_opt
	}
| select_statement union_op select_statement %prec UNION
	{
		$$ = $2.PushTwo($1, $3)
	}

insert_statement:
	INSERT comment_opt INTO ID column_list_opt values on_dup_opt
	{
		$$ = $1
		$$.Push($2) // 0: comment_opt
		$$.Push($4) // 1: table_name
		$$.Push($5) // 2: column_list_opt
		$$.Push($6) // 3: values
		$$.Push($7) // 4: on_dup_opt
	}

update_statement:
	UPDATE comment_opt ID SET update_list where_expression_opt order_by_opt limit_opt
	{
		$$ = $1
		$$.Push($2) // 0: comment_opt
		$$.Push($3) // 1: table_name
		$$.Push($5) // 2: update_list
		$$.Push($6) // 3: where_expression_opt
		$$.Push($7) // 4: order_by_opt
		$$.Push($8) // 5: limit_opt
	}

delete_statement:
	DELETE comment_opt FROM ID where_expression_opt order_by_opt limit_opt
	{
		$$ = $1
		$$.Push($2) // 0: comment_opt
		$$.Push($4) // 1: table_name
		$$.Push($5) // 2: where_expression_opt
		$$.Push($6) // 3: order_by_opt
		$$.Push($7) // 4: limit_opt
	}

set_statement:
	SET comment_opt update_list
	{
		$$ = $1
		$$.Push($2)
		$$.Push($3)
	}

create_statement:
	CREATE TABLE not_exists_opt ID force_eof
	{
		$$.Push($4)
	}
| CREATE constraint_opt INDEX ID using_opt ON ID force_eof
	{
		// Change this to an alter statement
		$$ = NewSimpleParseNode(ALTER, "alter")
		$$.Push($7)
	}

alter_statement:
	ALTER ignore_opt TABLE ID non_rename_operation force_eof
	{
		$$.Push($4)
	}
| ALTER ignore_opt TABLE ID RENAME to_opt ID
	{
		// Change this to a rename statement
		$$ = NewSimpleParseNode(RENAME, "rename")
		$$.PushTwo($4, $7)
	}

rename_statement:
	RENAME TABLE ID TO ID
	{
		$$.PushTwo($3, $5)
	}

drop_statement:
	DROP TABLE exists_opt ID
	{
		$$.Push($4)
	}
| DROP INDEX ID ON ID
	{
		// Change this to an alter statement
		$$ = NewSimpleParseNode(ALTER, "alter")
		$$.Push($5)
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
		$$ = NewSimpleParseNode(COMMENT_LIST, "")
	}
| comment_list COMMENT
	{
		$$ = $1.Push($2)
	}

union_op:
	UNION
| UNION ALL
	{
		$$ = NewSimpleParseNode(UNION_ALL, "union all")
	}
| MINUS
| EXCEPT
| INTERSECT

distinct_opt:
	{
		$$ = NewSimpleParseNode(NO_DISTINCT, "")
	}
| DISTINCT
	{
		$$ = NewSimpleParseNode(DISTINCT, "distinct")
	}

select_expression_list:
	select_expression
	{
		$$ = NewSimpleParseNode(NODE_LIST, "node_list")
		$$.Push($1)
	}
| select_expression_list ',' select_expression
	{
		$$.Push($3)
	}

select_expression:
	'*'
	{
		$$ = NewSimpleParseNode(SELECT_STAR, "*")
	}
| expression
| expression as_opt ID
	{
		$$ = $2.PushTwo($1, $3)
	}
| ID '.' '*'
	{
		$$ = $2.PushTwo($1, NewSimpleParseNode(SELECT_STAR, "*"))
	}

expression:
	boolean_expression
| value_expression

as_opt:
	{
		$$ = NewSimpleParseNode(AS, "as")
	}
| AS

table_expression_list:
	table_expression
	{
		$$ = NewSimpleParseNode(NODE_LIST, "node_list")
		$$.Push($1)
	}
| '(' table_expression ')'
	{
		$$ = $1.Push($2)
	}
| table_expression_list ',' table_expression
	{
		$$.Push($3)
	}

table_expression:
	simple_table_expression index_hint_list
  {
    $$ = NewSimpleParseNode(TABLE_EXPR, "")
    $$.Push($1)
    $$.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
    $$.Push($2)
  }
| simple_table_expression as_opt ID index_hint_list
	{
    $$ = NewSimpleParseNode(TABLE_EXPR, "")
    $$.Push($1)
    $$.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push($3))
    $$.Push($4)
	}
| table_expression join_type table_expression %prec JOIN
	{
		$$ = $2.PushTwo($1, $3)
	}
| table_expression join_type table_expression ON boolean_expression %prec JOIN
	{
		$$ = $2
		$$.Push($1)
		$$.Push($3)
		$$.Push($5)
	}

join_type:
	JOIN
| STRAIGHT_JOIN
| LEFT JOIN
	{
		$$ = NewSimpleParseNode(LEFT, "left join")
	}
| LEFT OUTER JOIN
	{
		$$ = NewSimpleParseNode(LEFT, "left join")
	}
| RIGHT JOIN
	{
		$$ = NewSimpleParseNode(RIGHT, "right join")
	}
| RIGHT OUTER JOIN
	{
		$$ = NewSimpleParseNode(RIGHT, "right join")
	}
| INNER JOIN
	{
		$$ = $2
	}
| CROSS JOIN
	{
		$$ = NewSimpleParseNode(CROSS, "cross join")
	}
| NATURAL JOIN
	{
		$$ = NewSimpleParseNode(NATURAL, "natural join")
	}

simple_table_expression:
ID
| ID '.' ID
	{
		$$ = $2.PushTwo($1, $3)
	}
| '(' select_statement ')'
	{
		$$ = $1.Push($2)
	}

index_hint_list:
  {
		$$ = NewSimpleParseNode(USE, "use")
  }
| USE INDEX '(' column_list ')'
  {
    $$.Push($4)
  }

where_expression_opt:
	{
		$$ = NewSimpleParseNode(WHERE, "where")
	}
| WHERE boolean_expression
	{
		$$ = $1.Push($2)
	}

boolean_expression:
	condition
| boolean_expression AND boolean_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| boolean_expression OR boolean_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| NOT boolean_expression
	{
		$$ = $1.Push($2)
	}
| '(' boolean_expression ')'
	{
		$$ = $1.Push($2)
	}

condition:
	value_expression compare value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression IN parenthesised_list
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression NOT IN parenthesised_list
	{
		$$ = NewSimpleParseNode(NOT_IN, "not in").PushTwo($1, $4)
	}
| value_expression LIKE value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression NOT LIKE value_expression
	{
		$$ = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo($1, $4)
	}
| value_expression BETWEEN value_expression AND value_expression
	{
		$$ = $2
		$$.Push($1)
		$$.Push($3)
		$$.Push($5)
	}
| value_expression NOT BETWEEN value_expression AND value_expression
	{
		$$ = NewSimpleParseNode(NOT_BETWEEN, "not between")
		$$.Push($1)
		$$.Push($4)
		$$.Push($6)
	}
| value_expression IS NULL
	{
		$$ = NewSimpleParseNode(IS_NULL, "is null").Push($1)
	}
| value_expression IS NOT NULL
	{
		$$ = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push($1)
	}
| EXISTS '(' select_statement ')'
	{
		$$ = $1.Push($3)
	}

compare:
	'='
| '<'
| '>'
| LE
| GE
| NE
| NULL_SAFE_EQUAL

values:
	VALUES parenthesised_lists
	{
		$$ = $1.Push($2)
	}
| select_statement

parenthesised_lists:
	parenthesised_list
	{
		$$ = NewSimpleParseNode(NODE_LIST, "node_list")
		$$.Push($1)
	}
| parenthesised_lists ',' parenthesised_list
	{
		$$.Push($3)
	}

parenthesised_list:
	'(' value_expression_list ')'
	{
		$$ = $1.Push($2)
	}
| '(' select_statement ')'
	{
		$$ = $1.Push($2)
	}

value_expression_list:
	value_expression
	{
		$$ = NewSimpleParseNode(NODE_LIST, "node_list")
		$$.Push($1)
	}
| value_expression_list ',' value_expression
	{
		$$.Push($3)
	}

value_expression:
	value
| column_name
| '(' select_statement ')'
	{
		$$ = $1.Push($2)
	}
| '(' value_expression_list ')'
	{
    if $2.Len() == 1 {
      $2 = $2.At(0)
    }
    switch $2.Type {
    case NUMBER, STRING, ID, VALUE_ARG, '(', '.':
      $$ = $2
    default:
      $$ = $1.Push($2)
    }
	}
| value_expression '&' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '|' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '^' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '+' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '-' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '*' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '/' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| value_expression '%' value_expression
	{
		$$ = $2.PushTwo($1, $3)
	}
| unary_operator value_expression %prec UNARY
	{
    if $2.Type == NUMBER { // Simplify trivial unary expressions
      switch $1.Type {
      case UMINUS:
        $2.Value = append($1.Value, $2.Value...)
        $$ = $2
      case UPLUS:
        $$ = $2
      default:
        $$ = $1.Push($2)
      }
    } else {
      $$ = $1.Push($2)
    }
	}
| ID '(' ')'
	{
		$1.Type = FUNCTION
		$$ = $1.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
	}
| ID '(' select_expression_list ')'
	{
		$1.Type = FUNCTION
		$$ = $1.Push($3)
	}
| ID '(' DISTINCT select_expression_list ')'
	{
		$1.Type = FUNCTION
		$$ = $1.Push($3)
		$$ = $1.Push($4)
	}
| keyword_as_func '(' select_expression_list ')'
	{
		$1.Type = FUNCTION
		$$ = $1.Push($3)
	}
| case_expression

keyword_as_func:
	IF
| VALUES

unary_operator:
	'+'
	{
		$$ = NewSimpleParseNode(UPLUS, "+")
	}
| '-'
	{
		$$ = NewSimpleParseNode(UMINUS, "-")
	}
| '~'

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
	ID
| ID '.' ID
	{
		$$ = $2.PushTwo($1, $3)
	}

value:
	STRING
| NUMBER
| VALUE_ARG
| NULL

group_by_opt:
	{
		$$ = NewSimpleParseNode(GROUP, "group")
	}
| GROUP BY value_expression_list
	{
		$$ = $1.Push($3)
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

for_update_opt:
	{
		$$ = NewSimpleParseNode(NOT_FOR_UPDATE, "")
	}
| FOR UPDATE
	{
		$$ = NewSimpleParseNode(FOR_UPDATE, " for update")
	}

column_list_opt:
	{
		$$ = NewSimpleParseNode(COLUMN_LIST, "")
	}
| '(' column_list ')'
	{
		$$ = $2
	}

column_list:
	ID
	{
		$$ = NewSimpleParseNode(COLUMN_LIST, "")
		$$.Push($1)
	}
| column_list ',' ID
	{
		$$ = $1.Push($3)
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
	ID '=' expression
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
| USING ID

force_eof:
{
	ForceEOF(yylex)
}
