#include <stddef.h>

#define FALSE 0
#define TRUE !FALSE

/* #define DEBUG */

typedef struct {
  size_t size;
  char **elems;
} array;

struct value_t;

typedef struct {
  size_t size;
  struct value_t **elems;
} value_array;

typedef struct value_t {
  char *name;
  enum {
    VALUE_REGEX,
    VALUE_INT,
    VALUE_FLOAT,
    VALUE_BOOLEAN,
    VALUE_STRING,
    VALUE_INTO_NAME,
    VALUE_TABLE_NAME,
    VALUE_SIMPLE_NAME,
    VALUE_DURATION,
    VALUE_WILDCARD,
    VALUE_FUNCTION_CALL,
    VALUE_EXPRESSION
  } value_type;
  char *alias;
  char is_case_insensitive;
  value_array *args;
} value;

typedef struct condition_t {
  char is_bool_expression;
  void *left;
  char* op;                             /* AND, OR or NULL if there's no right operand */
  struct condition_t *right;
} condition;

typedef struct groupby_clause_t {
  value_array *elems;
  value *fill_function;
} groupby_clause;

typedef struct {
  int first_line;
  int first_column;
  int last_line;
  int last_column;
  char *err;
} error;

typedef struct {
  value *name;
  char *alias;
} table_name;

typedef struct {
  size_t size;
  table_name **elems;
} table_name_array;

typedef struct {
  enum {
    FROM_ARRAY,
    FROM_MERGE,
    FROM_JOIN,
    FROM_MERGE_REGEX,
    FROM_JOIN_REGEX,
  } from_clause_type;
  // in case of merge or join, it's guaranteed that the names array
  // will have two table names only and they aren't regex.
  table_name_array *names;
  value *regex_value;                   /* regex merge */
} from_clause;

typedef struct {
  value *target;
  value *backfill_function;
} into_clause;

typedef struct {
  value_array *c;
  from_clause *from_clause;
  groupby_clause *group_by;
  into_clause *into_clause;
  condition *where_condition;
  int limit;
  char ascending;
  char explain;
} select_query;

typedef struct {
  char has_regex;
  char include_spaces;
  value *regex;
} list_series_query;

typedef struct {
  from_clause *from_clause;
  condition *where_condition;
  error *error;
} delete_query;

typedef struct {
  value *name;
} drop_series_query;

typedef struct {
  int id;
} drop_query;

typedef struct {
  select_query *select_query;
  delete_query *delete_query;
  drop_series_query *drop_series_query;
  drop_query *drop_query;
  list_series_query *list_series_query;
  char list_continuous_queries_query;
} query;

// queries is an array of query
typedef struct {
  size_t size;
  query **qs;
  error *error;
} queries;

// some funcs for freeing our types
void free_array(array *array);
void free_value_array(value_array *array);
void free_value(value *value);
void free_condition(condition *condition);
void free_error (error *error);

// this is the api that is used in GO
queries parse_query(char *const query_s);
void  close_query (query *q);
void  close_queries (queries *queries);
