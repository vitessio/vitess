"""Helper classes for building queries.

Helper classes and fucntions for building queries.
"""

import itertools
import pprint
import time

# TODO(dumbunny): integration with SQL Alchemy ?


class DBRow(object):
  """An object with an attr for every column returned by a query."""

  def __init__(self, column_names=None, row_tuple=None, **overrides):
    """Init DBRow from zip(column_names, row_tuple) and/or overrides.

    Args:
      column_names: List or tuple of str column names.
      row_tuple: List or tuple of str column values.
      **overrides: Additional (str name: value) pairs.

    Raises:
      ValueError: If len(column_names) and len(row_tuple) differ.
    """
    column_names = column_names or ()
    row_tuple = row_tuple or ()
    if len(column_names) != len(row_tuple):
      raise ValueError('column_names / row_tuple mismatch.')
    self.__dict__ = dict(zip(column_names, row_tuple), **overrides)

  def __repr__(self):
    return pprint.pformat(self.__dict__, 4)


def select_clause(
    select_columns, table_name, alias=None, order_by=None):
  """Build the select clause for a query.

  Args:
    select_columns: Str column names.
    table_name: Str table name.
    alias: Str alias for table if defined.
    order_by: Str, str list, or str list list of words.
      where each list element is an order by expr where each expr is a
      str ('col_a ASC' or 'col_a') or a list of words (['col_a', 'ASC']).

  Returns:
    Str like "SELECT col_a, col_b FROM my_table".
  """

  if alias:
    return 'SELECT %s FROM %s %s' % (
        colstr(select_columns, alias, order_by=order_by),
        table_name, alias)
  return 'SELECT %s FROM %s' % (
      colstr(select_columns, alias, order_by=order_by),
      table_name)


def colstr(
    select_columns, alias=None, bind=None, order_by=None):
  """Return columns clause for a SELECT query.

  Args:
    select_columns: Str column names.
    alias: Table alias for these columns.
    bind: A list of columns to get. Ignore columns not in bind.
    order_by: A str or item list, where each item is a str or a str list
      of words. Example: ['col_a', ('col_b', 'ASC')]. This is only
      used in the client_aggregate option of select_by_columns_query;
      normally, order_by should be None.

  Returns:
    Comma-delimited names of columns.
  """
  # avoid altering select_columns parameter.
  cols = select_columns[:]

  # In the case of a scatter/gather, prepend these columns to
  # facilitate an in-code sort - after that, we can just strip these
  # off and process normally.
  if order_by:
    words_list = _normalize_order_by(order_by)
    cols = [words[0] for words in words_list] + cols
  if not bind:
    bind = cols

  def col_with_prefix(col):
    """Prepend alias to col if it makes sense."""
    if isinstance(col, BaseSQLSelectExpr):
      return col.select_sql(alias)
    if alias and '.' not in col:
      col = '%s.%s' % (alias, col)
    return col

  return ', '.join([col_with_prefix(c) for c in cols if c in bind])


def build_values_clause(columns, bind_vars):
  """Builds values clause for an INSERT query.

  Ignore columns that do not have an associated bind var.

  Update bind_vars.

  Args:
    columns: Str column list.
    bind_vars: A (str: value) dict of bind variables.

  Returns:
    Str comma-delimited SQL format like '%(status)s, %(type)s',
      list of names of columns, like ['status', 'type'].
  """

  clause_parts = []
  bind_list = []
  for column in columns:
    if column in bind_vars:
      bind_list.append(column)
      if isinstance(bind_vars[column], BaseSQLInsertExpr):
        sql, new_bind_vars = bind_vars[column].build_insert_sql()
        bind_vars[column] = sql
        update_bind_vars(bind_vars, new_bind_vars)
      clause_parts.append('%%(%s)s' % column)
    elif column in ('time_created', 'time_updated'):
      bind_list.append(column)
      clause_parts.append('%%(%s)s' % column)
      bind_vars[column] = int(time.time())
  return ', '.join(clause_parts), bind_list


def build_in(column, items, alt_name=None, counter=None):
  """Build SQL IN statement and bind dict.

  Args:
    column: Str column name.
    items: List of 1 or more values for IN statement.
    alt_name: Name to use for format token keys. Use column by default.
    counter: An itertools.count object.

  Returns:
    Str comma-delimited SQL format, (str: value) dict corresponding
      to format tokens.

  Raises:
    ValueError: On bad input.
  """

  if not items:
    raise ValueError('Called with empty items')

  base = alt_name if alt_name else column
  bind_list = make_bind_list(base, items, counter=counter)
  sql = '%s IN (%s)' % (
      column, ', '.join('%(' + pair[0] + ')s' for pair in bind_list))
  return sql, dict(bind_list)


def build_order_clause(order_by):
  """Get SQL for ORDER BY clause.

  Args:
    order_by: A str or item list, where each item is a str or a str list
      of words. Example: ['col_a', ('col_b', 'ASC')].

  Returns:
    The str 'ORDER BY ...' clause or ''.
  """
  if not order_by:
    return ''

  words_list = _normalize_order_by(order_by)
  return 'ORDER BY %s' % ', '.join(' '.join(words) for words in words_list)


def build_group_clause(group_by):
  """Build group_by clause for a query."""

  if not group_by:
    return ''

  if not isinstance(group_by, (tuple, list)):
    group_by = (group_by,)

  return 'GROUP BY %s' % ', '.join(group_by)


def build_limit_clause(limit):
  """Build limit clause for a query.

  Get a LIMIT clause and bind vars. The LIMIT clause will have either
  the form "LIMIT count" "LIMIT offset, count", or be the empty string.
  or the empty string.

  Args:
    limit: None, int or 1- or 2-element list or tuple.

  Returns:
    A (str LIMIT clause, bind vars) pair.
  """

  if limit is None:
    return '', {}

  if not isinstance(limit, (list, tuple)):
    limit = (limit,)

  bind_vars = {'limit_row_count': limit[0]}
  if len(limit) == 1:
    return 'LIMIT %(limit_row_count)s', bind_vars

  bind_vars = {'limit_offset': limit[0],
               'limit_row_count': limit[1]}
  return 'LIMIT %(limit_offset)s, %(limit_row_count)s', bind_vars


def build_where_clause(column_value_pairs):
  """Build the WHERE clause for a query.

  Args:
    column_value_pairs: A (str, value) list of where expr pairs.

  Returns:
    A (str WHERE clause, (str: value) dict bind vars) pair.
  """

  condition_list = []
  bind_vars = {}

  counter = itertools.count(1)

  for column, value in column_value_pairs:
    if isinstance(value, BaseSQLWhereExpr):
      clause, clause_bind_vars = value.build_where_sql(column, counter=counter)
      update_bind_vars(bind_vars, clause_bind_vars)
      condition_list.append(clause)
    elif isinstance(value, (tuple, list, set)):
      if value:
        if isinstance(value, set):
          value = sorted(value)
        in_clause, in_bind_variables = build_in(
            column, value, counter=counter)
        update_bind_vars(bind_vars, in_bind_variables)
        condition_list.append(in_clause)
      else:
        condition_list.append('1 = 0')
    else:
      bind_name = choose_bind_name(column, counter=counter)
      update_bind_vars(bind_vars, {bind_name: value})
      condition_list.append('%s = %%(%s)s' % (column, bind_name))

  # This seems like a hack to avoid returning an empty bind_vars.
  if not bind_vars:
    bind_vars = dict(column_value_pairs)

  where_clause = ' AND '.join(condition_list)
  return where_clause, bind_vars


def select_by_columns_query(
    select_column_list, table_name, column_value_pairs=None,
    order_by=None, group_by=None, limit=None, for_update=False,
    client_aggregate=False, vt_routing_info=None):
  """Get query and bind vars for a SELECT statement.

  Args:
    select_column_list: Str column names.
    table_name: Str name of table.
    column_value_pairs: A (str, value) list of where expr pairs.
    order_by: A str or item list, where each item is a str or a str list
      of words. Example: ['col_a', ('col_b', 'ASC')]. This is only
      used if client_aggregate is True.
    group_by: A str or str list of comma-delimited exprs.
    limit: An int count or (int offset, int count) pair.
    for_update: True for SELECT ... FOR UPDATE query.
    client_aggregate: If True, a fetch_aggregate will be sent to
      the cursor. This is used in a few places to return a sorted,
      limited list from a scatter query. It does not seem very useful.
    vt_routing_info: A vtrouting.VTRoutingInfo object that specifies
      a keyrange and a keyspace_id-bounding where clause.

  Returns:
    A (str SELECT query, (str: value) dict bind vars) pair.
  """

  if client_aggregate:
    clause_list = [select_clause(select_column_list, table_name,
                                 order_by=order_by)]
  else:
    clause_list = [select_clause(select_column_list, table_name)]

  # generate WHERE clause and bind variables
  if column_value_pairs:
    where_clause, bind_vars = build_where_clause(column_value_pairs)
    # add vt routing info
    if vt_routing_info:
      where_clause, bind_vars = vt_routing_info.update_where_clause(
          where_clause, bind_vars)
    clause_list += ['WHERE', where_clause]
  else:
    bind_vars = {}

  if group_by:
    clause_list.append(build_group_clause(group_by))
  if order_by:
    clause_list.append(build_order_clause(order_by))
  if limit:
    clause, limit_bind_vars = build_limit_clause(limit)
    clause_list.append(clause)
    update_bind_vars(bind_vars, limit_bind_vars)
  if for_update:
    clause_list.append('FOR UPDATE')

  query = ' '.join(clause_list)
  return query, bind_vars


def update_columns_query(table_name, where_column_value_pairs=None,
                         update_column_value_pairs=None, limit=None,
                         order_by=None):
  """Get query and bind vars for an update statement.

  Args:
    table_name: Str name of table.
    where_column_value_pairs: A (str, value) list of where expr pairs.
    update_column_value_pairs: A (str, value) list of update set
      pairs.
    limit: An optional int count or (int offset, int count) pair.
    order_by: A str or expr list, where each expr is a str or a str list
      of words. Example: ['col_a', ('col_b', 'ASC')].

  Returns:
    A (str UPDATE query, (str: value) dict bind vars) pair.

  Raises:
    ValueError: On bad input.
  """
  if not where_column_value_pairs:
    # We could allow for no where clause, but this is a notoriously
    # error-prone construct, so, no.
    raise ValueError(
        'No where_column_value_pairs: %s.' % (where_column_value_pairs,))

  if not update_column_value_pairs:
    raise ValueError(
        'No update_column_value_pairs: %s.' % (update_column_value_pairs,))

  clause_list = []
  bind_vars = {}
  for i, (column, value) in enumerate(update_column_value_pairs):
    if isinstance(value, BaseSQLUpdateExpr):
      clause, clause_bind_vars = value.build_update_sql(column)
      clause_list.append(clause)
      update_bind_vars(bind_vars, clause_bind_vars)
    else:
      clause_list.append('%s = %%(update_set_%s)s' % (column, i))
      bind_vars['update_set_%s' % i] = value

  set_clause = ', '.join(clause_list)

  where_clause, where_bind_vars = build_where_clause(where_column_value_pairs)
  update_bind_vars(bind_vars, where_bind_vars)

  query = ('UPDATE %(table)s SET %(set_clause)s WHERE %(where_clause)s'
           % {'table': table_name, 'set_clause': set_clause,
              'where_clause': where_clause})

  additional_clauses = []
  if order_by:
    additional_clauses.append(build_order_clause(order_by))
  if limit:
    limit_clause, limit_bind_vars = build_limit_clause(limit)
    additional_clauses.append(limit_clause)
    update_bind_vars(bind_vars, limit_bind_vars)
  if additional_clauses:
    query += ' ' + ' '.join(additional_clauses)
  return query, bind_vars


def delete_by_columns_query(table_name, where_column_value_pairs=None,
                            limit=None):
  """Get query and bind vars for a delete statement.

  Args:
    table_name: Str name of table.
    where_column_value_pairs: A (str, value) list of where expr pairs.
    limit: An optional int count or (int offset, int count) pair.

  Returns:
    A (str delete SQL query, (str: value) dict) pair.
  """

  where_clause, bind_vars = build_where_clause(where_column_value_pairs)
  limit_clause, limit_bind_vars = build_limit_clause(limit)
  update_bind_vars(bind_vars, limit_bind_vars)

  query = (
      'DELETE FROM %(table_name)s WHERE %(where_clause)s %(limit_clause)s' %
      {'table_name': table_name, 'where_clause': where_clause,
       'limit_clause': limit_clause})
  return query, bind_vars


def insert_query(table_name, columns, **bind_vars):
  """Return SQL for an INSERT INTO ... VALUES call.

  Args:
    table_name: Str name of table.
    columns: Str column names.
    **bind_vars: (str: value) dict of variables, with automatic
      columns like 'time_created' possibly added.

  Returns:
    A (str SQL, (str: value) dict bind vars pair.
  """
  values_clause, bind_list = build_values_clause(
      columns, bind_vars)
  query = 'INSERT INTO %s (%s) VALUES (%s)' % (
      table_name, colstr(columns, bind=bind_list), values_clause)
  return query, bind_vars


def build_aggregate_query(table_name, id_column_name, is_asc=False):
  """Return query, bind_vars for a table-wide min or max query."""
  query, bind_vars = select_by_columns_query(
      select_column_list=[id_column_name], table_name=table_name,
      order_by=[(id_column_name, 'ASC' if is_asc else 'DESC')],
      limit=1)
  return query, bind_vars


def build_count_query(table_name, column_value_pairs):
  """Return query, bind_vars for a count query."""
  return select_by_columns_query(
      select_column_list=[Count()], table_name=table_name,
      column_value_pairs=column_value_pairs)


def choose_bind_name(base, counter):
  return  '%s_%d' % (base, counter.next())


def make_bind_list(column, values, counter=None):
  """Return (bind_name, value) list for each value."""
  result = []
  bind_names = []
  if counter is None:
    counter = itertools.count(1)
  for value in values:
    bind_name = choose_bind_name(column, counter=counter)
    bind_names.append(bind_name)
    result.append((bind_name, value))
  return result


class BaseSQLUpdateExpr(object):
  """Return SQL for an UPDATE expression.

  Expr is used in: UPDATE ... SET expr [, expr ..] WHERE ...;

  It should have the form "col_name = ..."
  """

  def build_update_sql(self, column_name):
    """Return SQL and bind_vars for an UPDATE SET expression.

    Args:
      column_name: Str name of column to update.

    Returns:
      A (str SQL, (str: value) dict bind_vars) pair.
    """
    raise NotImplementedError


class RawSQLUpdateExpr(BaseSQLUpdateExpr):
  """A parameterized update expr.

  This is the simplest base class for an SQLUpdateExpr that is
  not also an SQLInsertExpr.

  See BaseSQLInsertExpr.
  """
  right_expr = None

  def __init__(self, right_expr=None, **bind_vars):
    """Pass in the right_expr and bind_vars.

    Either right_expr or the right_expr class variable should be
    defined.

    Args:
      right_expr: Str SQL on the right side of '=' in the update expr.
      **bind_vars: The (str: value) dict returned by build_update_sql.

    Raises:
      ValueError: If right_expr is not defined.
    """
    if right_expr:
      self.right_expr = right_expr
    elif not self.right_expr:
      raise ValueError('No right_expr.')
    self.bind_vars = bind_vars

  def build_update_sql(self, column_name):
    return '%s = %s' % (column_name, self.right_expr), self.bind_vars


class BaseSQLInsertExpr(BaseSQLUpdateExpr):
  """Return SQL for an INSERT VALUES expression.

  Expr is used in: INSERT ... VALUES (expr [, expr ...]) ...
  """

  def build_insert_sql(self):
    """Return SQL for an INSERT VALUES expression.

    Returns:
      A (str SQL, (str: value) dict bind_vars) pair.
    """
    raise NotImplementedError

  def build_update_sql(self, column_name):
    """Return the update SQL expr corresponding to the insert expr.

    Any insert expr should have a corresponding update expr; the reverse
    is not true ("failures = failures + 3" is an update expr, but
    "failures + 3" is not an insert expr).

    Args:
      column_name: Str name of column to update.

    Returns:
      A (str SQL, (str: value) dict bind_vars) pair.
    """
    insert_sql, bind_vars = self.build_insert_sql()
    return '%s = %s' % (column_name, insert_sql), bind_vars


class RawSQLInsertExpr(BaseSQLInsertExpr):
  """A parameterized insert expr.

  This is the simplest base class for an SQLInsertExpr.

  See BaseSQLInsertExpr.
  """
  insert_expr = None

  def __init__(self, insert_expr=None, **bind_vars):
    """Pass in the insert_expr and bind_vars.

    Either insert_expr or the insert_expr class variable should be
    defined.

    Args:
      insert_expr: Str SQL to be returned from build_insert_sql.
      **bind_vars: The (str: value) dict bind_vars to be returned from
        build_insert_sql.

    Raises:
      ValueError: If insert_expr is not defined.
    """
    if insert_expr:
      self.insert_expr = insert_expr
    elif not self.insert_expr:
      raise ValueError('No insert_expr.')
    self.bind_vars = bind_vars

  def build_insert_sql(self):
    return self.insert_expr, self.bind_vars


# Deprecated: Use RawSQLUpdateExpr instead.
class MySQLFunction(BaseSQLUpdateExpr):
  """A 'column = func' element of an update set clause.

  Example: "failures = failures + %(failures_1)s", {'failures_1': 3}
  """

  def __init__(self, func, bind_vars=None):
    """Init MySQLFunction.

    Args:
      func: Str of right-hand side of 'column = func', with formatting
        keys corresponding to bind vars.
      bind_vars: A (str: value) bind var dict corresponding
        to formatting keys found in func.
    """
    self.func = func
    self.bind_vars = bind_vars or {}

  def build_update_sql(self, column_name):
    """Return (str query, bind vars) for an UPDATE SET clause."""
    clause = '%s = %s' % (column_name, self.func)
    return clause, self.bind_vars


class BaseSQLSelectExpr(object):
  """Return SQL for a SELECT expression.

  Expr is used in: SELECT expr [, expr ...] FROM ...;
  """

  def select_sql(self, alias):
    """Return SQL for a SELECT expression.

    Args:
      alias: Str alias qualifier for column_name. If there is a column_name
        for this BaseSQLSelectExpr, it should be written as alias.column_name.

    Returns:
      Str SQL for a comma-delimited expr in a SELECT ... query.
    """
    raise NotImplementedError


class RawSQLSelectExpr(BaseSQLSelectExpr):
  """A SelectExpr that is raw SQL."""

  # Derived class must define select_expr.
  select_expr = None

  def __init__(self, select_expr=None):
    """Pass in the select_expr.

    Either select_expr or the select_expr class variable should be
    defined.

    Args:
      select_expr: Str SQL to be returned from select_sql.

    Raises:
      ValueError: If select_expr is not defined.
    """

    if select_expr:
      self.select_expr = select_expr
    elif not self.select_expr:
      raise ValueError('No select_expr.')

  def select_sql(self, alias):
    _ = alias
    return self.select_expr


class Count(RawSQLSelectExpr):

  select_expr = 'COUNT(1)'


# This is an overly restrictive class name. For instance,
# this could be used to create "FROM_UNIXTIME(time_created)",
# but this is not an aggregate.
class SQLAggregate(BaseSQLSelectExpr):
  """A 'func(column_name)' element of a select where clause.

  Example: "SUM(failures)".
  """

  function_name = None

  def __init__(self, column_name, function_name=None):
    """Init SQLAggregate.

    Either function_name or the function_name class variable should be
    defined.

    Args:
      column_name: Str column name.
      function_name: Optional str function name.

    Raises:
      ValueError: If function_name is not defined.
    """
    self.column_name = column_name
    if function_name:
      self.function_name = function_name
    elif not self.function_name:
      raise ValueError('No function_name.')

  def select_sql(self, alias):
    if alias:
      col_name = '%s.%s' % (alias, self.column_name)
    else:
      col_name = self.column_name
    clause = '%(function_name)s(%(col_name)s)' % dict(
        function_name=self.function_name, col_name=col_name)
    return clause


class Max(SQLAggregate):

  function_name = 'MAX'


class Min(SQLAggregate):

  function_name = 'MIN'


class Sum(SQLAggregate):

  function_name = 'SUM'


class BaseSQLWhereExpr(object):
  """Return SQL for a WHERE expression.

  Expr is used in WHERE clauses in various ways, like:
    ... WHERE expr [AND expr ...] ...;
  """

  def select_where_sql(self, column_name, counter):
    """Return SQL for a WHERE expression.

    Args:
      column_name: Name of a column on which this expr operates.
      counter: An itertools.count that returns a new number. This
        keeps the where clause from having colliding bind vars.

    Returns:
      A (str SQL, (str: value) bind_vars dict) pair.
    """
    raise NotImplementedError


class NullSafeNotValue(BaseSQLWhereExpr):
  """A null-safe inequality operator.

  For any [column] and [value] we do "NOT [column] <=> [value]".

  This is a bit of a hack because our framework assumes all operators are
  binary in nature (whereas we need a combination of unary and binary
  operators).
  """

  def __init__(self, value):
    self.value = value

  def build_where_sql(self, column_name, counter):
    bind_name = choose_bind_name(column_name, counter=counter)
    clause = 'NOT %(column_name)s <=> %%(%(bind_name)s)s' % dict(
        column_name=column_name, bind_name=bind_name)
    bind_vars = {bind_name: self.value}
    return clause, bind_vars


class SQLOperator(BaseSQLWhereExpr):
  """Base class for a column expression in a SQL WHERE clause."""

  op = None

  def __init__(self, value, op=None):
    """Constructor.

    Args:
      value: The value against which to compare the column, or an iterable of
          values if appropriate for the operator.
      op: The operator to use for comparison.
    """
    self.value = value
    if op:
      self.op = op

  def build_where_sql(self, column_name, counter):
    """Render this expression as a SQL string.

    Args:
      column_name: Name of the column being tested in this expression.
      counter: Instance of itertools.count supplying numeric suffixes for
          disambiguating bind_names, or None.  (See choose_bind_name
          for a discussion.)

    Returns:
      clause: The SQL expression, including a placeholder for the value.
      bind_vars: Dict mapping placeholder names to actual values.
    """

    op = self.op
    bind_name = choose_bind_name(column_name, counter=counter)
    clause = '%(column_name)s %(op)s %%(%(bind_name)s)s' % dict(
        column_name=column_name, op=op, bind_name=bind_name)
    bind_vars = {bind_name: self.value}
    return clause, bind_vars


class NotValue(SQLOperator):

  op = '!='

  def build_where_sql(self, column_name, counter):
    if self.value is None:
      return '%s IS NOT NULL' % column_name, {}
    return super(NotValue, self).build_where_sql(column_name, counter=counter)


class InValuesOperatorBase(SQLOperator):

  def __init__(self, *values):
    super(InValuesOperatorBase, self).__init__(values)

  def build_where_sql(self, column_name, counter):
    op = self.op
    bind_list = make_bind_list(column_name, self.value, counter=counter)
    in_clause = ', '.join(('%(' + key + ')s') for key, val in bind_list)
    clause = '%(column_name)s %(op)s (%(in_clause)s)' % dict(
        column_name=column_name, op=op, in_clause=in_clause)
    return clause, dict(bind_list)


# You rarely need to use InValues directly in your database classes.
# List and tuples are handled automatically by most database helper methods.
class InValues(InValuesOperatorBase):
  op = 'IN'


class NotInValues(InValuesOperatorBase):
  op = 'NOT IN'


class InValuesOrNull(InValues):

  def build_where_sql(self, column_name, counter):
    clause, bind_vars = super(InValuesOrNull, self).build_where_sql(
        column_name, counter=counter)
    clause = '(%s OR %s IS NULL)' % (clause, column_name)
    return clause, bind_vars


class BetweenValues(SQLOperator):

  def __init__(self, value0, value1):
    super(BetweenValues, self).__init__((value0, value1), 'BETWEEN')

  def build_where_sql(self, column_name, counter):
    op = self.op
    bind_list = make_bind_list(column_name, self.value, counter=counter)
    between_clause = ' AND '.join(('%(' + key + ')s') for key, val in bind_list)
    clause = '%(column_name)s %(op)s %(between_clause)s' % dict(
        column_name=column_name, op=op, between_clause=between_clause)
    return clause, dict(bind_list)


class OrValues(SQLOperator):

  def __init__(self, *values):
    if not values or len(values) == 1:
      raise ValueError('Two or more arguments expected.')
    super(OrValues, self).__init__(values, 'OR')

  def build_where_sql(self, column_name, counter):
    condition_list = []
    bind_vars = {}

    for v in self.value:
      if isinstance(v, BaseSQLWhereExpr):
        clause, clause_bind_vars = v.build_where_sql(
            column_name, counter=counter)
        update_bind_vars(bind_vars, clause_bind_vars)
        condition_list.append(clause)
      else:
        bind_name = choose_bind_name(column_name, counter=counter)
        bind_vars[bind_name] = v
        condition_list.append('%s = %%(%s)s' % (column_name, bind_name))

    or_clause = '((' + ') OR ('.join(condition_list) + '))'
    return or_clause, bind_vars


class LikeValue(SQLOperator):
  op = 'LIKE'


class GreaterThanValue(SQLOperator):
  op = '>'


class GreaterThanOrEqualToValue(SQLOperator):
  op = '>='


class LessThanValue(SQLOperator):
  op = '<'


class LessThanOrEqualToValue(SQLOperator):
  op = '<='


class ModuloEquals(SQLOperator):
  """column % modulus = value."""

  def __init__(self, modulus, value):
    super(ModuloEquals, self).__init__(value, '%')
    self.modulus = modulus

  def build_where_sql(self, column, counter):
    mod_bind_name = choose_bind_name('modulus', counter=counter)
    val_bind_name = choose_bind_name(column, counter=counter)
    sql = '(%(column)s %%%% %%(%(mod_bind_name)s)s) = %%(%(val_bind_name)s)s'
    return (sql % {'column': column,
                   'mod_bind_name': mod_bind_name,
                   'val_bind_name': val_bind_name},
            {mod_bind_name: self.modulus,
             val_bind_name: self.value})


class Expression(SQLOperator):
  """Operator where value is raw SQL rather than a variable.

  Example: "failures < attempts" rather than "failures < 3".
  """

  def build_where_sql(self, column_name, counter):
    op = self.op
    value = str(self.value)
    clause = '%(column_name)s %(op)s %(value)s' % dict(
        column_name=column_name, op=op, value=value)
    return clause, {}


class IsNullOrEmptyString(BaseSQLWhereExpr):

  def build_where_sql(self, column_name, counter):
    # Note: mysql treats '' the same as '   '
    _ = counter
    return "(%s IS NULL OR %s = '')" % (column_name, column_name), {}


class IsNullValue(BaseSQLWhereExpr):

  def build_where_sql(self, column_name, counter):
    _ = counter
    return '%s IS NULL' % column_name, {}


class IsNotNullValue(BaseSQLWhereExpr):

  def build_where_sql(self, column_name, counter):
    _ = counter
    return '%s IS NOT NULL' % column_name, {}


class Flag(BaseSQLUpdateExpr, BaseSQLWhereExpr):
  """A class with flags_present and flags_absent.

  This can create SELECT WHERE clause sql like "flags & 0x3 = 0x1" and
  UPDATE SET clause sql like "flags = (flags | 0x1) & ^0x2".
  """

  def __init__(self, flags_present=0x0, flags_absent=0x0):
    if flags_present & flags_absent:
      raise ValueError(
          'flags_present (0x%016x) and flags_absent (0x%016x)'
          ' overlap: 0x%016x' % (
              flags_present, flags_absent, flags_present & flags_absent))
    self.mask = flags_present | flags_absent
    self.value = flags_present
    self.flags_present = flags_present
    self.flags_absent = flags_absent
    # These are poorly named and should be deprecated.
    self.flags_to_remove = flags_absent
    self.flags_to_add = flags_present

  def __repr__(self):
    return '%s(flags_present=0x%X, flags_absent=0x%X)' % (
        self.__class__.__name__, self.flags_to_add, self.flags_to_remove)

  def __or__(self, other):
    return Flag(flags_present=self.flags_to_add | other.flags_to_add,
                flags_absent=self.flags_to_remove | other.flags_to_remove)

  def __eq__(self, other):
    if not isinstance(other, Flag):
      return False
    return self.mask == other.mask and self.value == other.value

  def __ne__(self, other):
    return not self.__eq__(other)

  def build_where_sql(self, column_name, counter):
    """Return SELECT WHERE clause and bind_vars.

    Args:
      column_name: Str name of SQL column.
      counter: An itertools.count to keep bind variable names from colliding.

    Returns:
      A (str clause, (str: obj) bind_vars dict) pair.
    """
    bind_name_mask = choose_bind_name(column_name + '_mask', counter=counter)
    bind_name_value = choose_bind_name(column_name + '_value', counter=counter)

    clause = (
        '{column_name} & %({bind_name_mask})s = '
        '%({bind_name_value})s'.format(
            bind_name_mask=bind_name_mask, bind_name_value=bind_name_value,
            column_name=column_name))

    bind_vars = {
        bind_name_mask: self.mask,
        bind_name_value: self.value
    }
    return clause, bind_vars

  def build_update_sql(self, column_name='flags'):
    """Return UPDATE WHERE clause and bind_vars.

    Args:
      column_name: Str name of SQL column.

    Returns:
      A (str clause, (str: obj) bind_vars dict) pair.
    """
    clause = (
        '%(column_name)s = (%(column_name)s | '
        '%%(update_%(column_name)s_add)s) & '
        '~%%(update_%(column_name)s_remove)s') % dict(
            column_name=column_name)
    bind_vars = {
        'update_%s_add' % column_name: self.flags_to_add,
        'update_%s_remove' % column_name: self.flags_to_remove}
    return clause, bind_vars


def make_flag(flag_mask, value):
  if value:
    return Flag(flags_present=flag_mask)
  else:
    return Flag(flags_absent=flag_mask)


def update_bind_vars(bind_vars, new_bind_vars):
  """Merge new_bind_vars into bind_vars, disallowing duplicates."""
  for k, v in new_bind_vars.iteritems():
    if k in bind_vars:
      raise ValueError(
          'Duplicate bind vars: cannot add %s to %s.' %
          (k, sorted(bind_vars)))
    bind_vars[k] = v


class Increment(BaseSQLUpdateExpr):

  def __init__(self, amount):
    self.amount = amount

  def build_update_sql(self, column_name):
    clause = (
        '%(column_name)s = (%(column_name)s + '
        '%%(update_%(column_name)s_amount)s)') % dict(
            column_name=column_name)
    bind_vars = {'update_%s_amount' % column_name: self.amount}
    return clause, bind_vars


def _normalize_order_by(order_by):
  """Return str list list."""
  if not isinstance(order_by, (tuple, list)):
    order_by = order_by,
  words_list = []
  for item in order_by:
    if not isinstance(item, (tuple, list)):
      item = item,
    words_list.append(' '.join(item).split())
  return words_list
