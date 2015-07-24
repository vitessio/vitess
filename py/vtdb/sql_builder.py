"""Helper classes for building queries.

Helper classes and fucntions for building queries.
"""

import itertools
import pprint
import time

# TODO(dumbunny): add unit-tests for the methods and classes.
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
    select_columns, table_name, alias=None, order_by_cols=None):
  """Build the select clause for a query."""

  if alias:
    return 'SELECT %s FROM %s %s' % (
        colstr(select_columns, alias, order_by_cols=order_by_cols),
        table_name, alias)
  return 'SELECT %s FROM %s' % (
      colstr(select_columns, alias, order_by_cols=order_by_cols),
      table_name)


def colstr(
    select_columns, alias=None, bind=None, order_by_cols=None):
  """Return str sql for select columns."""
  # avoid altering select_columns
  cols = select_columns[:]

  # in the case of a scatter/gather, prepend these columns to
  # facilitate an in-code sort - after that, we can just strip these
  # off and process normally.
  if order_by_cols:
    for order_col in reversed(order_by_cols):
      if type(order_col) in (tuple, list):
        cols.insert(0, order_col[0])
      else:
        cols.insert(0, order_col)

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
  """Builds values clause for an insert query.

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
      clause_parts.append('%%(%s)s' % column)
    elif column in ('time_created', 'time_updated'):
      bind_list.append(column)
      clause_parts.append('%%(%s)s' % column)
      bind_vars[column] = int(time.time())
  return ', '.join(clause_parts), bind_list


def build_in(column, items, alt_name=None, counter=None):
  """Build SQL IN statement and bind hash for use with pyformat.

  Args:
    column: Str column name.
    items: List of 1 or more values for IN statement.
    alt_name: Name to use for format token keys. Use column by default.

  Returns:
    Str comma-delimited SQL format, (str: value) dict corresponding
      to format tokens.
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
    order_by: A list, tuple or string. If a list or tuple, a
      comma-delimited str from each element is returned.

  Returns:
    The str 'ORDER BY ...' clause or ''.
  """
  if not order_by:
    return ''

  if not isinstance(order_by, (tuple, list)):
    order_by = (order_by,)
  return 'ORDER BY %s' % ', '.join(order_by)


def build_group_clause(group_by):
  """Build group_by clause for a query."""

  if not group_by:
    return ''

  if type(group_by) not in (tuple, list):
    group_by = (group_by,)

  return 'GROUP BY %s' % ', '.join(group_by)


def build_limit_clause(limit):
  """Build limit clause for a query.

  Get a LIMIT clause and bind vars. The LIMIT clause will have either
  the form "LIMIT count" "LIMIT offset, count", or be the empty string.
  or the empty string.

  Args:
    None, int or 1- or 2-element list or tuple.

  Returns:
    A (str LIMIT clause SQL, bind vars) pair.
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
  """Build the where clause for a query."""

  condition_list = []
  bind_vars = {}

  counter = itertools.count(1)

  def update_bindvars(newvars):
    for k, v in newvars.iteritems():
      if k in bind_vars:
        raise ValueError('Duplicate bind vars: cannot add %r to %r' %
                         (newvars, bind_vars))
      bind_vars[k] = v

  for column, value in column_value_pairs:
    if isinstance(value, BaseSQLWhereExpr):
      clause, clause_bind_vars = value.build_where_sql(column, counter=counter)
      update_bindvars(clause_bind_vars)
      condition_list.append(clause)
    elif isinstance(value, (tuple, list, set)):
      if value:
        if isinstance(value, set):
          value = sorted(value)
        in_clause, in_bind_variables = build_in(
            column, value, counter=counter)
        update_bindvars(in_bind_variables)
        condition_list.append(in_clause)
      else:
        condition_list.append('1 = 0')
    else:
      bind_name = choose_bind_name(column, counter=counter)
      update_bindvars({bind_name: value})
      condition_list.append('%s = %%(%s)s' % (column, bind_name))

  if not bind_vars:
    bind_vars = dict(column_value_pairs)

  where_clause = ' AND '.join(condition_list)
  return where_clause, bind_vars


def select_by_columns_query(
    select_column_list, table_name, column_value_pairs=None,
    order_by=None, group_by=None, limit=None, for_update=False,
    client_aggregate=False, vt_routing_info=None):
  """Get query and bind vars for a select statement."""

  if client_aggregate:
    clause_list = [select_clause(select_column_list, table_name,
                                 order_by_cols=order_by)]
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
    bind_vars.update(limit_bind_vars)
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
    order_by: Str or str list of order by exprs.

  Returns:
    A (str update SQL query, (str: value) dict) pair.
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
  bind_vals = {}
  for i, (column, value) in enumerate(update_column_value_pairs):
    if isinstance(value, BaseSQLUpdateExpr):
      clause, clause_bind_vals = value.build_update_sql(column)
      clause_list.append(clause)
      bind_vals.update(clause_bind_vals)
    else:
      clause_list.append('%s = %%(update_set_%s)s' % (column, i))
      bind_vals['update_set_%s' % i] = value

  set_clause = ', '.join(clause_list)

  where_clause, where_bind_vals = build_where_clause(where_column_value_pairs)
  bind_vals.update(where_bind_vals)

  query = ('UPDATE %(table)s SET %(set_clause)s WHERE %(where_clause)s'
           % {'table': table_name, 'set_clause': set_clause,
              'where_clause': where_clause})

  additional_clauses = []
  if order_by:
    additional_clauses.append(build_order_clause(order_by))
  if limit:
    limit_clause, limit_bind_vars = build_limit_clause(limit)
    additional_clauses.append(limit_clause)
    bind_vals.update(limit_bind_vars)
  if additional_clauses:
    query += ' ' + ' '.join(additional_clauses)
  return query, bind_vals


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
  bind_vars.update(limit_bind_vars)

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


def build_aggregate_query(table_name, id_column_name, sort_func='min'):
  query_clause = 'SELECT %(id_col)s FROM %(table_name)s ORDER BY %(id_col)s'
  if sort_func == 'max':
    query_clause += ' DESC'
  query_clause += ' LIMIT 1'
  query = query_clause % {'id_col': id_column_name, 'table_name': table_name}
  return query


def build_count_query(table_name, column_value_pairs):
  where_clause, bind_vars = build_where_clause(column_value_pairs)
  query = 'SELECT count(1) FROM %s WHERE %s' % (table_name, where_clause)
  return query, bind_vars


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
  """Return SQL for an UPDATE expression."""

  def build_update_sql(self, column_name):
    """Return SQL and bind_vars for an UPDATE SET expression.

    Args:
      column_name: Str name of column to update.

    Returns:
      A (str SQL, (str: value) dict bind_vars) pair.
    """
    raise NotImplementedError


class MySQLFunction(BaseSQLUpdateExpr):
  """A 'column = func' element of an update set clause."""

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

  def build_update_sql(self, column):
    """Return (str query, bind vars) for an UPDATE SET clause."""
    clause = '%s = %s' % (column, self.func)
    return clause, self.bind_vars


class BaseSQLSelectExpr(object):

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

  def select_sql(self, alias):
    _ = alias
    if not self.select_expr:
      raise ValueError('No select_expr.')
    return self.select_expr


class Count(RawSQLSelectExpr):

  select_expr = 'COUNT(1)'


class SQLAggregate(BaseSQLSelectExpr):
  """A 'func(column_name)' element of a select where clause."""

  # Derived class must define function_name.
  function_name = None

  def __init__(self, column_name):
    """Init SQLAggregate.

    Args:
      column_name: Str column name.
    """
    self.column_name = column_name

  def select_sql(self, alias):
    if alias:
      col_name = '%s.%s' % (alias, self.column_name)
    else:
      col_name = self.column_name
    if not self.function_name:
      raise ValueError('No function_name.')
    clause = '%(function_name)s(%(col_name)s)' % dict(
        function_name=self.function_name, col_name=col_name)
    return clause


class Max(SQLAggregate):

  function_name = 'MAX'


class Min(SQLAggregate):

  function_name = 'MIN'


class Sum(SQLAggregate):

  function_name = 'SUM'


# TODO(dumbunny): Add more tests.
class BaseSQLWhereExpr(object):

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

  def build_where_sql(self, column_name, counter=None):
    bind_name = choose_bind_name(column_name, counter=counter)
    clause = 'NOT %(column_name)s <=> %%(%(bind_name)s)s' % dict(
        column_name=column_name, bind_name=bind_name)
    bind_vars = {bind_name: self.value}
    return clause, bind_vars


class SQLOperator(BaseSQLWhereExpr):
  """Base class for a column expression in a SQL WHERE clause."""

  def __init__(self, value, op):
    """Constructor.

    Args:
      value: The value against which to compare the column, or an iterable of
          values if appropriate for the operator.
      op: The operator to use for comparison.
    """

    self.value = value
    self.op = op

  def build_where_sql(self, column_name, counter=None):
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

  def __init__(self, value):
    super(NotValue, self).__init__(value, '!=')

  def build_where_sql(self, column_name, counter=None):
    if self.value is None:
      return '%s IS NOT NULL' % column_name, {}
    return super(NotValue, self).build_where_sql(column_name, counter=counter)


class InValuesOperatorBase(SQLOperator):

  def __init__(self, op, *values):
    super(InValuesOperatorBase, self).__init__(values, op)

  def build_where_sql(self, column_name, counter=None):
    op = self.op
    bind_list = make_bind_list(column_name, self.value, counter=counter)
    in_clause = ', '.join(('%(' + key + ')s') for key, val in bind_list)
    clause = '%(column_name)s %(op)s (%(in_clause)s)' % dict(
        column_name=column_name, op=op, in_clause=in_clause)
    return clause, dict(bind_list)


# You rarely need to use InValues directly in your database classes.
# List and tuples are handled automatically by most database helper methods.
class InValues(InValuesOperatorBase):

  def __init__(self, *values):
    super(InValues, self).__init__('IN', *values)


class NotInValues(InValuesOperatorBase):

  def __init__(self, *values):
    super(NotInValues, self).__init__('NOT IN', *values)


class InValuesOrNull(InValues):

  def build_where_sql(self, column_name, counter=None):
    clause, bind_vars = super(InValuesOrNull, self).build_where_sql(
        column_name, counter=counter)
    clause = '(%s OR %s IS NULL)' % (clause, column_name)
    return clause, bind_vars


class BetweenValues(SQLOperator):

  def __init__(self, value0, value1):
    super(BetweenValues, self).__init__((value0, value1), 'BETWEEN')

  def build_where_sql(self, column_name, counter=None):
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

  def build_where_sql(self, column_name, counter=None):
    condition_list = []
    bind_vars = {}
    if counter is None:
      counter = itertools.count(1)

    for v in self.value:
      if isinstance(v, BaseSQLWhereExpr):
        clause, clause_bind_vars = v.build_where_sql(
            column_name, counter=counter)
        bind_vars.update(clause_bind_vars)
        condition_list.append(clause)
      else:
        bind_name = choose_bind_name(column_name, counter=counter)
        bind_vars[bind_name] = v
        condition_list.append('%s = %%(%s)s' % (column_name, bind_name))

    or_clause = '((' + ') OR ('.join(condition_list) + '))'
    return or_clause, bind_vars


class LikeValue(SQLOperator):

  def __init__(self, value):
    super(LikeValue, self).__init__(value, 'LIKE')


class GreaterThanValue(SQLOperator):

  def __init__(self, value):
    super(GreaterThanValue, self).__init__(value, '>')


class GreaterThanOrEqualToValue(SQLOperator):

  def __init__(self, value):
    super(GreaterThanOrEqualToValue, self).__init__(value, '>=')


class LessThanValue(SQLOperator):

  def __init__(self, value):
    super(LessThanValue, self).__init__(value, '<')


class LessThanOrEqualToValue(SQLOperator):

  def __init__(self, value):
    super(LessThanOrEqualToValue, self).__init__(value, '<=')


class ModuloEquals(SQLOperator):
  """column % modulus = value."""

  def __init__(self, modulus, value):
    super(ModuloEquals, self).__init__(value, '%')
    self.modulus = modulus

  def build_where_sql(self, column, counter=None):
    mod_bind_name = choose_bind_name('modulus', counter=counter)
    val_bind_name = choose_bind_name(column, counter=counter)
    sql = '(%(column)s %%%% %%(%(mod_bind_name)s)s) = %%(%(val_bind_name)s)s'
    return (sql % {'column': column,
                   'mod_bind_name': mod_bind_name,
                   'val_bind_name': val_bind_name},
            {mod_bind_name: self.modulus,
             val_bind_name: self.value})


class Expression(SQLOperator):

  def build_where_sql(self, column_name, counter=None):
    op = self.op
    value = str(self.value)
    clause = '%(column_name)s %(op)s %(value)s' % dict(
        column_name=column_name, op=op, value=value)
    return clause, {}


class IsNullOrEmptyString(SQLOperator):

  def __init__(self):
    super(IsNullOrEmptyString, self).__init__('', '')

  def build_where_sql(self, column_name, counter=None):
    # mysql treats '' the same as '   '
    return "(%s IS NULL OR %s = '')" % (column_name, column_name), {}


class IsNullValue(SQLOperator):

  def __init__(self):
    super(IsNullValue, self).__init__('NULL', 'IS')

  def build_where_sql(self, column_name, counter=None):
    return '%s IS NULL' % column_name, {}


class IsNotNullValue(SQLOperator):

  def __init__(self):
    super(IsNotNullValue, self).__init__('NULL', 'IS NOT')

  def build_where_sql(self, column_name, counter=None):
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
    self.flags_to_remove = flags_absent
    self.flags_to_add = flags_present

  def __repr__(self):
    return '%s(flags_present=0x%X, flags_absent=0x%X)' % (
        self.__class__.__name__, self.flags_to_add, self.flags_to_remove)

  def __or__(self, other):
    return Flag(flags_present=self.flags_to_add | other.flags_to_add,
                flags_absent=self.flags_to_remove | other.flags_to_remove)

  # Beware: this doesn't switch the present and absent flags, it makes
  # an object that *clears all the flags* that the operand would touch.
  def __invert__(self):
    return Flag(flags_absent=self.mask)

  def __eq__(self, other):
    if not isinstance(other, Flag):
      return False

    return (self.mask == other.mask
            and self.value == other.value
            and self.flags_to_add == other.flags_to_add
            and self.flags_to_remove == other.flags_to_remove)

  def build_where_sql(self, column_name='flags', counter=None):
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
