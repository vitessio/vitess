"""Helper classes for building queries.

Helper classes and fucntions for building queries.
"""

import itertools
import pprint

#TODO: add unit-tests for the methods and classes.
#TODO: integration with SQL Alchemy ?

class DBRow(object):

  def __init__(self, column_names, row_tuple, **overrides):
    self.__dict__ = dict(zip(column_names, row_tuple), **overrides)

  def __repr__(self):
    return pprint.pformat(self.__dict__, 4)


def select_clause(select_columns, table_name, alias=None, cols=None, order_by_cols=None):
  """Build the select clause for a query."""

  if alias:
    return 'SELECT %s FROM %s %s' % (
        colstr(select_columns, alias, cols, order_by_cols=order_by_cols),
        table_name, alias)
  return 'SELECT %s FROM %s' % (
        colstr(select_columns, alias, cols, order_by_cols=order_by_cols),
        table_name)


def colstr(select_columns, alias=None, cols=None, bind=None, order_by_cols=None):
  if not cols:
    cols = select_columns

  # in the case of a scatter/gather, prepend these columns to facilitate an in-code
  # sort - after that, we can just strip these off and process normally
  if order_by_cols:
    # avoid altering a class variable
    cols = cols[:]
    for order_col in reversed(order_by_cols):
      if type(order_col) in (tuple, list):
        cols.insert(0, order_col[0])
      else:
        cols.insert(0, order_col)

  if not bind:
    bind = cols

  def prefix(col):
    if isinstance(col, SQLAggregate):
      return col.sql()

    if alias and '.' not in col:
      col = '%s.%s' % (alias, col)

    return col
  return ', '.join([prefix(c) for c in cols if c in bind])


def build_values_clause(columns, bind_values):
  """Builds values clause for an insert query."""

  clause_parts = []
  bind_list = []
  for column in columns:
    if (column in ('time_created', 'time_updated') and
        column not in bind_values):
      bind_list.append(column)
      clause_parts.append('%%(%s)s' % column)
      bind_values[column] = int(time.time())
    elif column in bind_values:
      bind_list.append(column)
      if type(bind_values[column]) == MySQLFunction:
        clause_parts.append(bind_values[column])
        bind_values.update(column.bind_vals)
      else:
        clause_parts.append('%%(%s)s' % column)
  return ', '.join(clause_parts), bind_list


def build_in(column, items, alt_name=None, counter=None):
  """Build SQL IN statement and bind hash for use with pyformat."""

  if not items:
    raise ValueError('Called with empty "items"')

  base = alt_name if alt_name else column
  bind_list = make_bind_list(base, items, counter=counter)

  return ('%s IN (%s)' % (column,
                          str.join(',', ['%(' + pair[0] + ')s'
                                         for pair in bind_list])),
          dict(bind_list))


def build_order_clause(order_by):
  """order_by could be a list, tuple or string."""

  if not order_by:
    return ''

  if type(order_by) not in (tuple, list):
    order_by = (order_by,)

  subclause_list = []
  for subclause in order_by:
    if type(subclause) in (tuple, list):
      subclause = ' '.join(subclause)
    subclause_list.append(subclause)

  return 'ORDER BY %s' % ', '.join(subclause_list)


def build_group_clause(group_by):
  """Build group_by clause for a query."""

  if not group_by:
    return ''

  if type(group_by) not in (tuple, list):
    group_by = (group_by,)

  return 'GROUP BY %s' % ', '.join(group_by)


def build_limit_clause(limit):
  """Build limit clause for a query."""

  if not limit:
    return '', {}

  if not isinstance(limit, tuple):
    limit = (limit,)

  bind_vars = {'limit_row_count': limit[0]}
  if len(limit) == 1:
    return 'LIMIT %(limit_row_count)s', bind_vars

  bind_vars = {'limit_offset': limit[0],
               'limit_row_count': limit[1]}
  return 'LIMIT %(limit_offset)s,%(limit_row_count)s', bind_vars


def build_where_clause(column_value_pairs):
  """Build the where clause for a query."""

  condition_list = []
  bind_vars = {}

  counter = itertools.count(1)

  def update_bindvars(newvars):
    for k, v in newvars.iteritems():
      if k in bind_vars:
        raise ValueError('Duplicate bind vars: cannot add %r to %r',
                         newvars, bind_vars)
      bind_vars[k] = v

  for column, value in column_value_pairs:
    if isinstance(value, (Flag, SQLOperator, NullSafeNotValue)):
      clause, clause_bind_vars = value.build_sql(column, counter=counter)
      update_bindvars(clause_bind_vars)
      condition_list.append(clause)
    elif isinstance(value, (tuple, list, set)):
      if value:
        in_clause, in_bind_variables = build_in(column, value,
                                                counter=counter)
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


def select_by_columns_query(select_column_list, table_name, column_value_pairs=None,
                            order_by=None, group_by=None, limit=None,
                            for_update=False,client_aggregate=False,
                            vt_routing_info=None, **columns):

  # generate WHERE clause and bind variables
  if not column_value_pairs:
    column_value_pairs = columns.items()
    column_value_pairs.sort()

  if client_aggregate:
    clause_list = [select_clause(select_column_list, table_name,
                                 order_by_cols=order_by)]
  else:
    clause_list = [select_clause(select_column_list, table_name)]

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
                           order_by=None, **update_columns):
  if not update_column_value_pairs:
    update_column_value_pairs = update_columns.items()
    update_column_value_pairs.sort()

  clause_list = []
  bind_vals = {}
  for i, (column, value) in enumerate(update_column_value_pairs):
    if isinstance(value, (Flag, Increment, MySQLFunction)):
      clause, clause_bind_vals = value.build_update_sql(column)
      clause_list.append(clause)
      bind_vals.update(clause_bind_vals)
    else:
      clause_list.append('%s = %%(update_set_%s)s' % (column, i))
      bind_vals['update_set_%s' % i] = value

  if not clause_list:
    # this would be invalid syntax anyway, let's raise a nicer exception
    raise ValueError(
        'Expected nonempty update_column_value_pairs. Got: %r'
        % update_column_value_pairs)

  set_clause = ', '.join(clause_list)

  if not where_column_value_pairs:
    # same as above. We could allow for no where clause,
    # but this is a notoriously error-prone construct, so, no.
    raise ValueError(
        'Expected nonempty where_column_value_pairs. Got: %r'
        % where_column_value_pairs)

  where_clause, where_bind_vals = build_where_clause(where_column_value_pairs)
  bind_vals.update(where_bind_vals)

  query = ('UPDATE %(table)s SET %(set_clause)s WHERE %(where_clause)s'
           % {'table': table_name, 'set_clause': set_clause,
              'where_clause': where_clause})

  additional_clause = []
  if order_by:
    additional_clause.append(build_order_clause(order_by))
  if limit:
    limit_clause, limit_bind_vars = build_limit_clause(limit)
    additional_clause.append(limit_clause)
    bind_vals.update(limit_bind_vars)

  query += ' ' + ' '.join(additional_clause)
  return query, bind_vals


def delete_by_columns_query(table_name, where_column_value_pairs=None,
                            limit=None):
  where_clause, bind_vars = build_where_clause(where_column_value_pairs)
  limit_clause, limit_bind_vars = build_limit_clause(limit)
  bind_vars.update(limit_bind_vars)

  query = (
      'DELETE FROM %(table_name)s WHERE %(where_clause)s %(limit_clause)s' %
      {'table_name': table_name, 'where_clause': where_clause,
      'limit_clause': limit_clause})
  return query, bind_vars


def insert_query(table_name, columns_list, **bind_variables):
  values_clause, bind_list = sql_builder.build_values_clause(columns_list,
                                                             bind_variables)


  query = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name,
                                               colstr(columns_list,
                                                     bind=bind_list),
                                               values_clause)
  return query, bind_variables


def choose_bind_name(base, counter=None):
  if counter:
    base += '_%d' % counter.next()
    return base


class MySQLFunction(object):

  def __init__(self, func, bind_vals=()):
    self.bind_vals = bind_vals
    self.func = func

  def __str__(self):
    return self.func

  def build_update_sql(self, column):
    clause = '%s = %s' % (column, self.func)
    return clause, self.bind_vals


class SQLAggregate(object):

  def __init__(self, function_name, column_name):
    self.function_name = function_name
    self.column_name = column_name

  def sql(self):
    clause = '%(function_name)s(%(column_name)s)' % vars(self)
    return clause


def Sum(column_name):
  return SQLAggregate('SUM', column_name)


def Max(column_name):
  return SQLAggregate('MAX', column_name)


def Min(column_name):
  return SQLAggregate('MIN', column_name)


# A null-safe inequality operator. For any [column] and [value] we do
# "NOT [column] <=> [value]".
#
# This is a bit of a hack because our framework assumes all operators are
# binary in nature (whereas we need a combination of unary and binary
# operators).
#
# This is only enabled for use in the where clause. For use in select or
# update you'll need to do some additional work.
class NullSafeNotValue(object):

  def __init__(self, value):
    self.value = value

  def build_sql(self, column_name, counter=None):
    bind_name = choose_bind_name(column_name, counter=counter)
    clause = 'NOT %(column_name)s <=> %%(%(bind_name)s)s' % vars()
    bind_vars = {bind_name: self.value}
    return clause, bind_vars


class SQLOperator(object):
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

  def build_sql(self, column_name, counter=None):
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

    clause = '%(column_name)s %(op)s %%(%(bind_name)s)s' % vars()
    bind_vars = {bind_name: self.value}

    return clause, bind_vars


class NotValue(SQLOperator):

  def __init__(self, value):
    super(NotValue, self).__init__(value, '!=')

  def build_sql(self, column_name, counter=None):
    if self.value is None:
      return '%s IS NOT NULL' % column_name, {}
    return super(NotValue, self).build_sql(column_name, counter=counter)


class InValuesOperatorBase(SQLOperator):

  def __init__(self, op, *values):
    super(InValuesOperatorBase, self).__init__(values, op)

  def build_sql(self, column_name, counter=None):
    op = self.op
    bind_list = make_bind_list(column_name, self.value, counter=counter)
    in_clause = ', '.join(('%(' + key + ')s') for key, val in bind_list)
    clause = '%(column_name)s %(op)s (%(in_clause)s)' % vars()
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

  def build_sql(self, column_name, counter=None):
    clause, bind_vars = super(InValuesOrNull, self).build_sql(column_name,
                                                              counter=counter)
    clause = '(%s OR %s IS NULL)' % (clause, column_name)
    return clause, bind_vars


class BetweenValues(SQLOperator):

  def __init__(self, value0, value1):
    if value0 < value1:
      super(BetweenValues, self).__init__((value0, value1), 'BETWEEN')
    else:
      super(BetweenValues, self).__init__((value1, value0), 'BETWEEN')

  def build_sql(self, column_name, counter=None):
    op = self.op
    bind_list = make_bind_list(column_name, self.value, counter=counter)
    between_clause = ' AND '.join(('%(' + key + ')s') for key, val in bind_list)
    clause = '%(column_name)s %(op)s %(between_clause)s' % vars()
    return clause, dict(bind_list)


class OrValues(SQLOperator):

  def __init__(self, *values):
    if not values or len(values) == 1:
      raise errors.IllegalArgumentException

    super(OrValues, self).__init__(values, 'OR')

  def build_sql(self, column_name, counter=None):
    condition_list = []
    bind_vars = {}
    if counter is None:
      counter = itertools.count(1)

    for v in self.value:
      if isinstance(v, (SQLOperator, Flag, NullSafeNotValue)):
        clause, clause_bind_vars = v.build_sql(column_name, counter=counter)
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

  def build_sql(self, column, counter=None):
    mod_bind_name = choose_bind_name('modulus', counter=counter)
    val_bind_name = choose_bind_name(column, counter=counter)
    sql = '(%(column)s %%%% %%(%(mod_bind_name)s)s) = %%(%(val_bind_name)s)s'
    return (sql % {'column': column,
                   'mod_bind_name': mod_bind_name,
                   'val_bind_name': val_bind_name},
            {mod_bind_name: self.modulus,
             val_bind_name: self.value})


class Expression(SQLOperator):

  def build_sql(self, column_name, counter=None):
    op = self.op
    value = str(self.value)
    clause = '%(column_name)s %(op)s %(value)s' % vars()
    return clause, {}


class IsNullOrEmptyString(SQLOperator):

  def __init__(self):
    super(IsNullOrEmptyString, self).__init__('', '')

  def build_sql(self, column_name, counter=None):
    # mysql treats '' the same as '   '
    return "(%s IS NULL OR %s = '')" % (column_name, column_name), {}


class IsNullValue(SQLOperator):

  def __init__(self):
    super(IsNullValue, self).__init__('NULL', 'IS')

  def build_sql(self, column_name, counter=None):
    return '%s IS NULL' % column_name, {}


class IsNotNullValue(SQLOperator):

  def __init__(self):
    super(IsNotNullValue, self).__init__('NULL', 'IS NOT')

  def build_sql(self, column_name, counter=None):
    return '%s IS NOT NULL' % column_name, {}


class Flag(object):

  def __init__(self, flags_present=0x0, flags_absent=0x0):
    if flags_present & flags_absent:
      raise errors.InternalError(
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

  def sql(self, column_name='flags'):
    return '%s & %s = %s' % (column_name, self.mask, self.value)

  def build_sql(self, column_name='flags', counter=None):
    bind_name_mask = choose_bind_name(column_name + '_mask', counter=counter)
    bind_name_value = choose_bind_name(column_name + '_value', counter=counter)

    clause = '{column_name} & %({bind_name_mask})s = %({bind_name_value})s'.format(
        bind_name_mask=bind_name_mask, bind_name_value=bind_name_value,
        column_name=column_name)

    bind_vars = {
        bind_name_mask: self.mask,
        bind_name_value: self.value
    }
    return clause, bind_vars

  def update_sql(self, column_name='flags'):
    return '%s = (%s | %s) & ~%s' % (
        column_name, column_name, self.flags_to_add, self.flags_to_remove)

  def build_update_sql(self, column_name='flags'):
    clause = ('%(column_name)s = (%(column_name)s | '
              '%%(update_%(column_name)s_add)s) & '
              '~%%(update_%(column_name)s_remove)s') % vars(        )
    bind_vars = {
        'update_%s_add' % column_name: self.flags_to_add, 'update_%s_remove' %
        column_name: self.flags_to_remove}
    return clause, bind_vars


def make_flag(flag_mask, value):
  if value:
    return Flag(flags_present=flag_mask)
  else:
    return Flag(flags_absent=flag_mask)


class Increment(object):

  def __init__(self, amount):
    self.amount = amount

  def build_update_sql(self, column_name):
    clause = ('%(column_name)s = (%(column_name)s + '
              '%%(update_%(column_name)s_amount)s)') % vars()
    bind_vars = {'update_%s_amount' % column_name: self.amount}
    return clause, bind_vars
