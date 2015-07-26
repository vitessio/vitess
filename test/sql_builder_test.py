#!/usr/bin/env python
# coding: utf-8

import itertools
import unittest

from vtdb import sql_builder
from vtdb import vtrouting

import utils


class TestBuildValuesClause(unittest.TestCase):
  """Test sql_builder.build_values_clause."""

  def test_two_columns(self):
    sql, column_names = sql_builder.build_values_clause(
        columns=['col_a', 'col_b'], bind_vars=dict(col_a=1, col_b=2))
    self.assertEqual(sql, '%(col_a)s, %(col_b)s')
    self.assertEqual(column_names, ['col_a', 'col_b'])
    sql, column_names = sql_builder.build_values_clause(
        columns=['col_a', 'col_b'], bind_vars=dict(col_a=1))
    self.assertEqual(sql, '%(col_a)s')
    self.assertEqual(column_names, ['col_a'])

  def test_with_time_created_and_time_updated(self):
    """Check time_created and time_updated are added as needed."""
    bind_vars = dict(time_created=1, col_b=2)
    sql, column_names = sql_builder.build_values_clause(
        columns=['time_created', 'col_b'], bind_vars=bind_vars)
    self.assertEqual(sql, '%(time_created)s, %(col_b)s')
    self.assertEqual(column_names, ['time_created', 'col_b'])
    self.assertEqual(bind_vars['time_created'], 1)
    bind_vars = dict(col_b=2)
    sql, column_names = sql_builder.build_values_clause(
        columns=['time_created', 'col_b'], bind_vars=bind_vars)
    self.assertEqual(sql, '%(time_created)s, %(col_b)s')
    self.assertEqual(column_names, ['time_created', 'col_b'])
    self.assertIn('time_created', bind_vars)
    bind_vars = dict()
    sql, column_names = sql_builder.build_values_clause(
        columns=['time_created', 'time_updated'], bind_vars=bind_vars)
    self.assertEqual(sql, '%(time_created)s, %(time_updated)s')
    self.assertEqual(column_names, ['time_created', 'time_updated'])
    self.assertIn('time_created', bind_vars)
    self.assertIn('time_updated', bind_vars)

  def test_with_insert_expr(self):
    timestamp = sql_builder.RawSQLInsertExpr(
        'FROM_UNIXTIME(%(col_b_1)s)', col_b_1=1234567890)
    bind_vars = dict(col_a=1, col_b=timestamp)
    sql, column_names = sql_builder.build_values_clause(
        columns=['col_a', 'col_b'], bind_vars=bind_vars)
    self.assertEqual(sql, '%(col_a)s, %(col_b)s')
    self.assertEqual(column_names, ['col_a', 'col_b'])
    self.assertEqual(sorted(bind_vars), ['col_a', 'col_b', 'col_b_1'])
    self.assertEqual(bind_vars['col_a'], 1)
    self.assertEqual(bind_vars['col_b_1'], 1234567890)


class TestBuildIn(unittest.TestCase):
  """Test sql_builder.build_in."""

  def test_no_items(self):
    self.assertRaises(
        ValueError, sql_builder.build_in, column='col_a', items=[])

  def test_two_columns(self):
    sql, bind_vars = sql_builder.build_in('col', [2, 4])
    self.assertEqual(sql, 'col IN (%(col_1)s, %(col_2)s)')
    self.assertEqual(bind_vars, dict(col_1=2, col_2=4))

  def test_with_alt_name(self):
    sql, bind_vars = sql_builder.build_in('col', [2, 4], alt_name='alt_col')
    self.assertEqual(sql, 'col IN (%(alt_col_1)s, %(alt_col_2)s)')
    self.assertEqual(bind_vars, dict(alt_col_1=2, alt_col_2=4))

  def test_with_counter(self):
    counter = itertools.count(5)
    sql, bind_vars = sql_builder.build_in('col', [2, 4], counter=counter)
    self.assertEqual(sql, 'col IN (%(col_5)s, %(col_6)s)')
    self.assertEqual(bind_vars, dict(col_5=2, col_6=4))


class TestBuildLimitClause(unittest.TestCase):
  """Test sql_builder.build_limit_clause."""

  def test_with_no_limit(self):
    sql, bind_vars = sql_builder.build_limit_clause(None)
    self.assertEqual(sql, '')
    self.assertEqual(bind_vars, {})

  def test_with_row_count_limit(self):
    sql, bind_vars = sql_builder.build_limit_clause(5)
    self.assertEqual(sql, 'LIMIT %(limit_row_count)s')
    self.assertEqual(bind_vars, dict(limit_row_count=5))
    sql, bind_vars = sql_builder.build_limit_clause([5])
    self.assertEqual(sql, 'LIMIT %(limit_row_count)s')
    self.assertEqual(bind_vars, dict(limit_row_count=5))

  def test_with_offset_and_row_count_limit(self):
    sql, bind_vars = sql_builder.build_limit_clause([100, 5])
    self.assertEqual(sql, 'LIMIT %(limit_offset)s, %(limit_row_count)s')
    self.assertEqual(bind_vars, dict(limit_offset=100, limit_row_count=5))


class TestBuildOrderClause(unittest.TestCase):

  def test_empty(self):
    self.assertEqual(sql_builder.build_order_clause(None), '')

  def test_with_one_col(self):
    self.assertEqual(sql_builder.build_order_clause('col'), 'ORDER BY col')

  def test_with_two_cols(self):
    self.assertEqual(
        sql_builder.build_order_clause(['col_a', 'col_b']),
        'ORDER BY col_a, col_b')

  def test_with_two_multi_word_cols(self):
    self.assertEqual(
        sql_builder.build_order_clause(['col_a ASC', 'col_b DESC']),
        'ORDER BY col_a ASC, col_b DESC')


class TestBuildWhereClause(unittest.TestCase):
  """Test sql_builder.build_where_clause."""

  def test_two_simple_columns(self):
    sql, bind_vars = sql_builder.build_where_clause(
        column_value_pairs=[('col_a', 1), ('col_b', 'two')])
    self.assertEqual(sql, 'col_a = %(col_a_1)s AND col_b = %(col_b_2)s')
    self.assertEqual(bind_vars, dict(col_a_1=1, col_b_2='two'))

  def test_three_iterable_columns(self):
    sql, bind_vars = sql_builder.build_where_clause(
        column_value_pairs=[
            ('col_a', [1]), ('col_b', (2, 3)), ('col_c', set([4, 5]))])
    self.assertEqual(
        sql,
        'col_a IN (%(col_a_1)s) AND col_b IN (%(col_b_2)s, %(col_b_3)s) AND '
        'col_c IN (%(col_c_4)s, %(col_c_5)s)')
    self.assertEqual(
        bind_vars,
        dict(col_a_1=1, col_b_2=2, col_b_3=3, col_c_4=4, col_c_5=5))

  def test_empty_iterable(self):
    sql, bind_vars = sql_builder.build_where_clause(
        column_value_pairs=[('col_a', [])])
    self.assertEqual(sql, '1 = 0')
    self.assertEqual(bind_vars, dict(col_a=[]))

  def test_two_expr_columns(self):
    col_a = sql_builder.GreaterThanValue(1)
    col_b = sql_builder.LessThanValue(2)
    sql, bind_vars = sql_builder.build_where_clause(
        column_value_pairs=[('col_a', col_a), ('col_b', col_b)])
    self.assertEqual(sql, 'col_a > %(col_a_1)s AND col_b < %(col_b_2)s')
    self.assertEqual(bind_vars, dict(col_a_1=1, col_b_2=2))


class TestColstr(unittest.TestCase):
  """Test sql_builder.colstr."""

  def test_two_columns(self):
    self.assertEqual(
        sql_builder.colstr(select_columns=['col_a', 'col_b']),
        'col_a, col_b')

  def test_two_exprs(self):
    self.assertEqual(
        sql_builder.colstr(
            select_columns=[sql_builder.Count(), sql_builder.Min('col_a')]),
        'COUNT(1), MIN(col_a)')

  def test_order_by_cols(self):
    """This currently prepends the ORDER BY columns."""
    self.assertEqual(
        sql_builder.colstr(
            select_columns=['col_a', 'col_b', 'col_c'],
            order_by=['col_b', 'col_c']),
        'col_b, col_c, col_a, col_b, col_c')
    self.assertEqual(
        sql_builder.colstr(
            select_columns=['col_a', 'col_b', 'col_c'],
            order_by=['col_b ASC', ('col_c', 'ASC')]),
        'col_b, col_c, col_a, col_b, col_c')

  def test_bind(self):
    self.assertEqual(
        sql_builder.colstr(
            select_columns=['col_a', 'col_b', 'col_c'],
            bind=['col_c', 'col_a']),
        'col_a, col_c')


class TestDBRow(unittest.TestCase):
  """Test sql_builder.DBRow class."""

  def test_init_no_override(self):
    db_row = sql_builder.DBRow(['col_a', 'col_b'], ['val_1', 'val_2'])
    self.assertEqual(db_row.col_a, 'val_1')
    self.assertEqual(db_row.col_b, 'val_2')
    self.assertEqual(
        repr(db_row), "{   'col_a': 'val_1', 'col_b': 'val_2'}")

  def test_init_override(self):
    db_row = sql_builder.DBRow(
        ['col_a', 'col_b'], ['val_1', 'val_2'], col_b=22, col_c=33)
    self.assertEqual(db_row.col_a, 'val_1')
    self.assertEqual(db_row.col_b, 22)
    self.assertEqual(db_row.col_c, 33)
    self.assertEqual(
        repr(db_row), "{   'col_a': 'val_1', 'col_b': 22, 'col_c': 33}")

  def test_mismatch(self):
    self.assertRaises(
        ValueError, sql_builder.DBRow, ['col_a'], ['val_1', 'val_2'])
    self.assertRaises(
        ValueError, sql_builder.DBRow, ['col_a', 'col_b'], ['val_1'])


class TestDeleteByColumnsQuery(unittest.TestCase):
  """Test sql_builder.delete_by_columns_query."""

  def test_simple(self):
    sql, bind_vars = sql_builder.delete_by_columns_query(
        table_name='my_table',
        where_column_value_pairs=[('col_a', 1), ('col_b', 2)],
        limit=5)
    self.assertEqual(
        sql,
        'DELETE FROM my_table '
        'WHERE col_a = %(col_a_1)s AND col_b = %(col_b_2)s '
        'LIMIT %(limit_row_count)s')
    self.assertEqual(
        bind_vars,
        dict(col_a_1=1, col_b_2=2, limit_row_count=5))


class TestFlags(unittest.TestCase):
  """Test sql_builder.Flag class."""

  def test_overlap(self):
    self.assertRaises(
        ValueError, sql_builder.Flag, flags_present=0x3, flags_absent=0x5)

  def test_repr(self):
    self.assertEqual(
        repr(sql_builder.Flag(flags_present=0x1, flags_absent=0x2)),
        'Flag(flags_present=0x1, flags_absent=0x2)')

  def test_or(self):
    flags = sql_builder.Flag(flags_present=0x3, flags_absent=0x30)
    flags |= sql_builder.Flag(flags_present=0x5, flags_absent=0x50)
    self.assertEqual(flags.mask, 0x77)
    self.assertEqual(flags.value, 0x7)

  def test_eq(self):
    flags1 = sql_builder.Flag(flags_present=0x3, flags_absent=0x30)
    self.assertEqual(
        flags1, sql_builder.Flag(flags_present=0x3, flags_absent=0x30))
    self.assertNotEqual(
        flags1, sql_builder.Flag(flags_present=0x30, flags_absent=0x3))
    self.assertNotEqual(flags1, 'abcde')
    self.assertNotEqual('abcde', flags1)


class TestInsertQuery(unittest.TestCase):
  """Test sql_builder.insert_query."""

  def test_simple(self):
    sql, bind_vars = sql_builder.insert_query(
        table_name='my_table', columns=['col_a', 'col_b'], col_a=1, col_b=2)
    self.assertEqual(
        sql,
        'INSERT INTO my_table (col_a, col_b) VALUES (%(col_a)s, %(col_b)s)')
    self.assertEqual(bind_vars, dict(col_a=1, col_b=2))


class TestSelectByColumnsQuery(unittest.TestCase):
  """Test sql_builder.select_by_columns_query."""

  def test_simple(self):
    sql, bind_vars = sql_builder.select_by_columns_query(
        select_column_list=['col_a', sql_builder.Min('col_b')],
        table_name='my_table',
        column_value_pairs=[('col_a', [1, 2, 3])],
        order_by='col_b ASC', group_by='col_a', limit=10)
    self.assertEqual(
        sql,
        'SELECT col_a, MIN(col_b) FROM my_table '
        'WHERE col_a IN (%(col_a_1)s, %(col_a_2)s, %(col_a_3)s) '
        'GROUP BY col_a '
        'ORDER BY col_b ASC LIMIT %(limit_row_count)s')
    self.assertEqual(
        bind_vars,
        dict(col_a_1=1, col_a_2=2, col_a_3=3, limit_row_count=10))

  def test_vt_routing(self):
    key_range = '80-C0'
    routing_sql, routing_bind_vars = (
        vtrouting._create_where_clause_for_keyrange(key_range))
    vt_routing_info = vtrouting.VTRoutingInfo(
        key_range, routing_sql, routing_bind_vars)
    sql, bind_vars = sql_builder.select_by_columns_query(
        select_column_list=['col_a', sql_builder.Min('col_b')],
        table_name='my_table',
        column_value_pairs=[('col_a', [1, 2, 3])],
        vt_routing_info=vt_routing_info)
    self.assertEqual(
        sql,
        'SELECT col_a, MIN(col_b) FROM my_table '
        'WHERE col_a IN (%(col_a_1)s, %(col_a_2)s, %(col_a_3)s) '
        'AND keyspace_id >= %(keyspace_id0)s '
        'AND keyspace_id < %(keyspace_id1)s')
    self.assertEqual(
        bind_vars,
        dict(col_a_1=1, col_a_2=2, col_a_3=3,
             keyspace_id0=(0x80 << 56), keyspace_id1=(0xC0 << 56)))

  def test_for_update_and_client_aggregate(self):
    sql, bind_vars = sql_builder.select_by_columns_query(
        select_column_list=['col_a', sql_builder.Min('col_b')],
        table_name='my_table',
        column_value_pairs=[('col_a', [1, 2, 3])],
        order_by='col_b ASC', group_by='col_a', limit=10,
        for_update=True, client_aggregate=True)
    self.assertEqual(
        sql,
        'SELECT col_b, col_a, MIN(col_b) FROM my_table '
        'WHERE col_a IN (%(col_a_1)s, %(col_a_2)s, %(col_a_3)s) '
        'GROUP BY col_a '
        'ORDER BY col_b ASC LIMIT %(limit_row_count)s FOR UPDATE')
    self.assertEqual(
        bind_vars,
        dict(col_a_1=1, col_a_2=2, col_a_3=3, limit_row_count=10))

  def test_no_where_clause(self):
    sql, bind_vars = sql_builder.select_by_columns_query(
        select_column_list=['col_a', sql_builder.Min('col_b')],
        table_name='my_table', group_by='col_a')
    self.assertEqual(
        sql, 'SELECT col_a, MIN(col_b) FROM my_table GROUP BY col_a')
    self.assertEqual(bind_vars, {})


class TestSelectClause(unittest.TestCase):
  """Test sql_builder.select_clause."""

  def test_simple(self):
    self.assertEqual(
        sql_builder.select_clause(
            select_columns=['col_a', 'col_b'], table_name='my_table'),
        'SELECT col_a, col_b FROM my_table')

  def test_with_alias(self):
    self.assertEqual(
        sql_builder.select_clause(
            select_columns=['col_a', 'col_b'], alias='mt',
            table_name='my_table'),
        'SELECT mt.col_a, mt.col_b FROM my_table mt')


class TestSmallMethods(unittest.TestCase):
  """Test some very simple sql_builder methods."""

  def test_build_aggregate_query(self):
    sql, bind_vars = sql_builder.build_aggregate_query(
        table_name='my_table', id_column_name='row_id', is_asc=False)
    self.assertEqual(
        sql,
        'SELECT row_id FROM my_table ORDER BY row_id DESC '
        'LIMIT %(limit_row_count)s')
    self.assertEqual(bind_vars, dict(limit_row_count=1))
    sql, bind_vars = sql_builder.build_aggregate_query(
        table_name='my_table', id_column_name='row_id', is_asc=True)
    self.assertEqual(
        sql,
        'SELECT row_id FROM my_table ORDER BY row_id ASC '
        'LIMIT %(limit_row_count)s')
    self.assertEqual(bind_vars, dict(limit_row_count=1))

  def test_build_count_query(self):
    query, bind_vars = sql_builder.build_count_query(
        table_name='my_table', column_value_pairs=[('col_a', 2)])
    self.assertEqual(
        query, 'SELECT COUNT(1) FROM my_table WHERE col_a = %(col_a_1)s')
    self.assertEqual(bind_vars, dict(col_a_1=2))

  def test_build_group_clause(self):
    self.assertEqual(sql_builder.build_group_clause(None), '')
    self.assertEqual(
        sql_builder.build_group_clause(['col_a']), 'GROUP BY col_a')
    self.assertEqual(
        sql_builder.build_group_clause(['col_a', 'col_b']),
        'GROUP BY col_a, col_b')

  def test_choose_bind_name(self):
    counter = itertools.count(3)
    self.assertEqual(
        sql_builder.choose_bind_name('col_a', counter), 'col_a_3')
    self.assertEqual(
        sql_builder.choose_bind_name('col_b', counter), 'col_b_4')

  def test_make_bind_list(self):
    self.assertEqual(
        sql_builder.make_bind_list('col_a', [2, 4]),
        [('col_a_1', 2), ('col_a_2', 4)])
    self.assertEqual(
        sql_builder.make_bind_list('col_a', [2, 4], itertools.count(3)),
        [('col_a_3', 2), ('col_a_4', 4)])

  def test_make_flag(self):
    flag = sql_builder.make_flag(flag_mask=0x3, value=True)
    self.assertEqual(flag.mask, 0x3)
    self.assertEqual(flag.value, 0x3)
    flag = sql_builder.make_flag(flag_mask=0x3, value=False)
    self.assertEqual(flag.mask, 0x3)
    self.assertEqual(flag.value, 0x0)

  def test_update_bind_vars(self):
    bind_vars = dict(col_a_1=2)
    sql_builder.update_bind_vars(bind_vars, dict(col_b_2=4))
    self.assertEqual(bind_vars, dict(col_a_1=2, col_b_2=4))
    self.assertRaises(
        ValueError, sql_builder.update_bind_vars, bind_vars, dict(col_b_2=4))


class TestSqlSelectExprs(unittest.TestCase):
  """Test classes derived from sql_builder.BaseSQLSelectExpr."""

  def test_base_sql_select_expr(self):
    expr = sql_builder.BaseSQLSelectExpr()
    self.assertRaises(NotImplementedError, expr.select_sql, alias=None)

  def test_raw_sql_select_expr(self):
    self.assertRaises(ValueError, sql_builder.RawSQLSelectExpr)

    expr = sql_builder.RawSQLSelectExpr('UNIX_TIMESTAMP()')
    self.assertEqual(expr.select_sql(alias=None), 'UNIX_TIMESTAMP()')

  def test_count(self):
    self.assertEqual(sql_builder.Count().select_sql(alias=None), 'COUNT(1)')
    self.assertEqual(sql_builder.Count().select_sql(alias='mt'), 'COUNT(1)')

  def test_sql_aggregate(self):
    self.assertRaises(ValueError, sql_builder.SQLAggregate, 'col_a')

    expr = sql_builder.SQLAggregate('col_a', function_name='LENGTH')
    self.assertEqual(expr.select_sql(alias=None), 'LENGTH(col_a)')
    self.assertEqual(expr.select_sql(alias='mt'), 'LENGTH(mt.col_a)')

  def test_max(self):
    self.assertEqual(
        sql_builder.Max('col_a').select_sql(alias=None), 'MAX(col_a)')
    self.assertEqual(
        sql_builder.Max('col_a').select_sql(alias='mt'), 'MAX(mt.col_a)')

  def test_min(self):
    self.assertEqual(
        sql_builder.Min('col_a').select_sql(alias=None), 'MIN(col_a)')
    self.assertEqual(
        sql_builder.Min('col_a').select_sql(alias='mt'), 'MIN(mt.col_a)')

  def test_sum(self):
    self.assertEqual(
        sql_builder.Sum('col_a').select_sql(alias=None), 'SUM(col_a)')
    self.assertEqual(
        sql_builder.Sum('col_a').select_sql(alias='mt'), 'SUM(mt.col_a)')


class TestSQLInsertExprs(unittest.TestCase):
  """Test classes derived from sql_builder.BaseSQLInsertExpr."""

  def test_base_sql_insert_expr(self):
    expr = sql_builder.BaseSQLInsertExpr()
    self.assertRaises(NotImplementedError, expr.build_insert_sql)

  def test_raw_sql_insert_expr(self):
    expr = sql_builder.RawSQLInsertExpr('UNIX_TIMESTAMP()')
    sql, bind_vars = expr.build_insert_sql()
    self.assertEqual(sql, 'UNIX_TIMESTAMP()')
    self.assertEqual(bind_vars, {})

    sql, bind_vars = expr.build_update_sql('col_a')
    self.assertEqual(sql, 'col_a = UNIX_TIMESTAMP()')
    self.assertEqual(bind_vars, {})

    self.assertRaises(ValueError, sql_builder.RawSQLInsertExpr)


class TestSqlUpdateExprs(unittest.TestCase):
  """Test classes derived from sql_builder.BaseSQLUpdateExpr."""

  def test_base_sql_update_expr(self):
    expr = sql_builder.BaseSQLUpdateExpr()
    self.assertRaises(NotImplementedError, expr.build_update_sql, 'col_a')

  def test_raw_sql_update_expr(self):
    expr = sql_builder.RawSQLUpdateExpr('UNIX_TIMESTAMP()')
    sql, bind_vars = expr.build_update_sql('col_a')
    self.assertEqual(sql, 'col_a = UNIX_TIMESTAMP()')
    self.assertEqual(bind_vars, {})

    expr = sql_builder.RawSQLUpdateExpr('POW(2, %(col_a_1)s', col_a_1=8)
    sql, bind_vars = expr.build_update_sql('col_a')
    self.assertEqual(sql, 'col_a = POW(2, %(col_a_1)s')
    self.assertEqual(bind_vars, dict(col_a_1=8))

    self.assertRaises(ValueError, sql_builder.RawSQLUpdateExpr)

  def test_mysql_function(self):
    unix_function = sql_builder.MySQLFunction('unix_timestamp()')
    sql, bind_vars = unix_function.build_update_sql('col_now')
    self.assertEqual(sql, 'col_now = unix_timestamp()')
    self.assertEqual(bind_vars, {})
    area_function = sql_builder.MySQLFunction(
        '%(side_x)s * %(side_y)s', dict(side_x=3, side_y=4))
    sql, bind_vars = area_function.build_update_sql('area')
    self.assertEqual(sql, 'area = %(side_x)s * %(side_y)s')
    self.assertEqual(bind_vars, dict(side_x=3, side_y=4))

  def test_increment(self):
    increment = sql_builder.Increment(3)
    sql, bind_vars = increment.build_update_sql('col_a')
    self.assertEqual(
        sql, 'col_a = (col_a + %(update_col_a_amount)s)')
    self.assertEqual(bind_vars, dict(update_col_a_amount=3))

  def test_flag(self):
    flags = sql_builder.Flag(flags_present=0x1, flags_absent=0x2)
    sql, bind_vars = flags.build_update_sql('flags')
    self.assertEqual(
        sql,
        'flags = (flags | %(update_flags_add)s) & ~%(update_flags_remove)s')
    self.assertEqual(
        bind_vars, dict(update_flags_add=0x1, update_flags_remove=0x2))


class TestSqlWhereExprs(unittest.TestCase):
  """Test classes derived from sql_builder.BaseSQLWhereExpr."""

  def _check_build_where_sql(self, expr, expected_sql, expected_bind_vars):
    sql, bind_vars = expr.build_where_sql('col_a', itertools.count(3))
    self.assertEqual(sql, expected_sql)
    self.assertEqual(bind_vars, expected_bind_vars)

  def test_base_sql_where_expr(self):
    expr = sql_builder.BaseSQLWhereExpr()
    self.assertRaises(
        NotImplementedError, expr.select_where_sql,
        'col_a', itertools.count(3))

  def test_null_safe_not_value(self):
    self._check_build_where_sql(
        sql_builder.NullSafeNotValue(5),
        'NOT col_a <=> %(col_a_3)s', dict(col_a_3=5))

  def test_not_value(self):
    self._check_build_where_sql(
        sql_builder.NotValue(5), 'col_a != %(col_a_3)s', dict(col_a_3=5))
    self._check_build_where_sql(
        sql_builder.NotValue(None), 'col_a IS NOT NULL', {})

  def test_in_values(self):
    self._check_build_where_sql(
        sql_builder.InValues(2, 3, 5),
        'col_a IN (%(col_a_3)s, %(col_a_4)s, %(col_a_5)s)',
        dict(col_a_3=2, col_a_4=3, col_a_5=5))

  def test_not_in_values(self):
    self._check_build_where_sql(
        sql_builder.NotInValues(2, 3, 5),
        'col_a NOT IN (%(col_a_3)s, %(col_a_4)s, %(col_a_5)s)',
        dict(col_a_3=2, col_a_4=3, col_a_5=5))

  def test_in_values_or_null(self):
    self._check_build_where_sql(
        sql_builder.InValuesOrNull(2, 3, 5),
        '(col_a IN (%(col_a_3)s, %(col_a_4)s, %(col_a_5)s) OR col_a IS NULL)',
        dict(col_a_3=2, col_a_4=3, col_a_5=5))

  def test_between_values(self):
    self._check_build_where_sql(
        sql_builder.BetweenValues(11, 20),
        'col_a BETWEEN %(col_a_3)s AND %(col_a_4)s',
        dict(col_a_3=11, col_a_4=20))

  def test_or_values(self):
    self.assertRaises(ValueError, sql_builder.OrValues, 7)
    self._check_build_where_sql(
        sql_builder.OrValues(sql_builder.BetweenValues(11, 20), 30),
        '((col_a BETWEEN %(col_a_3)s AND %(col_a_4)s) OR '
        '(col_a = %(col_a_5)s))',
        dict(col_a_3=11, col_a_4=20, col_a_5=30))

  def test_like_value(self):
    self._check_build_where_sql(
        sql_builder.LikeValue('FOO%'),
        'col_a LIKE %(col_a_3)s', dict(col_a_3='FOO%'))

  def test_greater_than_value(self):
    self._check_build_where_sql(
        sql_builder.GreaterThanValue(5),
        'col_a > %(col_a_3)s', dict(col_a_3=5))

  def test_greater_than_or_equal_to_value(self):
    self._check_build_where_sql(
        sql_builder.GreaterThanOrEqualToValue(5),
        'col_a >= %(col_a_3)s', dict(col_a_3=5))

  def test_less_than_value(self):
    self._check_build_where_sql(
        sql_builder.LessThanValue(5),
        'col_a < %(col_a_3)s', dict(col_a_3=5))

  def test_less_than_or_equal_to_value(self):
    self._check_build_where_sql(
        sql_builder.LessThanOrEqualToValue(5),
        'col_a <= %(col_a_3)s', dict(col_a_3=5))

  def test_modulo_equals(self):
    self._check_build_where_sql(
        sql_builder.ModuloEquals(modulus=5, value=3),
        '(col_a %% %(modulus_3)s) = %(col_a_4)s',
        dict(modulus_3=5, col_a_4=3))

  def test_expression(self):
    self._check_build_where_sql(
        sql_builder.Expression('col_b', '<'), 'col_a < col_b', {})

  def test_is_null_or_empty_string(self):
    self._check_build_where_sql(
        sql_builder.IsNullOrEmptyString(),
        '(col_a IS NULL OR col_a = \'\')', {})

  def test_is_null_value(self):
    self._check_build_where_sql(
        sql_builder.IsNullValue(), 'col_a IS NULL', {})

  def test_is_not_null_value(self):
    self._check_build_where_sql(
        sql_builder.IsNotNullValue(), 'col_a IS NOT NULL', {})

  def test_flag(self):
    self._check_build_where_sql(
        sql_builder.Flag(flags_present=0x1, flags_absent=0x2),
        'col_a & %(col_a_mask_3)s = %(col_a_value_4)s',
        dict(col_a_mask_3=3, col_a_value_4=1))


class TestUpdateColumnsQuery(unittest.TestCase):
  """Test sql_builder.update_columns_query."""

  def test_simple(self):
    sql, bind_vars = sql_builder.update_columns_query(
        table_name='my_table',
        where_column_value_pairs=[('col_a', 1), ('col_b', 2)],
        update_column_value_pairs=[('col_c', 3)],
        limit=5, order_by='col_d')
    self.assertEqual(
        sql,
        'UPDATE my_table SET col_c = %(update_set_0)s '
        'WHERE col_a = %(col_a_1)s AND col_b = %(col_b_2)s '
        'ORDER BY col_d LIMIT %(limit_row_count)s')
    self.assertEqual(
        bind_vars,
        dict(update_set_0=3, col_a_1=1, col_b_2=2, limit_row_count=5))

  def test_with_update_expr(self):
    sql, bind_vars = sql_builder.update_columns_query(
        table_name='my_table',
        where_column_value_pairs=[('col_a', 1), ('col_b', 2)],
        update_column_value_pairs=[('col_c', sql_builder.Increment(5))])
    self.assertEqual(
        sql,
        'UPDATE my_table SET col_c = (col_c + %(update_col_c_amount)s) '
        'WHERE col_a = %(col_a_1)s AND col_b = %(col_b_2)s')
    self.assertEqual(
        bind_vars,
        dict(update_col_c_amount=5, col_a_1=1, col_b_2=2))

  def test_no_update_column_value_pairs(self):
    self.assertRaises(
        ValueError, sql_builder.update_columns_query,
        table_name='my_table',
        where_column_value_pairs=[('col_a', 1), ('col_b', 2)],
        update_column_value_pairs=None)

  def test_no_where_column_value_pairs(self):
    self.assertRaises(
        ValueError, sql_builder.update_columns_query,
        table_name='my_table',
        where_column_value_pairs=None,
        update_column_value_pairs=[('col_c', 3)])

if __name__ == '__main__':
  utils.main()
