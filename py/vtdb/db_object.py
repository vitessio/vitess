"""Module containing the base class for database classes and decorator for db method.

The base class DBObjectBase is the base class for all other database base classes.
It has methods for common database operations like select, insert, update and delete.
This module also contains the definition for ShardRouting which is used for determining
the routing of a query during cursor creation.
The module also has the db_class_method decorator and db_wrapper method which are
used for cursor creation and calling the database method.
"""
import functools
import logging
import struct

from vtdb import database_context
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import sql_builder
from vtdb import vtgate_cursor


class ShardRouting(object):
  """VTGate Shard Routing Class.

  Attributes:
  keyspace: keyspace where the table resides.
  sharding_key: sharding key of the table.
  keyrange: keyrange for the query.
  entity_column_name: the name of the lookup based entity used for routing.
  entity_id_sharding_key_map: this map is used for in clause queries.
  shard_name: this is used to route queries for custom sharded keyspaces.
  """
  def __init__(self, keyspace):
    # keyspace of the table.
    self.keyspace = keyspace
    # sharding_key, entity_column_name and entity_id_sharding_key
    # are used primarily for routing range-sharded keyspace queries.
    self.sharding_key = None
    self.entity_column_name = None
    self.entity_id_sharding_key_map = None
    self.keyrange = None
    self.shard_name = None


def _is_iterable_container(x):
  return hasattr(x, '__iter__')


def create_cursor_from_params(vtgate_conn, tablet_type, is_dml, table_class):
  """This method creates the cursor from the required params.

  This is mainly used for creating lookup cursor during create_shard_routing,
  as there is no real cursor available.

  Args:
    vtgate_conn: connection to vtgate server.
    tablet_type: tablet type for the cursor.
    is_dml: indicates writable cursor or not.
    table_class: table for which the cursor is being created.

  Returns:
    cursor
  """
  cursor = table_class.create_vtgate_cursor(vtgate_conn, tablet_type, is_dml)
  return cursor


def create_cursor_from_old_cursor(old_cursor, table_class):
  """This method creates the cursor from an existing cursor.

  This is mainly used for creating lookup cursor during db operations on
  other database tables.

  Args:
    old_cursor: existing cursor from which important params are evaluated.
    table_class: table for which the cursor is being created.

  Returns:
    cursor
  """
  cursor = table_class.create_vtgate_cursor(old_cursor._conn,
                                            old_cursor.tablet_type,
                                            old_cursor.is_writable())
  return cursor




def db_wrapper(method):
  """Decorator that is used to create the appropriate cursor
  for the table and call the database method with it.

  Args:
    method: Method to decorate.

  Returns:
    Decorated method.
  """
  @functools.wraps(method)
  def _db_wrapper(*pargs, **kargs):
    table_class = pargs[0]
    if not issubclass(table_class, DBObjectBase):
      raise dbexceptions.ProgrammingError(
          "table class '%s' is not inherited from DBObjectBase" % table_class)
    cursor_method = pargs[1]
    cursor = cursor_method(table_class)
    if pargs[2:]:
      return method(table_class, cursor, *pargs[2:], **kargs)
    else:
      return method(table_class, cursor, **kargs)
  return _db_wrapper


def db_class_method(*pargs, **kargs):
  """This function calls db_wrapper to create the appropriate cursor."""
  return classmethod(db_wrapper(*pargs, **kargs))


class DBObjectBase(object):
  """Base class for db classes.

  This abstracts sharding information and provides helper methods
  for common database access operations.
  """
  keyspace = None
  sharding = None
  table_name = None


  @classmethod
  def create_shard_routing(class_, *pargs, **kwargs):
    """This method is used to create ShardRouting object which is
    used for determining routing attributes for the vtgate cursor.

    Returns:
    ShardRouting object.
    """
    raise NotImplementedError

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **cursor_kargs):
    """This creates the VTGateCursor object which is used to make
    all the rpc calls to VTGate.

    Args:
    vtgate_conn: connection to vtgate.
    tablet_type: tablet type to connect to.
    is_dml: Makes the cursor writable, enforces appropriate constraints.

    Returns:
    VTGateCursor for the query.
    """
    raise NotImplementedError

  @db_class_method
  def select_by_columns(class_, cursor, where_column_value_pairs,
                        columns_list = None,order_by=None, group_by=None,
                        limit=None, **kwargs):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    if columns_list is None:
      columns_list = class_.columns_list
    query, bind_vars = sql_builder.select_by_columns_query(columns_list,
                                                           class_.table_name,
                                                           where_column_value_pairs,
                                                           order_by=order_by,
                                                           group_by=group_by,
                                                           limit=limit,
                                                           **kwargs)

    rowcount = cursor.execute(query, bind_vars)
    rows = cursor.fetchall()
    return [sql_builder.DBRow(columns_list, row) for row in rows]

  @db_class_method
  def insert(class_, cursor, **bind_vars):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query, bind_vars = sql_builder.insert_query(class_.table_name,
                                                class_.columns_list,
                                                **bind_vars)
    cursor.execute(query, bind_vars)
    return cursor.lastrowid

  @db_class_method
  def update_columns(class_, cursor, where_column_value_pairs,
                     **update_columns):

    query, bind_vars = sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs, **update_columns)

    return cursor.execute(query, bind_vars)

  @db_class_method
  def delete_by_columns(class_, cursor, where_column_value_pairs, limit=None,
                        **columns):
    if not where_column_value_pairs:
      where_column_value_pairs = columns.items()
      where_column_value_pairs.sort()

    if not where_column_value_pairs:
      raise dbexceptions.ProgrammingError("deleting the whole table is not allowed")

    query, bind_vars = sql_builder.delete_by_columns_query(class_.table_name,
                                                              where_column_value_pairs,
                                                              limit=limit)
    cursor.execute(query, bind_vars)
    if cursor.rowcount == 0:
      raise dbexceptions.DatabaseError("DB Row not found")
    return cursor.rowcount
