"""Module containing the base class for database classes and decorator for db method.

The base class DBObjectBase is the base class for all other database base classes.
It has methods for common database operations like select, insert, update and delete.
This module also contains the definition for ShardRouting which is used for determining
the routing of a query during cursor creation.
The module also has the db_class_method decorator and db_wrapper method which are
used for cursor creation and calling the database method.
"""
import functools
import struct

from vtdb import database_context
from vtdb import dbexceptions
from vtdb import db_validator
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import sql_builder
from vtdb import vtgate_cursor

class __EmptyBindVariables(frozenset):
    pass
EmptyBindVariables = __EmptyBindVariables()


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


INSERT_KW = "insert"
UPDATE_KW = "update"
DELETE_KW = "delete"


def is_dml(sql):
  first_kw = sql.split(' ')[0]
  first_kw = first_kw.lower()
  if first_kw == INSERT_KW or first_kw == UPDATE_KW or first_kw == DELETE_KW:
    return True
  return False


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


def create_stream_cursor_from_cursor(original_cursor):
  """
  This method creates streaming cursor from a regular cursor.

  Args:
    original_cursor: Cursor of VTGateCursor type

  Returns:
    Returns StreamVTGateCursor that is not writable.
  """
  if not isinstance(original_cursor, vtgate_cursor.VTGateCursor):
    raise dbexceptions.ProgrammingError(
        "Original cursor should be of VTGateCursor type.")
  stream_cursor = vtgate_cursor.StreamVTGateCursor(
      original_cursor._conn, original_cursor.keyspace,
      original_cursor.tablet_type,
      keyspace_ids=original_cursor.keyspace_ids,
      keyranges=original_cursor.keyranges,
      writable=False)
  return stream_cursor


def create_batch_cursor_from_cursor(original_cursor, writable=False):
  """
  This method creates a batch cursor from a regular cursor.

  Args:
    original_cursor: Cursor of VTGateCursor type

  Returns:
    Returns BatchVTGateCursor that has same attributes as original_cursor.
  """
  if not isinstance(original_cursor, vtgate_cursor.VTGateCursor):
    raise dbexceptions.ProgrammingError(
        "Original cursor should be of VTGateCursor type.")
  batch_cursor = vtgate_cursor.BatchVTGateCursor(
      original_cursor._conn,
      original_cursor.tablet_type,
      writable=writable)
  return batch_cursor


def db_wrapper(method, **decorator_kwargs):
  """Decorator that is used to create the appropriate cursor
  for the table and call the database method with it.

  Args:
    method: Method to decorate.
    decorator_kwargs: Keyword args for db_wrapper.

  Returns:
    Decorated method.
  """
  @functools.wraps(method)
  def _db_wrapper(*pargs, **kwargs):
    table_class = pargs[0]
    write_method = kwargs.get("write_method", False)
    if not issubclass(table_class, DBObjectBase):
      raise dbexceptions.ProgrammingError(
          "table class '%s' is not inherited from DBObjectBase" % table_class)

    # Create the cursor using cursor_method
    cursor_method = pargs[1]
    cursor = cursor_method(table_class)

    # DML verification.
    if write_method:
      if not cursor.is_writable():
        raise dbexceptions.ProgrammingError(
            "Executing dmls on a non-writable cursor is not allowed.")
      if table_class.is_mysql_view:
          raise dbexceptions.ProgrammingError(
              "writes disabled on view", table_class)

    if pargs[2:]:
      return method(table_class, cursor, *pargs[2:], **kwargs)
    else:
      return method(table_class, cursor, **kwargs)
  return _db_wrapper


def write_db_class_method(*pargs, **kwargs):
  """Used for DML methods. Calls db_class_method."""
  kwargs["write_method"] = True
  return db_class_method(*pargs, **kwargs)


def db_class_method(*pargs, **kwargs):
  """This function calls db_wrapper to create the appropriate cursor."""
  return classmethod(db_wrapper(*pargs, **kwargs))


class InvalidUtf8DbWrite(dbexceptions.Error):
  """Raised when an attempt to write invalid utf-8 to the DB is made.
  """
  template = ("Attempt to write invalid utf-8 strings to the DB table '%s' in "
              "columns %s")

  def __init__(self, table_name, columns):
    self.table_name = table_name
    self.columns = columns
    self.template_args = (table_name, columns)
    super(InvalidUtf8DbWrite, self).__init__(self.template % self.template_args)


class DBObjectBase(object):
  """Base class for db classes.

  This abstracts sharding information and provides helper methods
  for common database access operations.
  """
  keyspace = None
  sharding = None
  table_name = None
  id_column_name = None
  is_mysql_view = False
  utf8_columns = None


  @classmethod
  def create_shard_routing(class_, *pargs, **kwargs):
    """This method is used to create ShardRouting object which is
    used for determining routing attributes for the vtgate cursor.

    Returns:
    ShardRouting object.
    """
    raise NotImplementedError

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **cursor_kwargs):
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

  @classmethod
  def _validate_column_value_pairs_for_write(class_, **column_values):
    invalid_columns = db_validator.invalid_utf8_columns(class_.utf8_columns or [],
                                                        column_values)
    if invalid_columns:
      exc = InvalidUtf8DbWrite(class_.table_name, invalid_columns)
      raise exc

  @db_class_method
  def select_by_columns(class_, cursor, where_column_value_pairs,
                        columns_list=None, order_by=None, group_by=None,
                        limit=None):
    if columns_list is None:
      columns_list = class_.columns_list
    query, bind_vars = class_.create_select_query(where_column_value_pairs,
                                                  columns_list=columns_list,
                                                  order_by=order_by,
                                                  group_by=group_by,
                                                  limit=limit)

    rowcount = cursor.execute(query, bind_vars)
    rows = cursor.fetchall()
    return [sql_builder.DBRow(columns_list, row) for row in rows]

  @classmethod
  def create_insert_query(class_, **bind_vars):
    class_._validate_column_value_pairs_for_write(**bind_vars)
    return sql_builder.insert_query(class_.table_name,
                                    class_.columns_list,
                                    **bind_vars)

  @classmethod
  def create_update_query(class_, where_column_value_pairs,
                          update_column_value_pairs):
    class_._validate_column_value_pairs_for_write(
        **dict(update_column_value_pairs))
    return sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs,
        update_column_value_pairs=update_column_value_pairs)

  @classmethod
  def create_delete_query(class_, where_column_value_pairs, limit=None):
    return sql_builder.delete_by_columns_query(class_.table_name,
                                               where_column_value_pairs,
                                               limit=limit)

  @classmethod
  def create_select_query(class_, where_column_value_pairs, columns_list=None,
                          order_by=None, group_by=None, limit=None):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    if columns_list is None:
      columns_list = class_.columns_list

    query, bind_vars = sql_builder.select_by_columns_query(columns_list,
                                                           class_.table_name,
                                                           where_column_value_pairs,
                                                           order_by=order_by,
                                                           group_by=group_by,
                                                           limit=limit)
    return query, bind_vars

  @write_db_class_method
  def insert(class_, cursor, **bind_vars):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query, bind_vars = class_.create_insert_query(**bind_vars)
    cursor.execute(query, bind_vars)
    return cursor.lastrowid

  @write_db_class_method
  def update_columns(class_, cursor, where_column_value_pairs,
                     update_column_value_pairs):
    query, bind_vars = class_.create_update_query(
        where_column_value_pairs,
        update_column_value_pairs=update_column_value_pairs)

    return cursor.execute(query, bind_vars)

  @write_db_class_method
  def delete_by_columns(class_, cursor, where_column_value_pairs, limit=None):
    if not where_column_value_pairs:
      raise dbexceptions.ProgrammingError("deleting the whole table is not allowed")

    query, bind_vars = class_.create_delete_query(where_column_value_pairs,
                                                  limit=limit)
    cursor.execute(query, bind_vars)
    if cursor.rowcount == 0:
      raise dbexceptions.DatabaseError("DB Row not found")
    return cursor.rowcount

  @db_class_method
  def select_by_columns_streaming(class_, cursor, where_column_value_pairs,
                        columns_list=None, order_by=None, group_by=None,
                        limit=None, fetch_size=100):
    query, bind_vars = class_.create_select_query(where_column_value_pairs,
                                                  columns_list=columns_list,
                                                  order_by=order_by,
                                                  group_by=group_by,
                                                  limit=limit)

    return class_._stream_fetch(cursor, query, bind_vars, fetch_size)

  @classmethod
  def _stream_fetch(class_, cursor, query, bind_vars, fetch_size=100):
    stream_cursor = create_stream_cursor_from_cursor(cursor)
    stream_cursor.execute(query, bind_vars)
    while True:
      rows = stream_cursor.fetchmany(size=fetch_size)

      # NOTE: fetchmany returns an empty list when there are no more items.
      # But an empty generator is still "true", so we have to count if we
      # actually returned anything.
      i = 0
      for r in rows:
        i += 1
        yield sql_builder.DBRow(class_.columns_list, r)
      if i == 0:
        break
    stream_cursor.close()

  @db_class_method
  def get_count(class_, cursor, column_value_pairs=None, **columns):
    if not column_value_pairs:
      column_value_pairs = columns.items()
      column_value_pairs.sort()
    query, bind_vars = sql_builder.build_count_query(class_.table_name,
                                                     column_value_pairs)
    cursor.execute(query, bind_vars)
    return cursor.fetch_aggregate_function(sum)

  @db_class_method
  def get_min(class_, cursor):
    if class_.id_column_name is None:
      raise dbexceptions.ProgrammingError("id_column_name not set.")

    query, bind_vars = sql_builder.build_aggregate_query(
        class_.table_name, class_.id_column_name, is_asc=True)
    cursor.execute(query, bind_vars)
    return cursor.fetch_aggregate_function(min)

  @db_class_method
  def get_max(class_, cursor):
    if class_.id_column_name is None:
      raise dbexceptions.ProgrammingError("id_column_name not set.")

    query, bind_vars = sql_builder.build_aggregate_query(
        class_.table_name, class_.id_column_name, is_asc=False)
    cursor.execute(query, bind_vars)
    return cursor.fetch_aggregate_function(max)
