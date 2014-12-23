"""Module containing base classes and helper methods for database objects.

The base classes represent different sharding schemes like
unsharded, range-sharded and custom-sharded tables.
This abstracts sharding details and provides methods
for common database access patterns.
"""
import functools
import logging

from vtdb import database_context
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import sql_builder
from vtdb import vtgate_cursor


class Unimplemented(Exception):
    pass


class ShardRouting(object):
  """VTGate Shard Routing Class.

  Attributes:
  keyspace: keyspace where the table resides.
  sharding_key: sharding key of the table.
  keyrange: keyrange for the query.
  entity_id_sharding_key_map: this map is used for in clause queries.
  shard_name: this is used to route queries for custom sharded keyspaces.
  """

  keyspace = None
  sharding_key = None
  keyrange = None
  entity_id_sharding_key_map = None
  shard_name = None # For custom sharding

  def __init__(self, keyspace):
    self.keyspace = keyspace


def _is_iterable_container(x):
  return hasattr(x, '__iter__')


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
    cursor_method = pargs[1]
    cursor = cursor_method(table_class, **kargs)
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

  id_column_name = 'id'
  sharding_key_column_name = None
  entity_id_columns = None

  @classmethod
  def create_shard_routing(class_, is_dml, *pargs, **kwargs):
    """This method is used to create ShardRouting object which is
    used for determining routing attributes for the vtgate cursor.

    Args:
    is_dml: This is used to santiy check required params for writes.

    Returns:
    ShardRouting object.
    """
    raise Unimplemented

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml,
                           *pargs, **kwargs):
    """This creates the VTGateCursor object which is used to make
    all the rpc calls to VTGate.

    Args:
    vtgate_conn: connection to vtgate.
    tablet_type: tablet type to connect to.
    is_dml: Makes the cursor writable, enforces appropriate constraints.

    Returns:
    VTGateCursor for the query.
    """
    raise Unimplemented

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
  def insert(class_, cursor, **bind_variables):
    values_clause, bind_list = sql_builder.build_values_clause(
        class_.columns_list,
        bind_variables)

    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query = 'INSERT INTO %s (%s) VALUES (%s)' % (class_.table_name,
                                                 sql_builder.colstr(
                                                     class_.columns_list,
                                                     bind=bind_list),
                                                 values_clause)
    cursor.execute(query, bind_variables)
    return cursor.lastrowid

  @db_class_method
  def update_columns(class_, cursor, where_column_value_pairs,
                     **update_columns):

    query, bind_variables = sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs, **update_columns)

    return cursor.execute(query, bind_variables)

  @db_class_method
  def delete_by_columns(class_, cursor, where_column_value_pairs, limit=None,
                        **columns):
    if not where_column_value_pairs:
      where_column_value_pairs = columns.items()
      where_column_value_pairs.sort()

    if not where_column_value_pairs:
      raise dbexceptions.ProgrammingError("deleting the whole table is not allowed")

    query, bind_variables = sql_builder.delete_by_columns_query(class_.table_name,
                                                              where_column_value_pairs,
                                                              limit=limit)
    cursor.execute(query, bind_variables)
    if cursor.rowcount == 0:
      raise dbexceptions.DatabaseError("DB Row not found")
    return cursor.rowcount


class DBObjectUnsharded(DBObjectBase):
  """Base class for unsharded db classes.

  This provides default implementation of routing helper methods, cursor
  creation and common database access operations.
  """
  keyspace = None
  sharding = shard_constants.UNSHARDED

  table_name = None
  columns_list = None

  id_column_name = 'id'
  sharding_key_column_name = None
  entity_id_columns = None

  @classmethod
  def create_shard_routing(class_, is_dml, *pargs, **kwargs):
    routing = ShardRouting(class_.keyspace)
    routing.keyrange = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **kargs):
    routing = class_.create_shard_routing(is_dml)

    if routing.keyrange is not None:
      keyranges = [routing.keyrange,]
    else:
      dbexceptions.ProgrammingError("Empty Keyrange")

    cursor = vtgate_cursor.VTGateCursor(vtgate_conn,
                                        class_.keyspace,
                                        tablet_type,
                                        keyranges=keyranges,
                                        writable=is_dml)
    return cursor


class DBObjectRangeSharded(DBObjectBase):
  """Base class for range-sharded db classes.

  This provides default implementation of routing helper methods, cursor
  creation and common database access operations.
  This abstracts sharding information and provides helper methods
  for common database access operations.
  """
  keyspace = None
  sharding = shard_constants.RANGE_SHARDED

  table_name = None
  columns_list = None

  id_column_name = 'id'
  sharding_key_column_name = None
  entity_id_columns = None

  @classmethod
  def create_shard_routing(class_, is_dml,  **kargs):
    routing = ShardRouting(class_.keyspace)

    keyrange = kargs.get("keyrange", None)
    if keyrange is not None:
      if is_dml:
        dbexceptions.InternalError(
            "Writes require unique sharding_key and not keyrange.")
      routing.keyrange = keyrange
      return routing

    routing.sharding_key = kargs.get('sharding_key', None)
    if routing.sharding_key is None:
      try:
        entity_id_column = kargs['entity_id_column']
        entity_id = kargs['entity_id']
        # this may involve a lookup of the index from db.
        # consider caching it at the app layer for performance.
        entity_id_sharding_key_map = class_.map_entity_id_sharding_key(
            entity_id_column, entity_id)
        routing.entity_id_sharding_key_map = entity_id_sharding_key_map
        routing = entity_id_sharding_key_map.values()
      except KeyError, e:
        raise dbexceptions.ProgrammingError(
            "For sharded table, sharding_key and entity_id cannot both be empty.")

    if not class_.is_sharding_key_valid(routing.sharding_key):
      raise dbexceptions.InternalError("Invalid sharding_key %s" % sharding_key)

    if (_is_iterable_container(routing.sharding_key)
        and is_dml):
      raise dbexceptions.InternalError(
          "Writes are not allowed on multiple sharding_keys.")
    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **kargs):
    routing = class_.create_shard_routing(is_dml, kargs)

    keyspace_ids = None
    keyranges = None
    if routing.sharding_key is not None:
      keysapce_ids = [class_.sharding_key_to_keyspace_id(routing.sharding_key),]
    elif routing.entity_id_sharding_key_map is not None:
      keyspace_ids = []
      for sharding_key in routing.entity_id_sharding_key_map.values():
        keysapce_ids.append(class_.sharding_key_to_keyspace_id(sharding_key))
    elif routing.keyrange:
      keyranges = [routing.keyrange,]

    cursor = vtgate_cursor.VTGateCursor(vtgate_conn,
                                        class_.keyspace,
                                        tablet_type,
                                        keyspace_ids=keyspace_ids,
                                        keyranges=keyranges,
                                        writable=is_dml)
    return cursor


  @classmethod
  def map_entity_id_sharding_key(class_, entity_id_column, entity_id):
    """This method is used to map any entity id to sharding key.

    Args:
      entity_id_column: Non-sharding key indexes that can be used for query routing.
      entity_id: entity id value.

    Returns:
      sharding key to be used for routing.
    """
    raise Unimplemented

  @classmethod
  def is_sharding_key_valid(class_, sharding_key):
    """Method to check the validity of sharding key for the table.

    Args:
      sharding_key: sharding_key to be validated.

    Returns:
      bool
    """
    raise Unimplemented

  @classmethod
  def sharding_key_to_keyspace_id(class_, sharding_key):
    """Method to check the validity of sharding key for the table.

    Args:
      sharding_key: sharding_key

    Returns:
      keyspace_id
    """
    raise Unimplemented


class DBObjectCustomSharded(DBObjectBase):
  """Base class for custom-sharded db classes.

  This class is intended to support a custom sharding scheme, where the user
  controls the routing of their queries by passing in the shard_name
  explicitly.This provides helper methods for common database access operations.
  """
  keyspace = None
  sharding = shard_constants.CUSTOM_SHARDED

  table_name = None
  columns_list = None

  id_column_name = 'id'
  sharding_key_column_name = None
  entity_id_columns = None

  @classmethod
  def create_shard_routing(class_, is_dml, **kargs):
    routing = shard_routing.ShardRouting(keyspace)
    routing.shard_name = kargs.get('shard_name')
    if routing.shard_name is None:
      dbexceptions.InternalError("For custom sharding, shard_name cannot be None.")

    if (_is_iterable_container(routing.shard_name)
        and is_dml):
      raise dbexceptions.InternalError(
          "Writes are not allowed on multiple shards.")
    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **kargs):
    routing = class_.create_shard_routing(is_dml, kargs)

    # FIXME:extend VTGateCursor's api to accept shard_names
    # and allow queries based on that.
    cursor = vtgate_cursor.VTGateCursor(vtgate_conn, class_.keyspace,
                                        tablet_type,
                                        keyranges=[routing.shard_name,],
                                        writable=is_dml)
    return cursor
