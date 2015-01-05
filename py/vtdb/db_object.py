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

def get_cursor(table_class, cursor_method, **kargs):
  if cursor_method is None:
    raise dbexceptions.ProgrammingError("cursor method cannot be None")
  # cursor_method maybe an actual cursor
  cursor = None
  # This mechanism is typically used for obtaining cursor
  # for lookup classes.
  if isinstance(cursor_method, vtgate_cursor.VTGateCursor):
    old_cursor = cursor_method
    tablet_type = old_cursor.tablet_type
    vtgate_conn = old_cursor._conn
    is_dml = old_cursor.is_writable()
    if vtgate_conn is None or vtgate_conn.is_closed():
      raise dbexceptions.Error("Cannot create cursor, invalid vtgate connection")
    routing = create_shard_routing(table_class, **kargs)
    cursor = table_class.create_vtgate_cursor(routing, vtgate_conn, tablet_type, is_dml, **kargs)
  else:
   routing = create_shard_routing(table_class, cursor_method, **kargs)
   cursor = cursor_method(table_class, routing, **kargs)

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
    if not isinstance(table_class, DBObjectBase):
      raise dbexceptions.ProgrammingError(
          "table class '%s' is not inherited from DBObjectBase" % table_class)
    cursor_method = pargs[1]
    cursor = get_cursor(table_class, cursor_method, **kargs)
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
  def create_vtgate_cursor(class_, routing, vtgate_conn, tablet_type, is_dml,
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
  def insert(class_, cursor, **bind_variables):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query, bind_vars = sql_builder.insert_query(class_.table_name,
                                                class_.columns_list,
                                                **bind_variables)
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


  @classmethod
  def create_shard_routing(class_, *pargs, **kwargs):
    routing = ShardRouting(class_.keyspace)
    routing.keyrange = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return routing

  @classmethod
  def create_vtgate_cursor(class_, routing, vtgate_conn, tablet_type, is_dml, **kargs):
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

class LookupBase(object):
  # FIXME:  does it always need to be a classmethod ?
  # FIXME: what is the best way of creating an overridable interface ?
  @classmethod
  def get(class_, entity_id_column, entity_id):
    raise NotImplementedError

  @classmethod
  def create(class_, sharding_key_column, sharding_key, entity_id_column, entity_id):
    raise NotImplementedError

  @classmethod
  def update(class_, entity_id_column, entity_id, new_entity_id):
    raise NotImplementedError

  @classmethod
  def delete(class_, sharding_key_column_name, sharding_key):
    raise NotImplementedError


class LookupDBObject(LookupBase, DBObjectUnsharded):
  """This is an example implementation of lookup class where it is stored
  in unsharded db.
  """
  @db_class_method
  def get(class_, cursor, entity_id_column, entity_id):
    where_column_value_pairs = [(entity_id_column, entity_id),]
    rows =  class_.select_by_columns(class_, cursor, where_column_value_pairs)
    return [row.__dict__ for row in rows]

  @db_class_method
  def create(class_, cursor, sharding_key_column, sharding_key, entity_id_column, entity_id):
    return class_.insert(sharding_key_column=sharding_key, entity_id_column=entity_id)

  @db_class_method
  def update(class_, cursor, sharding_key_column_name, sharding_key,
             entity_id_column, new_entity_id):
    where_column_value_pairs = [(sharding_key_column_name, sharding_key),]
    return class_.update_columns(class_, cursor, where_column_value_pairs,
                                 entity_id_column=new_entity_id)

  @db_class_method
  def delete(class_, cursor, sharding_key_column_name, sharding_key):
    where_column_value_pairs = [(sharding_key_column_name, sharding_key),]
    return class_.delete_by_columns(class_, cursor, where_column_value_pairs)


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

  id_column_name = None
  sharding_key_column_name = None
  entity_id_lookup_map = None
  lookup_writable_entities = None

  @classmethod
  def create_shard_routing(class_, *pargs,  **kargs):
    cursor_method = pargs[0]
    routing = ShardRouting(class_.keyspace)
    routing.sharding_key = kargs.get('sharding_key', None)

    keyrange = kargs.get("keyrange", None)
    if keyrange is not None:
      routing.keyrange = keyrange
      return routing

    if routing.sharding_key is None:
      try:
        entity_id_column = kargs['entity_id_column']
        entity_id = kargs['entity_id']
        # this may involve a lookup of the index from db.
        # consider caching it at the app layer for performance.
        entity_id_sharding_key_map = class_.lookup_sharding_key_from_entity_id(
            cursor_method, entity_id_column, entity_id)
        routing.entity_id_sharding_key_map = entity_id_sharding_key_map
        # FIXME: should this be an iterable or do we need to index this ?
        routing.sharding_key = entity_id_sharding_key_map.values()
      except KeyError, e:
        raise dbexceptions.ProgrammingError(
            "For sharded table, sharding_key and entity_id cannot both be empty.")

    if not class_.is_sharding_key_valid(routing.sharding_key):
      raise dbexceptions.InternalError("Invalid sharding_key %s" % sharding_key)

    return routing

  @classmethod
  def create_vtgate_cursor(class_, routing, vtgate_conn, tablet_type, is_dml, **kargs):
    if is_dml:
      if routing.sharding_key is None or _is_iterable_container(routing.sharding_key):
        dbexceptions.InternalError(
            "Writes require unique sharding_key")

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
  def lookup_sharding_key_from_entity_id(class_, cursor_method, entity_id_column, entity_id):
    """This method is used to map any entity id to sharding key.

    Args:
      entity_id_column: Non-sharding key indexes that can be used for query routing.
      entity_id: entity id value.

    Returns:
      sharding key to be used for routing.
    """
    lookup_class = class_.entity_id_lookup_map[entity_id_column]
    return lookup_class.get(class_, cursor_method, entity_id_column, entity_id)

  @db_class_method
  def select_by_ids(class_, cursor, where_column_value_pairs,
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
    rowcount = cursor.execute_entity_ids(query, bind_vars, class_.id_column_name)
    rows = cursor.fetchall()
    return [sql_builder.DBRow(columns_list, row) for row in rows]

  @classmethod
  def is_sharding_key_valid(class_, sharding_key):
    """Method to check the validity of sharding key for the table.

    Args:
      sharding_key: sharding_key to be validated.

    Returns:
      bool
    """
    raise NotImplementedError

  @classmethod
  def sharding_key_to_keyspace_id(class_, sharding_key):
    """Method to create keyspace_id from sharding_key.

    Args:
      sharding_key: sharding_key

    Returns:
      keyspace_id
    """
    raise NotImplementedError


class DBObjectEntityRangeSharded(DBObjectRangeSharded):
  """Base class for sharded tables that also need to create and manage lookup
  entities.

  This provides default implementation of routing helper methods, cursor
  creation and common database access operations.
  This abstracts sharding information and provides helper methods
  for common database access operations.
  """
  keyspace = None
  sharding = shard_constants.RANGE_SHARDED

  table_name = None
  columns_list = None

  id_column_name = None
  sharding_key_column_name = None
  entity_id_lookup_map = None
  lookup_writable_entities = None

  @classmethod
  def create_shard_routing(class_, *pargs,  **kargs):
    cursor_method = pargs[0]
    routing = ShardRouting(class_.keyspace)
    routing.sharding_key = kargs.get('sharding_key', None)

    keyrange = kargs.get("keyrange", None)
    if keyrange is not None:
      routing.keyrange = keyrange
      return routing

    if routing.sharding_key is None:
      try:
        entity_id_column = kargs['entity_id_column']
        entity_id = kargs['entity_id']
        # this may involve a lookup of the index from db.
        # consider caching it at the app layer for performance.
        entity_id_sharding_key_map = class_.lookup_sharding_key_from_entity_id(
            cursor_method, entity_id_column, entity_id)
        routing.entity_id_sharding_key_map = entity_id_sharding_key_map
        routing.sharding_key = entity_id_sharding_key_map.values()
      except KeyError, e:
        raise dbexceptions.ProgrammingError(
            "For sharded table, sharding_key and entity_id cannot both be empty.")

    if not class_.is_sharding_key_valid(routing.sharding_key):
      raise dbexceptions.InternalError("Invalid sharding_key %s" % sharding_key)

    return routing

  @classmethod
  def create_vtgate_cursor(class_, routing, vtgate_conn, tablet_type, is_dml, **kargs):
    if is_dml:
      if routing.sharding_key is None or _is_iterable_container(routing.sharding_key):
        dbexceptions.InternalError(
            "Writes require unique sharding_key")

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
  def create_sharding_key_entity_id_lookup(class_, cursor_method, sharding_key,
                                           entity_id_column, entity_id):
    """This method is used to map any entity id to sharding key.

    Args:
      entity_id_column: Non-sharding key indexes that can be used for query routing.
      entity_id: entity id value.

    Returns:
      sharding key to be used for routing.
    """
    lookup_class = class_.entity_id_lookup_map[entity_id_column]
    return lookup_class.create(class_, cursor_method,
                               class_.sharding_key_column_name,
                               sharding_key, entity_id_column, entity_id)

  @classmethod
  def delete_sharding_key_entity_id_lookup(class_, cursor_method,
                                           sharding_key):
    for lookup_class in class_.entity_id_lookup_map.values():
      lookup_class.delete(class_, cursor_method,
                          class_.sharding_key_column_name,
                          sharding_key)


  @classmethod
  def update_sharding_key_entity_id_lookup(class_, cursor_method,
                                           sharding_key, entity_id_column,
                                           new_entity_id):
    lookup_class = class_.entity_id_lookup_map[entity_id_column]
    return lookup_class.update(class_, cursor_method,
                               class_.sharding_key_column_name,
                               sharding_key,
                               entity_id_column,
                               new_entity_id)


  @db_class_method
  def insert_primary(class_, cursor, sharding_key, **bind_vars):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query, bind_vars = sql_builder.insert_query(class_.table_name,
                                                class_.columns_list,
                                                **bind_variables)
    cursor.execute(query, bind_variables)
    return cursor.lastrowid


  @classmethod
  def insert(class_, cursor, **bind_variables):
    sharding_key = None

    # no lookup relationship to be created.
    if not class_.lookup_writable_entities:
      try:
        sharding_key = bind_variables[class_.sharding_key_column_name]
      except KeyError:
        raise dbexceptions.ProgrammingError("sharding key column '%s' cannot be absent from bind_variables" % class_.sharding_key_column_name)

    for entity_col in class_.entity_id_lookup_map.keys():
      entity_id = bind_variables[entity_col]
      sharding_key = class_.create_sharding_key_entity_id_lookup(
          cursor, class_.sharding_key_column_name, sharding_key, entity_col,
          entity_id)

    bind_variables[class_.sharding_key_column_name] = sharding_key
    class_.insert_primary(cursor, sharding_key, **bind_variables)
    return sharding_key

  @db_class_method
  def update_columns(class_, cursor, sharding_key, where_column_value_pairs,
                     **update_columns):

    # update the primary table first.
    query, bind_variables = sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs, **update_columns)

    rowcount = cursor.execute(query, bind_variables)

    # If the entity_id column is being updated, update lookup map.
    for entity_col in class_.entity_id_lookup_map.keys():
      if entity_col in update_columns:
        class_.update_sharding_key_entity_id_lookup(cursor, col, sharding_key,
                                                    entity_col,
                                                    update_columns[entity_col])

    return rowcount

  @db_class_method
  def delete_by_columns(class_, cursor, sharding_key, where_column_value_pairs, limit=None,
                        **columns):
    # delete the rows from primary table.
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

    rowcount = cursor.rowcount

    #delete the lookup map.
    class_.delete_sharding_key_entity_id_lookup(cursor, sharding_key)

    return rowcount


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

  @classmethod
  def create_shard_routing(class_, *pargs, **kargs):
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
  def create_vtgate_cursor(class_, routing, vtgate_conn, tablet_type, is_dml, **kargs):

    # FIXME:extend VTGateCursor's api to accept shard_names
    # and allow queries based on that.
    cursor = vtgate_cursor.VTGateCursor(vtgate_conn, class_.keyspace,
                                        tablet_type,
                                        keyranges=[routing.shard_name,],
                                        writable=is_dml)
    return cursor
