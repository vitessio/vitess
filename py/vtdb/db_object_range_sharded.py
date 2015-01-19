"""Module containing base classes for range-sharded database objects.

There are two base classes for tables that live in range-sharded keyspace -
1. DBObjectRangeSharded -  This should be used for tables that only reference lookup entities
but don't create or manage them. Please see examples in test/clientlib_tests/db_class_sharded.py.
2. DBObjectEntityRangeSharded - This inherits from DBObjectRangeSharded and is used for tables
and also create new lookup relationships.
This module also contains helper methods for cursor creation for accessing lookup tables
and methods for dml and select for the above mentioned base classes.
"""
import functools
import logging
import struct

from vtdb import db_object
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import sql_builder
from vtdb import vtgate_cursor


# This creates a 64 binary packed string for keyspace_id.
# This is used for cursor creation so that keyspace_id can
# be passed as rpc param for vtgate.
pack_keyspace_id = struct.Struct('!Q').pack


# This unpacks the keyspace_id so that it can be used
# in bind variables.
def unpack_keyspace_id(kid):
 return struct.Struct('!Q').unpack(kid)[0]




class DBObjectRangeSharded(db_object.DBObjectBase):
  """Base class for range-sharded db classes.

  This provides default implementation of routing helper methods, cursor
  creation and common database access operations.
  """
  # keyspace of this table. This is needed for routing.
  keyspace = None

  # sharding scheme for this base class.
  sharding = shard_constants.RANGE_SHARDED

  # table name for the corresponding database table. This is used in query
  # construction.
  table_name = None

  # List of columns on the database table. This is used in query construction.
  columns_list = None

  #FIXME: is this needed ?
  id_column_name = None

  # sharding_key_column_name defines column name for sharding key for this
  # table.
  sharding_key_column_name = None

  # entity_id_lookup_map defines entity lookup relationship. It is a map of
  # column names for this table to lookup class that contains the mapping of
  # this entity to sharding key for this keyspace.
  entity_id_lookup_map = None

  # column_lookup_name_map defines the map of column names from this table
  # to the corresponding lookup table. This should be used when column_names
  # in the main table and lookup table is different. This is used in
  # conjunction with sharding_key_column_name and entity_id_lookup_map.
  column_lookup_name_map = None

  @classmethod
  def create_shard_routing(class_, *pargs,  **kargs):
    """This creates the ShardRouting object based on the kargs.
    This prunes the routing kargs so as not to interfere with the
    actual database method.

    Args:
      *pargs: Positional arguments
      **kargs: Routing key-value params. These are used to determine routing.
      There are two mutually exclusive mechanisms to indicate routing.
      1. entity_id_map {"entity_id_column": entity_id_value} where entity_id_column
      could be the sharding key or a lookup based entity column of this table. This
      helps determine the keyspace_ids for the cursor.
      2. keyrange - This helps determine the keyrange for the cursor.

    Returns:
     ShardRouting object and modified kargs
    """
    lookup_cursor_method = pargs[0]
    routing = db_object.ShardRouting(class_.keyspace)
    entity_id_map = None

    entity_id_map = kargs.get("entity_id_map", None)
    if entity_id_map is None:
      kr = None
      key_range = kargs.get("keyrange", None)
      if isinstance(key_range, keyrange.KeyRange):
        kr = key_range
      else:
        kr = keyrange.KeyRange(key_range)
      if kr is not None:
        routing.keyrange = kr
      # Both entity_id_map and keyrange have been evaluated. Return.
      return routing

    # entity_id_map is not None
    if len(entity_id_map) != 1:
      dbexceptions.ProgrammingError("Invalid entity_id_map '%s'" % entity_id_map)

    entity_id_col = entity_id_map.keys()[0]
    entity_id = entity_id_map[entity_id_col]

    #TODO: the current api means that if a table doesn't have the sharding key column name
    # then it cannot pass in sharding key for routing purposes. Will this cause
    # extra load on lookup db/cache ? This is cleaner from a design perspective.
    if entity_id_col == class_.sharding_key_column_name:
      # Routing using sharding key.
      routing.sharding_key = entity_id
      if not class_.is_sharding_key_valid(routing.sharding_key):
        raise dbexceptions.InternalError("Invalid sharding_key %s" % routing.sharding_key)
    else:
      # Routing using lookup based entity.
      routing.entity_column_name = entity_id_col
      routing.entity_id_sharding_key_map = class_.lookup_sharding_key_from_entity_id(
          lookup_cursor_method, entity_id_col, entity_id)

    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **cursor_kargs):
    cursor_method = functools.partial(db_object.create_cursor_from_params,
                                      vtgate_conn, tablet_type, False)
    routing = class_.create_shard_routing(cursor_method, **cursor_kargs)
    if is_dml:
      if routing.sharding_key is None or db_object._is_iterable_container(routing.sharding_key):
        dbexceptions.InternalError(
            "Writes require unique sharding_key")

    keyspace_ids = None
    keyranges = None
    if routing.sharding_key is not None:
      keyspace_ids = []
      if db_object._is_iterable_container(routing.sharding_key):
        for sk in routing.sharding_key:
          kid = class_.sharding_key_to_keyspace_id(sk)
          keyspace_ids.append(pack_keyspace_id(kid))
      else:
        kid = class_.sharding_key_to_keyspace_id(routing.sharding_key)
        keyspace_ids = [pack_keyspace_id(kid),]
    elif routing.entity_id_sharding_key_map is not None:
      keyspace_ids = []
      for sharding_key in routing.entity_id_sharding_key_map.values():
        keyspace_ids.append(pack_keyspace_id(class_.sharding_key_to_keyspace_id(sharding_key)))
    elif routing.keyrange:
      keyranges = [routing.keyrange,]

    cursor = vtgate_cursor.VTGateCursor(vtgate_conn,
                                        class_.keyspace,
                                        tablet_type,
                                        keyspace_ids=keyspace_ids,
                                        keyranges=keyranges,
                                        writable=is_dml)
    cursor.routing = routing
    return cursor

  @classmethod
  def get_lookup_column_name(class_, column_name):
    """Return the lookup column name for a column name from this table.
    If the entry doesn't exist it is assumed that the column_name is same.
    """
    return class_.column_lookup_name_map.get(column_name, column_name)

  @classmethod
  def lookup_sharding_key_from_entity_id(class_, cursor_method, entity_id_column, entity_id):
    """This method is used to map any entity id to sharding key.

    Args:
      entity_id_column: Non-sharding key indexes that can be used for query routing.
      entity_id: entity id value.

    Returns:
      sharding key to be used for routing.
    """
    lookup_column = class_.get_lookup_column_name(entity_id_column)
    lookup_class = class_.entity_id_lookup_map[lookup_column]
    rows = lookup_class.get(cursor_method, entity_id_column, entity_id)

    sk_lookup_column = class_.get_lookup_column_name(class_.sharding_key_column_name)
    entity_id_sharding_key_map = {}
    for row in rows:
      en_id = row[lookup_column]
      sk = row[sk_lookup_column]
      entity_id_sharding_key_map[en_id] = sk

    return entity_id_sharding_key_map

  @db_object.db_class_method
  def select_by_ids(class_, cursor, where_column_value_pairs,
                        columns_list = None,order_by=None, group_by=None,
                        limit=None, **kwargs):
    """This method is used to perform in-clause queries.

    Such queries can cause vtgate to scatter over multiple shards.
    This uses execute_entity_ids method of vtgate cursor and the entity
    column and the associated entity_keyspace_id_map is computed based
    on the routing used - sharding_key or entity_id_map.
    """

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

    entity_col_name = None
    entity_id_keyspace_id_map = {}
    if cursor.routing.sharding_key is not None:
      # If the in-clause is based on sharding key
      entity_col_name = class_.sharding_key_column_name
      if db_object._is_iterable_container(cursor.routing.sharding_key):
        for sk in list(cursor.routing.sharding_key):
          entity_id_keyspace_id_map[sk] = pack_keyspace_id(class_.sharding_key_to_keyspace_id(sk))
      else:
        sk = cursor.routing.sharding_key
        entity_id_keyspace_id_map[sk] = pack_keyspace_id(class_.sharding_key_to_keyspace_id(sk))
    elif cursor.routing.entity_id_sharding_key_map is not None:
      # If the in-clause is based on entity column
      entity_col_name = cursor.routing.entity_column_name
      for en_id, sk in cursor.routing.entity_id_sharding_key_map.iteritems():
        entity_id_keyspace_id_map[en_id] = pack_keyspace_id(class_.sharding_key_to_keyspace_id(sk))
    else:
      dbexceptions.ProgrammingError("Invalid routing method used.")

    # cursor.routing.entity_column_name is set while creating shard routing.
    rowcount = cursor.execute_entity_ids(query, bind_vars,
                                         entity_id_keyspace_id_map,
                                         entity_col_name)
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

  @db_object.db_class_method
  def insert(class_, cursor, **bind_vars):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    keyspace_id = bind_vars.get('keyspace_id', None)
    if keyspace_id is None:
      kid = cursor.keyspace_ids[0]
      keyspace_id = unpack_keyspace_id(kid)
      bind_vars['keyspace_id'] = keyspace_id

    query, bind_vars = sql_builder.insert_query(class_.table_name,
                                                class_.columns_list,
                                                **bind_vars)
    cursor.execute(query, bind_vars)
    return cursor.lastrowid

  @classmethod
  def _add_keyspace_id(class_, keyspace_id, where_column_value_pairs):
    where_col_dict = dict(where_column_value_pairs)
    if 'keyspace_id' not in where_col_dict:
      where_column_value_pairs.append(('keyspace_id', keyspace_id))

    return where_column_value_pairs

  @db_object.db_class_method
  def update_columns(class_, cursor, where_column_value_pairs,
                     **update_columns):

    where_column_value_pairs = class_._add_keyspace_id(
        unpack_keyspace_id(cursor.keyspace_ids[0]),
        where_column_value_pairs)

    query, bind_vars = sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs, **update_columns)

    rowcount = cursor.execute(query, bind_vars)

    # If the entity_id column is being updated, update lookup map.
    if class_.entity_id_lookup_map is not None:
      for entity_col in class_.entity_id_lookup_map.keys():
        if entity_col in update_columns:
          class_.update_sharding_key_entity_id_lookup(cursor, sharding_key,
                                                      entity_col,
                                                      update_columns[entity_col])

    return rowcount

  @db_object.db_class_method
  def delete_by_columns(class_, cursor, where_column_value_pairs, limit=None):

    if not where_column_value_pairs:
      raise dbexceptions.ProgrammingError("deleting the whole table is not allowed")

    where_column_value_pairs = class_._add_keyspace_id(
        unpack_keyspace_id(cursor.keyspace_ids[0]), where_column_value_pairs)

    query, bind_vars = sql_builder.delete_by_columns_query(class_.table_name,
                                                              where_column_value_pairs,
                                                              limit=limit)
    cursor.execute(query, bind_vars)
    if cursor.rowcount == 0:
      raise dbexceptions.DatabaseError("DB Row not found")

    return cursor.rowcount


class DBObjectEntityRangeSharded(DBObjectRangeSharded):
  """Base class for sharded tables that also needs to create and manage lookup
  entities.

  This provides default implementation of routing helper methods, cursor
  creation and common database access operations.
  """

  @classmethod
  def get_insert_id_from_lookup(class_, cursor_method, entity_id_col, **bind_vars):
    """This method is used to map any entity id to sharding key.

    Args:
      entity_id_column: Non-sharding key indexes that can be used for query routing.
      entity_id: entity id value.

    Returns:
      sharding key to be used for routing.
    """
    lookup_class = class_.entity_id_lookup_map[entity_id_col]
    new_bind_vars = {}
    for col, value in bind_vars.iteritems():
      lookup_col = class_.get_lookup_column_name(col)
      new_bind_vars[lookup_col] = value
    return lookup_class.create(cursor_method, **new_bind_vars)

  @classmethod
  def delete_sharding_key_entity_id_lookup(class_, cursor_method,
                                           sharding_key):
    sharding_key_lookup_column = class_.get_lookup_column_name(class_.sharding_key_column_name)
    for lookup_class in class_.entity_id_lookup_map.values():
      lookup_class.delete(cursor_method,
                          sharding_key_lookup_column,
                          sharding_key)


  @classmethod
  def update_sharding_key_entity_id_lookup(class_, cursor_method,
                                           sharding_key, entity_id_column,
                                           new_entity_id):
    sharding_key_lookup_column = class_.get_lookup_column_name(class_.sharding_key_column_name)
    entity_id_lookup_column = class_.get_lookup_column_name(entity_id_column)
    lookup_class = class_.entity_id_lookup_map[entity_id_column]
    return lookup_class.update(cursor_method,
                               sharding_key_lookup_column,
                               sharding_key,
                               entity_id_lookup_column,
                               new_entity_id)


  @db_object.db_class_method
  def insert_primary(class_, cursor, **bind_vars):
    if class_.columns_list is None:
      raise dbexceptions.ProgrammingError("DB class should define columns_list")

    query, bind_vars = sql_builder.insert_query(class_.table_name,
                                                class_.columns_list,
                                                **bind_vars)
    cursor.execute(query, bind_vars)
    return cursor.lastrowid


  @classmethod
  def insert(class_, cursor_method, **bind_vars):
    """ This method creates the lookup relationship as well as the insert
    in the primary table. The creation of the lookup entry also creates the
    primary key for the row in the primary table.

    The lookup relationship is determined by class_.column_lookup_name_map and the bind
    variables passed in. There are two types of entities -
    1. Table for which the entity that is also the primary sharding key for this keyspace.
    2. Entity table that creates a new entity and needs to create a lookup between
    that entity and sharding key.
    """
    if class_.sharding_key_column_name is None:
      raise dbexceptions.ProgrammingError(
          "sharding_key_column_name empty for DBObjectEntityRangeSharded")

    # Used for insert into class_.table_name
    new_inserted_key = None
    # Used for routing the insert_primary
    entity_id_map = {}


    # Create the lookup entry first
    if class_.sharding_key_column_name in bind_vars:
      # Secondary entity creation
      sharding_key = bind_vars[class_.sharding_key_column_name]
      entity_col = class_.entity_id_lookup_map.keys()[0]
      lookup_bind_vars = {class_.sharding_key_column_name, sharding_key}
      entity_id = class_.get_insert_id_from_lookup(cursor_method, entity_col,
                                                   **lookup_bind_vars)
      bind_vars[entity_col] = entity_id
      new_inserted_key = entity_id
      entity_id_map[entity_col] = entity_id
    else:
      # Primary sharding key creation
      # FIXME: what if class_.entity_id_lookup_map was empty ?
      # there would need to be some table on which there was an auto-inc
      # to generate the primary sharding key.
      entity_col = class_.entity_id_lookup_map.keys()[0]
      entity_id = bind_vars[entity_col]
      lookup_bind_vars = {entity_col: entity_id}
      sharding_key = class_.get_insert_id_from_lookup(cursor_method, entity_col,
                                                      **lookup_bind_vars)
      bind_vars[class_.sharding_key_column_name] = sharding_key
      new_inserted_key = sharding_key
      entity_id_map[class_.sharding_key_column_name] = sharding_key

    # FIXME: is the not value check correct ?
    if 'keyspace_id' not in bind_vars or not bind_vars['keyspace_id']:
      keyspace_id = class_.sharding_key_to_keyspace_id(sharding_key)
      bind_vars['keyspace_id'] = keyspace_id

    # entity_id_map is used for routing and hence passed to cursor_method
    #bind_vars['entity_id_map'] = entity_id_map
    new_cursor = functools.partial(cursor_method, entity_id_map=entity_id_map)
    class_.insert_primary(new_cursor, **bind_vars)
    return new_inserted_key

  @db_object.db_class_method
  def update_columns(class_, cursor, where_column_value_pairs,
                     **update_columns):

    sharding_key = cursor.routing.sharding_key
    if sharding_key is None:
      raise dbexceptions.ProgrammingError("sharding_key cannot be empty")

    # update the primary table first.
    query, bind_vars = sql_builder.update_columns_query(
        class_.table_name, where_column_value_pairs, **update_columns)

    rowcount = cursor.execute(query, bind_vars)

    # If the entity_id column is being updated, update lookup map.
    lookup_cursor_method = functools.partial(
        db_object.create_cursor_from_old_cursor, cursor)
    for entity_col in class_.entity_id_lookup_map.keys():
      if entity_col in update_columns:
        class_.update_sharding_key_entity_id_lookup(lookup_cursor_method,
                                                    sharding_key,
                                                    entity_col,
                                                    update_columns[entity_col])

    return rowcount

  @db_object.db_class_method
  def delete_by_columns(class_, cursor, where_column_value_pairs,
                        limit=None):
    sharding_key = cursor.routing.sharding_key
    if sharding_key is None:
      raise dbexceptions.ProgrammingError("sharding_key cannot be empty")

    if not where_column_value_pairs:
      raise dbexceptions.ProgrammingError("deleting the whole table is not allowed")

    query, bind_vars = sql_builder.delete_by_columns_query(class_.table_name,
                                                              where_column_value_pairs,
                                                              limit=limit)
    cursor.execute(query, bind_vars)
    if cursor.rowcount == 0:
      raise dbexceptions.DatabaseError("DB Row not found")

    rowcount = cursor.rowcount

    #delete the lookup map.
    lookup_cursor_method = functools.partial(
        db_object.create_cursor_from_old_cursor, cursor)
    class_.delete_sharding_key_entity_id_lookup(lookup_cursor_method, sharding_key)

    return rowcount
