"""Module containing base classes and helper methods for database objects.

The base classes represent different sharding schemes like
unsharded, range-sharded and custom-sharded tables.
This abstracts sharding details and provides methods
for common database access patterns.
"""
import logging

from vtdb import db_object
from vtdb import dbexceptions
from vtdb import shard_constants
from vtdb import vtgate_cursor


class DBObjectCustomSharded(db_object.DBObjectBase):
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
  def create_shard_routing(class_, *pargs, **kwargs):
    routing = db_object.ShardRouting(keyspace)
    routing.shard_name = kargs.get('shard_name')
    if routing.shard_name is None:
      dbexceptions.InternalError("For custom sharding, shard_name cannot be None.")

    if (_is_iterable_container(routing.shard_name)
        and is_dml):
      raise dbexceptions.InternalError(
          "Writes are not allowed on multiple shards.")
    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **cursor_kargs):
    # FIXME:extend VTGateCursor's api to accept shard_names
    # and allow queries based on that.
    routing = class_.create_shard_routing(**cursor_kargs)
    cursor = vtgate_cursor.VTGateCursor(vtgate_conn, class_.keyspace,
                                        tablet_type,
                                        keyranges=[routing.shard_name,],
                                        writable=is_dml)
    return cursor
