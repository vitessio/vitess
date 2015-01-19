"""Module containing base class for tables in unsharded keyspace.

DBObjectUnsharded inherits from DBObjectBase, the implementation
for the common database operations is defined in DBObjectBase.
DBObjectUnsharded defines the cursor creation methods for the same.
"""
import functools
import logging
import struct

from vtdb import db_object
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import vtgate_cursor


class DBObjectUnsharded(db_object.DBObjectBase):
  """Base class for unsharded db classes.

  This provides default implementation of routing helper methods and cursor
  creation. The common database access operations are defined in the base class.
  """
  keyspace = None
  sharding = shard_constants.UNSHARDED

  table_name = None
  columns_list = None


  @classmethod
  def create_shard_routing(class_, *pargs, **kwargs):
    routing = db_object.ShardRouting(class_.keyspace)
    routing.keyrange = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    return routing

  @classmethod
  def create_vtgate_cursor(class_, vtgate_conn, tablet_type, is_dml, **cursor_kargs):
    routing = class_.create_shard_routing(**cursor_kargs)
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
