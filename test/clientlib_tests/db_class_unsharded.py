"""DB Classes for testing client library.

These classes are used for testing various types of scenarios like
- sharding schemes
- schema types
- lookup

This module contains the schema and class definitions.
"""

import topo_schema
from vtdb import db_object
from vtdb import database_context

class VtUnsharded(db_object.DBObjectUnsharded):
  keyspace = topo_schema.KS_UNSHARDED[0]
  table_name = "vt_unsharded"
  columns_list = ['id', 'msg']

  @classmethod
  def select_by_id(class_, cursor, id_val):
    where_column_value_pairs = [('id', id_val),]
    return class_.select_by_columns(cursor, where_column_value_pairs)
