"""Module containing base classes and helper methods for database objects.

The base classes represent different sharding schemes like
unsharded, range-sharded and custom-sharded tables.
This abstracts sharding details and provides methods
for common database access patterns.
"""
import functools
import logging
import struct

from vtdb import db_object
from vtdb import db_object_unsharded
from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import shard_constants
from vtdb import vtgate_cursor


# TODO: is a generic Lookup interface for non-db based look classes needed ?
class LookupDBObject(db_object_unsharded.DBObjectUnsharded):
  """This is an example implementation of lookup class where it is stored
  in unsharded db.
  """
  @classmethod
  def get(class_, cursor, entity_id_column, entity_id):
    where_column_value_pairs = [(entity_id_column, entity_id),]
    rows =  class_.select_by_columns(cursor, where_column_value_pairs)
    return [row.__dict__ for row in rows]

  @classmethod
  def create(class_, cursor, **bind_vars):
    return class_.insert(cursor, **bind_vars)

  @classmethod
  def update(class_, cursor, sharding_key_column_name, sharding_key,
             entity_id_column, new_entity_id):
    where_column_value_pairs = [(sharding_key_column_name, sharding_key),]
    return class_.update_columns(cursor, where_column_value_pairs,
                                 entity_id_column=new_entity_id)

  @classmethod
  def delete(class_, cursor, sharding_key_column_name, sharding_key):
    where_column_value_pairs = [(sharding_key_column_name, sharding_key),]
    return class_.delete_by_columns(cursor, where_column_value_pairs)
