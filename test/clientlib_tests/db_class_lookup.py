"""DB Classes for testing client library.

These classes are used for testing various types of scenarios like
- sharding schemes
- schema types
- lookup

This module contains the schema and class definitions.
"""

import topo_schema
from vtdb import db_object


class VtUsernameLookup(db_object.DBObjectUnsharded):
  keyspace = topo_schema.KS_LOOKUP[0]
  table_name = "vt_username_lookup"
  columns_list = ['user_id', 'username']


class VtSongUserLookup(db_object.DBObjectUnsharded):
  keyspace = topo_schema.KS_LOOKUP[0]
  table_name = "vt_song_user_lookup"
  columns_list = ['song_id', 'user_id']
