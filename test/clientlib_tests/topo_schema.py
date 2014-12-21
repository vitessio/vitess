"""This module contains keyspace and schema definitions for the client tests.

The keyspaces define various sharding schemes. And tables represent
different schema types and relationships.
"""

from vtdb import shard_constants

KS_UNSHARDED = ("KS_UNSHARDED", shard_constants.UNSHARDED)
KS_RANGE_SHARDED = ("KS_RANGE_SHARDED", shard_constants.RANGE_SHARDED)
KS_LOOKUP = ("KS_LOOKUP", shard_constants.UNSHARDED)

#KS_UNSHARDED tables
create_vt_unsharded = '''create table vt_unsharded (
id bigint,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

#KS_RANGE_SHARDED tables
create_vt_user = '''create table vt_user (
id bigint auto_increment,
username varchar(64),
msg varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
primary key (id),
key idx_username (username)
) Engine=InnoDB'''

create_vt_song = '''create table vt_song (
id bigint auto_increment,
user_id bigint,
title varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
primary key (user_id, id),
unique key id_idx (id)
) Engine=InnoDB'''

#KS_LOOKUP tables
create_vt_username_lookup = '''create table vt_username_lookup (
user_id bigint(20) NOT NULL AUTO_INCREMENT,
username varchar(20) NOT NULL,
primary key (user_id),
unique key idx_username (username)
) ENGINE=InnoDB'''

create_vt_song_user_lookup = '''create table vt_song_user_lookup (
song_id bigint(20) NOT NULL AUTO_INCREMENT,
user_id varchar(20) NOT NULL,
primary key (song_id)
) ENGINE=InnoDB'''


keyspaces = [KS_UNSHARDED, KS_RANGE_SHARDED, KS_LOOKUP]

keyspace_table_map = {KS_UNSHARDED[0]: [('vt_unsharded', create_vt_unsharded),],
                      KS_RANGE_SHARDED[0]: [('vt_user', create_vt_user),
                                           ('vt_song', create_vt_song),
                                          ],
                      KS_LOOKUP[0]: [('vt_username_lookup', create_vt_username_lookup),
                                   ],
                      }
