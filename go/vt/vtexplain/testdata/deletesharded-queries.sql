delete from music_extra where id=1;
delete from music_extra where id=1 and extra='abc';

/* with lookup index by pk */
delete from user where id=1;
/* with lookup index by other field */
delete from user where name='billy';

/* multi-shard delete is supported but racy due to the commit ordering */
-- delete from music_extra where extra='abc';

/* multi-shard delete with autocommit is supported */
delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from music_extra where extra='abc';

/* multi-shard delete with limit and autocommit is supported using keyrange targeting */
delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from `ks_sharded[-]`.music_extra where extra='abc' LIMIT 10;
