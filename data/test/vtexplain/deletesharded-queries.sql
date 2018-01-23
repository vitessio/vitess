delete from music_extra where id=1;
delete from music_extra where id=1 and extra='abc';

/* with lookup index by pk */
delete from user where id=1;
/* with lookup index by other field */
delete from user where name='billy';

/* not supported - multi-shard delete */
--delete from user where pet='rover';
