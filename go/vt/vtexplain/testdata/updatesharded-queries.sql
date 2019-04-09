update user set nickname='alice' where id=1;
update user set nickname='alice' where name='alice';
update user set pet='fido' where id=1;

/* update secondary vindex value */
update user set name='alicia' where id=1;
update user set name='alicia' where name='alice';

/* scatter update -- supported but with nondeterministic output */
/* update name_info set has_nickname=1 where nickname != ''; */

/* scatter update autocommit */
update /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ name_info set has_nickname=1 where nickname != '';

/* multi-shard update by secondary vindex */
update user set pet='rover' where name='alice';

/* update in a transaction on one shard */
begin;
update user set nickname='alice' where id=1;
update user set nickname='bob' where id=1;
commit;

/* update in a transaction on multiple shards */
begin;
update user set nickname='alice' where id=1;
update user set nickname='bob' where id=3;
commit;
