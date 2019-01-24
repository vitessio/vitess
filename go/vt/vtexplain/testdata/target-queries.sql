/*
 * These are the same set of queries from "options-queries.sql" but
 * are run with an explicit shard target of ks_sharded/40-80 which
 * bypasses the normal v3 routing.
 */
select * from user where email='null@void.com';
select * from user where id in (1,2,3,4,5,6,7,8);
insert into user (id, name) values (2, 'bob');
