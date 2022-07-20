insert into user (id, name) values(1, 'alice');
insert into user (id, name) values(2, 'bob');
insert ignore into user (id, name) values(2, 'bob');
insert ignore into user (id, name, nickname) values(2, 'bob', 'bob');
insert into user (id, name, nickname) values(2, 'bob', 'bobby') on duplicate key update nickname='bobby';
insert into user (id, name, nickname, address) values(2, 'bob', 'bobby', '123 main st') on duplicate key update nickname=values(nickname), address=values(address);

/*

TODO(demmer) These test cases induce a fundamental race in vtgate because the
insert statements race to execute on different shards, but the subsequent
commit statements are executed serially in the order that the inserts were
run.

This means that in the test output, the commits can end up in different epochs,
which causes travis test failures.

insert ignore into user (id, name) values(2, 'bob'),(3, 'charlie');
insert into user (id, name, nickname, address) values(2, 'bob', 'bobby', '123 main st'), (3, 'jane', 'janie', '456 elm st')on duplicate key update nickname=values(nickname), address=values(address);

*/

/*
With the multi-shard autocommit option selected all inserts happen in one
round trip so there is no race
*/
insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into music_extra (id, extra) values (1, 'a'), (2, 'b'), (3, 'c');

/*
 Explicit begin and commit are needed to make the vtexplain output predicable for tests to pass.
 the lookup queries gets executed without a transaction.
 That is evident from the fact that begin only gets executed when origin insert statement is executed
 and not the lookup query.
 */
begin;
insert into member (lkp, more_id, id) values ("a", 1, 1), ("b", 1, 3), ("c", 1, 1) on duplicate key update more_id = 2;
commit;