update user set nickname='alice' where id=1;
update user set nickname='alice' where name='alice';
update user set pet='fido' where id=1;

/* not supported - multi-shard update */
-- update user set pet='rover' where name='alice';
