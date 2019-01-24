update user set nickname='alice' where id=1;
update user set nickname='alice' where name='alice';
update user set pet='fido' where id=1;

/* update secondary vindex value */
update user set name='alicia' where id=1;
update user set name='alicia' where name='alice';

/* not supported - multi-shard update */
-- update user set pet='rover' where name='alice';
