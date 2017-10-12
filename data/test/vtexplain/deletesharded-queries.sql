delete from user where id=10;
delete from user where name='billy';

/* not supported - multi-shard delete */
--delete from user where pet='rover';
