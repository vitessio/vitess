
select * from user_region where regionId = 2305843009213693952 and userId = 100 /* exact shard */;
select * from user_region where regionId = 2305843009213693952 /* subshard */;
