
select * from user_region where regionId = 4611686018427387904 and userId = 100 /* exact shard */;
select * from user_region where regionId = 4611686018427387904 /* subshard */;
select * from user_region where regionId in (4611686018427387903, 4611686018427387904) /* subshard */;
select * from user_region where userId = 100 /* scatter, needs prefix columns for subshard routing */;
