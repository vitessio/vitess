drop table if exists onlineddl_test;
create table onlineddl_test (
  id int(11) NOT NULL AUTO_INCREMENT,
  name varchar(512) DEFAULT NULL,
  v varchar(255) DEFAULT NULL COMMENT '添加普通列测试',
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=gbk;

insert into onlineddl_test values (null, 'gbk-test-initial', '添加普通列测试-添加普通列测试');
insert into onlineddl_test values (null, 'gbk-test-initial', '添加普通列测试-添加普通列测试');

drop event if exists onlineddl_test;
delimiter ;;
create event onlineddl_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into onlineddl_test (name) values ('gbk-test-default');
  insert into onlineddl_test values (null, 'gbk-test', '添加普通列测试-添加普通列测试');
  update onlineddl_test set v='添加普通列测试' where v='添加普通列测试-添加普通列测试' order by id desc limit 1;
end ;;
