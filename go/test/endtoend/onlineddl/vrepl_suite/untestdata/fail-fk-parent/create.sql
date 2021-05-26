drop table if exists gh_ost_test_child;
drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  primary key(id)
) engine=innodb auto_increment=1;

create table gh_ost_test_child (
  id int auto_increment,
  i int not null,
  parent_id int not null,
  constraint test_fk foreign key (parent_id) references onlineddl_test (id) on delete no action,
  primary key(id)
) engine=innodb;
insert into onlineddl_test (id) values (1),(2),(3);

drop event if exists onlineddl_test;
drop event if exists gh_ost_test_cleanup;

delimiter ;;
create event onlineddl_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into gh_ost_test_child values (null, 11, 1);
  insert into gh_ost_test_child values (null, 13, 2);
  insert into gh_ost_test_child values (null, 17, 3);
end ;;

create event gh_ost_test_cleanup
  on schedule at current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  drop table if exists gh_ost_test_child;
end ;;
