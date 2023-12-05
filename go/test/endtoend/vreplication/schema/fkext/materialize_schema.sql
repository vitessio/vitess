create table parent_copy(id int, name varchar(128), primary key(id)) engine=innodb;
create table child_copy(id int, parent_id int, name varchar(128), primary key(id)) engine=innodb;