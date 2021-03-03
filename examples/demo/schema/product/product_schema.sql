create table product(product_id bigint auto_increment, pname varchar(128), primary key(product_id));
create table customer_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into customer_seq(id, next_id, cache) values(0, 1, 3);
create table corder_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into corder_seq(id, next_id, cache) values(0, 1, 3);
create table corder_event_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into corder_event_seq(id, next_id, cache) values(0, 1, 3);
create table corder_keyspace_idx(corder_id bigint not null auto_increment, keyspace_id varbinary(10), primary key(corder_id));
