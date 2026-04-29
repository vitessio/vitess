create table if not exists customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into customer_seq(id, next_id, cache) values(0, 1000, 100);
create table if not exists order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into order_seq(id, next_id, cache) values(0, 1000, 100);
