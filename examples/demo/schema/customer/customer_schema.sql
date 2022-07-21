create table customer(customer_id bigint, uname varchar(128), primary key(customer_id));
create table corder(corder_id bigint, customer_id bigint, product_id bigint, oname varchar(128), primary key(corder_id));
create table corder_event(corder_event_id bigint, corder_id bigint, ename varchar(128), keyspace_id varbinary(10), primary key(corder_id, corder_event_id));
create table oname_keyspace_idx(oname varchar(128), corder_id bigint, keyspace_id varbinary(10), primary key(oname, corder_id));
