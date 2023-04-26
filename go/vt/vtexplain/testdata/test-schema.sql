create table t1 (
	id bigint(20) unsigned not null,
	intval bigint(20) unsigned not null default 0,
	floatval float not null default 0,
	primary key (id)
);

create table user (
	id bigint,
	name varchar(64),
	email varchar(64),
	nickname varchar(64),
	pet varchar(64),
	primary key (id)
) Engine=InnoDB;

create table name_user_map (
	name varchar(64),
	user_id bigint,
	primary key (name, user_id)
) Engine=InnoDB;

create table name_info(
	name varchar(128),
	info varchar(128),
	primary key(name)
);

create table email_info(
	name varchar(128),
	info varchar(128),
	primary key(name)
);

create table music (
	user_id bigint,
	id bigint,
	song varchar(64),
	primary key (user_id, id)
) Engine=InnoDB;

create table music_extra (
	id bigint,
	extra varchar(64),
	primary key (id)
) Engine=InnoDB;

create table table_not_in_vschema (
	id bigint,
	primary key (id)
) Engine=InnoDB;

/*
 * This is not used by the tests themselves, but is used to verify
 * that the vtexplain schema parsing logic can properly skip past
 * the mysql comments used by partitioned tables.
 */
create table test_partitioned (
	id bigint,
	date_create int,
	primary key(id)
) Engine=InnoDB
;

create table customer (
  id bigint,
  email varchar(64),
  primary key (id)
) Engine=InnoDB;

create table email_customer_map (
   email varchar(64),
   user_id bigint,
   primary key (email, user_id)
) Engine=InnoDB;

create table user_region (
   regionId bigint,
   userId bigint,
   name varchar(64),
   email varchar(64),
   primary key (regionId,userId)
) Engine=InnoDB;

create table member (
    id bigint,
    lkp binary(16) NOT NULL,
    more_id int not null,
    primary key (id)
) Engine=InnoDB;

create table lkp_idx (
    lkp binary(16) NOT NULL,
    id bigint,
    primary key (lkp)
) Engine=InnoDB;

CREATE TABLE orders (
  id bigint,
	customer_id bigint
);

CREATE TABLE orders_id_lookup (
  id int NOT NULL,
  keyspace_id varbinary(128),
  primary key(id)
);
