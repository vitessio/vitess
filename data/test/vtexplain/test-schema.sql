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
