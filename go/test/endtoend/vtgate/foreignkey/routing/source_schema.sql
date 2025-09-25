-- Source keyspace (sks) schema
create table t1(
    id int,
    name varchar(100),
    primary key(id)
) Engine=InnoDB;

create table t2(
    id int,
    t1_id int,
    value varchar(100),
    primary key(id),
    foreign key (t1_id) references t1(id) on delete cascade on update cascade
) Engine=InnoDB;

create table t3(
    id int,
    name varchar(100),
    primary key(id)
) Engine=InnoDB;

create table t4(
    id int,
    t3_id int,
    value varchar(100),
    primary key(id),
    foreign key (t3_id) references t3(id) on delete cascade on update cascade
) Engine=InnoDB;
