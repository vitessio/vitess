-- Target keyspace (tks) schema - only has t1 and t2 with FK relationship
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

-- t3 and t4 in target keyspace - no FK relationship between them
create table t3(
    id int,
    name varchar(100),
    primary key(id)
) Engine=InnoDB;

create table t4(
    id int,
    t3_id int,
    value varchar(100),
    primary key(id)
) Engine=InnoDB;
