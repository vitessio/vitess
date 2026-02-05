create table test(
    id bigint,
    val1 varchar(16),
    val2 int,
    val3 float,
    primary key(id)
)Engine=InnoDB;

create table test_vdx (
    val1 varchar(16) not null,
    keyspace_id binary(8),
    unique key(val1)
)ENGINE=Innodb;
